//! In-memory cache of the `html/` tree, built once at webserver startup.
//!
//! The tree is only a few hundred KB across ~60 files, so we pay a
//! negligible memory cost to avoid hitting the disk on every request.
//! Response time drops from "stat + open + read" to a `DashMap` lookup +
//! `Bytes` clone (which is a refcount bump, not a copy).
//!
//! On miss (404 or outside the cached tree), we return 404 — we do not
//! silently fall back to disk. Everything deployed is present at startup;
//! anything that isn't shouldn't be served.
//!
//! The cache is read-only after initialisation: changes to the files on
//! disk require a restart. That's acceptable for our deploy flow but is a
//! deliberate trade-off to avoid the complexity (and per-request stat cost)
//! of inode-watching.

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode, Uri, header};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct CachedFile {
    /// The raw bytes. `Bytes` makes every clone a refcount bump.
    body: Bytes,
    /// `Content-Type` header for the response, including charset where
    /// relevant. Computed once at load time.
    content_type: HeaderValue,
}

/// A snapshot of the `html/` directory. Shareable across tasks via `Arc`.
#[derive(Clone, Debug)]
pub struct StaticCache {
    files: Arc<HashMap<String, CachedFile>>,
}

impl StaticCache {
    /// Walk `root` recursively and load every file into memory. The keys
    /// are the URL paths *without* leading slash, e.g. `index.html`,
    /// `vue-components/catalog_editor.js`.
    pub fn load(root: &Path) -> Result<Self> {
        let mut files = HashMap::new();
        load_dir(root, root, &mut files)
            .with_context(|| format!("loading static cache from {}", root.display()))?;
        Ok(Self {
            files: Arc::new(files),
        })
    }

    /// Number of cached files. Useful for startup logging.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Total bytes held. Useful for startup logging.
    pub fn total_bytes(&self) -> usize {
        self.files.values().map(|f| f.body.len()).sum()
    }

    /// Resolve a request URI to a response, mirroring `ServeDir`'s most
    /// common behaviour:
    ///   - `/` → `index.html`
    ///   - paths ending in `/` → `<path>index.html`
    ///   - no query, no range: keep it simple — our assets don't use them
    ///
    /// Anything not in the cache returns 404 (no disk fallback).
    pub fn serve(&self, uri: &Uri) -> Response<Body> {
        let raw = uri.path();
        let trimmed = raw.trim_start_matches('/');
        // Reject traversal attempts up front. The keys we stored are all
        // relative paths without `..`; a request containing `..` can't hit
        // anything legitimate, so short-circuit to 404.
        if trimmed.split('/').any(|seg| seg == "..") {
            return not_found();
        }
        let key = if trimmed.is_empty() {
            "index.html".to_string()
        } else if trimmed.ends_with('/') {
            format!("{trimmed}index.html")
        } else {
            trimmed.to_string()
        };

        match self.files.get(&key) {
            Some(file) => {
                let mut builder = Response::builder().status(StatusCode::OK);
                if let Some(h) = builder.headers_mut() {
                    h.insert(header::CONTENT_TYPE, file.content_type.clone());
                    // Hardcode a short cache lifetime so browsers pick up
                    // deployments quickly; we don't version asset URLs.
                    h.insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("public, max-age=300"),
                    );
                }
                builder
                    .body(Body::from(file.body.clone()))
                    .unwrap_or_else(|_| not_found())
            }
            None => not_found(),
        }
    }
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not found"))
        .expect("404 response is always valid")
}

fn load_dir(root: &Path, dir: &Path, out: &mut HashMap<String, CachedFile>) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            load_dir(root, &path, out)?;
            continue;
        }
        if !ft.is_file() {
            // Symlinks, sockets, etc. — skip.
            continue;
        }
        let bytes = std::fs::read(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let rel = path
            .strip_prefix(root)
            .with_context(|| format!("path {} not under {}", path.display(), root.display()))?;
        let key = path_to_key(rel);
        let content_type = mime_for(&path);
        out.insert(
            key,
            CachedFile {
                body: Bytes::from(bytes),
                content_type,
            },
        );
    }
    Ok(())
}

fn path_to_key(rel: &Path) -> String {
    // Normalise separators so Windows-style paths (if this ever runs there)
    // match URL paths.
    rel.components()
        .filter_map(|c| c.as_os_str().to_str().map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join("/")
}

fn mime_for(path: &Path) -> HeaderValue {
    // Force UTF-8 on text types; everything else we hand straight through
    // from mime_guess. Browsers otherwise sniff and occasionally render JS
    // as plain text on older setups.
    let guess = mime_guess::from_path(path).first_or_octet_stream();
    let essence = guess.essence_str().to_string();
    let with_charset = match guess.type_().as_str() {
        "text" => format!("{essence}; charset=utf-8"),
        "application" if matches!(guess.subtype().as_str(), "javascript" | "json") => {
            format!("{essence}; charset=utf-8")
        }
        _ => essence,
    };
    HeaderValue::from_str(&with_charset).unwrap_or_else(|_| {
        HeaderValue::from_static("application/octet-stream")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn make_tree() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let root = dir.path().to_path_buf();
        fs::write(root.join("index.html"), b"<!doctype html>hi").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join("a.js"), b"console.log(1)").unwrap();
        (dir, root)
    }

    #[test]
    fn loads_and_counts_files() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_bytes(), "<!doctype html>hi".len() + "console.log(1)".len());
    }

    #[test]
    fn serves_root_as_index_html() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/"));
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().starts_with("text/html"));
    }

    #[test]
    fn serves_nested_file_with_charset() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/sub/a.js"));
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().contains("charset=utf-8"));
    }

    #[test]
    fn missing_file_returns_404() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/nope.css"));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn rejects_parent_traversal() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/../etc/passwd"));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
