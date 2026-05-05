//! Static file serving for the webserver.
//!
//! Files under `html/` are loaded into RAM at startup so each request is a
//! `DashMap` lookup + `Bytes` clone (a refcount bump, not a copy) instead of
//! a fresh `stat`/`open`/`read` cycle. A background watcher
//! (inotify / FSEvents / ReadDirectoryChangesW, polling fallback) reloads
//! individual files when they change on disk, so updates land in the cache
//! without a server restart. Browsers get `Cache-Control: max-age=300`.
//!
//! `/` and any trailing-slash path resolve to `index.html`; `..` segments
//! are rejected; misses return 404.

use anyhow::{Context, Result, anyhow};
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode, Uri, header};
use bytes::Bytes;
use dashmap::DashMap;
use log::warn;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One file in the in-memory snapshot. Public only because it appears in
/// `StaticCache`'s field type signature; its fields stay private, so external
/// code can't construct or read one — only this module can.
#[derive(Clone, Debug)]
pub struct CachedFile {
    body: Bytes,
    content_type: HeaderValue,
}

/// Wraps a `RecommendedWatcher` so `StaticCache` can derive `Clone` and
/// `Debug` even though the watcher itself implements neither.
struct WatcherHandle(#[allow(dead_code)] RecommendedWatcher);

impl fmt::Debug for WatcherHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WatcherHandle")
    }
}

/// In-memory cache of `html/` plus a background watcher that reloads files
/// when they change on disk. Cheaply cloneable (the inner state is `Arc`).
#[derive(Clone, Debug)]
#[allow(private_interfaces)]
pub struct StaticCache {
    files: Arc<DashMap<String, CachedFile>>,
    root: Arc<PathBuf>,
    /// Kept alive for the lifetime of the cache; dropping it stops the watcher.
    _watcher: Arc<WatcherHandle>,
}

impl StaticCache {
    /// Walk `root`, load every file into memory, and start a background
    /// watcher that reloads individual files when they change on disk.
    pub fn load(root: &Path) -> Result<Self> {
        if !root.exists() {
            return Err(anyhow!("html directory does not exist: {}", root.display()));
        }
        // Canonicalize so the stored root is absolute. `notify` delivers events
        // with absolute paths; `strip_prefix` in `handle_watch_event` would
        // silently fail (and every reload would be dropped) if `root` were a
        // relative path like `"html"`.
        let root = root
            .canonicalize()
            .with_context(|| format!("canonicalize {}", root.display()))?;
        let root = root.as_path();

        // Initial snapshot.
        let mut initial = HashMap::new();
        load_dir(root, root, &mut initial)
            .with_context(|| format!("loading static cache from {}", root.display()))?;
        let files: Arc<DashMap<String, CachedFile>> = Arc::new(DashMap::new());
        for (k, v) in initial {
            files.insert(k, v);
        }

        let root_arc = Arc::new(root.to_path_buf());

        // Channel from the notify callback (sync) into the async handler task.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(64);

        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| {
                // blocking_send is fine here: the channel has 64 slots and the
                // handler task drains it promptly; a saturated channel just
                // drops the event, which is acceptable for a watcher.
                let _ = tx.blocking_send(result);
            },
            notify::Config::default(),
        )
        .context("creating file-system watcher")?;

        watcher
            .watch(root, RecursiveMode::Recursive)
            .with_context(|| format!("watching {}", root.display()))?;

        // Spawn the handler that updates the DashMap on every FS event.
        let files_bg = Arc::clone(&files);
        let root_bg = Arc::clone(&root_arc);
        tokio::spawn(async move {
            while let Some(result) = rx.recv().await {
                match result {
                    Ok(event) => handle_watch_event(&event, &root_bg, &files_bg),
                    Err(e) => warn!("static_cache watcher error: {e}"),
                }
            }
        });

        Ok(Self {
            files,
            root: root_arc,
            _watcher: Arc::new(WatcherHandle(watcher)),
        })
    }

    /// Number of cached files.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Total bytes held in memory.
    pub fn total_bytes(&self) -> usize {
        self.files.iter().map(|e| e.body.len()).sum()
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Resolve a request URI to a response, mirroring `ServeDir`'s most
    /// common behaviour:
    ///   - `/` → `index.html`
    ///   - paths ending in `/` → `<path>index.html`
    ///   - no query, no range: keep it simple — our assets don't use them
    ///
    /// Anything not in the cache returns 404.
    pub async fn serve(&self, uri: &Uri) -> Response<Body> {
        let Some(key) = resolve_request_key(uri) else {
            return not_found();
        };
        serve_from_cache(&self.files, &key)
    }
}

/// Map a request URI's path to the relative key we use for lookup. Returns
/// None if the request has obvious traversal segments — short-circuits to
/// 404 before any file system work.
fn resolve_request_key(uri: &Uri) -> Option<String> {
    let raw = uri.path();
    let trimmed = raw.trim_start_matches('/');
    if trimmed.split('/').any(|seg| seg == "..") {
        return None;
    }
    Some(if trimmed.is_empty() {
        "index.html".to_string()
    } else if trimmed.ends_with('/') {
        format!("{trimmed}index.html")
    } else {
        trimmed.to_string()
    })
}

fn serve_from_cache(files: &DashMap<String, CachedFile>, key: &str) -> Response<Body> {
    match files.get(key) {
        Some(file) => {
            let mut builder = Response::builder().status(StatusCode::OK);
            if let Some(h) = builder.headers_mut() {
                h.insert(header::CONTENT_TYPE, file.content_type.clone());
                // Short cache lifetime so browsers pick up deployments
                // quickly; we don't version asset URLs.
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

/// Called from the background watcher task for each notify event. Updates
/// the DashMap in-place: reloads created/modified files, removes deleted ones.
fn handle_watch_event(
    event: &notify::Event,
    root: &Path,
    files: &DashMap<String, CachedFile>,
) {
    for path in &event.paths {
        let Ok(rel) = path.strip_prefix(root) else {
            continue;
        };
        let key = path_to_key(rel);
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                if !path.is_file() {
                    continue;
                }
                match std::fs::read(path) {
                    Ok(bytes) => {
                        files.insert(
                            key,
                            CachedFile {
                                body: Bytes::from(bytes),
                                content_type: mime_for(path),
                            },
                        );
                    }
                    Err(e) => warn!("static_cache watch: failed to reload {}: {e}", path.display()),
                }
            }
            EventKind::Remove(_) => {
                files.remove(&key);
            }
            _ => {}
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
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
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
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
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
    HeaderValue::from_str(&with_charset)
        .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"))
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

    #[tokio::test]
    async fn loads_and_counts_files() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.total_bytes(),
            "<!doctype html>hi".len() + "console.log(1)".len()
        );
    }

    #[tokio::test]
    async fn serves_root_as_index_html() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().starts_with("text/html"));
    }

    #[tokio::test]
    async fn serves_nested_file_with_charset() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/sub/a.js")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().contains("charset=utf-8"));
    }

    #[tokio::test]
    async fn missing_file_returns_404() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/nope.css")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_parent_traversal() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/../etc/passwd")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sets_max_age_cache_header() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        let cc = resp.headers().get(header::CACHE_CONTROL).unwrap();
        assert!(cc.to_str().unwrap().contains("max-age"));
    }
}
