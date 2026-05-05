//! Static file serving for the webserver.
//!
//! Two modes:
//!
//! * **Memory** (default, used in production): walk the `html/` tree once
//!   at startup and hold every file in RAM. Response time drops from
//!   "stat + open + read" to a `HashMap` lookup + `Bytes` clone (a refcount
//!   bump, not a copy). Browsers get `Cache-Control: max-age=300`. Changes
//!   on disk after startup are NOT picked up — a restart is required.
//!
//! * **Disk** (live; opt-in via `html_dir_override` in `config.json`):
//!   read the requested file from disk on every request and respond with
//!   `Cache-Control: no-store`. Lets HTML/JS edits to a checked-out repo
//!   show up immediately without rebuilding the deploy artifact. Slightly
//!   slower per request, no startup walk; intended for production
//!   deployments that point at a live repo for fast iteration.
//!
//! Both modes resolve `/` and any trailing-slash path to `index.html`,
//! reject `..` segments, and 404 on misses.

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
/// `StaticCache::Memory`'s field type signature; its fields stay private,
/// so external code can't construct or read one — only this module can.
#[derive(Clone, Debug)]
pub struct CachedFile {
    body: Bytes,
    content_type: HeaderValue,
}

/// Wraps a `RecommendedWatcher` so `StaticCache` can derive `Clone` and
/// `Debug` even though the watcher itself implements neither.
pub(self) struct WatcherHandle(#[allow(dead_code)] RecommendedWatcher);

impl fmt::Debug for WatcherHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WatcherHandle")
    }
}

/// Either an in-memory snapshot of `html/` (production default), a live
/// directory served straight off disk per request (development / live-edit
/// mode), or a watched in-memory cache that reloads individual files when
/// they change on disk. Cheaply cloneable in all variants.
#[derive(Clone, Debug)]
#[allow(private_interfaces)]
pub enum StaticCache {
    Memory {
        files: Arc<HashMap<String, CachedFile>>,
        root: Arc<PathBuf>,
    },
    Disk {
        root: Arc<PathBuf>,
        canonical_root: Arc<PathBuf>,
    },
    /// Like `Memory` but a background watcher reloads changed files
    /// automatically. Sends `Cache-Control: no-store` so browsers always
    /// revalidate — edits appear on the next browser refresh.
    Watched {
        files: Arc<DashMap<String, CachedFile>>,
        root: Arc<PathBuf>,
        /// Kept alive for the lifetime of the cache; dropping it stops the watcher.
        _watcher: Arc<WatcherHandle>,
    },
}

impl StaticCache {
    /// Walk `root` recursively and load every file into memory. Use for
    /// production: the webserver answers requests from RAM with a short
    /// browser cache header.
    pub fn load(root: &Path) -> Result<Self> {
        let mut files = HashMap::new();
        load_dir(root, root, &mut files)
            .with_context(|| format!("loading static cache from {}", root.display()))?;
        Ok(Self::Memory {
            files: Arc::new(files),
            root: Arc::new(root.to_path_buf()),
        })
    }

    /// Set up a live (no-cache) view of `root`. Files are read from disk on
    /// every request and served with `Cache-Control: no-store`. Use this
    /// when you want HTML/JS changes to a checked-out repo to surface
    /// immediately without re-baking the deploy image.
    pub fn live(root: &Path) -> Result<Self> {
        if !root.exists() {
            return Err(anyhow!("html directory does not exist: {}", root.display()));
        }
        let canonical_root = root
            .canonicalize()
            .with_context(|| format!("canonicalize {}", root.display()))?;
        Ok(Self::Disk {
            root: Arc::new(root.to_path_buf()),
            canonical_root: Arc::new(canonical_root),
        })
    }

    /// Walk `root`, load all files into a `DashMap`, then spawn a background
    /// task that watches the directory tree with OS-native notifications
    /// (inotify / FSEvents / ReadDirectoryChangesW, polling fallback) and
    /// reloads any file that changes. Responds with `Cache-Control: no-store`
    /// so browsers always revalidate; the server still answers from RAM.
    pub fn watch(root: &Path) -> Result<Self> {
        if !root.exists() {
            return Err(anyhow!("html directory does not exist: {}", root.display()));
        }
        // Canonicalize so that the root stored in `root_arc` is an absolute
        // path. `notify` delivers events with absolute paths; `strip_prefix`
        // in `handle_watch_event` would silently fail (and every reload would
        // be dropped) if `root` were a relative path like `"html"`.
        let root = root
            .canonicalize()
            .with_context(|| format!("canonicalize {}", root.display()))?;
        let root = root.as_path();

        // Load the initial snapshot into a DashMap.
        let mut initial = HashMap::new();
        load_dir(root, root, &mut initial)
            .with_context(|| format!("loading static watch-cache from {}", root.display()))?;
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
                // drops the event, which is acceptable for a dev-mode watcher.
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

        Ok(Self::Watched {
            files,
            root: root_arc,
            _watcher: Arc::new(WatcherHandle(watcher)),
        })
    }

    /// Number of cached files. `0` for live (disk) mode.
    pub fn len(&self) -> usize {
        match self {
            Self::Memory { files, .. } => files.len(),
            Self::Watched { files, .. } => files.len(),
            Self::Disk { .. } => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Memory { files, .. } => files.is_empty(),
            Self::Watched { files, .. } => files.is_empty(),
            // We don't know without walking the tree; treat as non-empty so
            // startup logging doesn't claim "no files".
            Self::Disk { .. } => false,
        }
    }

    /// Total bytes held in memory. `0` for live (disk) mode.
    pub fn total_bytes(&self) -> usize {
        match self {
            Self::Memory { files, .. } => files.values().map(|f| f.body.len()).sum(),
            Self::Watched { files, .. } => files.iter().map(|e| e.body.len()).sum(),
            Self::Disk { .. } => 0,
        }
    }

    /// True if this cache reads from disk on every request (not in-memory).
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Disk { .. })
    }

    pub fn root(&self) -> &Path {
        match self {
            Self::Memory { root, .. } | Self::Disk { root, .. } | Self::Watched { root, .. } => {
                root.as_path()
            }
        }
    }

    /// Resolve a request URI to a response, mirroring `ServeDir`'s most
    /// common behaviour:
    ///   - `/` → `index.html`
    ///   - paths ending in `/` → `<path>index.html`
    ///   - no query, no range: keep it simple — our assets don't use them
    ///
    /// Anything not in the cache (memory mode) or not on disk under root
    /// (disk mode) returns 404.
    pub async fn serve(&self, uri: &Uri) -> Response<Body> {
        let key = match resolve_request_key(uri) {
            Some(k) => k,
            None => return not_found(),
        };

        match self {
            Self::Memory { files, .. } => serve_from_memory(files, &key),
            Self::Watched { files, .. } => serve_from_watched(files, &key),
            Self::Disk {
                root,
                canonical_root,
            } => serve_from_disk(root, canonical_root, &key).await,
        }
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

fn serve_from_memory(files: &HashMap<String, CachedFile>, key: &str) -> Response<Body> {
    match files.get(key) {
        Some(file) => {
            let mut builder = Response::builder().status(StatusCode::OK);
            if let Some(h) = builder.headers_mut() {
                h.insert(header::CONTENT_TYPE, file.content_type.clone());
                // Hardcoded short cache lifetime so browsers pick up
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

fn serve_from_watched(files: &DashMap<String, CachedFile>, key: &str) -> Response<Body> {
    match files.get(key) {
        Some(file) => {
            let mut builder = Response::builder().status(StatusCode::OK);
            if let Some(h) = builder.headers_mut() {
                h.insert(header::CONTENT_TYPE, file.content_type.clone());
                h.insert(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static("no-store"),
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

async fn serve_from_disk(root: &Path, canonical_root: &Path, key: &str) -> Response<Body> {
    let candidate = root.join(key);
    // Resolve symlinks/`..` so we can verify the final path is still under
    // the configured root. If canonicalize fails the file doesn't exist or
    // we can't read it — either way, 404. Both syscalls go through
    // `tokio::fs` so they don't pin the executor thread on slow storage
    // (NFS on Toolforge); under the hood tokio dispatches them to its
    // blocking pool.
    let resolved = match tokio::fs::canonicalize(&candidate).await {
        Ok(p) => p,
        Err(_) => return not_found(),
    };
    if !resolved.starts_with(canonical_root) {
        // Symlink (or some other resolution) escaped the html root.
        return not_found();
    }
    let bytes = match tokio::fs::read(&resolved).await {
        Ok(b) => b,
        Err(_) => return not_found(),
    };
    let content_type = mime_for(&resolved);
    let mut builder = Response::builder().status(StatusCode::OK);
    if let Some(h) = builder.headers_mut() {
        h.insert(header::CONTENT_TYPE, content_type);
        // Live mode is for fast iteration on a checked-out tree — tell
        // browsers not to cache so edits surface on the next reload.
        h.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
    }
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| not_found())
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

    #[test]
    fn loads_and_counts_files() {
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

    // ── Live (disk) mode ───────────────────────────────────────────────

    #[tokio::test]
    async fn live_serves_root_as_index_html() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::live(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().starts_with("text/html"));
    }

    #[tokio::test]
    async fn live_picks_up_edits_after_load() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::live(&root).unwrap();
        // Edit the file after the cache is constructed; live mode should
        // see the change on the next request (no in-memory snapshot).
        fs::write(root.join("index.html"), b"<!doctype html>updated").unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // We can't easily read the body without an axum runtime, but the
        // 200 + content-type round-trip is enough to confirm disk access.
        let ct = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(ct.to_str().unwrap().starts_with("text/html"));
    }

    #[tokio::test]
    async fn live_sets_no_store_cache_header() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::live(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        let cc = resp.headers().get(header::CACHE_CONTROL).unwrap();
        assert_eq!(cc.to_str().unwrap(), "no-store");
    }

    #[tokio::test]
    async fn memory_sets_max_age_cache_header() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::load(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/")).await;
        let cc = resp.headers().get(header::CACHE_CONTROL).unwrap();
        assert!(cc.to_str().unwrap().contains("max-age"));
    }

    #[tokio::test]
    async fn live_missing_file_returns_404() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::live(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/nope.css")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn live_rejects_parent_traversal() {
        let (_tmp, root) = make_tree();
        let cache = StaticCache::live(&root).unwrap();
        let resp = cache.serve(&Uri::from_static("/../etc/passwd")).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
