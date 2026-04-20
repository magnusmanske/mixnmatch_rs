//! Static-asset proxy: `/resources/<path>` is forwarded to magnustools.toolforge,
//! results cached in-process so we don't re-fetch the same JS/CSS/image on every hit.

use crate::api::common::ApiError;
use axum::response::{IntoResponse, Response};
use std::sync::{Arc, OnceLock};

/// Base URL that `/resources/*` requests are internally proxied to.
/// Avoids deploying a symlink to a sibling tool's tree.
const MAGNUSTOOLS_RESOURCES_BASE: &str = "https://magnustools.toolforge.org/resources/";

/// Only cache successful responses — non-2xx bodies are rarely worth caching
/// and may include transient errors from the upstream proxy.
const RESOURCES_CACHE_MAX_ENTRIES: usize = 2000;

/// Don't cache anything larger than this (mostly a guard against surprise
/// huge files — static JS/CSS/images are typically well under 1 MB).
const RESOURCES_CACHE_MAX_ENTRY_BYTES: usize = 5 * 1024 * 1024;

#[derive(Clone)]
struct CachedResource {
    status: u16,
    content_type: Option<axum::http::HeaderValue>,
    cache_control: Option<axum::http::HeaderValue>,
    etag: Option<axum::http::HeaderValue>,
    last_modified: Option<axum::http::HeaderValue>,
    content_encoding: Option<axum::http::HeaderValue>,
    body: bytes::Bytes,
}

fn resources_cache() -> &'static dashmap::DashMap<String, Arc<CachedResource>> {
    static CACHE: OnceLock<dashmap::DashMap<String, Arc<CachedResource>>> = OnceLock::new();
    CACHE.get_or_init(dashmap::DashMap::new)
}

/// Reused HTTP client (pool/connection reuse). Built once per process so we
/// don't pay TLS-handshake costs per asset.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent("mix-n-match (https://mix-n-match.toolforge.org)")
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

fn build_resource_response(entry: &CachedResource) -> Response {
    let mut builder = axum::http::Response::builder().status(
        axum::http::StatusCode::from_u16(entry.status)
            .unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
    );
    if let Some(v) = &entry.content_type {
        builder = builder.header(axum::http::header::CONTENT_TYPE, v);
    }
    if let Some(v) = &entry.cache_control {
        builder = builder.header(axum::http::header::CACHE_CONTROL, v);
    }
    if let Some(v) = &entry.etag {
        builder = builder.header(axum::http::header::ETAG, v);
    }
    if let Some(v) = &entry.last_modified {
        builder = builder.header(axum::http::header::LAST_MODIFIED, v);
    }
    if let Some(v) = &entry.content_encoding {
        builder = builder.header(axum::http::header::CONTENT_ENCODING, v);
    }
    builder
        .body(axum::body::Body::from(entry.body.clone()))
        .unwrap_or_else(|_| {
            ApiError("resources proxy: cannot build response".into()).into_response()
        })
}

/// Transparently proxy `GET /resources/<path>` to magnustools. Responses are
/// cached in-process — these are static JS/CSS/image assets that change
/// rarely, so re-fetching on every hit wastes several hundred ms per asset.
pub async fn proxy_magnustools_resources(
    axum::extract::Path(path): axum::extract::Path<String>,
    uri: axum::http::Uri,
) -> Response {
    // Cache key includes the query string so cache-busters (e.g. ?v=123) hit
    // distinct entries.
    let query = uri.query().unwrap_or("");
    let cache_key = if query.is_empty() {
        path.clone()
    } else {
        format!("{path}?{query}")
    };
    let cache = resources_cache();
    if let Some(hit) = cache.get(&cache_key) {
        return build_resource_response(&hit);
    }
    let query_suffix = if query.is_empty() {
        String::new()
    } else {
        format!("?{query}")
    };
    let upstream_url = format!("{MAGNUSTOOLS_RESOURCES_BASE}{path}{query_suffix}");

    let resp = match http_client().get(&upstream_url).send().await {
        Ok(r) => r,
        Err(e) => {
            return ApiError(format!("resources proxy fetch failed: {e}")).into_response();
        }
    };
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .cloned();
    let cache_control = resp
        .headers()
        .get(axum::http::header::CACHE_CONTROL)
        .cloned();
    let etag = resp.headers().get(axum::http::header::ETAG).cloned();
    let last_modified = resp
        .headers()
        .get(axum::http::header::LAST_MODIFIED)
        .cloned();
    let content_encoding = resp
        .headers()
        .get(axum::http::header::CONTENT_ENCODING)
        .cloned();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return ApiError(format!("resources proxy read failed: {e}")).into_response();
        }
    };

    let entry = Arc::new(CachedResource {
        status: status.as_u16(),
        content_type,
        cache_control,
        etag,
        last_modified,
        content_encoding,
        body: bytes,
    });
    // Only cache successful, reasonably-sized responses. Skip when full so
    // we don't evict hot entries — the cap is generous enough that it
    // shouldn't matter in practice.
    if status.is_success()
        && entry.body.len() <= RESOURCES_CACHE_MAX_ENTRY_BYTES
        && cache.len() < RESOURCES_CACHE_MAX_ENTRIES
    {
        cache.insert(cache_key, entry.clone());
    }
    build_resource_response(&entry)
}
