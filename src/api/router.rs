//! Axum router + the `/api.php` dispatcher that fans out to every per-feature
//! handler. Two entrypoints exist on `/api.php`: a GET form (`api_dispatcher`)
//! which uses query-string params, and a POST form (`api_dispatcher_form`)
//! which reads either form-urlencoded bodies or multipart uploads.

use crate::api::common::{ApiError, Params};
use crate::api::{
    admin, auth, catalog, code_fragments, data, dg, download, entry, import, issues, jobs,
    large_catalogs, locations, lua, matching, misc, navigation, proxy, quick_compare, rc,
    sparql, sync, upload,
};
use crate::app_state::AppState;
use axum::Router;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::future::BoxFuture;
use std::sync::Arc;
use tower_sessions::Session;

/// Maximum number of concurrent in-flight `/api.php` requests. Above
/// this, additional requests are immediately rejected with HTTP 503.
///
/// Sized for steady-state SPA traffic plus burst headroom: a single
/// page load fans out ~20 calls, so 256 supports a dozen users in
/// flight without rejecting any. The cap exists primarily to prevent
/// runaway pile-up when an upstream (Wikidata, WDQS) stalls and slow
/// requests accumulate. `/metrics`, `/resources/*` and the import
/// endpoint are not behind this limit — they're either size-bounded
/// (uploads) or low-cost (Prometheus scrape, cached static assets).
const API_PHP_MAX_CONCURRENT_REQUESTS: usize = 256;

/// Permits guarding in-flight `/api.php` requests. `Semaphore::const_new`
/// makes this a true `static` — no `OnceLock` or `Lazy` needed.
static API_PHP_SEMAPHORE: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(API_PHP_MAX_CONCURRENT_REQUESTS);

/// `?query=…` names that mutate server state and must NEVER be JSONP-wrapped.
/// Each entry corresponds to a `ROUTES` row that takes a session and writes
/// to the DB or runs user-supplied code. JSONP-wrapping one of these would
/// turn an OAuth-guarded mutation into a cross-origin CSRF (the
/// session cookie is attached automatically; the JSONP wrapper hands the
/// response body to the attacker).
///
/// Keep the list in sync with the `route!` full-arity entries below — the
/// `mutating_queries_are_registered_routes` test pins this so a typo here
/// is caught before merge, and the `mutating_queries_match_full_arity_routes`
/// test catches drift when a new mutating handler is added without a matching
/// entry in this list.
#[rustfmt::skip]
const MUTATING_QUERIES: &[&str] = &[
    // Matching writes
    "match_q", "match_q_multi", "sync_match_q_multi",
    "remove_q", "remove_all_q", "remove_all_multimatches",
    "suggest",
    // Jobs
    "start_new_job", "manage_job",
    // Issues
    "resolve_issue",
    // Code fragments + Lua executor (executes user-supplied code)
    "save_code_fragment", "test_code_fragment",
    // Large catalogs
    "lc_set_status",
    // Catalog edits
    "edit_catalog", "delete_autoscraper",
    "set_top_group", "remove_empty_top_group",
    // Admin
    "update_overview", "update_ext_urls",
    "add_aliases", "set_missing_properties_status",
    // Misc writes
    "set_statement_text_q",
    // Import / scraper config writes
    "import_source", "save_scraper",
    // Distributed game logging
    "dg_log_action",
    // File upload (POST-only at the router level, but defensive)
    "upload_import_file",
];

/// True if `query` may be wrapped with `?callback=cb` JSONP. JSONP is blocked
/// for the OAuth flow and for every state-changing endpoint; everything else
/// (pure reads) may still be wrapped, preserving compatibility with any
/// external consumer that legitimately uses JSONP for cross-origin reads.
fn jsonp_allowed_for_query(query: &str) -> bool {
    query != "auth" && !MUTATING_QUERIES.contains(&query)
}

// ─── Per-IP rate limit on POST /api.php ────────────────────────────────────
//
// Audit M-6. The previous tower_governor config tripped real users because
// it metered GETs too — a single SPA page load fans out ~20 reads, and the
// 60-burst limit ran out before the user clicked anything. This limiter
// fires only on POST, which the SPA reserves for state changes; a typical
// matcher does 1-2 POSTs/s, so the 30-burst leaves ~30 s of headroom.
// Hand-rolled (token bucket via DashMap) rather than re-introducing
// tower_governor with a different config — easier to revert, no key-extractor
// dance.

/// Token-bucket burst size per IP. A power-user matching at full speed
/// (1-2 POSTs/sec) drains this in 15-30 s; abusive bots saturate in a
/// few hundred ms.
const MUTATION_BURST_CAPACITY: f32 = 30.0;
/// Steady-state refill rate per IP, tokens per second.
const MUTATION_REFILL_PER_SEC: f32 = 10.0;
/// Buckets idle this long are evicted on the next opportunistic GC pass.
/// Keeps the per-IP map from growing unboundedly under a botnet probe.
const STALE_BUCKET_AGE_SECS: u64 = 300;
/// GC the bucket map every Nth check. Cheap (one `retain` pass) and
/// amortizes across many requests.
const MUTATION_BUCKET_GC_INTERVAL: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct IpBucket {
    tokens: f32,
    last_check: std::time::Instant,
}

impl IpBucket {
    fn new(now: std::time::Instant) -> Self {
        Self {
            tokens: MUTATION_BURST_CAPACITY,
            last_check: now,
        }
    }

    /// Refill based on elapsed wall time since the last check, then try
    /// to consume one token. Returns `true` when allowed. Capacity is
    /// capped at [`MUTATION_BURST_CAPACITY`], so an idle IP doesn't
    /// accumulate unbounded credit.
    fn try_consume(&mut self, now: std::time::Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last_check).as_secs_f32();
        self.tokens = (self.tokens + elapsed * MUTATION_REFILL_PER_SEC).min(MUTATION_BURST_CAPACITY);
        self.last_check = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

fn mutation_buckets() -> &'static dashmap::DashMap<std::net::IpAddr, IpBucket> {
    static BUCKETS: std::sync::OnceLock<dashmap::DashMap<std::net::IpAddr, IpBucket>> =
        std::sync::OnceLock::new();
    BUCKETS.get_or_init(dashmap::DashMap::new)
}

pub type SharedState = Arc<AppState>;

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    // 512 MB: big enough for realistic catalog uploads, still bounded.
    const UPLOAD_MAX_BYTES: usize = 512 * 1024 * 1024;

    // Per-IP rate limit on /api.php is now POST-only — see audit M-6.
    // The old tower_governor config (30/s steady + 60 burst on all
    // methods) tripped real users because one SPA page load fans out
    // ~20 GET reads and chewed through the burst before any click.
    // The replacement (`rate_limit_post_mutations_middleware`) ignores
    // GET entirely and applies a per-IP token bucket sized for the
    // POST volume an actual matcher generates (30 burst, 10/s refill).
    // tower_governor stays in Cargo.toml in case a future need calls
    // for a more sophisticated key extractor.

    // /api.php is the user-supplied query path: it needs the origin
    // check (cross-origin browsers can still send simple GETs even
    // when CORS would block the response) and panic recovery (a single
    // MySQL-row-decode panic shouldn't kill the connection). Other
    // routes don't take user-supplied query strings, so they don't get
    // the same treatment.
    let api_php_routes = Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher_form))
        .route_layer(axum::middleware::from_fn(panic_recovery_middleware))
        .route_layer(axum::middleware::from_fn(origin_check_middleware))
        .route_layer(axum::middleware::from_fn(rate_limit_post_mutations_middleware))
        .route_layer(axum::middleware::from_fn(concurrency_limit_middleware));

    Router::new()
        .merge(api_php_routes)
        .route(
            "/api/v1/import_catalog",
            post(upload::api_import_catalog),
        )
        .route(
            "/resources/{*path}",
            get(proxy::proxy_magnustools_resources),
        )
        // Prometheus scrape endpoint. Plain-text body, no auth — the
        // exporter only emits the counters/histograms we explicitly
        // register (see `crate::metrics`), so there's nothing sensitive.
        .route("/metrics", get(metrics_endpoint))
        .layer(axum::extract::DefaultBodyLimit::max(UPLOAD_MAX_BYTES))
        .with_state(state)
}

async fn metrics_endpoint() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        crate::metrics::render(),
    )
}

async fn api_dispatcher(
    State(app): State<SharedState>,
    session: Session,
    Query(params): Query<Params>,
) -> Response {
    dispatcher_common(&app, &session, params).await
}

async fn api_dispatcher_form(
    State(app): State<SharedState>,
    session: Session,
    req: axum::extract::Request,
) -> Response {
    use axum::extract::FromRequest;
    // Sniff content-type to decide between form-urlencoded (the common case)
    // and multipart uploads (used only by `upload_import_file`).
    let ct = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();

    if ct.starts_with("multipart/form-data") {
        return upload::handle_multipart_upload(&app, &session, req).await;
    }

    match axum::extract::Form::<Params>::from_request(req, &app).await {
        Ok(axum::extract::Form(params)) => dispatcher_common(&app, &session, params).await,
        Err(e) => ApiError::BadRequest(format!("invalid form body: {e}")).into_response(),
    }
}

/// Middleware: reject requests with a cross-origin `Origin` header
/// that isn't in the allowlist. Browsers send simple cross-origin
/// GETs regardless of CORS response headers — they only block the
/// *response* from reaching the attacker, which is too late for
/// mutation endpoints. Lifted out of the per-handler dispatch so
/// every /api.php request is checked uniformly without each
/// handler having to remember.
async fn origin_check_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(origin) = req
        .headers()
        .get(axum::http::header::ORIGIN)
        .and_then(|v| v.to_str().ok())
    {
        if !crate::api::cors::is_allowed_origin(origin) {
            // Don't echo the attacker-controlled origin in the response body.
            // The header value is already in the attacker's request, so the
            // echo adds nothing for legitimate clients, but a future change
            // that surfaces this body in an HTML context would become an
            // XSS vector. The rejected origin is still useful for operators,
            // so log it server-side.
            log::warn!("rejected cross-origin /api.php request from origin: {origin}");
            use axum::http::StatusCode;
            return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }
    next.run(req).await
}

/// Middleware: catch panics in any downstream handler and convert
/// them into a clean JSON error. Without this, a single MySQL row
/// decode panic (e.g. an unexpected NULL) would kill the connection
/// mid-flight; the browser sees that as a `NetworkError` and the
/// user gets no clue what failed. The recovery body matches the
/// pre-middleware behaviour that lived inside `dispatcher_common`.
async fn panic_recovery_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use futures::FutureExt;
    let path = req.uri().path().to_string();
    let fut = std::panic::AssertUnwindSafe(next.run(req));
    match fut.catch_unwind().await {
        Ok(resp) => resp,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_else(|| "unknown panic".to_string());
            // Log the panic detail server-side; the response body deliberately
            // omits it so that DB row content, file paths, or other internal
            // strings embedded in a panic payload don't leak to the client.
            log::error!("{path} panicked: {msg}");
            ApiError::Internal("internal error".to_string()).into_response()
        }
    }
}

/// Per-IP token-bucket rate limit, applied only to POST `/api.php`
/// requests. Reads pass through untouched so a SPA page-load fan-out
/// (~20 GETs) never trips the limiter — that was the failure mode of
/// the previous tower_governor config. Audit reference: M-6 in
/// `audits/comprehensive_security_report.md`.
async fn rate_limit_post_mutations_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // Only POSTs (mutations). GET reads pass through unmetered.
    if req.method() != axum::http::Method::POST {
        return next.run(req).await;
    }
    // ConnectInfo is set by `into_make_service_with_connect_info`; without
    // it we can't bucket per-IP. Fall open rather than lock everyone out —
    // some test harnesses skip the connect-info wiring, and production has
    // it (pinned by `router_responds_through_real_listener`).
    let ip = match req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        Some(ci) => ci.0.ip(),
        None => return next.run(req).await,
    };
    let now = std::time::Instant::now();
    let allowed = {
        let buckets = mutation_buckets();
        let mut entry = buckets.entry(ip).or_insert_with(|| IpBucket::new(now));
        entry.try_consume(now)
    };
    if !allowed {
        metrics::counter!("mnm_api_mutation_rate_limited_total").increment(1);
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            )],
            "rate limit exceeded; try again in 1s",
        )
            .into_response();
    }
    // Opportunistic GC: every Nth call, sweep stale buckets so a botnet
    // probe can't grow the map unboundedly.
    static GC_COUNTER: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    let n = GC_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if n.is_multiple_of(MUTATION_BUCKET_GC_INTERVAL) {
        let buckets = mutation_buckets();
        let stale_age = std::time::Duration::from_secs(STALE_BUCKET_AGE_SECS);
        buckets.retain(|_, b| now.saturating_duration_since(b.last_check) < stale_age);
    }
    next.run(req).await
}

/// Reject `/api.php` requests above [`API_PHP_MAX_CONCURRENT_REQUESTS`]
/// concurrent in-flight calls with HTTP 503 Service Unavailable. The
/// permit is held for the lifetime of the response future, so a slow
/// handler counts against the limit until it actually returns.
///
/// `try_acquire` (not `acquire().await`) is intentional: under overload
/// we want callers to see fast failures and back off, not queue up and
/// drive memory growth.
async fn concurrency_limit_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    match API_PHP_SEMAPHORE.try_acquire() {
        Ok(permit) => {
            let response = next.run(req).await;
            drop(permit);
            response
        }
        Err(_) => {
            metrics::counter!("mnm_api_overload_total").increment(1);
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                [(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                )],
                "server overloaded; try again",
            )
                .into_response()
        }
    }
}

async fn dispatcher_common(app: &AppState, session: &Session, params: Params) -> Response {
    // Intercept the OAuth callback (user returning from Special:OAuth/authorize).
    if params.contains_key("oauth_verifier") && params.contains_key("oauth_token") {
        return auth::handle_oauth_callback(app, session, &params).await;
    }

    // Mirror the PHP behaviour: legacy callers may use "action" instead of "query"
    // to reach the distributed-game endpoints, which become "dg_<action>".
    let query = match params.get("query").filter(|s| !s.is_empty()).cloned() {
        Some(q) => q,
        None => match params.get("action").filter(|s| !s.is_empty()) {
            Some(a) => format!("dg_{a}"),
            None => String::new(),
        },
    };
    // JSONP wrapping is disabled on the OAuth flow *and* every state-changing
    // endpoint — cookies + JSONP is a CSRF + exfiltration vector (an attacker
    // page can `<script src="…&callback=fn">` to issue an authenticated
    // mutation cross-origin and read the response). SameSite=None is needed
    // for the Wikidata gadget, so SameSite alone doesn't stop this; the
    // `origin_check_middleware` doesn't either because browsers don't send
    // `Origin` on `<script src>` requests.
    let callback_allowed = jsonp_allowed_for_query(&query);
    let callback = if callback_allowed {
        params.get("callback").cloned().unwrap_or_default()
    } else {
        String::new()
    };
    // `autoclose=1` is used by the Wikidata gadget: it opens the API URL in a
    // popup (top-level navigation carries the first-party session cookie, unlike
    // a cross-origin fetch), and we return HTML that closes the popup on success.
    let autoclose = params.get("autoclose").map(String::as_str) == Some("1");
    // Panic recovery and origin rejection are handled by the
    // tower middleware layered onto /api.php (see `router()`); a
    // panic inside `dispatch` propagates up to the catch_unwind
    // there. Per-handler errors are still mapped to ApiError here.
    let started = std::time::Instant::now();
    let resp = match dispatch(&query, app, session, &params).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    };
    // Record one data point per `/api.php?query=…` dispatch — the
    // query name is bounded by the ROUTES table so cardinality is
    // safe. Unknown queries are bucketed under "_unknown" so a
    // bot probing random names can't blow up the time-series store.
    let metric_query = if ROUTES.iter().any(|(n, _)| *n == query) {
        query.as_str()
    } else if query.is_empty() {
        "_empty"
    } else {
        "_unknown"
    };
    crate::metrics::record_api_request(
        metric_query,
        resp.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    if autoclose {
        return wrap_autoclose(resp).await;
    }
    if callback.is_empty() {
        return resp;
    }
    // JSONP wrapping: only apply to JSON responses; pass non-JSON payloads through unchanged.
    if resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"))
    {
        let (parts, body) = resp.into_parts();
        // Body may be arbitrarily large; cap at 100MB for safety.
        let bytes = match axum::body::to_bytes(body, 100_000_000).await {
            Ok(b) => b,
            Err(_) => return axum::http::Response::from_parts(parts, axum::body::Body::empty()),
        };
        let wrapped = format!("{callback}({})", String::from_utf8_lossy(&bytes));
        return (
            [(
                axum::http::header::CONTENT_TYPE,
                "application/javascript; charset=UTF-8",
            )],
            wrapped,
        )
            .into_response();
    }
    resp
}

/// Wrap a JSON response in minimal HTML that closes the popup on success.
/// Errors keep the popup open so the user can see what went wrong.
async fn wrap_autoclose(resp: Response) -> Response {
    let (parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, 10_000_000).await {
        Ok(b) => b,
        Err(_) => return axum::http::Response::from_parts(parts, axum::body::Body::empty()),
    };
    let json_val: serde_json::Value =
        serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({}));
    let status = json_val
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let html = if status == "OK" {
        "<!DOCTYPE html><html><head><title>Mix'n'match</title></head>\
         <body style=\"font-family:sans-serif\"><script>window.close();</script>\
         <p>Done. You can close this window.</p></body></html>"
            .to_string()
    } else {
        let safe = html_escape::encode_text(status);
        format!(
            "<!DOCTYPE html><html><head><title>Mix'n'match error</title></head>\
             <body style=\"font-family:sans-serif\">\
             <h3>Mix'n'match error</h3><p>{safe}</p>\
             <p>Close this window to continue.</p></body></html>"
        )
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/html; charset=UTF-8",
        )],
        html,
    )
        .into_response()
}

/// Erased async-fn signature every API handler is wrapped to. The
/// HRTB on `'a` lets one `fn`-pointer type cover handlers that
/// borrow from any of the three inputs for the future's lifetime.
type ApiHandler =
    for<'a> fn(&'a AppState, &'a Session, &'a Params) -> BoxFuture<'a, Result<Response, ApiError>>;

/// Wrap a `(app)` / `(app, params)` / `(app, session, params)` /
/// `(params)` handler into the unified `ApiHandler` shape. The form
/// argument selects which of the four signatures is being adapted;
/// each generates one `Box::pin(async move { … })` shim so the
/// `const ROUTES` table stays one line per route.
macro_rules! route {
    // (app, session, params) — the full-arity form.
    ($name:literal, $h:path) => {
        ($name, ((|app, session, params| Box::pin(async move { $h(app, session, params).await })) as ApiHandler))
    };
    // (app, params) — most read-only handlers.
    ($name:literal, $h:path, app_params) => {
        ($name, ((|app, _, params| Box::pin(async move { $h(app, params).await })) as ApiHandler))
    };
    // (app) — handlers with no per-request input.
    ($name:literal, $h:path, app_only) => {
        ($name, ((|app, _, _| Box::pin(async move { $h(app).await })) as ApiHandler))
    };
    // (params) — exactly one handler (`dg_desc`) doesn't need app state.
    ($name:literal, $h:path, params_only) => {
        ($name, ((|_, _, params| Box::pin(async move { $h(params).await })) as ApiHandler))
    };
}

/// One `Err` shim used by `upload_import_file` (the GET path is a
/// hard-coded "you must POST multipart" error). Lifted to a real
/// `fn` so the macro's `as ApiHandler` cast resolves.
fn upload_import_file_get<'a>(
    _: &'a AppState,
    _: &'a Session,
    _: &'a Params,
) -> BoxFuture<'a, Result<Response, ApiError>> {
    Box::pin(async {
        Err(ApiError::Internal(
            "upload_import_file must be POSTed as multipart/form-data".into(),
        ))
    })
}

/// Single source of truth for `/api.php?query=…` routing. Adding a
/// new endpoint = one line here. The grouping comments mirror the
/// previous match's section dividers so existing readers can find
/// what they're looking for.
#[rustfmt::skip]
const ROUTES: &[(&str, ApiHandler)] = &[
    // Catalog
    route!("catalogs",                     catalog::query_catalogs, app_only),
    route!("single_catalog",               catalog::query_single_catalog, app_params),
    route!("catalog_details",              catalog::query_catalog_details, app_params),
    route!("get_catalog_info",             catalog::query_get_catalog_info, app_params),
    route!("catalog",                      catalog::query_catalog, app_params),
    route!("edit_catalog",                 catalog::query_edit_catalog),
    route!("delete_autoscraper",           catalog::query_delete_autoscraper),
    route!("catalog_overview",             catalog::query_catalog_overview, app_params),

    // Entry
    route!("get_entry",                    entry::query_get_entry, app_params),
    route!("get_entry_by_extid",           entry::query_get_entry_by_extid, app_params),
    route!("search",                       entry::query_search, app_params),
    route!("random",                       entry::query_random, app_params),
    route!("entries_query",                entry::query_entries_query, app_params),
    route!("entries_via_property_value",   entry::query_entries_via_property_value, app_params),
    route!("get_entries_by_q_or_value",    entry::query_get_entries_by_q_or_value, app_params),

    // Matching — all DB-writing actions are gated behind OAuth
    route!("match_q",                      matching::query_match_q),
    route!("match_q_multi",                matching::query_match_q_multi),
    route!("sync_match_q_multi",           matching::query_sync_match_q_multi),
    route!("remove_q",                     matching::query_remove_q),
    route!("remove_all_q",                 matching::query_remove_all_q),
    route!("remove_all_multimatches",      matching::query_remove_all_multimatches),
    route!("suggest",                      matching::query_suggest),

    // Jobs
    route!("get_jobs",                     jobs::query_get_jobs, app_params),
    route!("start_new_job",                jobs::query_start_new_job),
    route!("manage_job",                   jobs::query_manage_job),

    // Issues
    route!("get_issues",                   issues::query_get_issues, app_params),
    route!("all_issues",                   issues::query_all_issues, app_params),
    route!("resolve_issue",                issues::query_resolve_issue),

    // User & auth
    route!("get_user_info",                misc::query_get_user_info, app_params),

    // Recent changes
    route!("rc",                           rc::query_rc, app_params),
    route!("rc_atom",                      rc::query_rc_atom, app_params),

    // Data & analysis
    route!("get_wd_props",                 data::query_get_wd_props, app_only),
    route!("top_missing",                  data::query_top_missing, app_params),
    route!("get_common_names",             data::query_get_common_names, app_params),
    route!("same_names",                   data::query_same_names, app_only),
    route!("random_person_batch",          data::query_random_person_batch, app_params),
    route!("get_property_cache",           data::query_get_property_cache, app_only),
    route!("mnm_unmatched_relations",      data::query_mnm_unmatched_relations, app_params),
    route!("creation_candidates",          data::query_creation_candidates, app_params),

    // Locations
    route!("locations",                    locations::query_locations, app_params),
    route!("get_locations_in_catalog",     locations::query_get_locations_in_catalog, app_params),

    // Download & export
    route!("download",                     download::query_download, app_params),
    route!("download2",                    download::query_download2, app_params),

    // Navigation
    route!("redirect",                     navigation::query_redirect, app_params),
    route!("proxy_entry_url",              navigation::query_proxy_entry_url, app_params),
    route!("cersei_forward",               navigation::query_cersei_forward, app_params),

    // Admin & config — writes require OAuth; admin checks go via require_catalog_admin
    route!("update_overview",              admin::query_update_overview),
    route!("update_ext_urls",              admin::query_update_ext_urls),
    route!("add_aliases",                  admin::query_add_aliases),
    route!("get_missing_properties",       admin::query_get_missing_properties, app_only),
    route!("set_missing_properties_status",admin::query_set_missing_properties_status),
    route!("get_top_groups",               catalog::query_get_top_groups, app_only),
    route!("set_top_group",                catalog::query_set_top_group),
    route!("remove_empty_top_group",       catalog::query_remove_empty_top_group),
    route!("quick_compare_list",           admin::query_quick_compare_list, app_only),
    route!("get_flickr_key",               admin::query_get_flickr_key, app_only),

    // Per-feature endpoints, formerly all in `delegated.rs`.
    route!("get_sync",                     sync::query_get_sync, app_params),
    route!("sparql_list",                  sparql::query_sparql_list, app_params),
    route!("quick_compare",                quick_compare::query_quick_compare, app_params),
    route!("get_code_fragments",           code_fragments::query_get_code_fragments, app_params),
    route!("get_code_examples",            code_fragments::query_get_code_examples, app_params),
    route!("save_code_fragment",           code_fragments::query_save_code_fragment),
    route!("test_code_fragment",           lua::query_test_code_fragment),

    // Large-catalogs endpoints (backed by the large_catalogs DB)
    route!("lc_catalogs",                  large_catalogs::query_lc_catalogs, app_only),
    route!("lc_bbox",                      large_catalogs::query_lc_bbox, app_params),
    route!("lc_report",                    large_catalogs::query_lc_report, app_params),
    route!("lc_report_list",               large_catalogs::query_lc_report_list, app_params),
    route!("lc_rc",                        large_catalogs::query_lc_rc, app_params),
    route!("lc_set_status",                large_catalogs::query_lc_set_status),

    // Newly ported lightweight catalog endpoints
    route!("batch_catalogs",               catalog::query_batch_catalogs, app_params),
    route!("search_catalogs",              catalog::query_search_catalogs, app_params),
    route!("catalog_type_counts",          catalog::query_catalog_type_counts, app_only),
    route!("latest_catalogs",              catalog::query_latest_catalogs, app_params),
    route!("new_catalogs_atom",            catalog::query_new_catalogs_atom, app_params),
    route!("catalogs_with_locations",      catalog::query_catalogs_with_locations, app_only),
    route!("catalog_property_groups",      catalog::query_catalog_property_groups, app_only),
    route!("check_wd_prop_usage",          catalog::query_check_wd_prop_usage, app_params),
    route!("catalog_by_group",             catalog::query_catalog_by_group, app_params),

    // Newly ported misc endpoints
    route!("create",                       misc::query_create, app_params),
    route!("user_edits",                   misc::query_user_edits, app_params),
    route!("get_statement_text_groups",    misc::query_get_statement_text_groups, app_params),
    route!("set_statement_text_q",         misc::query_set_statement_text_q),
    route!("missingpages",                 misc::query_missingpages, app_params),
    route!("sitestats",                    misc::query_sitestats, app_params),

    // Distributed-game endpoints (also dispatched via action=… → query=dg_…)
    route!("dg_desc",                      dg::query_dg_desc, params_only),
    route!("dg_tiles",                     dg::query_dg_tiles, app_params),
    route!("dg_log_action",                dg::query_dg_log_action),

    route!("prep_new_item",                data::query_prep_new_item, app_params),
    route!("prep_match_claim",             data::query_prep_match_claim, app_params),
    route!("autoscrape_test",              import::query_autoscrape_test, params_only),
    route!("save_scraper",                 import::query_save_scraper),
    route!("get_scraper",                  import::query_get_scraper, app_params),
    ("upload_import_file", upload_import_file_get),
    route!("import_source",                import::query_import_source),
    route!("get_source_headers",           import::query_get_source_headers, app_params),
    route!("test_import_source",           import::query_test_import_source, app_params),
    route!("auth",                         auth::query_auth),
];

async fn dispatch(
    query: &str,
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let handler = ROUTES
        .iter()
        .find(|(name, _)| *name == query)
        .map(|(_, h)| *h)
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown query '{query}'")))?;
    handler(app, session, params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Pin the size of the concurrency cap. Too low would reject during
    /// normal SPA traffic (one page-load fans out ~20 calls); too high
    /// would defeat the point of the cap entirely. Also sanity-check
    /// that the static `Semaphore` was actually initialised with the
    /// constant value — a wiring regression would leave the available
    /// count at a different number.
    #[test]
    fn concurrency_limit_constants_are_sane() {
        // `const _: () = assert!(...)` would push these to compile time, but
        // clippy still flags the const-condition pattern; an inline binding
        // hides the constness from the lint without weakening the assertion.
        let cap = API_PHP_MAX_CONCURRENT_REQUESTS;
        assert!(cap >= 64, "cap is too tight to absorb normal SPA fanout");
        assert!(cap <= 10_000, "cap is so loose it provides no protection");
        assert!(
            API_PHP_SEMAPHORE.available_permits() <= cap,
            "static Semaphore has more permits than the configured cap — \
             initialisation is wrong"
        );
    }

    /// Catches a copy-paste typo in the route table that would silently
    /// shadow an existing route name and never call the second handler.
    #[test]
    fn route_names_are_unique() {
        let mut seen: HashSet<&'static str> = HashSet::new();
        for (name, _) in ROUTES {
            assert!(seen.insert(name), "duplicate route registered: {name}");
        }
    }

    /// Spot-check a handful of well-known endpoints — guards against an
    /// accidental deletion that wouldn't show up in the build (the lookup
    /// would just start returning "Unknown query" at runtime).
    #[test]
    fn route_table_contains_critical_endpoints() {
        let names: HashSet<&'static str> = ROUTES.iter().map(|(n, _)| *n).collect();
        for required in [
            "catalogs",
            "search",
            "match_q",
            "sync_match_q_multi",
            "auth",
            "get_jobs",
            "start_new_job",
            "manage_job",
            "rc",
            "download2",
            "upload_import_file",
            "dg_desc",
            "prep_match_claim",
        ] {
            assert!(
                names.contains(required),
                "ROUTES missing required endpoint: {required}"
            );
        }
    }

    /// Pin the production serve path end-to-end. Issues a real HTTP
    /// request to a real TCP listener serving the production router via
    /// `into_make_service_with_connect_info::<SocketAddr>()`.
    ///
    /// Originally added as a regression guard for the 2026-05-11 outage
    /// where every /api.php request returned 500 "Unable To Extract
    /// Key!" because the rate-limit layer's key extractor needs
    /// ConnectInfo and the serve call used the connect-info-less
    /// `into_make_service()`. The rate-limit layer is currently
    /// disabled, but the test stays as a smoke test that the live
    /// serve path doesn't 5xx — if/when the limiter is re-enabled this
    /// test once again pins the ConnectInfo wiring.
    ///
    /// Picks `query=unknown` so the dispatch path goes through every
    /// layer (origin → panic recovery → dispatch) without needing a
    /// live DB; the handler just returns an "Unknown query" ApiError.
    /// We only assert the response is NOT 5xx — the exact body doesn't
    /// matter, the point is the middleware stack produced a normal
    /// response.
    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn router_responds_through_real_listener() {
        let app = crate::test_support::test_app().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Mirror the production layer stack from cli.rs::run_webserver
        // closely enough that real-listener concerns (connect-info,
        // session extraction, …) work end-to-end. The dispatcher
        // unconditionally extracts a Session, so without the layer
        // every request 500s with "Can't extract session" — masking
        // the connect-info bug the test is actually guarding against.
        let session_layer = tower_sessions::SessionManagerLayer::new(
            tower_sessions::MemoryStore::default(),
        );
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                router(app)
                    .layer(session_layer)
                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://{addr}/api.php?query=unknown"))
            .send()
            .await
            .expect("request through real listener must complete");
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        server.abort();

        assert!(
            !status.is_server_error(),
            "request through real listener returned {status} (body: {body:?}); \
             likely the ConnectInfo wiring or the rate-limit key extractor \
             is broken — see commit `592669d` for the prior incident",
        );
    }

    /// JSONP wrapping must be off for every state-changing endpoint —
    /// otherwise an attacker page can `<script src="…&callback=fn">` to
    /// issue an authenticated mutation cross-origin (SameSite=None
    /// attaches the cookie, the missing-Origin path bypasses the
    /// origin allowlist) and exfiltrate the response via the
    /// callback wrapper. Audit reference: H-1 in
    /// `audits/comprehensive_security_report.md`.
    #[test]
    fn jsonp_blocked_on_every_mutating_query() {
        for q in MUTATING_QUERIES {
            assert!(
                !jsonp_allowed_for_query(q),
                "JSONP must be blocked on mutating query '{q}'"
            );
        }
    }

    #[test]
    fn jsonp_blocked_on_auth_query() {
        // The OAuth flow has always had JSONP off; keep the explicit
        // assertion so a refactor doesn't silently re-enable it.
        assert!(!jsonp_allowed_for_query("auth"));
    }

    /// Sanity check that pure-read endpoints still accept `?callback=…`
    /// — some external consumers (the old PHP gadget, embed scripts on
    /// third-party wikis) rely on JSONP for cross-origin reads, and we
    /// don't want this PR to break them.
    #[test]
    fn jsonp_allowed_on_known_read_queries() {
        for q in [
            "catalogs",
            "single_catalog",
            "search",
            "get_entry",
            "get_jobs",
            "rc",
            "top_missing",
            "sitestats",
            "get_user_info",
        ] {
            assert!(
                jsonp_allowed_for_query(q),
                "JSONP must stay enabled on known-read query '{q}'"
            );
        }
    }

    /// Every name in MUTATING_QUERIES must be an actual registered
    /// route. A typo here would silently leave JSONP enabled for the
    /// real (correctly-spelled) handler.
    #[test]
    fn mutating_queries_are_registered_routes() {
        let names: HashSet<&'static str> = ROUTES.iter().map(|(n, _)| *n).collect();
        for q in MUTATING_QUERIES {
            assert!(
                names.contains(q),
                "MUTATING_QUERIES entry '{q}' has no matching row in ROUTES"
            );
        }
    }

    /// Token bucket admits the full burst in a single instant — that's the
    /// whole point of the burst capacity. The exact number pins audit M-6's
    /// configured threshold; lowering it would tighten the limit and
    /// potentially trip real users.
    #[test]
    fn rate_limit_bucket_admits_full_burst() {
        let t0 = std::time::Instant::now();
        let mut b = IpBucket::new(t0);
        for i in 0..(MUTATION_BURST_CAPACITY as usize) {
            assert!(b.try_consume(t0), "burst request #{i} should be allowed");
        }
        assert!(
            !b.try_consume(t0),
            "request beyond burst capacity must be rejected"
        );
    }

    #[test]
    fn rate_limit_bucket_refills_at_configured_rate() {
        let t0 = std::time::Instant::now();
        let mut b = IpBucket::new(t0);
        // Drain.
        for _ in 0..(MUTATION_BURST_CAPACITY as usize) {
            let _ = b.try_consume(t0);
        }
        // Wait 2 s — should refill ~20 tokens.
        let t2 = t0 + std::time::Duration::from_secs(2);
        let expected = (2.0 * MUTATION_REFILL_PER_SEC) as usize;
        for i in 0..expected {
            assert!(
                b.try_consume(t2),
                "post-refill request #{i} should be allowed (refill={MUTATION_REFILL_PER_SEC}/s)"
            );
        }
        assert!(
            !b.try_consume(t2),
            "request beyond refilled tokens must be rejected"
        );
    }

    /// Idle buckets must NOT accumulate infinite credit — long idle
    /// followed by a burst should still cap at the configured capacity.
    #[test]
    fn rate_limit_bucket_caps_idle_credit() {
        let t0 = std::time::Instant::now();
        let mut b = IpBucket::new(t0);
        for _ in 0..(MUTATION_BURST_CAPACITY as usize) {
            let _ = b.try_consume(t0);
        }
        // Idle for an hour.
        let t1h = t0 + std::time::Duration::from_secs(3600);
        // Only BURST_CAPACITY more should be allowed, not 3600 * refill.
        for i in 0..(MUTATION_BURST_CAPACITY as usize) {
            assert!(
                b.try_consume(t1h),
                "post-long-idle burst request #{i} should be allowed"
            );
        }
        assert!(
            !b.try_consume(t1h),
            "idle credit must cap at MUTATION_BURST_CAPACITY"
        );
    }

    /// The four handlers below previously accepted form-supplied
    /// usernames with no session check. If any disappears from the
    /// route table the OAuth gate is effectively gone: clients route
    /// around to whatever surfaces remain. Keep this assertion close
    /// to the security boundary so the regression is loud.
    #[test]
    fn previously_open_mutation_endpoints_still_routed() {
        let names: HashSet<&'static str> = ROUTES.iter().map(|(n, _)| *n).collect();
        for required in [
            "dg_log_action",
            "save_scraper",
            "import_source",
            "upload_import_file",
        ] {
            assert!(
                names.contains(required),
                "ROUTES missing security-sensitive endpoint: {required}"
            );
        }
    }
}
