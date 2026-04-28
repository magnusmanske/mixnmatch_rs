//! Axum router + the `/api.php` dispatcher that fans out to every per-feature
//! handler. Two entrypoints exist on `/api.php`: a GET form (`api_dispatcher`)
//! which uses query-string params, and a POST form (`api_dispatcher_form`)
//! which reads either form-urlencoded bodies or multipart uploads.

use crate::api::common::{ApiError, Params};
use crate::api::{
    admin, catalog, code_fragments, data, dg, download, entry, import, issues, jobs,
    large_catalogs, locations, lua, matching, misc, navigation, proxy, quick_compare, rc,
    sparql, sync, upload, widar,
};
use crate::app_state::AppState;
use axum::Router;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::future::BoxFuture;
use std::sync::Arc;
use tower_sessions::Session;

pub type SharedState = Arc<AppState>;

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    // 512 MB: big enough for realistic catalog uploads, still bounded.
    const UPLOAD_MAX_BYTES: usize = 512 * 1024 * 1024;

    // /api.php is the user-supplied query path: it needs the origin
    // check (cross-origin browsers can still send simple GETs even
    // when CORS would block the response) and panic recovery (a
    // single MySQL-row-decode panic shouldn't kill the connection).
    // Other routes don't take user-supplied query strings, so they
    // get neither.
    let api_php_routes = Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher_form))
        .route_layer(axum::middleware::from_fn(panic_recovery_middleware))
        .route_layer(axum::middleware::from_fn(origin_check_middleware));

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
        .layer(axum::extract::DefaultBodyLimit::max(UPLOAD_MAX_BYTES))
        .with_state(state)
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
        Err(e) => ApiError(format!("invalid form body: {e}")).into_response(),
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
            use axum::http::StatusCode;
            return (
                StatusCode::FORBIDDEN,
                format!("origin {origin} not allowed"),
            )
                .into_response();
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
            log::error!("{path} panicked: {msg}");
            ApiError(format!("internal error: {msg}")).into_response()
        }
    }
}

async fn dispatcher_common(app: &AppState, session: &Session, params: Params) -> Response {
    // Intercept the OAuth callback (user returning from Special:OAuth/authorize).
    // This mirrors PHP's constructor-time check in MW_OAuth::__construct.
    if params.contains_key("oauth_verifier") && params.contains_key("oauth_token") {
        return widar::handle_oauth_callback(app, session, &params).await;
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
    // JSONP wrapping is disabled on auth endpoints — cookies + JSONP is a CSRF vector.
    let callback_allowed = query != "widar";
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
    let resp = match dispatch(&query, app, session, &params).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    };
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
        Err(ApiError(
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
    route!("remove_q",                     matching::query_remove_q),
    route!("remove_all_q",                 matching::query_remove_all_q),
    route!("remove_all_multimatches",      matching::query_remove_all_multimatches),
    route!("suggest",                      matching::query_suggest),

    // Jobs
    route!("get_jobs",                     jobs::query_get_jobs, app_params),
    route!("start_new_job",                jobs::query_start_new_job),

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
    route!("dg_log_action",                dg::query_dg_log_action, app_params),

    route!("prep_new_item",                data::query_prep_new_item, app_params),
    route!("autoscrape_test",              import::query_autoscrape_test, app_params),
    route!("save_scraper",                 import::query_save_scraper, app_params),
    route!("get_scraper",                  import::query_get_scraper, app_params),
    ("upload_import_file", upload_import_file_get),
    route!("import_source",                import::query_import_source),
    route!("get_source_headers",           import::query_get_source_headers, app_params),
    route!("test_import_source",           import::query_test_import_source, app_params),
    route!("widar",                        widar::query_widar),
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
        .ok_or_else(|| ApiError(format!("Unknown query '{query}'")))?;
    handler(app, session, params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

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
            "widar",
            "get_jobs",
            "rc",
            "download2",
            "upload_import_file",
            "dg_desc",
        ] {
            assert!(
                names.contains(required),
                "ROUTES missing required endpoint: {required}"
            );
        }
    }
}
