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
use std::sync::Arc;
use tower_sessions::Session;

pub type SharedState = Arc<AppState>;

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    // 512 MB: big enough for realistic catalog uploads, still bounded.
    const UPLOAD_MAX_BYTES: usize = 512 * 1024 * 1024;
    Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher_form))
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
    // Wrap the dispatcher in catch_unwind so a panic in a single handler
    // (typically a MySQL type mismatch inside `row.get`) returns a clean
    // error response instead of killing the connection mid-flight — the
    // latter would surface in the browser as a NetworkError.
    use futures::FutureExt;
    let dispatch_fut = std::panic::AssertUnwindSafe(dispatch(&query, app, session, &params));
    let result = dispatch_fut.catch_unwind().await;
    let resp = match result {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => e.into_response(),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_else(|| "unknown panic".to_string());
            log::error!("api.php query={query} panicked: {msg}");
            ApiError(format!("internal error: {msg}")).into_response()
        }
    };
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

#[allow(clippy::cognitive_complexity)]
async fn dispatch(
    query: &str,
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    match query {
        // Catalog
        "catalogs" => catalog::query_catalogs(app).await,
        "single_catalog" => catalog::query_single_catalog(app, params).await,
        "catalog_details" => catalog::query_catalog_details(app, params).await,
        "get_catalog_info" => catalog::query_get_catalog_info(app, params).await,
        "catalog" => catalog::query_catalog(app, params).await,
        "edit_catalog" => catalog::query_edit_catalog(app, session, params).await,
        "catalog_overview" => catalog::query_catalog_overview(app, params).await,

        // Entry
        "get_entry" => entry::query_get_entry(app, params).await,
        "get_entry_by_extid" => entry::query_get_entry_by_extid(app, params).await,
        "search" => entry::query_search(app, params).await,
        "random" => entry::query_random(app, params).await,
        "entries_query" => entry::query_entries_query(app, params).await,
        "entries_via_property_value" => entry::query_entries_via_property_value(app, params).await,
        "get_entries_by_q_or_value" => entry::query_get_entries_by_q_or_value(app, params).await,

        // Matching — all DB-writing actions are gated behind OAuth
        "match_q" => matching::query_match_q(app, session, params).await,
        "match_q_multi" => matching::query_match_q_multi(app, session, params).await,
        "remove_q" => matching::query_remove_q(app, session, params).await,
        "remove_all_q" => matching::query_remove_all_q(app, session, params).await,
        "remove_all_multimatches" => {
            matching::query_remove_all_multimatches(app, session, params).await
        }
        "suggest" => matching::query_suggest(app, session, params).await,

        // Jobs
        "get_jobs" => jobs::query_get_jobs(app, params).await,
        "start_new_job" => jobs::query_start_new_job(app, session, params).await,

        // Issues
        "get_issues" => issues::query_get_issues(app, params).await,
        "all_issues" => issues::query_all_issues(app, params).await,
        "resolve_issue" => issues::query_resolve_issue(app, session, params).await,

        // User & auth
        "get_user_info" => misc::query_get_user_info(app, params).await,

        // Recent changes
        "rc" => rc::query_rc(app, params).await,
        "rc_atom" => rc::query_rc_atom(app, params).await,

        // Data & analysis
        "get_wd_props" => data::query_get_wd_props(app).await,
        "top_missing" => data::query_top_missing(app, params).await,
        "get_common_names" => data::query_get_common_names(app, params).await,
        "same_names" => data::query_same_names(app).await,
        "random_person_batch" => data::query_random_person_batch(app, params).await,
        "get_property_cache" => data::query_get_property_cache(app).await,
        "mnm_unmatched_relations" => data::query_mnm_unmatched_relations(app, params).await,
        "creation_candidates" => data::query_creation_candidates(app, params).await,

        // Locations
        "locations" => locations::query_locations(app, params).await,
        "get_locations_in_catalog" => locations::query_get_locations_in_catalog(app, params).await,

        // Download & export
        "download" => download::query_download(app, params).await,
        "download2" => download::query_download2(app, params).await,

        // Navigation
        "redirect" => navigation::query_redirect(app, params).await,
        "proxy_entry_url" => navigation::query_proxy_entry_url(app, params).await,
        "cersei_forward" => navigation::query_cersei_forward(app, params).await,

        // Admin & config — writes require OAuth; admin checks go via require_catalog_admin
        "update_overview" => admin::query_update_overview(app, session, params).await,
        "update_ext_urls" => admin::query_update_ext_urls(app, session, params).await,
        "add_aliases" => admin::query_add_aliases(app, session, params).await,
        "get_missing_properties" => admin::query_get_missing_properties(app).await,
        "set_missing_properties_status" => {
            admin::query_set_missing_properties_status(app, session, params).await
        }
        "get_top_groups" => catalog::query_get_top_groups(app).await,
        "set_top_group" => catalog::query_set_top_group(app, session, params).await,
        "remove_empty_top_group" => {
            catalog::query_remove_empty_top_group(app, session, params).await
        }
        "quick_compare_list" => admin::query_quick_compare_list(app).await,
        "get_flickr_key" => admin::query_get_flickr_key(app).await,

        // Per-feature endpoints, formerly all in `delegated.rs`.
        "get_sync" => sync::query_get_sync(app, params).await,
        "sparql_list" => sparql::query_sparql_list(app, params).await,
        "quick_compare" => quick_compare::query_quick_compare(app, params).await,
        "get_code_fragments" => code_fragments::query_get_code_fragments(app, params).await,
        "save_code_fragment" => code_fragments::query_save_code_fragment(app, session, params).await,
        "test_code_fragment" => lua::query_test_code_fragment(app, session, params).await,

        // Large-catalogs endpoints (backed by the large_catalogs DB)
        "lc_catalogs" => large_catalogs::query_lc_catalogs(app).await,
        "lc_bbox" => large_catalogs::query_lc_bbox(app, params).await,
        "lc_report" => large_catalogs::query_lc_report(app, params).await,
        "lc_report_list" => large_catalogs::query_lc_report_list(app, params).await,
        "lc_rc" => large_catalogs::query_lc_rc(app, params).await,
        "lc_set_status" => large_catalogs::query_lc_set_status(app, session, params).await,

        // Newly ported lightweight catalog endpoints
        "batch_catalogs" => catalog::query_batch_catalogs(app, params).await,
        "search_catalogs" => catalog::query_search_catalogs(app, params).await,
        "catalog_type_counts" => catalog::query_catalog_type_counts(app).await,
        "latest_catalogs" => catalog::query_latest_catalogs(app, params).await,
        "catalogs_with_locations" => catalog::query_catalogs_with_locations(app).await,
        "catalog_property_groups" => catalog::query_catalog_property_groups(app).await,
        "check_wd_prop_usage" => catalog::query_check_wd_prop_usage(app, params).await,
        "catalog_by_group" => catalog::query_catalog_by_group(app, params).await,

        // Newly ported misc endpoints
        "create" => misc::query_create(app, params).await,
        "user_edits" => misc::query_user_edits(app, params).await,
        "get_statement_text_groups" => misc::query_get_statement_text_groups(app, params).await,
        "set_statement_text_q" => misc::query_set_statement_text_q(app, session, params).await,
        "missingpages" => misc::query_missingpages(app, params).await,
        "sitestats" => misc::query_sitestats(app, params).await,

        // Distributed-game endpoints (also dispatched via action=… → query=dg_…)
        "dg_desc" => dg::query_dg_desc(params).await,
        "dg_tiles" => dg::query_dg_tiles(app, params).await,
        "dg_log_action" => dg::query_dg_log_action(app, params).await,

        "prep_new_item" => data::query_prep_new_item(app, params).await,
        "autoscrape_test" => import::query_autoscrape_test(app, params).await,
        "save_scraper" => import::query_save_scraper(app, params).await,
        "get_scraper" => import::query_get_scraper(app, params).await,
        "upload_import_file" => Err(ApiError(
            "upload_import_file must be POSTed as multipart/form-data".into(),
        )),
        "import_source" => import::query_import_source(app, session, params).await,
        "get_source_headers" => import::query_get_source_headers(app, params).await,
        "test_import_source" => import::query_test_import_source(app, params).await,
        "widar" => widar::query_widar(app, session, params).await,

        _ => Err(ApiError(format!("Unknown query '{query}'"))),
    }
}
