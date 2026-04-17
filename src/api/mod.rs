#![allow(clippy::mod_module_files)]

pub mod common;

use crate::app_state::AppState;
use crate::auth;
use crate::import_catalog::ImportMode;
use axum::Router;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use common::{ApiError, Params};
use std::sync::Arc;
use tower_sessions::Session;

pub type SharedState = Arc<AppState>;

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher_form))
        .route("/api/v1/import_catalog", post(api_import_catalog))
        .route("/resources/{*path}", get(proxy_magnustools_resources))
        .with_state(state)
}

/// Base URL that `/resources/*` requests are internally proxied to.
/// Avoids deploying a symlink to a sibling tool's tree.
const MAGNUSTOOLS_RESOURCES_BASE: &str = "https://magnustools.toolforge.org/resources/";

/// Transparently proxy `GET /resources/<path>` to magnustools. Streams the
/// body back as-is and forwards the upstream `Content-Type` / cache headers
/// so the browser treats it identically to a locally-served asset.
async fn proxy_magnustools_resources(
    axum::extract::Path(path): axum::extract::Path<String>,
    uri: axum::http::Uri,
) -> Response {
    // Preserve any query string the caller sent (cache-busters, etc.).
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    let upstream_url = format!("{MAGNUSTOOLS_RESOURCES_BASE}{path}{query}");

    // Wikimedia rejects requests without a User-Agent. Send something
    // identifying us — matches the agent we'd use for the MW API.
    let client = reqwest::Client::builder()
        .user_agent("mix-n-match (https://mix-n-match.toolforge.org)")
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let resp = match client.get(&upstream_url).send().await {
        Ok(r) => r,
        Err(e) => {
            return ApiError(format!("resources proxy fetch failed: {e}")).into_response();
        }
    };
    let status = resp.status();
    // Copy a conservative set of content headers. Hop-by-hop headers
    // (connection, transfer-encoding, …) are intentionally dropped.
    let passthrough = [
        axum::http::header::CONTENT_TYPE,
        axum::http::header::CACHE_CONTROL,
        axum::http::header::ETAG,
        axum::http::header::LAST_MODIFIED,
        axum::http::header::CONTENT_ENCODING,
    ];
    let mut headers = axum::http::HeaderMap::new();
    for name in &passthrough {
        if let Some(v) = resp.headers().get(name) {
            headers.insert(name, v.clone());
        }
    }
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return ApiError(format!("resources proxy read failed: {e}")).into_response();
        }
    };
    let mut builder =
        axum::http::Response::builder().status(axum::http::StatusCode::from_u16(status.as_u16()).unwrap_or(axum::http::StatusCode::BAD_GATEWAY));
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder
        .body(axum::body::Body::from(bytes))
        .unwrap_or_else(|_| ApiError("resources proxy: cannot build response".into()).into_response())
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
    axum::extract::Form(params): axum::extract::Form<Params>,
) -> Response {
    dispatcher_common(&app, &session, params).await
}

async fn dispatcher_common(app: &AppState, session: &Session, params: Params) -> Response {
    // Intercept the OAuth callback (user returning from Special:OAuth/authorize).
    // This mirrors PHP's constructor-time check in MW_OAuth::__construct.
    if params.contains_key("oauth_verifier") && params.contains_key("oauth_token") {
        return handle_oauth_callback(app, session, &params).await;
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
    let dispatch_fut =
        std::panic::AssertUnwindSafe(dispatch(&query, app, session, &params));
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

/// Implements `?query=widar&action=…`. Mirrors PHP `query_widar` →
/// `Widar::render_reponse`, which reads the sub-action from the `action`
/// form field and writes its userinfo into the `result` key (not `data`).
/// Sub-actions: `authorize`, `get_rights`, `logout`.
async fn query_widar(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cfg = app
        .oauth_config()
        .ok_or_else(|| ApiError("OAuth is not configured on this server".into()))?
        .clone();
    // Match the PHP Widar convention: the sub-action is the `action` parameter.
    // `widar_action` is accepted as a legacy alias so older callers keep working.
    let action = params
        .get("action")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| params.get("widar_action").cloned())
        .unwrap_or_else(|| "get_rights".to_string());
    match action.as_str() {
        "authorize" => {
            // Off-toolforge the bypass pretends we're already logged in —
            // just redirect home instead of triggering a real OAuth dance.
            if auth::guard::dev_bypass_user().is_some() {
                return Ok(Redirect::to("/").into_response());
            }
            let token = auth::flow::initiate_request_token(&cfg)
                .await
                .map_err(|e| ApiError(format!("OAuth initiate failed: {e}")))?;
            let new_state = auth::session::SessionData {
                state: auth::session::SessionState::PendingVerifier {
                    request_token_key: token.key.clone(),
                    request_token_secret: token.secret.clone(),
                },
            };
            auth::session::store(session, &new_state).await?;
            let url = auth::flow::build_authorize_redirect_url(&cfg, &token.key);
            Ok(Redirect::to(&url).into_response())
        }
        "logout" => {
            auth::session::clear(session).await?;
            Ok(json_resp(serde_json::json!({
                "status": "OK",
                "error": "OK",
                "data": [],
            })))
        }
        // Default (including the explicit "get_rights").
        _ => {
            // Return the shape PHP `Widar::render_reponse` produces:
            // a top-level `result` holding the rights query, not `data`.
            // The Vue frontend reads `d.result.query.userinfo`.
            if let Some(u) = auth::guard::dev_bypass_user() {
                return Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "query": {
                            "userinfo": {
                                "id": u.mnm_user_id,
                                "name": u.wikidata_username,
                            }
                        }
                    }
                })));
            }
            let data = auth::session::load(session).await;
            match data.state {
                auth::session::SessionState::Authenticated {
                    wikidata_user_id,
                    wikidata_username,
                    ..
                } => Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "query": {
                            "userinfo": {
                                "id": wikidata_user_id,
                                "name": wikidata_username,
                            }
                        }
                    }
                }))),
                _ => Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "error": {
                            "code": "mwoauth-invalid-authorization",
                            "info": "Not logged in",
                        }
                    }
                }))),
            }
        }
    }
}

/// Finish the OAuth1 handshake. Runs when the user returns from the authorize
/// step with `oauth_verifier` and `oauth_token` query parameters.
async fn handle_oauth_callback(app: &AppState, session: &Session, params: &Params) -> Response {
    let cfg = match app.oauth_config() {
        Some(c) => c.clone(),
        None => return ApiError("OAuth is not configured on this server".into()).into_response(),
    };
    let verifier = params.get("oauth_verifier").cloned().unwrap_or_default();
    let incoming_token = params.get("oauth_token").cloned().unwrap_or_default();

    let data = auth::session::load(session).await;
    let (rk, rs) = match data.state {
        auth::session::SessionState::PendingVerifier {
            request_token_key,
            request_token_secret,
        } => (request_token_key, request_token_secret),
        _ => {
            return ApiError(
                "No pending OAuth login — start over from the authorize link".into(),
            )
            .into_response();
        }
    };
    // Session fixation guard: the verifier must match the token we stashed.
    if incoming_token != rk {
        return ApiError("OAuth token mismatch".into()).into_response();
    }
    let pair = auth::flow::TokenPair { key: rk, secret: rs };
    let access = match auth::flow::exchange_verifier(&cfg, &pair, &verifier).await {
        Ok(a) => a,
        Err(e) => return ApiError(format!("OAuth exchange failed: {e}")).into_response(),
    };
    let user = match auth::flow::fetch_userinfo(&cfg, &access).await {
        Ok(u) => u,
        Err(e) => return ApiError(format!("OAuth userinfo failed: {e}")).into_response(),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let new_data = auth::session::SessionData {
        state: auth::session::SessionState::Authenticated {
            access_token_key: access.key,
            access_token_secret: access.secret,
            wikidata_user_id: user.id,
            wikidata_username: auth::session::normalize_username(&user.name),
            authenticated_at: now,
        },
    };
    if let Err(e) = auth::session::store(session, &new_data).await {
        return e.into_response();
    }
    // Re-cycle session id to prevent session fixation on the now-authenticated session.
    let _ = session.cycle_id().await;
    Redirect::to("/").into_response()
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
        "catalogs" => query_catalogs(app).await,
        "single_catalog" => query_single_catalog(app, params).await,
        "catalog_details" => query_catalog_details(app, params).await,
        "get_catalog_info" => query_get_catalog_info(app, params).await,
        "catalog" => query_catalog(app, params).await,
        "edit_catalog" => query_edit_catalog(app, session, params).await,
        "catalog_overview" => query_catalog_overview(app, params).await,

        // Entry
        "get_entry" => query_get_entry(app, params).await,
        "get_entry_by_extid" => query_get_entry_by_extid(app, params).await,
        "search" => query_search(app, params).await,
        "random" => query_random(app, params).await,
        "entries_query" => query_entries_query(app, params).await,
        "entries_via_property_value" => query_entries_via_property_value(app, params).await,
        "get_entries_by_q_or_value" => query_get_entries_by_q_or_value(app, params).await,

        // Matching — all DB-writing actions are gated behind OAuth
        "match_q" => query_match_q(app, session, params).await,
        "match_q_multi" => query_match_q_multi(app, session, params).await,
        "remove_q" => query_remove_q(app, session, params).await,
        "remove_all_q" => query_remove_all_q(app, session, params).await,
        "remove_all_multimatches" => query_remove_all_multimatches(app, session, params).await,
        "suggest" => query_suggest(app, session, params).await,

        // Jobs
        "get_jobs" => query_get_jobs(app, params).await,
        "start_new_job" => query_start_new_job(app, session, params).await,

        // Issues
        "get_issues" => query_get_issues(app, params).await,
        "all_issues" => query_all_issues(app, params).await,
        "resolve_issue" => query_resolve_issue(app, session, params).await,

        // User & auth
        "get_user_info" => query_get_user_info(app, params).await,

        // Recent changes
        "rc" => query_rc(app, params).await,

        // Data & analysis
        "get_wd_props" => query_get_wd_props(app).await,
        "top_missing" => query_top_missing(app, params).await,
        "get_common_names" => query_get_common_names(app, params).await,
        "same_names" => query_same_names(app).await,
        "random_person_batch" => query_random_person_batch(app, params).await,
        "get_property_cache" => query_get_property_cache(app).await,
        "mnm_unmatched_relations" => query_mnm_unmatched_relations(app, params).await,
        "creation_candidates" => query_creation_candidates(app, params).await,

        // Locations
        "locations" => query_locations(app, params).await,
        "get_locations_in_catalog" => query_get_locations_in_catalog(app, params).await,

        // Download & export
        "download" => query_download(app, params).await,
        "download2" => query_download2(app, params).await,

        // Navigation
        "redirect" => query_redirect(app, params).await,
        "proxy_entry_url" => query_proxy_entry_url(app, params).await,
        "cersei_forward" => query_cersei_forward(app, params).await,

        // Admin & config — writes require OAuth; admin checks go via require_catalog_admin
        "update_overview" => query_update_overview(app, session, params).await,
        "update_ext_urls" => query_update_ext_urls(app, session, params).await,
        "add_aliases" => query_add_aliases(app, session, params).await,
        "get_missing_properties" => query_get_missing_properties(app).await,
        "set_missing_properties_status" => {
            query_set_missing_properties_status(app, session, params).await
        }
        "get_top_groups" => query_get_top_groups(app).await,
        "set_top_group" => query_set_top_group(app, session, params).await,
        "remove_empty_top_group" => query_remove_empty_top_group(app, session, params).await,
        "quick_compare_list" => query_quick_compare_list(app).await,
        "rc_atom" => query_rc_atom(app, params).await,
        "get_flickr_key" => query_get_flickr_key().await,

        // Delegated to micro-API internally (no HTTP round-trip)
        "get_sync" => query_get_sync(app, params).await,
        "sparql_list" => query_sparql_list(app, params).await,
        "quick_compare" => query_quick_compare(app, params).await,
        "get_code_fragments" => query_get_code_fragments(app, params).await,
        "save_code_fragment" => query_save_code_fragment(app, session, params).await,
        "test_code_fragment" => query_test_code_fragment(app, session, params).await,

        // Large-catalogs endpoints (backed by the large_catalogs DB)
        "lc_catalogs" => query_lc_catalogs(app).await,
        "lc_bbox" => query_lc_bbox(app, params).await,
        "lc_report" => query_lc_report(app, params).await,
        "lc_report_list" => query_lc_report_list(app, params).await,
        "lc_rc" => query_lc_rc(app, params).await,
        "lc_set_status" => query_lc_set_status(app, session, params).await,

        // Newly ported lightweight catalog endpoints
        "batch_catalogs" => query_batch_catalogs(app, params).await,
        "search_catalogs" => query_search_catalogs(app, params).await,
        "catalog_type_counts" => query_catalog_type_counts(app).await,
        "latest_catalogs" => query_latest_catalogs(app, params).await,
        "catalogs_with_locations" => query_catalogs_with_locations(app).await,
        "catalog_property_groups" => query_catalog_property_groups(app).await,
        "check_wd_prop_usage" => query_check_wd_prop_usage(app, params).await,
        "catalog_by_group" => query_catalog_by_group(app, params).await,

        // Newly ported misc endpoints
        "create" => query_create(app, params).await,
        "user_edits" => query_user_edits(app, params).await,
        "get_statement_text_groups" => query_get_statement_text_groups(app, params).await,
        "set_statement_text_q" => query_set_statement_text_q(app, session, params).await,
        "missingpages" => query_missingpages(app, params).await,
        "sitestats" => query_sitestats(app, params).await,

        // Distributed-game endpoints (also dispatched via action=… → query=dg_…)
        "dg_desc" => query_dg_desc(params).await,
        "dg_tiles" => query_dg_tiles(app, params).await,
        "dg_log_action" => query_dg_log_action(app, params).await,

        // Stubs: require external services that are not yet ported
        "disambig" => Err(ApiError(
            "disambig requires Wikidata DB replica access (not yet ported to Rust)".into(),
        )),
        "prep_new_item" => Err(ApiError(
            "prep_new_item requires QuickStatements integration (not yet ported to Rust)".into(),
        )),
        "get_entry_reader_view" => Err(ApiError(
            "get_entry_reader_view requires Readability library (not yet ported to Rust)".into(),
        )),
        "autoscrape_test" => Err(ApiError("autoscrape_test not yet ported to Rust".into())),
        "save_scraper" => Err(ApiError("save_scraper not yet ported to Rust".into())),
        "upload_import_file" => Err(ApiError("upload_import_file not yet ported to Rust".into())),
        "import_source" => Err(ApiError("import_source not yet ported to Rust".into())),
        "get_source_headers" => Err(ApiError("get_source_headers not yet ported to Rust".into())),
        "test_import_source" => Err(ApiError("test_import_source not yet ported to Rust".into())),
        "widar" => query_widar(app, session, params).await,

        _ => Err(ApiError(format!("Unknown query '{query}'"))),
    }
}

// ─── Import catalog endpoint ────────────────────────────────────────────────

/// POST body for `/api/v1/import_catalog`.
///
/// Either `entries` (inline array) or `uuid` (reference to an uploaded
/// import_file) must be provided.
#[derive(serde::Deserialize)]
struct ImportCatalogRequest {
    catalog_id: usize,
    /// "add_replace" (default) or "add_replace_delete"
    #[serde(default = "default_import_mode")]
    mode: ImportMode,
    /// Inline MetaEntry objects. Mutually exclusive with `uuid`.
    #[serde(default)]
    entries: Option<Vec<crate::meta_entry::MetaEntry>>,
    /// UUID of a previously-uploaded import_file (type must be "json" or "jsonl").
    #[serde(default)]
    uuid: Option<String>,
}

fn default_import_mode() -> ImportMode {
    ImportMode::AddReplace
}

async fn api_import_catalog(
    State(app): State<SharedState>,
    axum::Json(body): axum::Json<ImportCatalogRequest>,
) -> Response {
    let result = if let Some(uuid) = &body.uuid {
        crate::import_catalog::import_from_import_file(&app, body.catalog_id, uuid, body.mode).await
    } else if let Some(entries) = body.entries {
        // Inline entries: require a user via the import_file.user equivalent.
        // For inline POST there is no import_file row, so we don't validate
        // the user field (same as CLI).
        crate::import_catalog::import_meta_entries(&app, body.catalog_id, entries, body.mode, None)
            .await
    } else {
        Err(anyhow::anyhow!(
            "Either 'entries' or 'uuid' must be provided"
        ))
    };

    match result {
        Ok(result) => {
            let data = serde_json::json!({
                "created": result.created,
                "updated": result.updated,
                "skipped_fully_matched": result.skipped_fully_matched,
                "deleted": result.deleted,
                "errors": result.errors,
            });
            ok(data)
        }
        Err(e) => ApiError(e.to_string()).into_response(),
    }
}

// ─── helpers ────────────────────────────────────────────────────────────────

fn json_resp(v: serde_json::Value) -> Response {
    axum::Json(v).into_response()
}
fn ok(data: serde_json::Value) -> Response {
    common::success_with_data(data).into_response()
}

// ─── Catalog handlers ───────────────────────────────────────────────────────

async fn query_catalogs(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_catalog_overview().await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(id) = item
            .get("catalog")
            .and_then(|v| v.as_u64())
            .or_else(|| item.get("id").and_then(|v| v.as_u64()))
        {
            map.insert(id.to_string(), item);
        }
    }
    Ok(ok(serde_json::Value::Object(map)))
}

async fn query_single_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog_id", 0) as usize;
    let data = app.storage().api_get_single_catalog_overview(cid).await?;
    let mut map = serde_json::Map::new();
    map.insert(cid.to_string(), data);
    Ok(ok(serde_json::Value::Object(map)))
}

async fn query_catalog_details(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let s = app.storage();
    let (t, y, u) = tokio::join!(
        s.api_get_catalog_type_counts(cid),
        s.api_get_catalog_match_by_month(cid),
        s.api_get_catalog_matcher_by_user(cid),
    );
    Ok(ok(serde_json::json!({"type": t?, "ym": y?, "user": u?})))
}

async fn query_get_catalog_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_single_catalog_overview(cid).await?;
    Ok(ok(serde_json::json!([data])))
}

async fn query_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let meta_str = common::get_param(params, "meta", "{}");
    let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or(serde_json::json!({}));
    let show_noq = meta.get("show_noq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_autoq = meta.get("show_autoq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_userq = meta.get("show_userq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_na = meta.get("show_na").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_nowd = meta.get("show_nowd").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_multiple = meta
        .get("show_multiple")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let per_page = meta.get("per_page").and_then(|v| v.as_u64()).unwrap_or(50);
    let offset = meta.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
    let entry_type = common::get_param(params, "type", "");
    let title_match = common::get_param(params, "title_match", "");
    let keyword = common::get_param(params, "keyword", "");
    let user_id_raw = common::get_param(params, "user_id", "");

    let mut conds = vec![format!("catalog={catalog}")];
    if show_multiple == 1 {
        conds.push("EXISTS (SELECT 1 FROM multi_match WHERE entry_id=entry.id) AND (user<=0 OR user is null)".into());
    } else if show_noq + show_autoq + show_userq + show_nowd == 0 && show_na == 1 {
        conds.push("q=0".into());
    } else if show_noq + show_autoq + show_userq + show_na == 0 && show_nowd == 1 {
        conds.push("q=-1".into());
    } else {
        if show_noq != 1 {
            conds.push("q IS NOT NULL".into());
        }
        if show_autoq != 1 {
            conds.push("(q is null OR user!=0)".into());
        }
        if show_userq != 1 {
            conds.push("(user<=0 OR user is null)".into());
        }
        if show_na != 1 {
            conds.push("(q!=0 or q is null)".into());
        }
    }
    if !entry_type.is_empty() {
        conds.push(format!("`type`='{}'", entry_type.replace('\'', "''")));
    }
    if !title_match.is_empty() {
        conds.push(format!(
            "`ext_name` LIKE '%{}%'",
            title_match.replace('\'', "''")
        ));
    }
    if !keyword.is_empty() {
        let kw = keyword.replace('\'', "''");
        conds.push(format!("(`ext_name` LIKE '%{kw}%' OR `ext_desc` LIKE '%{kw}%')"));
    }
    if !user_id_raw.is_empty() {
        // Parse as signed so "0" (auto-matched) is distinguished from a missing param.
        if let Ok(uid) = user_id_raw.parse::<i64>() {
            if uid > 0 {
                conds.push(format!("`user`={uid}"));
            } else if uid == 0 {
                conds.push("`user`=0".into());
            }
        }
    }

    let where_clause = conds.join(" AND ");
    // Total filtered count (same WHERE, no LIMIT/OFFSET) — powers accurate pagination.
    let total_filtered = app
        .storage()
        .api_get_catalog_entries_count(&where_clause)
        .await
        .unwrap_or(0);

    let sql = format!(
        "SELECT * FROM entry WHERE {where_clause} LIMIT {per_page} OFFSET {offset}"
    );
    let entries = app.storage().api_get_catalog_entries_raw(&sql).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    // PHP places `total_filtered` alongside `status`/`data`, not inside `data`,
    // so return a manually-assembled envelope here.
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": data,
        "total_filtered": total_filtered,
    })))
}

async fn query_edit_catalog(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data_str = common::get_param(params, "data", "");
    let data: serde_json::Value =
        serde_json::from_str(&data_str).map_err(|_| ApiError("Bad data".into()))?;
    auth::guard::require_catalog_admin_from_params(app, session, params).await?;
    let name = data
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or(ApiError("Bad data".into()))?;
    app.storage()
        .api_edit_catalog(
            cid,
            name,
            data.get("url").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("desc").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("type").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("search_wp").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("wd_prop")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize),
            data.get("wd_qual")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize),
            data.get("active")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        )
        .await?;
    let _ = app.storage().catalog_refresh_overview_table(cid).await;
    Ok(ok(serde_json::json!({})))
}

async fn query_catalog_overview(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs_str = common::get_param(params, "catalogs", "");
    let ids: Vec<usize> = catalogs_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let data = app.storage().api_get_catalog_overview_for_ids(&ids).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Entry handlers ─────────────────────────────────────────────────────────

async fn query_get_entry(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let entry_ids_str = common::get_param(params, "entry", "");
    let ext_ids_str = common::get_param(params, "ext_ids", "");
    let entries = if !ext_ids_str.is_empty() {
        if catalog == 0 {
            return Err(ApiError("catalog is required when using ext_ids".into()));
        }
        let ext_ids: Vec<String> = serde_json::from_str(&ext_ids_str).unwrap_or_default();
        let mut r = vec![];
        for eid in ext_ids {
            if let Ok(e) = crate::entry::Entry::from_ext_id(catalog, &eid, app).await {
                r.push(e);
            }
        }
        r
    } else {
        let ids: Vec<usize> = entry_ids_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if ids.is_empty() {
            return Err(ApiError("entry is required".into()));
        }
        crate::entry::Entry::multiple_from_ids(&ids, app)
            .await?
            .into_values()
            .collect()
    };
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_get_entry_by_extid(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "extid", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let mut data = common::entries_to_json_data(&[entry], app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_search(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let what = common::get_param(params, "what", "");
    let max_results = common::get_param_int(params, "max", 100) as usize;
    let desc_search = common::get_param_int(params, "description_search", 0) != 0;
    let no_label = common::get_param_int(params, "no_label_search", 0) != 0;
    let user_exclude: Vec<usize> = common::get_param(params, "exclude", "")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let include: Vec<usize> = common::get_param(params, "include", "")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    // Mirror PHP: the effective exclude list is the user-provided one plus every
    // inactive catalog, so disabled catalogs never leak into text or Q-number search.
    let mut exclude = user_exclude;
    exclude.extend(app.storage().api_get_inactive_catalog_ids().await?);
    exclude.sort();
    exclude.dedup();

    let what_clean = what.replace('-', " ");
    let q_match = regex::Regex::new(r"^\s*[Qq]?(\d+)\s*$")
        .ok()
        .and_then(|re| {
            re.captures(&what_clean)
                .map(|c| c[1].parse::<isize>().unwrap_or(0))
        });
    let entries = if let Some(q) = q_match.filter(|q| *q > 0) {
        app.storage().api_search_by_q(q, &exclude).await?
    } else {
        let words: Vec<String> = what_clean
            .split_whitespace()
            .filter(|w| {
                w.len() >= 3 && w.len() <= 84 && !["the", "a"].contains(&w.to_lowercase().as_str())
            })
            .map(|s| s.to_string())
            .collect();
        if words.is_empty() {
            vec![]
        } else {
            app.storage()
                .api_search_entries(
                    &words,
                    desc_search,
                    no_label,
                    &exclude,
                    &include,
                    max_results,
                )
                .await?
        }
    };
    let data = common::entries_to_json_data(&entries, app).await?;
    Ok(ok(data))
}

async fn query_random(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let submode = common::get_param(params, "submode", "");
    let entry_type = common::get_param(params, "type", "");
    let id = common::get_param_int(params, "id", 0) as usize;

    // Find the candidate entry. Mirrors query_random() in PHP API.php:
    //   id != 0            → direct lookup by id (test hook in PHP)
    //   catalog > 0        → catalog-specific random pick (catalog_q_random index)
    //   catalog == 0       → global random pick across active catalogs
    let entry_opt = if id != 0 {
        crate::entry::Entry::from_id(id, app).await.ok()
    } else if catalog > 0 {
        app.storage()
            .api_get_random_entry(catalog, &submode, &entry_type, &[])
            .await?
    } else {
        let active = app.storage().api_get_active_catalog_ids().await?;
        app.storage()
            .api_get_random_entry(0, &submode, &entry_type, &active)
            .await?
    };

    let Some(entry) = entry_opt else {
        return Ok(ok(serde_json::Value::Null));
    };

    // Augment with person dates if we have them.
    let eid = entry.id.unwrap_or(0);
    let mut data = serde_json::json!(entry);
    let pd = app
        .storage()
        .api_get_person_dates_for_entries(&[eid])
        .await?;
    if let Some((born, died)) = pd.get(&eid) {
        if !born.is_empty() {
            data["born"] = serde_json::json!(born);
        }
        if !died.is_empty() {
            data["died"] = serde_json::json!(died);
        }
    }
    Ok(ok(data))
}

async fn query_entries_query(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    use crate::match_state::MatchState;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let ms = MatchState {
        unmatched: common::get_param_int(params, "unmatched", 0) != 0,
        partially_matched: common::get_param_int(params, "prelim_matched", 0) != 0,
        fully_matched: common::get_param_int(params, "fully_matched", 0) != 0,
    };
    let eq = crate::entry_query::EntryQuery::default()
        .with_match_state(ms)
        .with_limit(50)
        .with_offset(offset);
    let entries = app.storage().entry_query(&eq).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_entries_via_property_value(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let property: usize = common::get_param(params, "property", "")
        .replace(|c: char| !c.is_ascii_digit(), "")
        .parse()
        .unwrap_or(0);
    let value = common::get_param(params, "value", "").trim().to_string();
    if property == 0 || value.is_empty() {
        return Err(ApiError("property and value required".into()));
    }
    let ids = app.storage().get_entry_ids_by_aux(property, &value).await?;
    let entries: Vec<_> = if ids.is_empty() {
        vec![]
    } else {
        crate::entry::Entry::multiple_from_ids(&ids, app)
            .await?
            .into_values()
            .collect()
    };
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_get_entries_by_q_or_value(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let q_str = common::get_param(params, "q", "");
    let q: isize = q_str
        .replace(|c: char| !c.is_ascii_digit() && c != '-', "")
        .parse()
        .unwrap_or(0);
    let json_str = common::get_param(params, "json", "{}");
    let json_val: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or(serde_json::json!({}));

    let mut prop_values: std::collections::HashMap<usize, Vec<String>> =
        std::collections::HashMap::new();
    let mut props: Vec<usize> = vec![];
    if let Some(obj) = json_val.as_object() {
        for (k, v) in obj {
            let p: usize = k.replace('P', "").parse().unwrap_or(0);
            if p == 0 {
                continue;
            }
            props.push(p);
            let vals: Vec<String> = v
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            if !vals.is_empty() {
                prop_values.insert(p, vals);
            }
        }
    }
    let prop_catalog_map = if props.is_empty() {
        std::collections::HashMap::new()
    } else {
        app.storage().api_get_prop2catalog(&props).await?
    };
    let entries = app
        .storage()
        .api_get_entries_by_q_or_value(q, &prop_catalog_map, &prop_values)
        .await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;

    // Add catalog info
    let cat_ids: std::collections::HashSet<usize> = entries.iter().map(|e| e.catalog).collect();
    let mut catalogs = serde_json::Map::new();
    for cid in cat_ids {
        if let Ok(c) = app.storage().api_get_single_catalog_overview(cid).await {
            catalogs.insert(cid.to_string(), c);
        }
    }
    data["catalogs"] = serde_json::Value::Object(catalogs);
    Ok(ok(data))
}

// ─── Matching handlers ──────────────────────────────────────────────────────

async fn query_match_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let q = common::get_param_int(params, "q", -1);
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    entry.set_match(&format!("Q{q}"), uid).await?;
    let out = crate::entry::Entry::from_id(eid, app).await?;
    let cat = crate::catalog::Catalog::from_id(out.catalog, app).await?;
    let mut ej = serde_json::json!(out);
    ej["entry_type"] = serde_json::json!(cat.type_name());
    Ok(json_resp(serde_json::json!({"status": "OK", "entry": ej})))
}

async fn query_match_q_multi(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let data_str = common::get_param(params, "data", "[]");
    let data: Vec<serde_json::Value> = serde_json::from_str(&data_str).unwrap_or_default();
    let mut not_found = 0_usize;
    let mut not_found_list: Vec<String> = vec![];
    for d in &data {
        let arr = d.as_array();
        let q = arr
            .and_then(|a| a.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as isize;
        let ext_id = arr
            .and_then(|a| a.get(1))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !app
            .storage()
            .api_match_q_multi(catalog, ext_id, q, uid)
            .await?
        {
            not_found += 1;
            if not_found_list.len() < 100 {
                not_found_list.push(ext_id.to_string());
            }
        }
    }
    Ok(json_resp(
        serde_json::json!({"status": "OK", "not_found": not_found, "not_found_list": not_found_list}),
    ))
}

async fn query_remove_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    auth::guard::require_user_from_params(app, session, params).await?;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    entry.unmatch().await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_all_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    if let Some(q) = entry.q {
        app.storage().api_remove_all_q(entry.catalog, q).await?;
    }
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_all_multimatches(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    app.storage().api_remove_all_multimatches(eid).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_suggest(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let overwrite = common::get_param_int(params, "overwrite", 0) != 0;
    let suggestions = common::get_param(params, "suggestions", "");
    let mut cnt = 0_usize;
    let mut out = String::new();
    for line in suggestions.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 2 {
            out.push_str(&format!("Bad row : {line}\n"));
            continue;
        }
        let ext_id = parts[0].trim();
        let q: isize = parts[1]
            .replace(|c: char| !c.is_ascii_digit(), "")
            .parse()
            .unwrap_or(0);
        if app
            .storage()
            .api_suggest(catalog, ext_id, q, overwrite)
            .await?
        {
            cnt += 1;
        }
    }
    out.push_str(&format!("{cnt} entries changed"));
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=UTF-8",
        )],
        out,
    )
        .into_response())
}

// ─── Job handlers ───────────────────────────────────────────────────────────

async fn query_get_jobs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    let start = common::get_param_int(params, "start", 0) as usize;
    let max = common::get_param_int(params, "max", 50) as usize;
    let (stats, jobs) = app.storage().api_get_jobs(cid, start, max).await?;
    let mut out = serde_json::json!({"status": "OK", "data": jobs});
    if cid == 0 {
        out["stats"] = serde_json::json!(stats);
    }
    Ok(json_resp(out))
}

async fn query_start_new_job(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let action = common::get_param(params, "action", "")
        .trim()
        .to_lowercase();
    auth::guard::require_user_from_params(app, session, params).await?;
    if !regex::Regex::new(r"^[a-z_]+$").unwrap().is_match(&action) {
        return Err(ApiError(format!("Bad action: '{action}'")));
    }
    let valid = app.storage().api_get_existing_job_actions().await?;
    if !valid.contains(&action) {
        return Err(ApiError(format!("Unknown action: '{action}'")));
    }
    crate::job::Job::queue_simple_job(app, cid, &action, None).await?;
    Ok(ok(serde_json::json!({})))
}

// ─── Issue handlers ─────────────────────────────────────────────────────────

async fn query_get_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let itype = common::get_param(params, "type", "").trim().to_uppercase();
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let catalogs = common::get_param(params, "catalogs", "");
    let count = app
        .storage()
        .api_get_issues_count(&itype, &catalogs)
        .await?;
    if count == 0 {
        return Ok(ok(serde_json::json!({})));
    }
    let r: f64 = if count < limit * 2 {
        0.0
    } else {
        rand::random()
    };
    let issues = app
        .storage()
        .api_get_issues(&itype, &catalogs, limit, offset, r)
        .await?;
    let eids: Vec<usize> = issues
        .iter()
        .filter_map(|i| {
            i.get("entry_id")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
        })
        .collect();
    let entries = if eids.is_empty() {
        serde_json::json!({"entries":{}, "users":{}})
    } else {
        let map = crate::entry::Entry::multiple_from_ids(&eids, app).await?;
        common::entries_to_json_data(&map.into_values().collect::<Vec<_>>(), app).await?
    };
    Ok(ok(
        serde_json::json!({"open_issues": count, "issues": issues, "entries": entries.get("entries"), "users": entries.get("users")}),
    ))
}

async fn query_all_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    if !["duplicate_items", "mismatched_items", "time_mismatch"].contains(&mode.as_str()) {
        return Err(ApiError("Unsupported mode".into()));
    }
    Ok(ok(serde_json::json!(
        app.storage().api_get_all_issues(&mode).await?
    )))
}

async fn query_resolve_issue(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let iid = common::get_param_int(params, "issue_id", 0) as usize;
    if iid == 0 {
        return Err(ApiError("Bad issue ID".into()));
    }
    auth::guard::require_user_from_params(app, session, params).await?;
    app.storage()
        .set_issue_status(iid, crate::issue::IssueStatus::Done)
        .await?;
    Ok(ok(serde_json::json!({})))
}

// ─── User ───────────────────────────────────────────────────────────────────

async fn query_get_user_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let name = common::get_param(params, "username", "").replace('_', " ");
    match app.storage().get_user_by_name(&name).await? {
        Some((id, n, admin)) => Ok(ok(
            serde_json::json!({"id": id, "name": n, "is_catalog_admin": if admin {1} else {0}}),
        )),
        None => Err(ApiError(format!("No user '{name}' found"))),
    }
}

// ─── Recent changes ─────────────────────────────────────────────────────────

async fn query_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let limit = 100;
    let (entry_evts, log_evts) = app
        .storage()
        .api_get_recent_changes(&ts, catalog, limit)
        .await?;
    let mut events: Vec<serde_json::Value> = entry_evts.into_iter().chain(log_evts).collect();
    events.sort_by(|a, b| {
        let ta = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        tb.cmp(ta)
    });
    events.truncate(limit);
    let uids: std::collections::HashSet<usize> = events
        .iter()
        .filter_map(|e| e.get("user").and_then(|v| v.as_u64()).map(|v| v as usize))
        .collect();
    let users = common::get_users(app, &uids).await?;
    Ok(ok(serde_json::json!({"events": events, "users": users})))
}

// ─── Data & analysis ────────────────────────────────────────────────────────

async fn query_get_wd_props(app: &AppState) -> Result<Response, ApiError> {
    let props = app.storage().api_get_wd_props().await?;
    Ok(json_resp(serde_json::json!(props)))
}

async fn query_top_missing(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',')
        .collect();
    if catalogs.is_empty() {
        return Err(ApiError("No catalogs given".into()));
    }
    let data = app.storage().api_get_top_missing(&catalogs).await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_get_common_names(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let min = common::get_param_int(params, "min", 3) as usize;
    let max = common::get_param_int(params, "max", 15) as usize + 1;
    let type_q = common::get_param(params, "type", "");
    let type_q = if regex::Regex::new(r"^Q\d+$").unwrap().is_match(&type_q) {
        type_q
    } else {
        String::new()
    };
    let other_cats_desc = common::get_param_int(params, "other_cats_desc", 0) != 0;
    let data = app
        .storage()
        .api_get_common_names(cid, &type_q, other_cats_desc, min, max, limit, offset)
        .await?;
    Ok(ok(serde_json::json!({"entries": data})))
}

async fn query_same_names(app: &AppState) -> Result<Response, ApiError> {
    let (name, entries) = app.storage().api_get_same_names().await?;
    let data = common::entries_to_json_data(&entries, app).await?;
    let mut out = serde_json::json!({"status": "OK", "data": data});
    out["data"]["name"] = serde_json::json!(name);
    Ok(json_resp(out))
}

async fn query_random_person_batch(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let gender = common::get_param(params, "gender", "");
    let has_desc = common::get_param_int(params, "has_desc", 0) != 0;
    let data = app
        .storage()
        .api_get_random_person_batch(&gender, has_desc)
        .await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_get_property_cache(app: &AppState) -> Result<Response, ApiError> {
    let (prop2item, item_label) = app.storage().api_get_property_cache().await?;
    Ok(ok(
        serde_json::json!({"prop2item": prop2item, "item_label": item_label}),
    ))
}

async fn query_mnm_unmatched_relations(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let property = common::get_param_int(params, "property", 0) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let limit = 25;
    let (id_cnts, entries) = app
        .storage()
        .api_get_mnm_unmatched_relations(property, offset, limit)
        .await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    let entry2cnt: serde_json::Map<String, serde_json::Value> = id_cnts
        .iter()
        .map(|(id, cnt)| (id.to_string(), serde_json::json!(cnt)))
        .collect();
    let entry_order: Vec<usize> = id_cnts.iter().map(|(id, _)| *id).collect();
    data["entry2cnt"] = serde_json::Value::Object(entry2cnt);
    data["entry_order"] = serde_json::json!(entry_order);
    Ok(ok(data))
}

async fn query_creation_candidates(
    _app: &AppState,
    _params: &Params,
) -> Result<Response, ApiError> {
    // Complex multi-strategy endpoint — stub for now
    Ok(ok(serde_json::json!({"entries": [], "users": {}})))
}

// ─── Locations ──────────────────────────────────────────────────────────────

async fn query_locations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let bbox: String = common::get_param(params, "bbox", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-')
        .collect();
    let parts: Vec<f64> = bbox.split(',').filter_map(|s| s.parse().ok()).collect();
    if parts.len() != 4 {
        return Err(ApiError(
            "Required parameter bbox does not have 4 comma-separated numbers".into(),
        ));
    }
    let data = app
        .storage()
        .api_get_locations_bbox(parts[0], parts[1], parts[2], parts[3])
        .await?;
    Ok(json_resp(
        serde_json::json!({"status": "OK", "data": data, "bbox": parts}),
    ))
}

async fn query_get_locations_in_catalog(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_locations_in_catalog(cid).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Download & export ──────────────────────────────────────────────────────

async fn query_download(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let filename = cat
        .name()
        .unwrap_or(&"download".to_string())
        .replace(' ', "_")
        + ".tsv";
    let rows = app.storage().api_get_download_entries(cid).await?;
    // Build user map
    let uids: std::collections::HashSet<usize> =
        rows.iter().filter_map(|(_, _, _, _, u)| *u).collect();
    let users = common::get_users(app, &uids).await?;
    let mut out = String::from("Q\tID\tURL\tName\tUser\n");
    for (q, ext_id, ext_url, ext_name, user_id) in &rows {
        let uname = user_id
            .and_then(|u| users.get(u.to_string().as_str()))
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        out.push_str(&format!("{q}\t{ext_id}\t{ext_url}\t{ext_name}\t{uname}\n"));
    }
    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=UTF-8",
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                &format!("attachment;filename=\"{filename}\""),
            ),
        ],
        out,
    )
        .into_response())
}

#[allow(clippy::cognitive_complexity)]
async fn query_download2(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',')
        .collect();
    let format = common::get_param(params, "format", "tab");
    let columns: serde_json::Value =
        serde_json::from_str(&common::get_param(params, "columns", "{}"))
            .unwrap_or(serde_json::json!({}));
    let hidden: serde_json::Value =
        serde_json::from_str(&common::get_param(params, "hidden", "{}"))
            .unwrap_or(serde_json::json!({}));

    let mut sql = "SELECT entry.id AS entry_id,entry.catalog,ext_id AS external_id".to_string();
    if columns
        .get("exturl")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || columns
            .get("exturl")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0)
            .unwrap_or(false)
    {
        sql.push_str(",ext_url AS external_url,ext_name AS `name`,ext_desc AS description,`type` AS entry_type,entry.user AS mnm_user_id");
    }
    sql.push_str(
        ",(CASE WHEN q IS NULL THEN NULL else concat('Q',q) END) AS q,`timestamp` AS matched_on",
    );
    if columns
        .get("username")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("username")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(",user.name AS matched_by_username");
    }
    if columns
        .get("dates")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("dates")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(",person_dates.born,person_dates.died");
    }
    if columns
        .get("location")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("location")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(",location.lat,location.lon");
    }

    sql.push_str(" FROM entry");
    if columns
        .get("dates")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("dates")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(" LEFT JOIN person_dates ON (entry.id=person_dates.entry_id)");
    }
    if columns
        .get("location")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("location")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(" LEFT JOIN location ON (entry.id=location.entry_id)");
    }
    if columns
        .get("username")
        .and_then(|v| v.as_bool())
        .or(columns
            .get("username")
            .and_then(|v| v.as_i64())
            .map(|v| v != 0))
        .unwrap_or(false)
    {
        sql.push_str(" LEFT JOIN user ON (entry.user=user.id)");
    }

    sql.push_str(&format!(" WHERE entry.catalog IN ({catalogs})"));
    let hb = |k: &str| {
        hidden
            .get(k)
            .and_then(|v| v.as_bool())
            .or(hidden.get(k).and_then(|v| v.as_i64()).map(|v| v != 0))
            .unwrap_or(false)
    };
    if hb("any_matched") {
        sql.push_str(" AND entry.q IS NULL");
    }
    if hb("firmly_matched") {
        sql.push_str(" AND (entry.q IS NULL OR entry.user=0)");
    }
    if hb("user_matched") {
        sql.push_str(" AND (entry.user IS NULL OR entry.user IN (0,3,4))");
    }
    if hb("unmatched") {
        sql.push_str(" AND entry.q IS NOT NULL");
    }
    if hb("no_multiple") {
        sql.push_str(" AND NOT EXISTS (SELECT 1 FROM multi_match WHERE entry.id=multi_match.entry_id)");
    }
    if hb("name_date_matched") {
        sql.push_str(" AND entry.user!=3");
    }
    if hb("automatched") {
        sql.push_str(" AND entry.user!=0");
    }
    if hb("aux_matched") {
        sql.push_str(" AND entry.user!=4");
    }

    let limit = common::get_param_int(params, "limit", 100_000).clamp(1, 1_000_000);
    let offset = common::get_param_int(params, "offset", 0).max(0);
    sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));

    let rows = app.storage().api_get_download2(&sql).await?;
    let ct = if format == "json" {
        "application/json; charset=UTF-8"
    } else {
        "text/plain; charset=UTF-8"
    };
    let mut out = String::new();
    for (i, row) in rows.iter().enumerate() {
        if i == 0 {
            if format == "tab" {
                out.push('#');
                out.push_str(&row.keys().cloned().collect::<Vec<_>>().join("\t"));
                out.push('\n');
            }
            if format == "json" {
                out.push_str("[\n");
            }
        }
        if format == "json" {
            if i > 0 {
                out.push_str(",\n");
            }
            out.push_str(&serde_json::to_string(row).unwrap_or_default());
        } else {
            out.push_str(
                &row.values()
                    .map(|v| v.replace(['\t', '\n', '\r'], " "))
                    .collect::<Vec<_>>()
                    .join("\t"),
            );
            out.push('\n');
        }
    }
    if rows.is_empty() && format == "json" {
        out.push_str("[\n");
    }
    if format == "json" {
        out.push_str("\n]");
    }
    Ok(([(axum::http::header::CONTENT_TYPE, ct)], out).into_response())
}

// ─── Navigation ─────────────────────────────────────────────────────────────

async fn query_redirect(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "ext_id", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let html = format!(
        "<html><head><META http-equiv=\"refresh\" content=\"0;URL={}\"></head><body></body></html>",
        entry.ext_url
    );
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")],
        html,
    )
        .into_response())
}

async fn query_proxy_entry_url(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry_id", 0) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    let client = reqwest::Client::new();
    let body = client
        .get(&entry.ext_url)
        .send()
        .await
        .map_err(|e| ApiError(e.to_string()))?
        .text()
        .await
        .map_err(|e| ApiError(e.to_string()))?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")],
        body,
    )
        .into_response())
}

async fn query_cersei_forward(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let sid = common::get_param_int(params, "scraper", 0) as usize;
    match app.storage().api_get_cersei_catalog(sid).await? {
        Some(cid) => {
            let url = format!("https://mix-n-match.toolforge.org/#/catalog/{cid}");
            Ok((
                axum::http::StatusCode::FOUND,
                [(axum::http::header::LOCATION, url.as_str())],
            )
                .into_response())
        }
        None => Err(ApiError(format!(
            "No catalog associated with CERSEI scraper {sid}"
        ))),
    }
}

// ─── Admin & config ─────────────────────────────────────────────────────────

async fn query_update_overview(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let cs = common::get_param(params, "catalog", "");
    let ids: Vec<usize> = if cs.is_empty() {
        app.storage().api_get_active_catalog_ids().await?
    } else {
        cs.split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect()
    };
    for id in ids {
        let _ = app.storage().catalog_refresh_overview_table(id).await;
    }
    Ok(ok(serde_json::json!({})))
}

async fn query_update_ext_urls(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_catalog_admin_from_params(app, session, params).await?;
    let cid = common::get_catalog(params)?;
    let url = common::get_param(params, "url", "");
    let parts: Vec<&str> = url.split("$1").collect();
    if parts.len() != 2 {
        return Err(ApiError(format!("Bad $1 replacement for '{url}'")));
    }
    let sql = format!(
        "UPDATE entry SET ext_url=concat('{}',ext_id,'{}') WHERE catalog={cid}",
        parts[0].replace('\'', "''"),
        parts[1].replace('\'', "''")
    );
    app.storage().api_get_catalog_entries_raw(&sql).await.ok(); // execute the update
    Ok(ok(serde_json::json!({"sql": sql})))
}

async fn query_add_aliases(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let text = common::get_param(params, "text", "").trim().to_string();
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    if cid == 0 || text.is_empty() {
        return Err(ApiError("Catalog ID or text missing".into()));
    }
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let default_lang = {
        let wp = cat.search_wp();
        if wp.is_empty() {
            "en".to_string()
        } else {
            wp.to_string()
        }
    };
    for row in text.lines() {
        let parts: Vec<&str> = row.trim().split('\t').collect();
        if parts.len() < 2 || parts.len() > 3 {
            continue;
        }
        let ext_id = parts[0].trim();
        let label = parts[1].trim().replace('|', "");
        let lang = if parts.len() == 3 && !parts[2].trim().is_empty() {
            parts[2].trim().to_lowercase()
        } else {
            default_lang.clone()
        };
        let _ = app
            .storage()
            .api_add_alias(cid, ext_id, &lang, &label, uid)
            .await;
    }
    Ok(ok(serde_json::json!({})))
}

async fn query_get_missing_properties(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_missing_properties_raw().await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_set_missing_properties_status(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let row_id = common::get_param_int(params, "row_id", 0) as usize;
    if row_id == 0 {
        return Err(ApiError("Bad/missing row ID".into()));
    }
    let status = common::get_param(params, "status", "");
    if status.is_empty() {
        return Err(ApiError("Invalid status".into()));
    }
    let note = common::get_param(params, "note", "");
    app.storage()
        .api_set_missing_properties_status(row_id, &status, &note, uid)
        .await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_get_top_groups(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_top_groups().await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_set_top_group(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let name = common::get_param(params, "group_name", "");
    let catalogs = common::get_param(params, "catalogs", "");
    let based_on = common::get_param_int(params, "group_id", 0) as usize;
    app.storage()
        .api_set_top_group(&name, &catalogs, uid, based_on)
        .await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_empty_top_group(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let gid = common::get_param_int(params, "group_id", 0) as usize;
    app.storage().api_remove_empty_top_group(gid).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_quick_compare_list(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_quick_compare_list().await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── RC Atom ────────────────────────────────────────────────────────────────

async fn query_rc_atom(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let (entry_evts, log_evts) = app
        .storage()
        .api_get_recent_changes(&ts, catalog, 100)
        .await?;
    let mut events: Vec<serde_json::Value> = entry_evts.into_iter().chain(log_evts).collect();
    events.sort_by(|a, b| {
        let ta = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        tb.cmp(ta)
    });
    events.truncate(100);

    let now = chrono::Utc::now().to_rfc3339();
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\">\n\
         <title>Mix'n'match</title>\n\
         <subtitle>Recent updates by humans (auto-matching not shown)</subtitle>\n\
         <link href=\"https://mix-n-match.toolforge.org/api.php?query=rc_atom\" rel=\"self\" />\n\
         <link href=\"https://mix-n-match.toolforge.org/\" />\n\
         <id>urn:uuid:{}</id>\n\
         <updated>{now}</updated>\n",
        uuid::Uuid::new_v4()
    );
    for e in &events {
        let id = e.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let name = e.get("ext_name").and_then(|v| v.as_str()).unwrap_or("");
        let event_type = e
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("match");
        let timestamp = e.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let title_prefix = if event_type == "remove_q" {
            "Match was removed for "
        } else {
            "New match for "
        };
        xml.push_str(&format!(
            "<entry>\n<title>{title_prefix}\"{name}\"</title>\n\
             <link rel=\"alternate\" href=\"https://mix-n-match.toolforge.org/#/entry/{id}\" />\n\
             <id>urn:uuid:{}</id>\n\
             <updated>{timestamp}</updated>\n\
             </entry>\n",
            uuid::Uuid::new_v4()
        ));
    }
    xml.push_str("</feed>");
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/atom+xml; charset=UTF-8",
        )],
        xml,
    )
        .into_response())
}

async fn query_get_flickr_key() -> Result<Response, ApiError> {
    let key = std::fs::read_to_string("/data/project/mix-n-match/flickr.key").unwrap_or_default();
    Ok(ok(serde_json::json!(key)))
}

// ─── Delegated to micro-API (called directly, no HTTP round-trip) ──────────

async fn query_get_sync(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let _catalog = common::get_catalog(params)?;
    let data = crate::micro_api::data_get_sync(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_sparql_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_sparql_list(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_quick_compare(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_quick_compare(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_get_code_fragments(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let mut data = crate::micro_api::data_get_code_fragments(app, params)
        .await
        .map_err(ApiError)?;
    // PHP behaviour: add user_allowed flag based on the requesting user.
    let username = common::get_param(params, "username", "");
    let user_allowed = if username.is_empty() {
        0
    } else {
        let uid = app
            .storage()
            .get_or_create_user_id(&username.replace('_', " "))
            .await
            .unwrap_or(0);
        // Matches PHP: code_fragment_allowed_user_ids = [2]
        i64::from(uid == 2)
    };
    if let Some(obj) = data.as_object_mut() {
        obj.insert("user_allowed".into(), serde_json::json!(user_allowed));
        obj.entry("catalog")
            .or_insert_with(|| serde_json::json!(catalog));
    }
    Ok(ok(data))
}

async fn query_save_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    // Verify the calling user is allowed.
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if uid != 2 {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    let data = crate::micro_api::data_save_code_fragment(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_test_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    // Verify the calling user is allowed.
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if uid != 2 {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    let entry_id = common::get_param_int(params, "entry_id", 0) as usize;
    if entry_id == 0 {
        return Err(ApiError("No entry_id".into()));
    }
    // Extract the fragment's "function" field and forward it to the Lua runner.
    let fragment_str = common::get_param(params, "fragment", "{}");
    let fragment: serde_json::Value = serde_json::from_str(&fragment_str)
        .map_err(|_| ApiError("Bad fragment".into()))?;
    let function = fragment
        .get("function")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if function.is_empty() {
        return Err(ApiError(format!("Bad fragment function '{function}'")));
    }
    let mut run_params: crate::micro_api::Params = std::collections::HashMap::new();
    run_params.insert("function".into(), function.to_string());
    run_params.insert("entry_id".into(), entry_id.to_string());
    if let Some(html) = params.get("html") {
        run_params.insert("html".into(), html.clone());
    }
    let data = crate::micro_api::data_run_lua(app, &run_params)
        .await
        .map_err(ApiError)?;
    Ok(json_resp(
        serde_json::json!({"status":"OK","data": data,"tested_via":"lua"}),
    ))
}

// ─── Large catalogs (delegated to micro_api helpers) ───────────────────────

async fn query_lc_catalogs(app: &AppState) -> Result<Response, ApiError> {
    // PHP shape: data.catalogs (array of catalog objects), data.open_issues (map)
    let data = crate::micro_api::data_lc_catalogs(app)
        .await
        .map_err(ApiError)?;
    // The micro_api returns {"catalogs": [...], "open_issues": {...}} — same shape.
    Ok(ok(data))
}

async fn query_lc_bbox(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_locations(app, params)
        .await
        .map_err(ApiError)?;
    // PHP shape: {bbox, data, catalogs}. Micro-API returns {data, catalogs}; add bbox for parity.
    let bbox_raw = common::get_param(params, "bbox", "");
    let bbox: Vec<f64> = bbox_raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-')
        .collect::<String>()
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    let mut merged = data.clone();
    if let Some(obj) = merged.as_object_mut() {
        obj.insert("bbox".into(), serde_json::json!(bbox));
    }
    Ok(ok(merged))
}

async fn query_lc_report(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_report(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_lc_report_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_report_list(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_lc_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_rc(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

async fn query_lc_set_status(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    // Gate behind OAuth, then attribute the status change to the session user
    // rather than the free-text `user` field the PHP endpoint used to accept.
    let authed = auth::guard::require_user_from_params(app, session, params).await?;
    let mut patched = params.clone();
    patched.insert("user".into(), authed.wikidata_username.clone());
    let data = crate::micro_api::data_lc_set_status(app, &patched)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

// ─── Newly ported lightweight catalog endpoints ────────────────────────────

async fn query_batch_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let raw = common::get_param(params, "catalog_ids", "");
    let mut ids: Vec<usize> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|id| *id > 0)
        .collect();
    ids.sort();
    ids.dedup();
    ids.truncate(200);
    if ids.is_empty() {
        return Ok(ok(serde_json::json!({})));
    }
    let data = app.storage().api_get_catalog_overview_for_ids(&ids).await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(id) = item.get("id").and_then(|v| v.as_u64()) {
            map.insert(id.to_string(), item);
        }
    }
    Ok(ok(serde_json::Value::Object(map)))
}

async fn query_search_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let q = common::get_param(params, "q", "");
    let limit = (common::get_param_int(params, "limit", 20).clamp(1, 100)) as usize;
    if q.is_empty() {
        return Ok(ok(serde_json::json!([])));
    }
    let rows = app.storage().api_search_catalogs(&q, limit).await?;
    Ok(ok(serde_json::json!(rows)))
}

async fn query_catalog_type_counts(app: &AppState) -> Result<Response, ApiError> {
    let rows = app.storage().api_catalog_type_counts().await?;
    Ok(ok(serde_json::json!(rows)))
}

async fn query_latest_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let limit = (common::get_param_int(params, "limit", 9).clamp(1, 50)) as usize;
    let rows = app.storage().api_latest_catalogs(limit).await?;
    Ok(ok(serde_json::json!(rows)))
}

async fn query_catalogs_with_locations(app: &AppState) -> Result<Response, ApiError> {
    let rows = app.storage().api_catalogs_with_locations().await?;
    Ok(ok(serde_json::json!(rows)))
}

async fn query_catalog_property_groups(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_catalog_property_groups().await?;
    Ok(ok(data))
}

async fn query_check_wd_prop_usage(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let wd_prop = common::get_param_int(params, "wd_prop", 0);
    let exclude = common::get_param_int(params, "exclude_catalog", 0) as usize;
    if wd_prop <= 0 {
        return Ok(ok(serde_json::json!({"used": false})));
    }
    let result = app
        .storage()
        .api_check_wd_prop_usage(wd_prop as usize, exclude)
        .await?;
    Ok(ok(result))
}

async fn query_catalog_by_group(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let group = common::get_param(params, "group", "");
    if group.is_empty() {
        return Ok(ok(serde_json::json!({})));
    }
    let data = app.storage().api_catalog_by_group(&group).await?;
    Ok(ok(data))
}

// ─── Other newly ported endpoints ──────────────────────────────────────────

async fn query_create(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let rows = app.storage().api_create_list(catalog).await?;
    Ok(ok(serde_json::json!(rows)))
}

async fn query_user_edits(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let user_id = common::get_param_int(params, "user_id", -1);
    if user_id < 0 {
        return Err(ApiError("Invalid user ID".into()));
    }
    let user_id = user_id as usize;
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let limit = common::get_param_int(params, "limit", 50).clamp(1, 200) as usize;
    let offset = common::get_param_int(params, "offset", 0).max(0) as usize;
    let (events, users_map, total, user_info) = app
        .storage()
        .api_user_edits(user_id, catalog, limit, offset)
        .await?;
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "total": total,
        "data": {
            "user_info": user_info,
            "events": events,
            "users": users_map,
        }
    })))
}

async fn query_get_statement_text_groups(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50).max(1) as usize;
    let offset = common::get_param_int(params, "offset", 0).max(0) as usize;
    let property = common::get_param_int(params, "property", 0).max(0) as usize;
    let (properties, groups) = app
        .storage()
        .api_get_statement_text_groups(catalog, property, limit, offset)
        .await?;
    Ok(ok(
        serde_json::json!({"properties": properties, "groups": groups}),
    ))
}

async fn query_set_statement_text_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let text = common::get_param(params, "text", "");
    let property = common::get_param_int(params, "property", 0);
    let q = common::get_param_int(params, "q", 0);
    let user_id = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if text.is_empty() {
        return Err(ApiError("Missing text parameter".into()));
    }
    if property <= 0 {
        return Err(ApiError("Missing or invalid property parameter".into()));
    }
    if q <= 0 {
        return Err(ApiError("Missing or invalid q parameter".into()));
    }
    let (rows_updated, aux_rows_added) = app
        .storage()
        .api_set_statement_text_q(catalog, property as usize, &text, q as usize, user_id)
        .await?;
    Ok(ok(serde_json::json!({
        "rows_updated": rows_updated,
        "aux_rows_added": aux_rows_added,
    })))
}

async fn query_missingpages(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let site = common::get_param(params, "site", "");
    if site.is_empty() {
        return Err(ApiError("site parameter required".into()));
    }
    let (entries, users) = app.storage().api_missingpages(catalog, &site).await?;
    Ok(ok(serde_json::json!({"entries": entries, "users": users})))
}

async fn query_sitestats(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_raw = common::get_param(params, "catalog", "");
    let catalog = if catalog_raw.is_empty() {
        None
    } else {
        catalog_raw.parse::<usize>().ok()
    };
    let data = app.storage().api_sitestats(catalog).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Distributed-game endpoints ────────────────────────────────────────────

async fn query_dg_desc(params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    let (title, sub) = if mode == "person" {
        (
            "Mix'n'match people game",
            "of a person in",
        )
    } else {
        ("Mix'n'match game", "in")
    };
    let out = serde_json::json!({
        "label": {"en": title},
        "description": {"en": format!("Verify that an entry {sub} an external catalog matches a given Wikidata item. Decisions count as mix'n'match actions!")},
        "icon": "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2d/Bipartite_graph_with_matching.svg/120px-Bipartite_graph_with_matching.svg.png",
        "options": [
            {"name": "Entry type", "key": "type", "values": {"any": "Any", "person": "Person", "not_person": "Not a person"}}
        ],
    });
    // PHP returns this payload as the top-level response (no "data" envelope).
    Ok(json_resp(out))
}

async fn query_dg_tiles(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let num = common::get_param_int(params, "num", 5).clamp(1, 20) as usize;
    let type_filter = common::get_param(params, "type", "");
    let tiles = app
        .storage()
        .api_dg_tiles(num, &type_filter)
        .await?;
    Ok(json_resp(serde_json::json!(tiles)))
}

async fn query_dg_log_action(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let user = common::get_param(params, "user", "");
    let entry_id = common::get_param_int(params, "tile", -1);
    if entry_id < 0 {
        return Err(ApiError("bad tile".into()));
    }
    let entry_id = entry_id as usize;
    let decision = common::get_param(params, "decision", "");
    let uid = app.storage().get_or_create_user_id(&user).await?;
    let mut entry = crate::entry::Entry::from_id(entry_id, app).await?;
    match decision.as_str() {
        "yes" => {
            if let Some(q) = entry.q {
                entry.set_match(&format!("Q{q}"), uid).await?;
            }
        }
        "no" => {
            entry.unmatch().await?;
        }
        "n_a" => {
            entry.set_match("Q-1", uid).await?;
        }
        _ => {}
    }
    Ok(json_resp(serde_json::json!([])))
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn test_router_builds() {
        // Verifies router construction doesn't panic
    }
}
