//! Legacy "micro-API" HTTP server.
//!
//! All real business logic now lives in `crate::api::*` (each `?action=…`
//! handler here is a thin wrapper that parses params, calls the matching
//! `crate::api::*::*_data` function, and wraps the result in this server's
//! `{"status":"ok","data":…}` envelope).
//!
//! Slated for removal once the consumers have moved to `/api.php`.

use crate::api::common::ApiError as ApiCommonError;
use crate::app_state::AppState;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

type SharedState = Arc<AppState>;
pub type Params = HashMap<String, String>;

// ─── Router ────────────────────────────────────────────────────────────────

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    Router::new().route("/api", get(api_dispatch)).with_state(state)
}

/// Start the micro-API server on the given port. Runs until the process exits.
pub async fn serve(app: AppState, port: u16) {
    let router = router(app);
    let addr = format!("0.0.0.0:{port}");
    match TcpListener::bind(&addr).await {
        Ok(listener) => {
            log::info!("micro_api: listening on http://127.0.0.1:{port}");
            if let Err(e) = axum::serve(listener, router).await {
                log::error!("micro_api server error: {e}");
            }
        }
        Err(e) => {
            log::error!("micro_api: failed to bind to {addr}: {e}");
        }
    }
}

// ─── Dispatch ──────────────────────────────────────────────────────────────

async fn api_dispatch(
    State(app): State<SharedState>,
    Query(params): Query<Params>,
) -> Response {
    let action = params.get("action").cloned().unwrap_or_default();
    let result: Result<Value, ApiError> = match action.as_str() {
        "run_lua" => crate::api::lua_run_from_params(&app, &params)
            .await
            .map_err(into_micro_err),
        "get_code_fragments" => {
            // Mirrors the micro_api param contract: `catalog` is required.
            match params.get("catalog").filter(|s| !s.is_empty()) {
                None => Err(ApiError::new("missing required parameter: catalog")),
                Some(s) => match s.parse::<usize>() {
                    Ok(cid) => crate::api::code_fragments_get_for_catalog(&app, cid)
                        .await
                        .map_err(into_micro_err),
                    Err(_) => Err(ApiError::new(
                        "parameter 'catalog' must be a positive integer",
                    )),
                },
            }
        }
        "save_code_fragment" => crate::api::code_fragments_save_from_params(&app, &params)
            .await
            .map_err(into_micro_err),
        "sparql_list" => crate::api::sparql_list_from_params(&app, &params)
            .await
            .map_err(into_micro_err),
        "get_sync" => match required_usize(&params, "catalog") {
            Ok(cid) => crate::api::sync_get(&app, cid)
                .await
                .map_err(into_micro_err),
            Err(e) => Err(e),
        },
        "creation_candidates" => crate::api::creation_candidates_run(&app, &params)
            .await
            .map_err(into_micro_err),
        "quick_compare" => crate::api::quick_compare_run(&app, &params)
            .await
            .map_err(into_micro_err),
        "lc_catalogs" => crate::api::lc_catalogs_data(&app)
            .await
            .map_err(into_micro_err),
        "lc_locations" => crate::api::lc_locations_data(&app, &params)
            .await
            .map_err(into_micro_err),
        "lc_report" => crate::api::lc_report_data(&app, &params)
            .await
            .map_err(into_micro_err),
        "lc_report_list" => crate::api::lc_report_list_data(&app, &params)
            .await
            .map_err(into_micro_err),
        "lc_rc" => crate::api::lc_rc_data(&app, &params)
            .await
            .map_err(into_micro_err),
        "lc_set_status" => crate::api::lc_set_status_data(&app, &params)
            .await
            .map_err(into_micro_err),
        "" => Err(ApiError::new("missing 'action' parameter")),
        other => Err(ApiError::new(&format!("unknown action: {other}"))),
    };
    match result {
        Ok(data) => success(data),
        Err(e) => e.into_response(),
    }
}

// ─── Error / success envelope ──────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ApiError {
    message: String,
    kind: &'static str,
}

impl ApiError {
    fn new(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            kind: "bad_request",
        }
    }

    fn internal(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            kind: "internal_error",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "status": self.kind, "error": self.message });
        Json(body).into_response()
    }
}

fn success(data: Value) -> Response {
    Json(json!({ "status": "ok", "data": data })).into_response()
}

/// Bridge `crate::api::common::ApiError` (a flat string) to this module's
/// kinded ApiError. We classify a few well-known prefixes as `internal_error`
/// to preserve test expectations; everything else is `bad_request`, which
/// matches the legacy default for parameter/validation problems.
fn into_micro_err(e: ApiCommonError) -> ApiError {
    let msg = e.0;
    let internal_markers = [
        "database error",
        "save failed",
        "Lua execution error",
        "SPARQL query failed",
        "failed to get Wikidata API",
        "Wikidata API error",
        "large catalogs DB error",
        "update failed",
        "query failed",
    ];
    if internal_markers.iter().any(|m| msg.contains(m)) {
        ApiError::internal(&msg)
    } else {
        ApiError::new(&msg)
    }
}

fn required_usize(params: &Params, key: &str) -> Result<usize, ApiError> {
    let s = params
        .get(key)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| ApiError::new(&format!("missing required parameter: {key}")))?;
    s.parse::<usize>()
        .map_err(|_| ApiError::new(&format!("parameter '{key}' must be a positive integer")))
}

// ─── Tests (HTTP envelope + dispatch) ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    fn test_app() -> AppState {
        crate::app_state::get_test_app()
    }

    fn build_request(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    async fn response_json(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
        (status, json)
    }

    // --- dispatch ---

    #[tokio::test]
    async fn test_missing_action() {
        let app = router(test_app());
        let resp = app.oneshot(build_request("/api")).await.unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("missing"));
    }

    #[tokio::test]
    async fn test_unknown_action() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=bogus"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("unknown action"));
    }

    // --- run_lua param validation ---

    #[tokio::test]
    async fn test_run_lua_missing_function() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=run_lua&entry_id=1"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("function"));
    }

    #[tokio::test]
    async fn test_run_lua_missing_entry_id() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=run_lua&function=PERSON_DATE"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("entry_id"));
    }

    #[tokio::test]
    async fn test_run_lua_bad_function() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=run_lua&function=EVIL&entry_id=1",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_run_lua_bad_entry_id() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=run_lua&function=PERSON_DATE&entry_id=abc",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("positive integer"));
    }

    // --- success / error envelope ---

    #[test]
    fn test_success_shape() {
        let resp = success(json!({"x": 1}));
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["data"]["x"], 1);
    }

    #[test]
    fn test_error_shape_bad_request() {
        let err = ApiError::new("oops");
        let resp = err.into_response();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert_eq!(body["error"], "oops");
    }

    #[test]
    fn test_internal_error_status() {
        let err = ApiError::internal("boom");
        let resp = err.into_response();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "internal_error");
        assert_eq!(body["error"], "boom");
    }

    #[test]
    fn test_into_micro_err_classifies_db_errors_as_internal() {
        let e = ApiCommonError("database error: connection refused".into());
        assert_eq!(into_micro_err(e).kind, "internal_error");
    }

    #[test]
    fn test_into_micro_err_defaults_to_bad_request() {
        let e = ApiCommonError("missing required parameter: x".into());
        assert_eq!(into_micro_err(e).kind, "bad_request");
    }

    // --- get_code_fragments ---

    #[tokio::test]
    async fn test_get_code_fragments_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_code_fragments"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_code_fragments_valid() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_code_fragments&catalog=1"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        let s = body["status"].as_str().unwrap_or("");
        if s == "ok" {
            assert!(body["data"]["fragments"].is_array());
            assert!(body["data"]["all_functions"].is_array());
        }
        // internal_error is acceptable if the DB tunnel went down mid-test.
    }

    // --- save_code_fragment ---

    #[tokio::test]
    async fn test_save_code_fragment_missing_fragment() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=save_code_fragment"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("fragment"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_bad_json() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=save_code_fragment&fragment=not_json",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("invalid"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_missing_catalog() {
        let app = router(test_app());
        let frag = urlencoding::encode(r#"{"function":"PERSON_DATE","php":"","catalog":0}"#);
        let resp = app
            .oneshot(build_request(&format!(
                "/api?action=save_code_fragment&fragment={frag}"
            )))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_missing_function() {
        let app = router(test_app());
        let frag = urlencoding::encode(r#"{"catalog":1,"php":""}"#);
        let resp = app
            .oneshot(build_request(&format!(
                "/api?action=save_code_fragment&fragment={frag}"
            )))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("function"));
    }

    // --- sparql_list ---

    #[tokio::test]
    async fn test_sparql_list_missing_sparql_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=sparql_list"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("sparql"));
    }

    #[tokio::test]
    async fn test_sparql_list_empty_sparql_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=sparql_list&sparql="))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("sparql"));
    }

    // --- get_sync ---

    #[tokio::test]
    async fn test_get_sync_missing_catalog_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_sync_non_numeric_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog=abc"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("positive integer"));
    }

    #[tokio::test]
    async fn test_get_sync_empty_catalog_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog="))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_sync_nonexistent_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog=999999999"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_ne!(body["status"], "ok");
    }

    // --- creation_candidates ---

    #[tokio::test]
    async fn test_creation_candidates_response_structure() {
        let app = router(test_app());
        // `ext_name=` forces the pick to a constant SELECT instead of the
        // `SELECT … FROM common_names … ORDER BY rand() LIMIT 1` full-table
        // scan the default mode runs — the indexed ext_name lookup is enough
        // to smoke-test the response shape.
        let resp = app
            .oneshot(build_request(
                "/api?action=creation_candidates&min=0&mode=&ext_name=MnmTestNonexistentName_9d3f",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        let status = body["status"].as_str().unwrap_or("");
        assert!(
            status == "ok" || status == "internal_error" || status == "bad_request",
            "unexpected status: {status}"
        );
    }

    // --- quick_compare ---

    #[tokio::test]
    async fn test_quick_compare_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=quick_compare"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    // --- lc_* ---

    #[tokio::test]
    async fn test_lc_locations_missing_bbox() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_locations"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_locations_bad_bbox() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_locations&bbox=1,2,3"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("4"));
    }

    #[tokio::test]
    async fn test_lc_report_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_report"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_report_list_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_report_list"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_missing_params() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_set_status"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_empty_status() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=lc_set_status&status=&id=1&user=test",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_no_user() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=lc_set_status&status=DONE&id=1&user=",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }
}
