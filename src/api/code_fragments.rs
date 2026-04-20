//! Read/write the catalog-attached Lua code fragments. Pure DB plumbing —
//! actual Lua execution lives in `crate::api::lua`.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use serde_json::{Value, json};
use tower_sessions::Session;

/// User IDs allowed to write/test code fragments. Mirrors PHP
/// `code_fragment_allowed_user_ids = [2]` (Magnus only, currently).
const ALLOWED_USER_IDS: &[usize] = &[2];

pub(super) fn user_is_allowed(uid: usize) -> bool {
    ALLOWED_USER_IDS.contains(&uid)
}

// ─── axum-shape entry points (used by `?query=…`) ──────────────────────────

/// `?query=get_code_fragments&catalog=…` — returns the catalog's fragments
/// plus a per-user `user_allowed` flag the frontend uses to gate the editor.
pub async fn query_get_code_fragments(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    // Resolve the user_id concurrently with fetching the fragments — they're
    // independent reads, and `get_or_create_user_id` adds an extra DB RTT.
    let username = common::get_param(params, "username", "");
    let (data_res, user_allowed) = tokio::join!(
        get_for_catalog(app, catalog),
        async {
            if username.is_empty() {
                0_i64
            } else {
                let uid = app
                    .storage()
                    .get_or_create_user_id(&username.replace('_', " "))
                    .await
                    .unwrap_or(0);
                i64::from(user_is_allowed(uid))
            }
        },
    );
    let mut data = data_res?;
    if let Some(obj) = data.as_object_mut() {
        obj.insert("user_allowed".into(), json!(user_allowed));
        obj.entry("catalog").or_insert_with(|| json!(catalog));
    }
    Ok(ok(data))
}

/// `?query=save_code_fragment&fragment=<json>` — gated behind OAuth +
/// `ALLOWED_USER_IDS`.
pub async fn query_save_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if !user_is_allowed(uid) {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    Ok(ok(save_from_params(app, params).await?))
}

/// Returns `{ fragments: [...], all_functions: [...] }` for a catalog.
pub async fn get_for_catalog(app: &AppState, catalog_id: usize) -> Result<Value, ApiError> {
    if catalog_id == 0 {
        return Err(ApiError("missing required parameter: catalog".into()));
    }

    // Both fetches are independent — fan out instead of awaiting serially.
    let s = app.storage();
    let (fragments_res, all_functions_res) = tokio::join!(
        s.get_code_fragments_for_catalog(catalog_id),
        s.get_all_code_fragment_functions(),
    );
    let fragments = fragments_res.map_err(|e| ApiError(format!("database error: {e}")))?;
    let all_functions =
        all_functions_res.map_err(|e| ApiError(format!("database error: {e}")))?;

    Ok(json!({
        "fragments": fragments,
        "all_functions": all_functions,
    }))
}

/// Save a fragment + queue any matching maintenance jobs (e.g. PERSON_DATE
/// triggers `update_person_dates` followed by `match_person_dates`).
pub async fn save(app: &AppState, fragment_json: &str) -> Result<Value, ApiError> {
    if fragment_json.is_empty() {
        return Err(ApiError("missing required parameter: fragment".into()));
    }
    let fragment: Value = serde_json::from_str(fragment_json)
        .map_err(|e| ApiError(format!("invalid fragment JSON: {e}")))?;

    let catalog = fragment["catalog"].as_u64().unwrap_or(0) as usize;
    if catalog == 0 {
        return Err(ApiError("fragment must have a positive catalog ID".into()));
    }

    let function = fragment["function"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if function.is_empty() {
        return Err(ApiError("fragment must have a function".into()));
    }

    let cfid = app
        .storage()
        .save_code_fragment(&fragment)
        .await
        .map_err(|e| ApiError(format!("save failed: {e}")))?;

    let queued_jobs = queue_jobs_for_function(app, catalog, &function).await;

    Ok(json!({
        "id": cfid,
        "queued_jobs": queued_jobs,
    }))
}

/// Convenience for the legacy micro_api shape: pull `fragment=<json>` out of params.
pub async fn save_from_params(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let fragment_json = params.get("fragment").map(String::as_str).unwrap_or("");
    save(app, fragment_json).await
}

/// Queue the maintenance jobs that match the saved function. Errors are
/// swallowed (the row was saved either way; users can re-queue manually).
async fn queue_jobs_for_function(
    app: &AppState,
    catalog: usize,
    function: &str,
) -> Vec<&'static str> {
    let mut queued = vec![];
    match function {
        "PERSON_DATE" => {
            let job_id = app
                .storage()
                .queue_job(catalog, "update_person_dates", None)
                .await
                .unwrap_or(0);
            queued.push("update_person_dates");
            let _ = app
                .storage()
                .queue_job(catalog, "match_person_dates", Some(job_id))
                .await;
            queued.push("match_person_dates");
        }
        "AUX_FROM_DESC" => {
            let _ = app
                .storage()
                .queue_job(catalog, "generate_aux_from_description", None)
                .await;
            queued.push("generate_aux_from_description");
        }
        "DESC_FROM_HTML" => {
            let _ = app
                .storage()
                .queue_job(catalog, "update_descriptions_from_url", None)
                .await;
            queued.push("update_descriptions_from_url");
        }
        _ => {}
    }
    queued
}
