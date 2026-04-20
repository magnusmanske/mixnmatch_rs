//! Job management endpoints.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use std::sync::OnceLock;
use tower_sessions::Session;

fn re_action_name() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[a-z_]+$").expect("valid regex"))
}

pub async fn query_get_jobs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    let start = common::get_param_int(params, "start", 0) as usize;
    let max = common::get_param_int(params, "max", 50) as usize;
    let status_filter = common::get_param(params, "status_filter", "");
    let (stats, jobs, total) = app.storage().api_get_jobs(cid, start, max, &status_filter).await?;
    let mut out = serde_json::json!({"status": "OK", "data": jobs, "total": total});
    if cid == 0 {
        out["stats"] = serde_json::json!(stats);
    }
    Ok(json_resp(out))
}

pub async fn query_start_new_job(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let action = common::get_param(params, "action", "")
        .trim()
        .to_lowercase();
    auth::guard::require_user_from_params(app, session, params).await?;
    if !re_action_name().is_match(&action) {
        return Err(ApiError(format!("Bad action: '{action}'")));
    }
    let valid = app.storage().api_get_existing_job_actions().await?;
    if !valid.contains(&action) {
        return Err(ApiError(format!("Unknown action: '{action}'")));
    }
    crate::job::Job::queue_simple_job(app, cid, &action, None).await?;
    Ok(ok(serde_json::json!({})))
}
