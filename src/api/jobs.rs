//! Job management endpoints.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use std::sync::OnceLock;
use tower_sessions::Session;
use wikimisc::timestamp::TimeStamp;

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
    let user = auth::guard::require_user_from_params(app, session, params).await?;
    if !re_action_name().is_match(&action) {
        return Err(ApiError::BadRequest(format!("Bad action: '{action}'")));
    }
    let valid = app.storage().api_get_existing_job_actions().await?;
    if !valid.contains(&action) {
        return Err(ApiError::BadRequest(format!("Unknown action: '{action}'")));
    }
    crate::job::Job::queue_simple_job_for_user(app, cid, &action, None, user.mnm_user_id).await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_manage_job(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let user = auth::guard::require_user_from_params(app, session, params).await?;
    let job_id = common::get_param_usize(params, "job_id")?;
    let action = common::get_param(params, "action", "");

    let job = app
        .storage()
        .jobs_row_from_id(job_id)
        .await
        .map_err(|_| ApiError::NotFound(format!("No job with id {job_id}")))?;

    if !is_job_manager(app, &user, &job).await? {
        return Err(ApiError::Internal(
            "Not authorized to manage this job".into(),
        ));
    }

    let new_status = match action.as_str() {
        "stop" => crate::job_status::JobStatus::Deactivated,
        "pause" => crate::job_status::JobStatus::Paused,
        "resume" => crate::job_status::JobStatus::Todo,
        _ => return Err(ApiError::BadRequest(format!("Unknown action: '{action}'"))),
    };

    app.storage()
        .jobs_set_status(&new_status, job_id, TimeStamp::now())
        .await?;
    Ok(ok(serde_json::json!({})))
}

/// Returns true if `user` is allowed to stop/pause/resume `job`.
/// Allowed when: catalog admin, owns the job, or the job belongs to the "automatic" system user.
async fn is_job_manager(
    app: &AppState,
    user: &auth::guard::AuthedUser,
    job: &crate::job_row::JobRow,
) -> Result<bool, ApiError> {
    // Dev mode: Magnus Manske is always admin.
    if auth::guard::dev_bypass_user().is_some() {
        return Ok(true);
    }
    // Catalog admins can manage any job.
    let user_info = app
        .storage()
        .get_user_by_name(&user.wikidata_username)
        .await
        .map_err(|e| ApiError::Internal(format!("Admin check failed: {e}")))?;
    if matches!(user_info, Some((_, _, true))) {
        return Ok(true);
    }
    // User owns the job (only valid when user_id is non-zero, i.e. properly tracked).
    if user.mnm_user_id != 0 && job.user_id == user.mnm_user_id {
        return Ok(true);
    }
    // Job belongs to the special "automatic" system user.
    if let Ok(Some((automatic_id, _, _))) = app.storage().get_user_by_name("automatic").await {
        if job.user_id == automatic_id {
            return Ok(true);
        }
    }
    Ok(false)
}
