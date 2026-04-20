//! Issue tracking endpoints.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_get_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let itype = common::get_param(params, "type", "").trim().to_uppercase();
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let catalogs = common::get_param(params, "catalogs", "");

    // Optional client-supplied seed so subsequent paginated requests scan the
    // same random ordering as the first page. Without this, every page click
    // re-rolled the threshold and could overlap or skip rows.
    let client_threshold = common::get_param(params, "random_threshold", "")
        .parse::<f64>()
        .ok()
        .filter(|v| (0.0..=1.0).contains(v));

    let count = app
        .storage()
        .api_get_issues_count(&itype, &catalogs)
        .await?;
    if count == 0 {
        return Ok(ok(serde_json::json!({})));
    }
    let r: f64 = match client_threshold {
        Some(t) => t,
        // Few issues → start from 0 so we always see them all; many issues →
        // pick a random window so first-page reloads sample different items.
        None if count < limit * 2 => 0.0,
        None => rand::random(),
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
    Ok(ok(serde_json::json!({
        "open_issues": count,
        "issues": issues,
        "entries": entries.get("entries"),
        "users": entries.get("users"),
        "random_threshold": r,
    })))
}

pub async fn query_all_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    if !["duplicate_items", "mismatched_items", "time_mismatch"].contains(&mode.as_str()) {
        return Err(ApiError("Unsupported mode".into()));
    }
    Ok(ok(serde_json::json!(
        app.storage().api_get_all_issues(&mode).await?
    )))
}

pub async fn query_resolve_issue(
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
