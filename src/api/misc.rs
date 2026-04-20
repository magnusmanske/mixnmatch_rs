//! Miscellaneous endpoints: user info, create-list, user-edits, statement
//! text groups, missingpages, sitestats.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_get_user_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let name = common::get_param(params, "username", "").replace('_', " ");
    match app.storage().get_user_by_name(&name).await? {
        Some((id, n, admin)) => Ok(ok(serde_json::json!({
            "id": id,
            "name": n,
            "is_catalog_admin": if admin {1} else {0},
        }))),
        None => Err(ApiError(format!("No user '{name}' found"))),
    }
}

pub async fn query_create(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let rows = app.storage().api_create_list(catalog).await?;
    Ok(ok(serde_json::json!(rows)))
}

pub async fn query_user_edits(app: &AppState, params: &Params) -> Result<Response, ApiError> {
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

pub async fn query_get_statement_text_groups(
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
    Ok(ok(serde_json::json!({
        "properties": properties,
        "groups": groups,
    })))
}

pub async fn query_set_statement_text_q(
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

pub async fn query_missingpages(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let site = common::get_param(params, "site", "");
    if site.is_empty() {
        return Err(ApiError("site parameter required".into()));
    }
    let (entries, users) = app.storage().api_missingpages(catalog, &site).await?;
    Ok(ok(serde_json::json!({"entries": entries, "users": users})))
}

pub async fn query_sitestats(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_raw = common::get_param(params, "catalog", "");
    let catalog = if catalog_raw.is_empty() {
        None
    } else {
        catalog_raw.parse::<usize>().ok()
    };
    let data = app.storage().api_sitestats(catalog).await?;
    Ok(ok(serde_json::json!(data)))
}
