//! `lc_*` endpoints — operate against the large_catalogs DB. All real work
//! lives in `crate::micro_api`; these wrappers just adapt the shape.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_lc_catalogs(app: &AppState) -> Result<Response, ApiError> {
    // PHP shape: data.catalogs (array of catalog objects), data.open_issues (map).
    let data = crate::micro_api::data_lc_catalogs(app)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_lc_bbox(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mut data = crate::micro_api::data_lc_locations(app, params)
        .await
        .map_err(ApiError)?;
    // PHP shape: {bbox, data, catalogs}. Insert bbox in-place rather than
    // cloning the whole payload.
    let bbox_raw = common::get_param(params, "bbox", "");
    let bbox: Vec<f64> = bbox_raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-')
        .collect::<String>()
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect();
    if let Some(obj) = data.as_object_mut() {
        obj.insert("bbox".into(), serde_json::json!(bbox));
    }
    Ok(ok(data))
}

pub async fn query_lc_report(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_report(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_lc_report_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_report_list(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_lc_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_lc_rc(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_lc_set_status(
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
