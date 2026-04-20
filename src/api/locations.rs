//! Geo / locations endpoints.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use axum::response::Response;

pub async fn query_locations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
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
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": data,
        "bbox": parts,
    })))
}

pub async fn query_get_locations_in_catalog(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_locations_in_catalog(cid).await?;
    Ok(ok(serde_json::json!(data)))
}
