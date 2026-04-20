//! Data & analysis read endpoints (props, common names, person batches,
//! the "creation candidates" wrapper that delegates to micro_api).

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use axum::response::Response;
use std::sync::OnceLock;

fn re_q_only() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^Q\d+$").expect("valid regex"))
}

pub async fn query_get_wd_props(app: &AppState) -> Result<Response, ApiError> {
    let props = app.storage().api_get_wd_props().await?;
    Ok(json_resp(serde_json::json!(props)))
}

pub async fn query_top_missing(app: &AppState, params: &Params) -> Result<Response, ApiError> {
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

pub async fn query_get_common_names(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let min = common::get_param_int(params, "min", 3) as usize;
    let max = common::get_param_int(params, "max", 15) as usize + 1;
    let type_q = common::get_param(params, "type", "");
    let type_q = if re_q_only().is_match(&type_q) {
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

pub async fn query_same_names(app: &AppState) -> Result<Response, ApiError> {
    let (name, entries) = app.storage().api_get_same_names().await?;
    let data = common::entries_to_json_data(&entries, app).await?;
    let mut out = serde_json::json!({"status": "OK", "data": data});
    out["data"]["name"] = serde_json::json!(name);
    Ok(json_resp(out))
}

pub async fn query_random_person_batch(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let gender = common::get_param(params, "gender", "");
    let has_desc = common::get_param_int(params, "has_desc", 0) != 0;
    let data = app
        .storage()
        .api_get_random_person_batch(&gender, has_desc)
        .await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_get_property_cache(app: &AppState) -> Result<Response, ApiError> {
    let (prop2item, item_label) = app.storage().api_get_property_cache().await?;
    Ok(ok(
        serde_json::json!({"prop2item": prop2item, "item_label": item_label}),
    ))
}

pub async fn query_mnm_unmatched_relations(
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

pub async fn query_creation_candidates(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    Ok(ok(crate::api::creation_candidates::run(app, params).await?))
}
