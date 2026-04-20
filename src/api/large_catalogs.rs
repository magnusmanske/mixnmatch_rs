//! `lc_*` endpoints — operate against the large_catalogs DB. Each handler
//! exposes both the public `query_lc_*` axum entry point and a `*_data`
//! function that returns the raw JSON payload (no envelope), used by the
//! legacy `micro_api` thin wrappers.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use serde_json::{Value, json};
use std::collections::HashMap;
use tower_sessions::Session;

// ─── lc_catalogs ────────────────────────────────────────────────────────────

pub async fn lc_catalogs_data(app: &AppState) -> Result<Value, ApiError> {
    let s = app.large_catalogs();
    let (catalogs_res, open_issues_res) =
        tokio::join!(s.get_catalogs(), s.get_open_issue_counts());
    let catalogs = catalogs_res.map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;
    let open_issues = open_issues_res.unwrap_or_default();
    Ok(json!({
        "catalogs": catalogs,
        "open_issues": open_issues,
    }))
}

pub async fn query_lc_catalogs(app: &AppState) -> Result<Response, ApiError> {
    Ok(ok(lc_catalogs_data(app).await?))
}

// ─── lc_bbox / lc_locations ────────────────────────────────────────────────

pub async fn lc_locations_data(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let limit = opt_usize(params, "limit").unwrap_or(100).min(10_000);
    let bbox = parse_bbox_required(params)?;
    let ignore_catalogs: Vec<usize> = common::get_param(params, "ignore_catalogs", "")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let catalogs = app
        .large_catalogs()
        .get_catalogs()
        .await
        .map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;

    let mut data: Vec<Value> = vec![];
    let mut used_catalogs: HashMap<usize, Value> = HashMap::new();

    for catalog in &catalogs {
        let cat_id = catalog["id"].as_u64().unwrap_or(0) as usize;
        if catalog["has_lat_lon"].as_u64().unwrap_or(0) == 0 {
            continue;
        }
        if ignore_catalogs.contains(&cat_id) {
            continue;
        }
        let table = match catalog["table"].as_str() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };

        let entries = app
            .large_catalogs()
            .get_entries_in_bbox(table, &bbox, limit)
            .await
            .unwrap_or_default();

        for mut entry in entries {
            entry["catalog"] = json!(cat_id);
            data.push(entry);
            if data.len() >= limit {
                break;
            }
        }
        if !data.is_empty() {
            used_catalogs.insert(cat_id, catalog.clone());
        }
        if data.len() >= limit {
            break;
        }
    }

    Ok(json!({
        "data": data,
        "catalogs": used_catalogs,
    }))
}

pub async fn query_lc_bbox(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mut data = lc_locations_data(app, params).await?;
    // PHP shape: {bbox, data, catalogs}. Insert bbox in-place rather than
    // cloning the whole payload.
    let bbox: Vec<f64> = parse_bbox_lenient(params);
    if let Some(obj) = data.as_object_mut() {
        obj.insert("bbox".into(), json!(bbox));
    }
    Ok(ok(data))
}

// ─── lc_report / lc_report_list ────────────────────────────────────────────

pub async fn lc_report_data(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let catalog_id = required_usize(params, "catalog")?;
    let s = app.large_catalogs();
    // Catalog metadata + matrix are independent reads.
    let (catalogs_res, matrix_res) = tokio::join!(
        s.get_catalogs_map(),
        s.get_report_matrix(catalog_id),
    );
    let catalogs =
        catalogs_res.map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;
    let matrix = matrix_res.map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;
    let catalog = catalogs.get(&catalog_id).cloned();
    Ok(json!({
        "catalog": catalog,
        "matrix": matrix,
    }))
}

pub async fn query_lc_report(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    Ok(ok(lc_report_data(app, params).await?))
}

pub async fn lc_report_list_data(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let catalog_id = required_usize(params, "catalog")?;
    let status = common::get_param(params, "status", "");
    let report_type = common::get_param(params, "type", "");
    let user = common::get_param(params, "user", "");
    let prop = common::get_param(params, "prop", "");
    let limit = opt_usize(params, "limit").unwrap_or(20).min(500);
    let offset = opt_usize(params, "offset").unwrap_or(0);

    let s = app.large_catalogs();
    let (catalogs_res, rows_res) = tokio::join!(
        s.get_catalogs_map(),
        s.get_report_list(catalog_id, &status, &report_type, &user, &prop, limit, offset),
    );
    let catalogs =
        catalogs_res.map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;
    let rows = rows_res.map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;

    Ok(json!({
        "catalog": catalogs.get(&catalog_id).cloned(),
        "rows": rows,
    }))
}

pub async fn query_lc_report_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    Ok(ok(lc_report_list_data(app, params).await?))
}

// ─── lc_rc ─────────────────────────────────────────────────────────────────

pub async fn lc_rc_data(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let limit = opt_usize(params, "limit").unwrap_or(50).min(500);
    let offset = opt_usize(params, "offset").unwrap_or(0);
    let users_only = common::get_param(params, "users", "") == "1";
    let rows = app
        .large_catalogs()
        .get_recent_changes(limit, offset, users_only)
        .await
        .map_err(|e| ApiError(format!("large catalogs DB error: {e}")))?;
    Ok(json!({"rows": rows}))
}

pub async fn query_lc_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    Ok(ok(lc_rc_data(app, params).await?))
}

// ─── lc_set_status ─────────────────────────────────────────────────────────

pub async fn lc_set_status_data(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let status = common::get_param(params, "status", "");
    if status.trim().is_empty() {
        return Err(ApiError("empty status".into()));
    }
    let id = required_usize(params, "id")?;
    let user = common::get_param(params, "user", "");
    if user.trim().is_empty() {
        return Err(ApiError("not logged in".into()));
    }
    app.large_catalogs()
        .set_report_status(id, &status, &user)
        .await
        .map_err(|e| ApiError(format!("update failed: {e}")))?;
    Ok(json!({}))
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
    let data = lc_set_status_data(app, &patched).await?;
    Ok(ok(data))
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn opt_usize(params: &Params, key: &str) -> Option<usize> {
    params
        .get(key)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

fn required_usize(params: &Params, key: &str) -> Result<usize, ApiError> {
    let raw = params
        .get(key)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError(format!("missing required parameter: {key}")))?;
    raw.parse::<usize>()
        .map_err(|_| ApiError(format!("parameter '{key}' must be a positive integer")))
}

/// Parse a strict `bbox=lat1,lon1,lat2,lon2` param. Errors if absent or malformed.
fn parse_bbox_required(params: &Params) -> Result<[f64; 4], ApiError> {
    let raw = params
        .get("bbox")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError("missing required parameter: bbox".into()))?;
    let parts: Vec<f64> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<f64>().ok())
        .collect();
    if parts.len() != 4 {
        return Err(ApiError("bbox must have 4 comma-separated numbers".into()));
    }
    Ok([parts[0], parts[1], parts[2], parts[3]])
}

/// Parse `bbox` lenient — strip any non-numeric junk first. Used by `lc_bbox`
/// which only echoes the parsed bbox back; bad input falls through as `[]`.
fn parse_bbox_lenient(params: &Params) -> Vec<f64> {
    common::get_param(params, "bbox", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-')
        .collect::<String>()
        .split(',')
        .filter_map(|s| s.parse().ok())
        .collect()
}
