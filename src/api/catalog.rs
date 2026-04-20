//! Catalog endpoints: overviews, single catalog, edit, lightweight catalog
//! lookups (batch / search / by-group), and the catalog "top group" admin
//! handlers (which read/write catalog grouping metadata, so they live here).

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_catalogs(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_catalog_overview().await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(id) = item
            .get("catalog")
            .and_then(|v| v.as_u64())
            .or_else(|| item.get("id").and_then(|v| v.as_u64()))
        {
            map.insert(id.to_string(), item);
        }
    }
    Ok(ok(serde_json::Value::Object(map)))
}

pub async fn query_single_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog_id", 0) as usize;
    let data = app.storage().api_get_single_catalog_overview(cid).await?;
    let mut map = serde_json::Map::new();
    map.insert(cid.to_string(), data);
    Ok(ok(serde_json::Value::Object(map)))
}

pub async fn query_catalog_details(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let s = app.storage();
    let (t, y, u) = tokio::join!(
        s.api_get_catalog_type_counts(cid),
        s.api_get_catalog_match_by_month(cid),
        s.api_get_catalog_matcher_by_user(cid),
    );
    Ok(ok(serde_json::json!({"type": t?, "ym": y?, "user": u?})))
}

pub async fn query_get_catalog_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_catalog_info(cid).await?;
    Ok(ok(serde_json::json!([data])))
}

pub async fn query_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let meta_str = common::get_param(params, "meta", "{}");
    let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or(serde_json::json!({}));
    let meta_flag = |k: &str| meta.get(k).and_then(|v| v.as_i64()).unwrap_or(0) == 1;
    let per_page = meta.get("per_page").and_then(|v| v.as_u64()).unwrap_or(50);
    let offset = meta.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
    let user_id_raw = common::get_param(params, "user_id", "");
    // Parse as signed so "0" (auto-matched) is distinguished from a missing param.
    let user_id = if user_id_raw.is_empty() {
        None
    } else {
        user_id_raw.parse::<i64>().ok().filter(|uid| *uid >= 0)
    };

    let filter = crate::storage::CatalogEntryListFilter {
        catalog_id: catalog,
        show_noq: meta_flag("show_noq"),
        show_autoq: meta_flag("show_autoq"),
        show_userq: meta_flag("show_userq"),
        show_na: meta_flag("show_na"),
        show_nowd: meta_flag("show_nowd"),
        show_multiple: meta_flag("show_multiple"),
        entry_type: common::get_param(params, "type", ""),
        title_match: common::get_param(params, "title_match", ""),
        keyword: common::get_param(params, "keyword", ""),
        user_id,
        per_page,
        offset,
    };

    let (entries, total_filtered) = app.storage().api_get_catalog_entries(&filter).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    // PHP places `total_filtered` alongside `status`/`data`, not inside `data`,
    // so return a manually-assembled envelope here.
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": data,
        "total_filtered": total_filtered,
    })))
}

pub async fn query_edit_catalog(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data_str = common::get_param(params, "data", "");
    let data: serde_json::Value =
        serde_json::from_str(&data_str).map_err(|_| ApiError("Bad data".into()))?;
    auth::guard::require_catalog_admin_from_params(app, session, params).await?;
    let name = data
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or(ApiError("Bad data".into()))?;
    app.storage()
        .api_edit_catalog(
            cid,
            name,
            data.get("url").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("desc").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("type").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("search_wp").and_then(|v| v.as_str()).unwrap_or(""),
            data.get("wd_prop")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize),
            data.get("wd_qual")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize),
            data.get("active")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        )
        .await?;
    // Refresh in the background — the materialised overview is not on the
    // critical response path, so don't make the user wait for the rebuild.
    let app_bg = app.clone();
    tokio::spawn(async move {
        let _ = app_bg.storage().catalog_refresh_overview_table(cid).await;
    });
    Ok(ok(serde_json::json!({})))
}

pub async fn query_catalog_overview(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs_str = common::get_param(params, "catalogs", "");
    let ids: Vec<usize> = catalogs_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let data = app.storage().api_get_catalog_overview_for_ids(&ids).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Newly ported lightweight catalog endpoints ─────────────────────────────

pub async fn query_batch_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let raw = common::get_param(params, "catalog_ids", "");
    let mut ids: Vec<usize> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|id| *id > 0)
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids.truncate(200);
    if ids.is_empty() {
        return Ok(ok(serde_json::json!({})));
    }
    let data = app.storage().api_get_catalog_overview_for_ids(&ids).await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(id) = item.get("id").and_then(|v| v.as_u64()) {
            map.insert(id.to_string(), item);
        }
    }
    Ok(ok(serde_json::Value::Object(map)))
}

pub async fn query_search_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let q = common::get_param(params, "q", "");
    let limit = (common::get_param_int(params, "limit", 20).clamp(1, 100)) as usize;
    if q.is_empty() {
        return Ok(ok(serde_json::json!([])));
    }
    let rows = app.storage().api_search_catalogs(&q, limit).await?;
    Ok(ok(serde_json::json!(rows)))
}

pub async fn query_catalog_type_counts(app: &AppState) -> Result<Response, ApiError> {
    let rows = app.storage().api_catalog_type_counts().await?;
    Ok(ok(serde_json::json!(rows)))
}

pub async fn query_latest_catalogs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let limit = (common::get_param_int(params, "limit", 9).clamp(1, 50)) as usize;
    let rows = app.storage().api_latest_catalogs(limit).await?;
    Ok(ok(serde_json::json!(rows)))
}

pub async fn query_catalogs_with_locations(app: &AppState) -> Result<Response, ApiError> {
    let rows = app.storage().api_catalogs_with_locations().await?;
    Ok(ok(serde_json::json!(rows)))
}

pub async fn query_catalog_property_groups(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_catalog_property_groups().await?;
    Ok(ok(data))
}

pub async fn query_check_wd_prop_usage(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let wd_prop = common::get_param_int(params, "wd_prop", 0);
    let exclude = common::get_param_int(params, "exclude_catalog", 0) as usize;
    if wd_prop <= 0 {
        return Ok(ok(serde_json::json!({"used": false})));
    }
    let result = app
        .storage()
        .api_check_wd_prop_usage(wd_prop as usize, exclude)
        .await?;
    Ok(ok(result))
}

pub async fn query_catalog_by_group(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let group = common::get_param(params, "group", "");
    if group.is_empty() {
        return Ok(ok(serde_json::json!({})));
    }
    let data = app.storage().api_catalog_by_group(&group).await?;
    Ok(ok(data))
}

// ─── Top-group admin endpoints ──────────────────────────────────────────────

pub async fn query_get_top_groups(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_top_groups().await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_set_top_group(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let name = common::get_param(params, "group_name", "");
    let catalogs = common::get_param(params, "catalogs", "");
    let based_on = common::get_param_int(params, "group_id", 0) as usize;
    app.storage()
        .api_set_top_group(&name, &catalogs, uid, based_on)
        .await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_remove_empty_top_group(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let gid = common::get_param_int(params, "group_id", 0) as usize;
    app.storage().api_remove_empty_top_group(gid).await?;
    Ok(ok(serde_json::json!({})))
}
