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
    let s = app.storage();
    // Fetch overview + kv_catalog in parallel. kv_catalog is cheap and lets
    // the jobs page know which optional actions (e.g. automatch_sparql)
    // are available for this catalog without a second round-trip.
    let (data, kvs) = tokio::join!(
        s.api_get_single_catalog_overview(cid),
        s.get_catalog_key_value_pairs(cid),
    );
    let mut data = data?;
    if let Some(obj) = data.as_object_mut() {
        let kvs = kvs.unwrap_or_default();
        obj.insert(
            "kv_pairs".into(),
            serde_json::to_value(&kvs).unwrap_or_default(),
        );
    }
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
    let data = common::entries_with_extended_data(&entries, app).await?;
    // PHP places `total_filtered` alongside `status`/`data`, not inside `data`,
    // so return a manually-assembled envelope here.
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": data,
        "total_filtered": total_filtered,
    })))
}

/// Keys the catalog editor is allowed to write into `kv_catalog`. Anything
/// outside this list is silently ignored — prevents a compromised frontend
/// from injecting arbitrary config into the table.
const EDITABLE_KV_KEYS: &[&str] = &[
    "use_automatchers",
    "use_description_for_new",
    "automatch_sparql",
    "automatch_complex",
    "allow_location_operations",
    "allow_location_match",
    "location_allow_full_match",
    "allow_location_create",
    "location_force_same_type",
    "location_distance",
];

pub async fn query_edit_catalog(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data_str = common::get_param(params, "data", "");
    let data: serde_json::Value =
        serde_json::from_str(&data_str).map_err(|_| ApiError("Bad data".into()))?;
    // Catalog creators (catalog.owner) can now edit their own catalog, not
    // only site-wide admins — mirrors who can realistically maintain the
    // catalog once it's imported.
    auth::guard::require_catalog_admin_or_owner_from_params(app, session, params, cid).await?;
    let name = data
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or(ApiError("Bad data".into()))?;
    // `active` comes back from the frontend as either a bool or the raw u8
    // from the catalog row (0/1). Accept both rather than silently flipping
    // the row to inactive.
    let active = active_flag_from(data.get("active"));
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
            active,
        )
        .await?;
    // Apply kv_catalog writes. The frontend sends a flat `kv` object; empty
    // or whitespace-only strings, explicit nulls, "[]", and empty arrays all
    // mean "delete the key", anything else is upserted. Unknown keys are
    // dropped so an out-of-date frontend can't wedge configuration into
    // unsupported keys.
    if let Some(kv) = data.get("kv").and_then(|v| v.as_object()) {
        let s = app.storage();
        for (k, v) in kv {
            if !EDITABLE_KV_KEYS.contains(&k.as_str()) {
                continue;
            }
            if kv_value_means_delete(v) {
                s.delete_catalog_kv(cid, k).await?;
                log::info!("edit_catalog cat={cid}: deleted kv key '{k}'");
                continue;
            }
            // All kv values are stored as strings — coerce numbers/booleans
            // on the way in so callers don't have to JSON-stringify them.
            // Trim whitespace off string values so a user-entered SPARQL
            // that happens to have trailing spaces still round-trips cleanly.
            let stored = match v {
                serde_json::Value::String(s2) => s2.trim().to_string(),
                other => other.to_string(),
            };
            s.set_catalog_kv(cid, k, &stored).await?;
            log::info!(
                "edit_catalog cat={cid}: set kv '{k}' (len={})",
                stored.len()
            );
        }
    }
    // Refresh in the background — the materialised overview is not on the
    // critical response path, so don't make the user wait for the rebuild.
    let app_bg = app.clone();
    tokio::spawn(async move {
        let _ = app_bg.storage().catalog_refresh_overview_table(cid).await;
    });
    Ok(ok(serde_json::json!({})))
}

/// Coerce a JSON value coming from the catalog editor's `active` field
/// into the bool the storage layer expects. Accepts `true/false`, `0/1`,
/// and `"0"/"1"` — the editor round-trips the raw u8 from the catalog row,
/// which would otherwise silently deactivate the catalog on every save.
fn active_flag_from(v: Option<&serde_json::Value>) -> bool {
    match v {
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::Number(n)) => n.as_i64().map(|x| x != 0).unwrap_or(false),
        Some(serde_json::Value::String(s)) => !matches!(s.as_str(), "" | "0" | "false"),
        _ => false,
    }
}

/// A kv value "means delete" when it carries no information worth storing:
/// null, an empty/whitespace string, the literal JSON "[]", or an empty
/// JSON array. Anything else survives and is persisted via set_catalog_kv.
fn kv_value_means_delete(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => true,
        serde_json::Value::String(s) => s.trim().is_empty() || s.trim() == "[]",
        serde_json::Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod kv_tests {
    use super::kv_value_means_delete;
    use serde_json::json;

    #[test]
    fn deletes_on_null() {
        assert!(kv_value_means_delete(&serde_json::Value::Null));
    }

    #[test]
    fn deletes_on_empty_or_whitespace_string() {
        assert!(kv_value_means_delete(&json!("")));
        assert!(kv_value_means_delete(&json!("   ")));
        assert!(kv_value_means_delete(&json!("\n\t  ")));
    }

    #[test]
    fn deletes_on_empty_json_array_literals() {
        assert!(kv_value_means_delete(&json!("[]")));
        assert!(kv_value_means_delete(&json!("  []  ")));
        assert!(kv_value_means_delete(&json!([])));
    }

    #[test]
    fn keeps_real_values() {
        assert!(!kv_value_means_delete(&json!("SELECT ?q ?qLabel WHERE {}")));
        assert!(!kv_value_means_delete(&json!("[[5,10]]")));
        assert!(!kv_value_means_delete(&json!([[5, 10]])));
        assert!(!kv_value_means_delete(&json!("0")));
        assert!(!kv_value_means_delete(&json!("1")));
    }
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
    let uid = common::require_user_id(app, session, params).await?;
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
