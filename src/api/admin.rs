//! Admin / config endpoints. All write paths gate behind `auth::guard`.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use futures::stream::{self, StreamExt};
use tower_sessions::Session;

/// Concurrency cap for the catalog overview-table refresh fan-out. Bounded by
/// the writable connection pool's `max_connections=4`.
const CATALOG_REFRESH_FANOUT: usize = 4;

pub async fn query_update_overview(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cs = common::get_param(params, "catalog", "");
    let ids: Vec<usize> = if cs.is_empty() {
        // Bulk refresh fans out across every active catalog and is
        // expensive, so keep it gated. A single-catalog refresh just
        // recomputes that catalog's overview row from authoritative
        // entry data — idempotent, no user attribution — so it stays
        // open to anonymous viewers (the catalog page auto-fires it
        // when it sees a stale negative count).
        auth::guard::require_user_from_params(app, session, params).await?;
        app.storage().api_get_active_catalog_ids().await?
    } else {
        cs.split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect()
    };
    // Refresh catalogs in parallel, bounded by the writable pool size so a
    // huge `catalog=""` request doesn't overwhelm MySQL.
    stream::iter(ids)
        .for_each_concurrent(CATALOG_REFRESH_FANOUT, |id| async move {
            let _ = app.storage().catalog_refresh_overview_table(id).await;
        })
        .await;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_update_ext_urls(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_catalog_admin_from_params(app, session, params).await?;
    let cid = common::get_catalog(params)?;
    let url = common::get_param(params, "url", "");
    let parts: Vec<&str> = url.split("$1").collect();
    if parts.len() != 2 {
        return Err(ApiError(format!("Bad $1 replacement for '{url}'")));
    }
    app.storage()
        .api_update_catalog_ext_urls(cid, parts[0], parts[1])
        .await?;
    Ok(ok(serde_json::json!({
        "catalog": cid,
        "prefix": parts[0],
        "suffix": parts[1],
    })))
}

pub async fn query_add_aliases(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let text = common::get_param(params, "text", "").trim().to_string();
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    if cid == 0 || text.is_empty() {
        return Err(ApiError("Catalog ID or text missing".into()));
    }
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let default_lang = {
        let wp = cat.search_wp();
        if wp.is_empty() {
            "en".to_string()
        } else {
            wp.to_string()
        }
    };
    for row in text.lines() {
        let parts: Vec<&str> = row.trim().split('\t').collect();
        if parts.len() < 2 || parts.len() > 3 {
            continue;
        }
        let ext_id = parts[0].trim();
        let label = parts[1].trim().replace('|', "");
        let lang = if parts.len() == 3 && !parts[2].trim().is_empty() {
            parts[2].trim().to_lowercase()
        } else {
            default_lang.clone()
        };
        let _ = app
            .storage()
            .api_add_alias(cid, ext_id, &lang, &label, uid)
            .await;
    }
    Ok(ok(serde_json::json!({})))
}

pub async fn query_get_missing_properties(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_missing_properties_raw().await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_set_missing_properties_status(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    let row_id = common::get_param_int(params, "row_id", 0) as usize;
    if row_id == 0 {
        return Err(ApiError("Bad/missing row ID".into()));
    }
    let status = common::get_param(params, "status", "");
    if status.is_empty() {
        return Err(ApiError("Invalid status".into()));
    }
    let note = common::get_param(params, "note", "");
    app.storage()
        .api_set_missing_properties_status(row_id, &status, &note, uid)
        .await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_quick_compare_list(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_quick_compare_list().await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_get_flickr_key(app: &AppState) -> Result<Response, ApiError> {
    // Read off the reactor — local fs reads are usually fast, but on the
    // tool host this file lives on NFS, where occasional latency spikes can
    // stall the runtime if read inline. Path is supplied via
    // `flickr_key_path` in config.json; absent config means the feature
    // is unconfigured and we return an empty string.
    let path = app.flickr_key_path().to_string();
    if path.is_empty() {
        return Ok(ok(serde_json::json!("")));
    }
    let key = tokio::task::spawn_blocking(move || {
        std::fs::read_to_string(&path).unwrap_or_default()
    })
    .await
    .unwrap_or_default();
    Ok(ok(serde_json::json!(key)))
}
