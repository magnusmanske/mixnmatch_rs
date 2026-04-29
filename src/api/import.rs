//! Frontend import wizard endpoints: discovery, preview, full import,
//! plus the scraper builder (`autoscrape_test`, `save_scraper`).

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::{AppContext, ExternalServicesContext};
use axum::response::Response;
use tower_sessions::Session;

fn parse_update_info(params: &Params) -> Result<serde_json::Value, ApiError> {
    let raw = common::get_param(params, "update_info", "");
    if raw.is_empty() {
        return Err(ApiError("missing 'update_info' parameter".into()));
    }
    serde_json::from_str(&raw)
        .map_err(|e| ApiError(format!("invalid update_info JSON: {e}")))
}

pub async fn query_get_source_headers(
    app: &dyn AppContext,
    params: &Params,
) -> Result<Response, ApiError> {
    let update_info = parse_update_info(params)?;
    let (headers, _preview) =
        crate::datasource::DataSource::read_headers_and_preview(app, &update_info, 0)
            .await
            .map_err(|e| ApiError(e.to_string()))?;
    Ok(ok(serde_json::json!(headers)))
}

pub async fn query_test_import_source(
    app: &dyn AppContext,
    params: &Params,
) -> Result<Response, ApiError> {
    let update_info = parse_update_info(params)?;
    let max_rows = update_info
        .get("read_max_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000) as usize;

    // Both calls are independent (each opens its own datasource read), so run
    // them concurrently — the count_rows pass scans the whole file, while
    // the preview only reads the first 10. Serial ran them back-to-back.
    let (preview_res, counts_res) = tokio::join!(
        crate::datasource::DataSource::read_headers_and_preview(app, &update_info, 10),
        crate::datasource::DataSource::count_rows(app, &update_info, max_rows),
    );
    let (headers, preview) = preview_res.map_err(|e| ApiError(e.to_string()))?;
    let (total, with_id, row_errors) = counts_res.map_err(|e| ApiError(e.to_string()))?;

    // Frontend reads counters from `data` (summary cards) and preview rows
    // from the top-level `rows` array (used by `load_preview_rows`).
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": {
            "rows_scanned": total,
            "rows_with_id": with_id,
            "errors": row_errors,
            "headers": headers,
        },
        "rows": preview,
    })))
}

pub async fn query_import_source(
    app: &dyn AppContext,
    _session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let update_info = parse_update_info(params)?;
    let meta_raw = common::get_param(params, "meta", "{}");
    let meta: serde_json::Value =
        serde_json::from_str(&meta_raw).unwrap_or(serde_json::Value::Null);
    let catalog_id = common::get_param_int(params, "catalog", 0) as usize;
    let _seconds = common::get_param_int(params, "seconds", 0) as u64;
    let username = common::get_param(params, "username", "").replace('_', " ");

    // Resolve the user id — required for anything that writes to the DB.
    let user_id = match app.storage().get_user_by_name(&username).await? {
        Some((id, _, _)) => id,
        None => return Err(ApiError(format!("unknown user '{username}'"))),
    };

    // Two modes:
    //  (a) file_uuid pointing at a json/jsonl upload → reuse import_catalog
    //  (b) CSV/TSV/SSV via source_url or file_uuid → reuse UpdateCatalog
    //
    // Mode (a) is the "JSON import" flow from the frontend; it requires a
    // catalog_id that already exists. Mode (b) is the tabbed-file flow.
    let uuid = update_info
        .get("file_uuid")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let data_format = update_info
        .get("data_format")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    if let Some(uuid) = &uuid {
        if data_format == "json" || data_format == "jsonl" {
            let cid = if catalog_id > 0 {
                catalog_id
            } else {
                meta.get("catalog_id")
                    .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                    .unwrap_or(0) as usize
            };
            if cid == 0 {
                return Err(ApiError(
                    "catalog_id required for JSON/JSONL import".into(),
                ));
            }
            let result = crate::import_catalog::import_from_import_file(
                app,
                cid,
                uuid,
                crate::import_catalog::ImportMode::AddReplace,
            )
            .await
            .map_err(|e| ApiError(e.to_string()))?;
            return Ok(ok(serde_json::json!({
                "catalog_id": cid,
                "created": result.created,
                "updated": result.updated,
                "skipped_fully_matched": result.skipped_fully_matched,
                "deleted": result.deleted,
                "errors": result.errors,
            })));
        }
    }

    // Tabbed-file path. For a "new" wizard, build the catalog row from
    // `meta` first; for "update", reuse the existing catalog_id. Then persist
    // the update_info and queue the importer as a job — the import itself
    // scans the whole file and can take minutes, so we don't want the HTTP
    // request to block on it.
    let catalog_id = if catalog_id > 0 {
        catalog_id
    } else {
        resolve_or_create_catalog(app, &meta, user_id).await?
    };

    let update_info_json = serde_json::to_string(&update_info)
        .map_err(|e| ApiError(format!("serialize update_info: {e}")))?;
    app.storage()
        .update_catalog_set_update_info(catalog_id, &update_info_json, user_id)
        .await
        .map_err(|e| ApiError(format!("persist update_info: {e}")))?;

    crate::job::Job::queue_simple_job(app, catalog_id, "update_from_tabbed_file", None)
        .await
        .map_err(|e| ApiError(format!("queue import job: {e}")))?;

    Ok(ok(serde_json::json!({
        "catalog_id": catalog_id,
        "queued": true,
    })))
}

/// Look up or create the target catalog from the wizard's `meta` block.
/// Used when the "new catalog" wizard submits without an existing id.
async fn resolve_or_create_catalog(
    app: &dyn ExternalServicesContext,
    meta: &serde_json::Value,
    user_id: usize,
) -> Result<usize, ApiError> {
    // `meta.catalog_id` can still be filled in by the wizard if it was set
    // (e.g. by a prior round-trip), so honour it before creating.
    let meta_cid = meta
        .get("catalog_id")
        .and_then(|v| {
            v.as_u64()
                .map(|n| n as usize)
                .or_else(|| v.as_str().and_then(|s| s.parse::<usize>().ok()))
        })
        .unwrap_or(0);
    if meta_cid > 0 {
        return Ok(meta_cid);
    }

    let name = meta
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if name.is_empty() {
        return Err(ApiError(
            "meta.name is required when creating a new catalog".into(),
        ));
    }
    let desc = meta.get("desc").and_then(|v| v.as_str()).unwrap_or("");
    let url = meta.get("url").and_then(|v| v.as_str()).unwrap_or("");
    let type_name = meta.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let wd_prop: Option<usize> = meta
        .get("property")
        .and_then(|v| v.as_str())
        .map(|s| s.trim_start_matches(['P', 'p']))
        .and_then(|s| s.parse::<usize>().ok());
    app.storage()
        .create_catalog_from_meta(name, desc, url, type_name, wd_prop, user_id)
        .await
        .map_err(|e| ApiError(format!("create catalog: {e}")))
}

pub async fn query_autoscrape_test(params: &Params) -> Result<Response, ApiError> {
    let json_str = common::get_param(params, "json", "");
    if json_str.is_empty() {
        return Err(ApiError("missing 'json' parameter".into()));
    }
    let json: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| ApiError(format!("invalid scraper JSON: {e}")))?;
    let res = crate::autoscrape::Autoscrape::test_fetch(&json)
        .await
        .map_err(|e| ApiError(e.to_string()))?;
    // `data.html` feeds the client-side regex preview, `data.url` is the
    // "URL fetched" link, `data.results` drives the entries table (and the
    // Save button gate), and `data.diagnostics` backs the scraper-test
    // diagnostics panel — there so the user can see *why* a test returned
    // zero rows (fetch failed, block regex matched but entry regex didn't,
    // unsupported regex feature, etc.).
    Ok(ok(serde_json::json!({
        "url":         res.url,
        "html":        res.html,
        "results":     res.results,
        "diagnostics": res.diagnostics,
    })))
}

pub async fn query_save_scraper(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    let scraper_str = common::get_param(params, "scraper", "");
    let options_str = common::get_param(params, "options", "{}");
    let levels_str = common::get_param(params, "levels", "[]");
    let meta_str = common::get_param(params, "meta", "{}");
    let username = common::get_param(params, "tusc_user", "").replace('_', " ");

    if scraper_str.is_empty() {
        return Err(ApiError("missing 'scraper' parameter".into()));
    }
    if username.is_empty() {
        return Err(ApiError("missing 'tusc_user' parameter".into()));
    }

    let scraper: serde_json::Value = serde_json::from_str(&scraper_str)
        .map_err(|e| ApiError(format!("invalid 'scraper' JSON: {e}")))?;
    let options: serde_json::Value = serde_json::from_str(&options_str)
        .map_err(|e| ApiError(format!("invalid 'options' JSON: {e}")))?;
    let levels: serde_json::Value = serde_json::from_str(&levels_str)
        .map_err(|e| ApiError(format!("invalid 'levels' JSON: {e}")))?;
    let meta: serde_json::Value = serde_json::from_str(&meta_str)
        .map_err(|e| ApiError(format!("invalid 'meta' JSON: {e}")))?;

    // Resolve user — fail loudly if the Widar-supplied username is unknown,
    // so the scraper doesn't get attributed to owner 0.
    let user_id = match app.storage().get_user_by_name(&username).await? {
        Some((id, _, _)) => id,
        None => return Err(ApiError(format!("unknown user '{username}'"))),
    };

    // Resolve target catalog_id. If the wizard left `meta.catalog_id` blank,
    // we create the catalog first so the autoscrape row has something to FK to.
    let meta_cid = meta
        .get("catalog_id")
        .and_then(|v| {
            v.as_u64()
                .map(|n| n as usize)
                .or_else(|| v.as_str().and_then(|s| s.parse::<usize>().ok()))
        })
        .unwrap_or(0);

    let catalog_id = if meta_cid > 0 {
        meta_cid
    } else {
        let name = meta
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        let desc = meta.get("desc").and_then(|v| v.as_str()).unwrap_or("");
        let url = meta.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let type_name = meta.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let wd_prop: Option<usize> = meta
            .get("property")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_start_matches(['P', 'p']))
            .and_then(|s| s.parse::<usize>().ok());
        if name.is_empty() {
            return Err(ApiError("meta.name is required when creating a new catalog".into()));
        }
        app.storage()
            .create_catalog_from_meta(name, desc, url, type_name, wd_prop, user_id)
            .await
            .map_err(|e| ApiError(format!("create catalog: {e}")))?
    };

    // Bundle scraper + options + levels into the single JSON column that
    // `Autoscrape::new` parses when a live scraper is later run.
    let combined = serde_json::json!({
        "scraper": scraper,
        "options": options,
        "levels": levels,
    });
    let combined_str = serde_json::to_string(&combined)
        .map_err(|e| ApiError(format!("serialize scraper JSON: {e}")))?;
    app.storage()
        .save_scraper(catalog_id, &combined_str, user_id)
        .await
        .map_err(|e| ApiError(format!("save scraper: {e}")))?;

    // Queue the matching autoscrape job so the runner picks the scraper
    // up. `jobs_queue_simple_job` upserts on (catalog, action), so
    // re-saving the scraper after an initial run won't create duplicates.
    // Scraper is already persisted at this point — if the job queue write
    // fails, log it but let the save succeed so the user can retry via
    // the jobs page.
    if let Err(e) = crate::job::Job::queue_simple_job(app, catalog_id, "autoscrape", None).await {
        log::warn!("catalog {catalog_id}: failed to queue autoscrape job after save: {e}");
    }

    Ok(ok(serde_json::json!({ "catalog": catalog_id })))
}

/// Dedicated loader for the scraper wizard. Given `catalog=<id>`, returns
/// the stored scraper/options/levels (parsed — not as a JSON string) plus
/// a `meta` block reconstructed from the catalog row so the wizard can
/// round-trip every visible form field.
///
/// Replaces the previous frontend approach of piggybacking on
/// `catalog_overview`, which returned a JSON array that the frontend then
/// tried to index by catalog id as if it were a map.
pub async fn query_get_scraper(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = common::get_param_int(params, "catalog", 0);
    if catalog_id <= 0 {
        return Err(ApiError("missing or invalid 'catalog' parameter".into()));
    }
    let catalog_id = catalog_id as usize;

    // Autoscrape row first — if missing, return 404-ish so the frontend
    // can show a helpful "no settings yet" message rather than a stack
    // trace.
    let rows = app
        .storage()
        .autoscrape_get_for_catalog(catalog_id)
        .await
        .map_err(|e| ApiError(format!("autoscrape lookup: {e}")))?;
    let (_autoscrape_id, raw) = match rows.into_iter().next() {
        Some(r) => r,
        None => {
            return Ok(ok(serde_json::json!({
                "found": false,
                "catalog": catalog_id,
            })));
        }
    };
    let stored: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| ApiError(format!("stored autoscrape JSON is malformed: {e}")))?;

    // Rebuild the `meta` block from the catalog row so the wizard form
    // fills every field it displays. `meta` is intentionally NOT part of
    // the stored autoscrape JSON (it lives on the catalog row itself),
    // so we read it fresh here — anything the user edited via the catalog
    // editor in the meantime shows up.
    let catalog = crate::catalog::Catalog::from_id(catalog_id, app)
        .await
        .map_err(|e| ApiError(format!("catalog lookup: {e}")))?;
    let meta = serde_json::json!({
        "catalog_id": catalog_id,
        "name": catalog.name().cloned().unwrap_or_default(),
        "desc": catalog.desc(),
        "url": catalog.url().cloned().unwrap_or_default(),
        "property": catalog
            .wd_prop()
            .map(|p| format!("P{p}"))
            .unwrap_or_default(),
        "lang": catalog.search_wp(),
        "type": catalog.type_name(),
    });

    // Keep scraper/options/levels exactly as they were stored so the
    // "load, then save unedited" round-trip is a no-op. `options` and
    // `levels` default to sensible empties if missing; `scraper` is
    // required (a row without one is effectively corrupted).
    let scraper = stored
        .get("scraper")
        .cloned()
        .ok_or_else(|| ApiError("stored autoscrape row has no `scraper` block".into()))?;
    let options = stored
        .get("options")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let levels = stored
        .get("levels")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));

    Ok(ok(serde_json::json!({
        "found": true,
        "catalog": catalog_id,
        "scraper": scraper,
        "options": options,
        "levels": levels,
        "meta": meta,
    })))
}
