//! Matching mutation endpoints. All write paths gate behind `auth::guard`.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use crate::entry::EntryWriter;
use axum::response::{IntoResponse, Response};
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_sessions::Session;

/// Concurrency cap for batch mutation handlers (`match_q_multi`, `suggest`).
/// Bounded by the writable connection pool's `max_connections=4` plus a small
/// safety margin so an in-flight request doesn't fully starve other writers.
const WRITE_FANOUT: usize = 3;

pub async fn query_match_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let q = common::get_param_int(params, "q", -1);
    let uid = common::require_user_id(app, session, params).await?;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    EntryWriter::new(app, &mut entry)
        .set_match(&format!("Q{q}"), uid)
        .await?;

    // After set_match, `entry` already holds the post-update fields, so we
    // don't need a second `from_id` round-trip just to read them back. The
    // Catalog::from_id lookup is independent — it can resolve in parallel
    // with `set_match` on a follow-up call, but we already awaited that.
    // Keep this single extra read so the response shape exactly matches PHP
    // (it includes any side-effects set during cleanup).
    let cat = crate::catalog::Catalog::from_id(entry.catalog, app).await?;
    let mut ej = serde_json::json!(entry);
    ej["entry_type"] = serde_json::json!(cat.type_name());
    Ok(json_resp(serde_json::json!({"status": "OK", "entry": ej})))
}

pub async fn query_match_q_multi(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let uid = common::require_user_id(app, session, params).await?;
    let data_str = common::get_param(params, "data", "[]");
    let data: Vec<serde_json::Value> = serde_json::from_str(&data_str).unwrap_or_default();

    // Sequentially-awaited `api_match_q_multi` calls used to dominate this
    // handler when the frontend sent a few hundred items. Run them with
    // bounded concurrency; results funnel through a single mutex so the
    // not_found list still reports the first 100 failures.
    let shared = Arc::new(Mutex::new((0_usize, Vec::<String>::new())));

    stream::iter(data)
        .for_each_concurrent(WRITE_FANOUT, |d| {
            let shared = shared.clone();
            async move {
                let arr = d.as_array();
                let q = arr
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as isize;
                let ext_id = arr
                    .and_then(|a| a.get(1))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                match app.storage().api_match_q_multi(catalog, ext_id, q, uid).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let mut g = shared.lock().await;
                        g.0 += 1;
                        if g.1.len() < 100 {
                            g.1.push(ext_id.to_string());
                        }
                    }
                    Err(e) => {
                        log::warn!("api_match_q_multi failed for ext_id={ext_id}: {e}");
                    }
                }
            }
        })
        .await;

    let (not_found, not_found_list) = {
        let g = shared.lock().await;
        (g.0, g.1.clone())
    };

    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "not_found": not_found,
        "not_found_list": not_found_list,
    })))
}

pub async fn query_remove_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    auth::guard::require_user_from_params(app, session, params).await?;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    EntryWriter::new(app, &mut entry).unmatch().await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_remove_all_q(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    if let Some(q) = entry.q {
        app.storage().api_remove_all_q(entry.catalog, q).await?;
    }
    Ok(ok(serde_json::json!({})))
}

pub async fn query_remove_all_multimatches(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    app.storage().api_remove_all_multimatches(eid).await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_suggest(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    auth::guard::require_user_from_params(app, session, params).await?;
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let overwrite = common::get_param_int(params, "overwrite", 0) != 0;
    let suggestions = common::get_param(params, "suggestions", "");

    // Pre-parse: keep bad-row diagnostics in source order (lines() iterator),
    // then dispatch the valid ones with bounded concurrency.
    let mut bad_lines: Vec<String> = Vec::new();
    let mut tasks: Vec<(String, isize)> = Vec::new();
    for line in suggestions.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 2 {
            bad_lines.push(line.to_string());
            continue;
        }
        let ext_id = parts[0].trim().to_string();
        let q: isize = parts[1]
            .replace(|c: char| !c.is_ascii_digit(), "")
            .parse()
            .unwrap_or(0);
        tasks.push((ext_id, q));
    }

    let cnt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    stream::iter(tasks)
        .for_each_concurrent(WRITE_FANOUT, |(ext_id, q)| {
            let cnt = cnt.clone();
            async move {
                if app
                    .storage()
                    .api_suggest(catalog, &ext_id, q, overwrite)
                    .await
                    .unwrap_or(false)
                {
                    cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        })
        .await;
    let cnt = cnt.load(std::sync::atomic::Ordering::Relaxed);

    let mut out = String::new();
    for line in &bad_lines {
        out.push_str(&format!("Bad row : {line}\n"));
    }
    out.push_str(&format!("{cnt} entries changed"));
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=UTF-8",
        )],
        out,
    )
        .into_response())
}
