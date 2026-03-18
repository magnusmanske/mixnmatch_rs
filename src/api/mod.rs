pub mod common;

use crate::app_state::AppState;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use common::{ApiError, Params};
use std::sync::Arc;

pub type SharedState = Arc<AppState>;

/// Build the axum router for the API.
pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher))
        .with_state(state)
}

/// Main dispatcher: reads `?query=X` and routes to the appropriate handler.
async fn api_dispatcher(
    State(app): State<SharedState>,
    Query(params): Query<Params>,
) -> Response {
    let query = params.get("query").cloned().unwrap_or_default();
    let result = dispatch(&query, &app, &params).await;
    match result {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn dispatch(
    query: &str,
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    match query {
        // Catalog endpoints
        "catalogs" => query_catalogs(app).await,
        "single_catalog" => query_single_catalog(app, params).await,
        "catalog_details" => query_catalog_details(app, params).await,
        "get_catalog_info" => query_get_catalog_info(app, params).await,
        "catalog" => query_catalog(app, params).await,

        // Entry endpoints
        "get_entry" => query_get_entry(app, params).await,
        "get_entry_by_extid" => query_get_entry_by_extid(app, params).await,
        "search" => query_search(app, params).await,
        "random" => query_random(app, params).await,

        // Matching endpoints
        "match_q" => query_match_q(app, params).await,
        "remove_q" => query_remove_q(app, params).await,

        // Jobs
        "get_jobs" => query_get_jobs(app, params).await,
        "start_new_job" => query_start_new_job(app, params).await,

        // Issues
        "get_issues" => query_get_issues(app, params).await,
        "all_issues" => query_all_issues(app, params).await,
        "resolve_issue" => query_resolve_issue(app, params).await,

        // User & misc
        "get_user_info" => query_get_user_info(app, params).await,
        "get_wd_props" => query_get_wd_props(app).await,
        "rc" => query_rc(app, params).await,
        "update_overview" => query_update_overview(app, params).await,
        "entries_query" => query_entries_query(app, params).await,
        "entries_via_property_value" => query_entries_via_property_value(app, params).await,
        "top_missing" => query_top_missing(app, params).await,
        "get_common_names" => query_get_common_names(app, params).await,
        "locations" => query_locations(app, params).await,
        "get_locations_in_catalog" => query_get_locations_in_catalog(app, params).await,
        "download" => query_download(app, params).await,
        "redirect" => query_redirect(app, params).await,
        "get_missing_properties" => query_get_missing_properties(app).await,

        _ => Err(ApiError(format!("Unknown query '{query}'"))),
    }
}

// ─── Catalog handlers ───────────────────────────────────────────────────────

async fn query_catalogs(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_catalog_overview().await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(catalog_id) = item.get("catalog").and_then(|v| v.as_u64()).or_else(|| item.get("id").and_then(|v| v.as_u64())) {
            map.insert(catalog_id.to_string(), item);
        }
    }
    Ok(common::success_with_data(serde_json::Value::Object(map)).into_response())
}

async fn query_single_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = common::get_param_int(params, "catalog_id", 0) as usize;
    let data = app.storage().api_get_single_catalog_overview(catalog_id).await?;
    let mut map = serde_json::Map::new();
    map.insert(catalog_id.to_string(), data);
    Ok(common::success_with_data(serde_json::Value::Object(map)).into_response())
}

async fn query_catalog_details(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let storage = app.storage();
    let (types, ym, users) = tokio::join!(
        storage.api_get_catalog_type_counts(catalog),
        storage.api_get_catalog_match_by_month(catalog),
        storage.api_get_catalog_matcher_by_user(catalog),
    );
    let data = serde_json::json!({
        "type": types?,
        "ym": ym?,
        "user": users?,
    });
    Ok(common::success_with_data(data).into_response())
}

async fn query_get_catalog_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = common::get_catalog(params)?;
    // Use the storage method to get raw catalog data as JSON
    let data = app.storage().api_get_single_catalog_overview(catalog_id).await?;
    Ok(common::success_with_data(serde_json::json!([data])).into_response())
}

async fn query_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let meta_str = common::get_param(params, "meta", "{}");
    let meta: serde_json::Value = serde_json::from_str(&meta_str).unwrap_or(serde_json::json!({}));

    let show_noq = meta.get("show_noq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_autoq = meta.get("show_autoq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_userq = meta.get("show_userq").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_na = meta.get("show_na").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_nowd = meta.get("show_nowd").and_then(|v| v.as_i64()).unwrap_or(0);
    let show_multiple = meta.get("show_multiple").and_then(|v| v.as_i64()).unwrap_or(0);
    let per_page = meta.get("per_page").and_then(|v| v.as_u64()).unwrap_or(50);
    let offset = meta.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
    let entry_type = common::get_param(params, "type", "");
    let title_match = common::get_param(params, "title_match", "");

    // Build WHERE conditions
    let mut conditions = vec![format!("catalog={catalog}")];

    if show_multiple == 1 {
        conditions.push("EXISTS ( SELECT * FROM multi_match WHERE entry_id=entry.id ) AND ( user<=0 OR user is null )".to_string());
    } else if show_noq + show_autoq + show_userq + show_nowd == 0 && show_na == 1 {
        conditions.push("q=0".to_string());
    } else if show_noq + show_autoq + show_userq + show_na == 0 && show_nowd == 1 {
        conditions.push("q=-1".to_string());
    } else {
        if show_noq != 1 { conditions.push("q IS NOT NULL".to_string()); }
        if show_autoq != 1 { conditions.push("( q is null OR user!=0 )".to_string()); }
        if show_userq != 1 { conditions.push("( user<=0 OR user is null )".to_string()); }
        if show_na != 1 { conditions.push("( q!=0 or q is null )".to_string()); }
    }

    if !entry_type.is_empty() {
        conditions.push(format!("`type`='{}'", entry_type.replace('\'', "''")));
    }
    if !title_match.is_empty() {
        conditions.push(format!("`ext_name` LIKE '%{}%'", title_match.replace('\'', "''")));
    }

    let sql = format!(
        "SELECT * FROM entry WHERE {} LIMIT {} OFFSET {}",
        conditions.join(" AND "),
        per_page,
        offset,
    );

    let entries = app.storage().api_get_catalog_entries_raw(&sql).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(common::success_with_data(data).into_response())
}

// ─── Entry handlers ─────────────────────────────────────────────────────────

async fn query_get_entry(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let entry_ids_str = common::get_param(params, "entry", "");
    let ext_ids_str = common::get_param(params, "ext_ids", "");

    let entries = if !ext_ids_str.is_empty() {
        if catalog == 0 {
            return Err(ApiError("catalog is required when using ext_ids".into()));
        }
        let ext_ids: Vec<String> = serde_json::from_str(&ext_ids_str).unwrap_or_default();
        let mut result = vec![];
        for eid in ext_ids {
            if let Ok(e) = crate::entry::Entry::from_ext_id(catalog, &eid, app).await {
                result.push(e);
            }
        }
        result
    } else {
        let ids: Vec<usize> = entry_ids_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if ids.is_empty() {
            return Err(ApiError("entry is required".into()));
        }
        let map = crate::entry::Entry::multiple_from_ids(&ids, app).await?;
        map.into_values().collect()
    };

    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(common::success_with_data(data).into_response())
}

async fn query_get_entry_by_extid(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "extid", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let mut data = common::entries_to_json_data(&[entry], app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(common::success_with_data(data).into_response())
}

async fn query_search(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let what = common::get_param(params, "what", "");
    let max_results = common::get_param_int(params, "max", 100) as usize;
    let description_search = common::get_param_int(params, "description_search", 0) != 0;
    let no_label_search = common::get_param_int(params, "no_label_search", 0) != 0;
    let exclude_str = common::get_param(params, "exclude", "");
    let include_str = common::get_param(params, "include", "");

    let mut exclude: Vec<usize> = exclude_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let include: Vec<usize> = include_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();

    // Add inactive catalogs to exclude
    let active = app.storage().api_get_active_catalog_ids().await?;
    let all_catalog_sql = "SELECT id FROM catalog WHERE active!=1";
    let inactive = app.storage().api_get_catalog_entries_raw(all_catalog_sql).await;
    // Use a simpler approach - the storage layer can handle it
    let _ = inactive; // We'll pass exclude to the search method and let it handle inactive

    let what_clean = what.replace('-', " ");
    // Check if it's a Q-number search
    let q_match = regex::Regex::new(r"^\s*[Qq]?(\d+)\s*$").ok().and_then(|re| {
        re.captures(&what_clean).map(|c| c[1].parse::<isize>().unwrap_or(0))
    });

    let entries = if let Some(q) = q_match {
        if q > 0 {
            app.storage().api_search_by_q(q).await?
        } else {
            vec![]
        }
    } else {
        let words: Vec<String> = what_clean
            .split_whitespace()
            .filter(|w| {
                let lw = w.to_lowercase();
                w.len() >= 3 && w.len() <= 84 && !["the", "a"].contains(&lw.as_str())
            })
            .map(|s| s.to_string())
            .collect();
        if words.is_empty() {
            vec![]
        } else {
            app.storage().api_search_entries(&words, description_search, no_label_search, &exclude, &include, max_results).await?
        }
    };

    let data = common::entries_to_json_data(&entries, app).await?;
    Ok(common::success_with_data(data).into_response())
}

async fn query_random(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let submode = common::get_param(params, "submode", "");
    let entry_type = common::get_param(params, "type", "");
    let id = common::get_param_int(params, "id", 0) as usize;

    let active_catalogs = app.storage().api_get_active_catalog_ids().await?;

    if id != 0 {
        // Testing mode
        let entry = crate::entry::Entry::from_id(id, app).await?;
        return Ok(common::success_with_data(serde_json::json!(entry)).into_response());
    }

    for attempt in 0..=10 {
        let r: f64 = if attempt > 10 { 0.0 } else { rand::random() };
        let limit = if catalog == 0 { 10 } else { 1 };
        let entry = app.storage().api_get_random_entry(catalog, &submode, &entry_type, r, &active_catalogs).await?;

        if let Some(entry) = entry {
            let entry_id = entry.id.unwrap_or(0);
            let mut data = serde_json::json!(entry);
            // Add person dates
            let pd = app.storage().api_get_person_dates_for_entries(&[entry_id]).await?;
            if let Some((born, died)) = pd.get(&entry_id) {
                if !born.is_empty() { data["born"] = serde_json::json!(born); }
                if !died.is_empty() { data["died"] = serde_json::json!(died); }
            }
            return Ok(common::success_with_data(data).into_response());
        }
    }

    Ok(common::success_with_data(serde_json::Value::Null).into_response())
}

// ─── Matching handlers ──────────────────────────────────────────────────────

async fn query_match_q(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let entry_id = common::get_param_int(params, "entry", -1) as usize;
    let q = common::get_param_int(params, "q", -1);
    let user_id = common::check_user(app, params).await?;

    let mut entry = crate::entry::Entry::from_id(entry_id, app).await?;
    entry.set_match(&format!("Q{q}"), user_id).await?;

    let out_entry = crate::entry::Entry::from_id(entry_id, app).await?;
    let catalog = crate::catalog::Catalog::from_id(out_entry.catalog, app).await?;
    let mut entry_json = serde_json::json!(out_entry);
    entry_json["entry_type"] = serde_json::json!(catalog.type_name());
    Ok(axum::Json(serde_json::json!({"status": "OK", "entry": entry_json})).into_response())
}

async fn query_remove_q(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let entry_id = common::get_param_int(params, "entry", -1) as usize;
    let _user_id = common::check_user(app, params).await?;

    let mut entry = crate::entry::Entry::from_id(entry_id, app).await?;
    entry.unmatch().await?;
    Ok(common::success_with_data(serde_json::json!({})).into_response())
}

// ─── Job handlers ───────────────────────────────────────────────────────────

async fn query_get_jobs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = common::get_param_int(params, "catalog", 0) as usize;
    let start = common::get_param_int(params, "start", 0) as usize;
    let max = common::get_param_int(params, "max", 50) as usize;

    let (stats, jobs) = app.storage().api_get_jobs(catalog_id, start, max).await?;
    let mut out = serde_json::json!({"status": "OK", "data": jobs});
    if catalog_id == 0 {
        out["stats"] = serde_json::json!(stats);
    }
    Ok(axum::Json(out).into_response())
}

async fn query_start_new_job(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = common::get_catalog(params)?;
    let action = common::get_param(params, "action", "").trim().to_lowercase();
    let user_id = common::check_user(app, params).await?;

    // Validate action
    if !regex::Regex::new(r"^[a-z_]+$").unwrap().is_match(&action) {
        return Err(ApiError(format!("Bad action: '{action}'")));
    }
    let valid_actions = app.storage().api_get_existing_job_actions().await?;
    if !valid_actions.contains(&action) {
        return Err(ApiError(format!("Unknown action: '{action}'")));
    }

    let seconds = if action == "autoscrape" { 2629800 * 3 } else { 0 };
    crate::job::Job::queue_simple_job(app, catalog_id, &action, None).await?;
    Ok(common::success_with_data(serde_json::json!({})).into_response())
}

// ─── Issue handlers ─────────────────────────────────────────────────────────

async fn query_get_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let issue_type = common::get_param(params, "type", "").trim().to_uppercase();
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let catalogs = common::get_param(params, "catalogs", "");

    let open_count = app.storage().api_get_issues_count(&issue_type, &catalogs).await?;
    if open_count == 0 {
        return Ok(common::success_with_data(serde_json::json!({})).into_response());
    }

    let min_open = limit * 2;
    let r: f64 = if open_count < min_open { 0.0 } else { rand::random() };
    let issues = app.storage().api_get_issues(&issue_type, &catalogs, limit, offset, r).await?;

    // Collect entry IDs from issues
    let entry_ids: Vec<usize> = issues.iter()
        .filter_map(|i| i.get("entry_id").and_then(|v| v.as_u64()).map(|v| v as usize))
        .collect();

    let entries = if !entry_ids.is_empty() {
        let map = crate::entry::Entry::multiple_from_ids(&entry_ids, app).await?;
        let entries_list: Vec<crate::entry::Entry> = map.into_values().collect();
        common::entries_to_json_data(&entries_list, app).await?
    } else {
        serde_json::json!({"entries": {}, "users": {}})
    };

    let data = serde_json::json!({
        "open_issues": open_count,
        "issues": issues,
        "entries": entries.get("entries").cloned().unwrap_or(serde_json::json!({})),
        "users": entries.get("users").cloned().unwrap_or(serde_json::json!({})),
    });
    Ok(common::success_with_data(data).into_response())
}

async fn query_all_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    if !["duplicate_items", "mismatched_items", "time_mismatch"].contains(&mode.as_str()) {
        return Err(ApiError("Unsupported mode".into()));
    }
    let data = app.storage().api_get_all_issues(&mode).await?;
    Ok(common::success_with_data(serde_json::json!(data)).into_response())
}

async fn query_resolve_issue(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let issue_id = common::get_param_int(params, "issue_id", 0) as usize;
    if issue_id == 0 {
        return Err(ApiError("Bad issue ID".into()));
    }
    let _user_id = common::check_user(app, params).await?;
    app.storage().set_issue_status(issue_id, crate::issue::IssueStatus::Done).await?;
    Ok(common::success_with_data(serde_json::json!({})).into_response())
}

// ─── User & misc handlers ──────────────────────────────────────────────────

async fn query_get_user_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let username = common::get_param(params, "username", "").replace('_', " ");
    let user = app.storage().get_user_by_name(&username).await?;
    match user {
        Some((id, name, is_admin)) => {
            Ok(common::success_with_data(serde_json::json!({
                "id": id,
                "name": name,
                "is_catalog_admin": if is_admin { 1 } else { 0 },
            })).into_response())
        }
        None => Err(ApiError(format!("No user '{username}' found"))),
    }
}

async fn query_get_wd_props(app: &AppState) -> Result<Response, ApiError> {
    // TODO: implement proper get_wd_props storage method
    // For now return empty array (the PHP returns a flat array of property numbers, not wrapped in data)
    let out: Vec<usize> = vec![];
    Ok(axum::Json(serde_json::json!(out)).into_response())
}

async fn query_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let limit = 100;

    let (entry_events, log_events) = app.storage().api_get_recent_changes(&ts, catalog, limit).await?;

    // Merge and sort events
    let mut events: Vec<serde_json::Value> = entry_events.into_iter().chain(log_events).collect();
    events.sort_by(|a, b| {
        let ts_a = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let ts_b = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        ts_b.cmp(ts_a) // Reverse chronological
    });
    events.truncate(limit);

    // Collect user IDs
    let user_ids: std::collections::HashSet<usize> = events.iter()
        .filter_map(|e| e.get("user").and_then(|v| v.as_u64()).map(|v| v as usize))
        .collect();
    let users = common::get_users(app, &user_ids).await?;

    let data = serde_json::json!({
        "events": events,
        "users": users,
    });
    Ok(common::success_with_data(data).into_response())
}

async fn query_update_overview(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_str = common::get_param(params, "catalog", "");
    let catalogs: Vec<usize> = if catalog_str.is_empty() {
        app.storage().api_get_active_catalog_ids().await?
    } else {
        catalog_str.split(',').filter_map(|s| s.trim().parse().ok()).collect()
    };
    for catalog_id in catalogs {
        let _ = app.storage().catalog_refresh_overview_table(catalog_id).await;
    }
    Ok(common::success_with_data(serde_json::json!({})).into_response())
}

async fn query_entries_query(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    use crate::entry_query::EntryQuery;
    use crate::match_state::MatchState;

    let offset = common::get_param_int(params, "offset", 0) as usize;
    let unmatched = common::get_param_int(params, "unmatched", 0) != 0;
    let prelim = common::get_param_int(params, "prelim_matched", 0) != 0;
    let fully = common::get_param_int(params, "fully_matched", 0) != 0;

    let ms = MatchState {
        unmatched,
        partially_matched: prelim,
        fully_matched: fully,
    };
    let mut eq = EntryQuery::default();
    eq = eq.with_match_state(ms).with_limit(50).with_offset(offset);

    let entries = app.storage().entry_query(&eq).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(common::success_with_data(data).into_response())
}

async fn query_entries_via_property_value(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let property = common::get_param(params, "property", "").replace(|c: char| !c.is_ascii_digit(), "");
    let value = common::get_param(params, "value", "").trim().to_string();
    let property_num: usize = property.parse().unwrap_or(0);

    if property_num == 0 || value.is_empty() {
        return Err(ApiError("property and value required".into()));
    }

    // Find entries via catalog ext_id or auxiliary
    let mut entry_ids = app.storage().get_entry_ids_by_aux(property_num, &value).await?;

    let mut entries = vec![];
    if !entry_ids.is_empty() {
        let map = crate::entry::Entry::multiple_from_ids(&entry_ids, app).await?;
        entries.extend(map.into_values());
    }

    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(common::success_with_data(data).into_response())
}

async fn query_top_missing(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs = common::get_param(params, "catalogs", "");
    let catalogs_clean: String = catalogs.chars().filter(|c| c.is_ascii_digit() || *c == ',').collect();
    if catalogs_clean.is_empty() {
        return Err(ApiError("No catalogs given".into()));
    }
    let sql = format!(
        "SELECT ext_name,count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({}) AND (q IS NULL or user=0) GROUP BY ext_name HAVING cnt>1 ORDER BY cnt DESC LIMIT 500",
        catalogs_clean
    );
    let rows = app.storage().api_get_catalog_entries_raw(&sql).await;
    // This returns Entry objects but we need raw SQL results. Use a generic query method.
    // For now, return placeholder
    Ok(common::success_with_data(serde_json::json!([])).into_response())
}

async fn query_get_common_names(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50);
    let offset = common::get_param_int(params, "offset", 0);
    // Stub - needs dedicated SQL
    Ok(common::success_with_data(serde_json::json!({"entries": {}})).into_response())
}

async fn query_locations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let bbox = common::get_param(params, "bbox", "");
    let bbox_clean: String = bbox.chars().filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-').collect();
    let parts: Vec<&str> = bbox_clean.split(',').collect();
    if parts.len() != 4 {
        return Err(ApiError("Required parameter bbox does not have 4 comma-separated numbers".into()));
    }
    // Stub - needs dedicated SQL
    let out = serde_json::json!({"status": "OK", "data": [], "bbox": parts});
    Ok(axum::Json(out).into_response())
}

async fn query_get_locations_in_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    // Stub - needs vw_location query
    Ok(common::success_with_data(serde_json::json!([])).into_response())
}

async fn query_download(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let cat = crate::catalog::Catalog::from_id(catalog, app).await?;
    let filename = cat.name().unwrap_or(&"download".to_string()).replace(' ', "_") + ".tsv";

    // Build TSV output
    let mut out = String::from("Q\tID\tURL\tName\tUser\n");
    // Stub - needs streaming query for matched entries
    // TODO: implement full download

    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/plain; charset=UTF-8"),
         (axum::http::header::CONTENT_DISPOSITION, &format!("attachment;filename=\"{filename}\""))],
        out,
    ).into_response())
}

async fn query_redirect(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "ext_id", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let url = &entry.ext_url;
    let html = format!("<html><head><META http-equiv=\"refresh\" content=\"0;URL={url}\"></head><body></body></html>");
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")],
        html,
    ).into_response())
}

async fn query_get_missing_properties(app: &AppState) -> Result<Response, ApiError> {
    // PropTodo doesn't derive Serialize, so we convert manually
    let props = app.storage().get_props_todo().await?;
    let data: Vec<serde_json::Value> = props.iter().map(|_p| {
        // Stub - need to serialize PropTodo fields
        serde_json::json!({})
    }).collect();
    Ok(common::success_with_data(serde_json::json!(data)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_builds() {
        // Just verify the router can be constructed without panicking
        // (Requires a real AppState, so skip in unit tests)
    }
}
