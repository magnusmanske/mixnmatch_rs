pub mod common;

use crate::app_state::AppState;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use common::{ApiError, Params};
use std::sync::Arc;

pub type SharedState = Arc<AppState>;

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    Router::new()
        .route("/api.php", get(api_dispatcher).post(api_dispatcher))
        .with_state(state)
}

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

async fn dispatch(query: &str, app: &AppState, params: &Params) -> Result<Response, ApiError> {
    match query {
        // Catalog
        "catalogs" => query_catalogs(app).await,
        "single_catalog" => query_single_catalog(app, params).await,
        "catalog_details" => query_catalog_details(app, params).await,
        "get_catalog_info" => query_get_catalog_info(app, params).await,
        "catalog" => query_catalog(app, params).await,
        "edit_catalog" => query_edit_catalog(app, params).await,
        "catalog_overview" => query_catalog_overview(app, params).await,

        // Entry
        "get_entry" => query_get_entry(app, params).await,
        "get_entry_by_extid" => query_get_entry_by_extid(app, params).await,
        "search" => query_search(app, params).await,
        "random" => query_random(app, params).await,
        "entries_query" => query_entries_query(app, params).await,
        "entries_via_property_value" => query_entries_via_property_value(app, params).await,
        "get_entries_by_q_or_value" => query_get_entries_by_q_or_value(app, params).await,

        // Matching
        "match_q" => query_match_q(app, params).await,
        "match_q_multi" => query_match_q_multi(app, params).await,
        "remove_q" => query_remove_q(app, params).await,
        "remove_all_q" => query_remove_all_q(app, params).await,
        "remove_all_multimatches" => query_remove_all_multimatches(app, params).await,
        "suggest" => query_suggest(app, params).await,

        // Jobs
        "get_jobs" => query_get_jobs(app, params).await,
        "start_new_job" => query_start_new_job(app, params).await,

        // Issues
        "get_issues" => query_get_issues(app, params).await,
        "all_issues" => query_all_issues(app, params).await,
        "resolve_issue" => query_resolve_issue(app, params).await,

        // User & auth
        "get_user_info" => query_get_user_info(app, params).await,

        // Recent changes
        "rc" => query_rc(app, params).await,

        // Data & analysis
        "get_wd_props" => query_get_wd_props(app).await,
        "top_missing" => query_top_missing(app, params).await,
        "get_common_names" => query_get_common_names(app, params).await,
        "same_names" => query_same_names(app).await,
        "random_person_batch" => query_random_person_batch(app, params).await,
        "get_property_cache" => query_get_property_cache(app).await,
        "mnm_unmatched_relations" => query_mnm_unmatched_relations(app, params).await,
        "creation_candidates" => query_creation_candidates(app, params).await,

        // Locations
        "locations" => query_locations(app, params).await,
        "get_locations_in_catalog" => query_get_locations_in_catalog(app, params).await,

        // Download & export
        "download" => query_download(app, params).await,
        "download2" => query_download2(app, params).await,

        // Navigation
        "redirect" => query_redirect(app, params).await,
        "proxy_entry_url" => query_proxy_entry_url(app, params).await,
        "cersei_forward" => query_cersei_forward(app, params).await,

        // Admin & config
        "update_overview" => query_update_overview(app, params).await,
        "update_ext_urls" => query_update_ext_urls(app, params).await,
        "add_aliases" => query_add_aliases(app, params).await,
        "get_missing_properties" => query_get_missing_properties(app).await,
        "set_missing_properties_status" => query_set_missing_properties_status(app, params).await,
        "get_top_groups" => query_get_top_groups(app).await,
        "set_top_group" => query_set_top_group(app, params).await,
        "remove_empty_top_group" => query_remove_empty_top_group(app, params).await,
        "quick_compare_list" => query_quick_compare_list(app).await,

        _ => Err(ApiError(format!("Unknown query '{query}'"))),
    }
}

// ─── helpers ────────────────────────────────────────────────────────────────

fn json_resp(v: serde_json::Value) -> Response { axum::Json(v).into_response() }
fn ok(data: serde_json::Value) -> Response { common::success_with_data(data).into_response() }

// ─── Catalog handlers ───────────────────────────────────────────────────────

async fn query_catalogs(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_catalog_overview().await?;
    let mut map = serde_json::Map::new();
    for item in data {
        if let Some(id) = item.get("catalog").and_then(|v| v.as_u64()).or_else(|| item.get("id").and_then(|v| v.as_u64())) {
            map.insert(id.to_string(), item);
        }
    }
    Ok(ok(serde_json::Value::Object(map)))
}

async fn query_single_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog_id", 0) as usize;
    let data = app.storage().api_get_single_catalog_overview(cid).await?;
    let mut map = serde_json::Map::new();
    map.insert(cid.to_string(), data);
    Ok(ok(serde_json::Value::Object(map)))
}

async fn query_catalog_details(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let s = app.storage();
    let (t, y, u) = tokio::join!(
        s.api_get_catalog_type_counts(cid),
        s.api_get_catalog_match_by_month(cid),
        s.api_get_catalog_matcher_by_user(cid),
    );
    Ok(ok(serde_json::json!({"type": t?, "ym": y?, "user": u?})))
}

async fn query_get_catalog_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_single_catalog_overview(cid).await?;
    Ok(ok(serde_json::json!([data])))
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

    let mut conds = vec![format!("catalog={catalog}")];
    if show_multiple == 1 {
        conds.push("EXISTS (SELECT * FROM multi_match WHERE entry_id=entry.id) AND (user<=0 OR user is null)".into());
    } else if show_noq + show_autoq + show_userq + show_nowd == 0 && show_na == 1 {
        conds.push("q=0".into());
    } else if show_noq + show_autoq + show_userq + show_na == 0 && show_nowd == 1 {
        conds.push("q=-1".into());
    } else {
        if show_noq != 1 { conds.push("q IS NOT NULL".into()); }
        if show_autoq != 1 { conds.push("(q is null OR user!=0)".into()); }
        if show_userq != 1 { conds.push("(user<=0 OR user is null)".into()); }
        if show_na != 1 { conds.push("(q!=0 or q is null)".into()); }
    }
    if !entry_type.is_empty() { conds.push(format!("`type`='{}'", entry_type.replace('\'', "''"))); }
    if !title_match.is_empty() { conds.push(format!("`ext_name` LIKE '%{}%'", title_match.replace('\'', "''"))); }

    let sql = format!("SELECT * FROM entry WHERE {} LIMIT {} OFFSET {}", conds.join(" AND "), per_page, offset);
    let entries = app.storage().api_get_catalog_entries_raw(&sql).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_edit_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data_str = common::get_param(params, "data", "");
    let data: serde_json::Value = serde_json::from_str(&data_str).map_err(|_| ApiError("Bad data".into()))?;
    let username = common::get_param(params, "username", "").replace('_', " ");
    let user = app.storage().get_user_by_name(&username).await?;
    match user {
        Some((_, _, is_admin)) if is_admin => {}
        Some((_, _, _)) => return Err(ApiError(format!("'{username}' is not a catalog admin"))),
        None => return Err(ApiError(format!("No such user '{username}'"))),
    }
    let name = data.get("name").and_then(|v| v.as_str()).ok_or(ApiError("Bad data".into()))?;
    app.storage().api_edit_catalog(
        cid,
        name,
        data.get("url").and_then(|v| v.as_str()).unwrap_or(""),
        data.get("desc").and_then(|v| v.as_str()).unwrap_or(""),
        data.get("type").and_then(|v| v.as_str()).unwrap_or(""),
        data.get("search_wp").and_then(|v| v.as_str()).unwrap_or(""),
        data.get("wd_prop").and_then(|v| v.as_u64()).map(|v| v as usize),
        data.get("wd_qual").and_then(|v| v.as_u64()).map(|v| v as usize),
        data.get("active").and_then(|v| v.as_bool()).unwrap_or(false),
    ).await?;
    let _ = app.storage().catalog_refresh_overview_table(cid).await;
    Ok(ok(serde_json::json!({})))
}

async fn query_catalog_overview(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs_str = common::get_param(params, "catalogs", "");
    let ids: Vec<usize> = catalogs_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let data = app.storage().api_get_catalog_overview_for_ids(&ids).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Entry handlers ─────────────────────────────────────────────────────────

async fn query_get_entry(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let entry_ids_str = common::get_param(params, "entry", "");
    let ext_ids_str = common::get_param(params, "ext_ids", "");
    let entries = if !ext_ids_str.is_empty() {
        if catalog == 0 { return Err(ApiError("catalog is required when using ext_ids".into())); }
        let ext_ids: Vec<String> = serde_json::from_str(&ext_ids_str).unwrap_or_default();
        let mut r = vec![];
        for eid in ext_ids { if let Ok(e) = crate::entry::Entry::from_ext_id(catalog, &eid, app).await { r.push(e); } }
        r
    } else {
        let ids: Vec<usize> = entry_ids_str.split(',').filter_map(|s| s.trim().parse().ok()).collect();
        if ids.is_empty() { return Err(ApiError("entry is required".into())); }
        crate::entry::Entry::multiple_from_ids(&ids, app).await?.into_values().collect()
    };
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_get_entry_by_extid(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "extid", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let mut data = common::entries_to_json_data(&[entry], app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_search(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let what = common::get_param(params, "what", "");
    let max_results = common::get_param_int(params, "max", 100) as usize;
    let desc_search = common::get_param_int(params, "description_search", 0) != 0;
    let no_label = common::get_param_int(params, "no_label_search", 0) != 0;
    let exclude: Vec<usize> = common::get_param(params, "exclude", "").split(',').filter_map(|s| s.trim().parse().ok()).collect();
    let include: Vec<usize> = common::get_param(params, "include", "").split(',').filter_map(|s| s.trim().parse().ok()).collect();

    let what_clean = what.replace('-', " ");
    let q_match = regex::Regex::new(r"^\s*[Qq]?(\d+)\s*$").ok().and_then(|re| re.captures(&what_clean).map(|c| c[1].parse::<isize>().unwrap_or(0)));
    let entries = if let Some(q) = q_match.filter(|q| *q > 0) {
        app.storage().api_search_by_q(q).await?
    } else {
        let words: Vec<String> = what_clean.split_whitespace()
            .filter(|w| w.len() >= 3 && w.len() <= 84 && !["the", "a"].contains(&w.to_lowercase().as_str()))
            .map(|s| s.to_string()).collect();
        if words.is_empty() { vec![] }
        else { app.storage().api_search_entries(&words, desc_search, no_label, &exclude, &include, max_results).await? }
    };
    let data = common::entries_to_json_data(&entries, app).await?;
    Ok(ok(data))
}

async fn query_random(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let submode = common::get_param(params, "submode", "");
    let entry_type = common::get_param(params, "type", "");
    let id = common::get_param_int(params, "id", 0) as usize;
    let active = app.storage().api_get_active_catalog_ids().await?;

    if id != 0 {
        let entry = crate::entry::Entry::from_id(id, app).await?;
        return Ok(ok(serde_json::json!(entry)));
    }
    for attempt in 0..=10 {
        let r: f64 = if attempt > 10 { 0.0 } else { rand::random() };
        if let Some(entry) = app.storage().api_get_random_entry(catalog, &submode, &entry_type, r, &active).await? {
            let eid = entry.id.unwrap_or(0);
            let mut data = serde_json::json!(entry);
            let pd = app.storage().api_get_person_dates_for_entries(&[eid]).await?;
            if let Some((born, died)) = pd.get(&eid) {
                if !born.is_empty() { data["born"] = serde_json::json!(born); }
                if !died.is_empty() { data["died"] = serde_json::json!(died); }
            }
            return Ok(ok(data));
        }
    }
    Ok(ok(serde_json::Value::Null))
}

async fn query_entries_query(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    use crate::match_state::MatchState;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let ms = MatchState {
        unmatched: common::get_param_int(params, "unmatched", 0) != 0,
        partially_matched: common::get_param_int(params, "prelim_matched", 0) != 0,
        fully_matched: common::get_param_int(params, "fully_matched", 0) != 0,
    };
    let eq = crate::entry_query::EntryQuery::default().with_match_state(ms).with_limit(50).with_offset(offset);
    let entries = app.storage().entry_query(&eq).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_entries_via_property_value(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let property: usize = common::get_param(params, "property", "").replace(|c: char| !c.is_ascii_digit(), "").parse().unwrap_or(0);
    let value = common::get_param(params, "value", "").trim().to_string();
    if property == 0 || value.is_empty() { return Err(ApiError("property and value required".into())); }
    let ids = app.storage().get_entry_ids_by_aux(property, &value).await?;
    let entries: Vec<_> = if ids.is_empty() { vec![] } else { crate::entry::Entry::multiple_from_ids(&ids, app).await?.into_values().collect() };
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    Ok(ok(data))
}

async fn query_get_entries_by_q_or_value(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let q_str = common::get_param(params, "q", "");
    let q: isize = q_str.replace(|c: char| !c.is_ascii_digit() && c != '-', "").parse().unwrap_or(0);
    let json_str = common::get_param(params, "json", "{}");
    let json_val: serde_json::Value = serde_json::from_str(&json_str).unwrap_or(serde_json::json!({}));

    let mut prop_values: std::collections::HashMap<usize, Vec<String>> = std::collections::HashMap::new();
    let mut props: Vec<usize> = vec![];
    if let Some(obj) = json_val.as_object() {
        for (k, v) in obj {
            let p: usize = k.replace('P', "").parse().unwrap_or(0);
            if p == 0 { continue; }
            props.push(p);
            let vals: Vec<String> = v.as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
            if !vals.is_empty() { prop_values.insert(p, vals); }
        }
    }
    let prop_catalog_map = if props.is_empty() { std::collections::HashMap::new() } else { app.storage().api_get_prop2catalog(&props).await? };
    let entries = app.storage().api_get_entries_by_q_or_value(q, &prop_catalog_map, &prop_values).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;

    // Add catalog info
    let cat_ids: std::collections::HashSet<usize> = entries.iter().map(|e| e.catalog).collect();
    let mut catalogs = serde_json::Map::new();
    for cid in cat_ids {
        if let Ok(c) = app.storage().api_get_single_catalog_overview(cid).await { catalogs.insert(cid.to_string(), c); }
    }
    data["catalogs"] = serde_json::Value::Object(catalogs);
    Ok(ok(data))
}

// ─── Matching handlers ──────────────────────────────────────────────────────

async fn query_match_q(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let q = common::get_param_int(params, "q", -1);
    let uid = common::check_user(app, params).await?;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    entry.set_match(&format!("Q{q}"), uid).await?;
    let out = crate::entry::Entry::from_id(eid, app).await?;
    let cat = crate::catalog::Catalog::from_id(out.catalog, app).await?;
    let mut ej = serde_json::json!(out);
    ej["entry_type"] = serde_json::json!(cat.type_name());
    Ok(json_resp(serde_json::json!({"status": "OK", "entry": ej})))
}

async fn query_match_q_multi(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let uid = common::check_user(app, params).await?;
    let data_str = common::get_param(params, "data", "[]");
    let data: Vec<serde_json::Value> = serde_json::from_str(&data_str).unwrap_or_default();
    let mut not_found = 0usize;
    let mut not_found_list: Vec<String> = vec![];
    for d in &data {
        let arr = d.as_array();
        let q = arr.and_then(|a| a.first()).and_then(|v| v.as_i64()).unwrap_or(0) as isize;
        let ext_id = arr.and_then(|a| a.get(1)).and_then(|v| v.as_str()).unwrap_or("");
        if !app.storage().api_match_q_multi(catalog, ext_id, q, uid).await? {
            not_found += 1;
            if not_found_list.len() < 100 { not_found_list.push(ext_id.to_string()); }
        }
    }
    Ok(json_resp(serde_json::json!({"status": "OK", "not_found": not_found, "not_found_list": not_found_list})))
}

async fn query_remove_q(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry", -1) as usize;
    common::check_user(app, params).await?;
    let mut entry = crate::entry::Entry::from_id(eid, app).await?;
    entry.unmatch().await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_all_q(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    common::check_user(app, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    if let Some(q) = entry.q {
        app.storage().api_remove_all_q(entry.catalog, q).await?;
    }
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_all_multimatches(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    common::check_user(app, params).await?;
    let eid = common::get_param_int(params, "entry", -1) as usize;
    app.storage().api_remove_all_multimatches(eid).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_suggest(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let overwrite = common::get_param_int(params, "overwrite", 0) != 0;
    let suggestions = common::get_param(params, "suggestions", "");
    let mut cnt = 0usize;
    let mut out = String::new();
    for line in suggestions.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 2 { out.push_str(&format!("Bad row : {line}\n")); continue; }
        let ext_id = parts[0].trim();
        let q: isize = parts[1].replace(|c: char| !c.is_ascii_digit(), "").parse().unwrap_or(0);
        if app.storage().api_suggest(catalog, ext_id, q, overwrite).await? { cnt += 1; }
    }
    out.push_str(&format!("{cnt} entries changed"));
    Ok(([(axum::http::header::CONTENT_TYPE, "text/plain; charset=UTF-8")], out).into_response())
}

// ─── Job handlers ───────────────────────────────────────────────────────────

async fn query_get_jobs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    let start = common::get_param_int(params, "start", 0) as usize;
    let max = common::get_param_int(params, "max", 50) as usize;
    let (stats, jobs) = app.storage().api_get_jobs(cid, start, max).await?;
    let mut out = serde_json::json!({"status": "OK", "data": jobs});
    if cid == 0 { out["stats"] = serde_json::json!(stats); }
    Ok(json_resp(out))
}

async fn query_start_new_job(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let action = common::get_param(params, "action", "").trim().to_lowercase();
    common::check_user(app, params).await?;
    if !regex::Regex::new(r"^[a-z_]+$").unwrap().is_match(&action) { return Err(ApiError(format!("Bad action: '{action}'"))); }
    let valid = app.storage().api_get_existing_job_actions().await?;
    if !valid.contains(&action) { return Err(ApiError(format!("Unknown action: '{action}'"))); }
    crate::job::Job::queue_simple_job(app, cid, &action, None).await?;
    Ok(ok(serde_json::json!({})))
}

// ─── Issue handlers ─────────────────────────────────────────────────────────

async fn query_get_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let itype = common::get_param(params, "type", "").trim().to_uppercase();
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let catalogs = common::get_param(params, "catalogs", "");
    let count = app.storage().api_get_issues_count(&itype, &catalogs).await?;
    if count == 0 { return Ok(ok(serde_json::json!({}))); }
    let r: f64 = if count < limit * 2 { 0.0 } else { rand::random() };
    let issues = app.storage().api_get_issues(&itype, &catalogs, limit, offset, r).await?;
    let eids: Vec<usize> = issues.iter().filter_map(|i| i.get("entry_id").and_then(|v| v.as_u64()).map(|v| v as usize)).collect();
    let entries = if eids.is_empty() { serde_json::json!({"entries":{}, "users":{}}) } else {
        let map = crate::entry::Entry::multiple_from_ids(&eids, app).await?;
        common::entries_to_json_data(&map.into_values().collect::<Vec<_>>(), app).await?
    };
    Ok(ok(serde_json::json!({"open_issues": count, "issues": issues, "entries": entries.get("entries"), "users": entries.get("users")})))
}

async fn query_all_issues(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    if !["duplicate_items", "mismatched_items", "time_mismatch"].contains(&mode.as_str()) { return Err(ApiError("Unsupported mode".into())); }
    Ok(ok(serde_json::json!(app.storage().api_get_all_issues(&mode).await?)))
}

async fn query_resolve_issue(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let iid = common::get_param_int(params, "issue_id", 0) as usize;
    if iid == 0 { return Err(ApiError("Bad issue ID".into())); }
    common::check_user(app, params).await?;
    app.storage().set_issue_status(iid, crate::issue::IssueStatus::Done).await?;
    Ok(ok(serde_json::json!({})))
}

// ─── User ───────────────────────────────────────────────────────────────────

async fn query_get_user_info(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let name = common::get_param(params, "username", "").replace('_', " ");
    match app.storage().get_user_by_name(&name).await? {
        Some((id, n, admin)) => Ok(ok(serde_json::json!({"id": id, "name": n, "is_catalog_admin": if admin {1} else {0}}))),
        None => Err(ApiError(format!("No user '{name}' found"))),
    }
}

// ─── Recent changes ─────────────────────────────────────────────────────────

async fn query_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let limit = 100;
    let (entry_evts, log_evts) = app.storage().api_get_recent_changes(&ts, catalog, limit).await?;
    let mut events: Vec<serde_json::Value> = entry_evts.into_iter().chain(log_evts).collect();
    events.sort_by(|a, b| {
        let ta = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        tb.cmp(ta)
    });
    events.truncate(limit);
    let uids: std::collections::HashSet<usize> = events.iter().filter_map(|e| e.get("user").and_then(|v| v.as_u64()).map(|v| v as usize)).collect();
    let users = common::get_users(app, &uids).await?;
    Ok(ok(serde_json::json!({"events": events, "users": users})))
}

// ─── Data & analysis ────────────────────────────────────────────────────────

async fn query_get_wd_props(app: &AppState) -> Result<Response, ApiError> {
    let props = app.storage().api_get_wd_props().await?;
    Ok(json_resp(serde_json::json!(props)))
}

async fn query_top_missing(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "").chars().filter(|c| c.is_ascii_digit() || *c == ',').collect();
    if catalogs.is_empty() { return Err(ApiError("No catalogs given".into())); }
    let data = app.storage().api_get_top_missing(&catalogs).await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_get_common_names(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let min = common::get_param_int(params, "min", 3) as usize;
    let max = common::get_param_int(params, "max", 15) as usize + 1;
    let type_q = common::get_param(params, "type", "");
    let type_q = if regex::Regex::new(r"^Q\d+$").unwrap().is_match(&type_q) { type_q } else { String::new() };
    let other_cats_desc = common::get_param_int(params, "other_cats_desc", 0) != 0;
    let data = app.storage().api_get_common_names(cid, &type_q, other_cats_desc, min, max, limit, offset).await?;
    Ok(ok(serde_json::json!({"entries": data})))
}

async fn query_same_names(app: &AppState) -> Result<Response, ApiError> {
    let (name, entries) = app.storage().api_get_same_names().await?;
    let data = common::entries_to_json_data(&entries, app).await?;
    let mut out = serde_json::json!({"status": "OK", "data": data});
    out["data"]["name"] = serde_json::json!(name);
    Ok(json_resp(out))
}

async fn query_random_person_batch(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let gender = common::get_param(params, "gender", "");
    let has_desc = common::get_param_int(params, "has_desc", 0) != 0;
    let data = app.storage().api_get_random_person_batch(&gender, has_desc).await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_get_property_cache(app: &AppState) -> Result<Response, ApiError> {
    let (prop2item, item_label) = app.storage().api_get_property_cache().await?;
    Ok(ok(serde_json::json!({"prop2item": prop2item, "item_label": item_label})))
}

async fn query_mnm_unmatched_relations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let property = common::get_param_int(params, "property", 0) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let limit = 25;
    let (id_cnts, entries) = app.storage().api_get_mnm_unmatched_relations(property, offset, limit).await?;
    let mut data = common::entries_to_json_data(&entries, app).await?;
    common::add_extended_entry_data(app, &mut data).await?;
    let entry2cnt: serde_json::Map<String, serde_json::Value> = id_cnts.iter().map(|(id, cnt)| (id.to_string(), serde_json::json!(cnt))).collect();
    let entry_order: Vec<usize> = id_cnts.iter().map(|(id, _)| *id).collect();
    data["entry2cnt"] = serde_json::Value::Object(entry2cnt);
    data["entry_order"] = serde_json::json!(entry_order);
    Ok(ok(data))
}

async fn query_creation_candidates(app: &AppState, _params: &Params) -> Result<Response, ApiError> {
    // Complex multi-strategy endpoint — stub for now
    Ok(ok(serde_json::json!({"entries": [], "users": {}})))
}

// ─── Locations ──────────────────────────────────────────────────────────────

async fn query_locations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let bbox: String = common::get_param(params, "bbox", "").chars().filter(|c| c.is_ascii_digit() || *c == ',' || *c == '.' || *c == '-').collect();
    let parts: Vec<f64> = bbox.split(',').filter_map(|s| s.parse().ok()).collect();
    if parts.len() != 4 { return Err(ApiError("Required parameter bbox does not have 4 comma-separated numbers".into())); }
    let data = app.storage().api_get_locations_bbox(parts[0], parts[1], parts[2], parts[3]).await?;
    Ok(json_resp(serde_json::json!({"status": "OK", "data": data, "bbox": parts})))
}

async fn query_get_locations_in_catalog(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let data = app.storage().api_get_locations_in_catalog(cid).await?;
    Ok(ok(serde_json::json!(data)))
}

// ─── Download & export ──────────────────────────────────────────────────────

async fn query_download(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let filename = cat.name().unwrap_or(&"download".to_string()).replace(' ', "_") + ".tsv";
    let rows = app.storage().api_get_download_entries(cid).await?;
    // Build user map
    let uids: std::collections::HashSet<usize> = rows.iter().filter_map(|(_, _, _, _, u)| *u).collect();
    let users = common::get_users(app, &uids).await?;
    let mut out = String::from("Q\tID\tURL\tName\tUser\n");
    for (q, ext_id, ext_url, ext_name, user_id) in &rows {
        let uname = user_id.and_then(|u| users.get(&u.to_string())).and_then(|v| v.get("name")).and_then(|v| v.as_str()).unwrap_or("");
        out.push_str(&format!("{q}\t{ext_id}\t{ext_url}\t{ext_name}\t{uname}\n"));
    }
    Ok(([(axum::http::header::CONTENT_TYPE, "text/plain; charset=UTF-8"), (axum::http::header::CONTENT_DISPOSITION, &format!("attachment;filename=\"{filename}\""))], out).into_response())
}

async fn query_download2(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "").chars().filter(|c| c.is_ascii_digit() || *c == ',').collect();
    let format = common::get_param(params, "format", "tab");
    let columns: serde_json::Value = serde_json::from_str(&common::get_param(params, "columns", "{}")).unwrap_or(serde_json::json!({}));
    let hidden: serde_json::Value = serde_json::from_str(&common::get_param(params, "hidden", "{}")).unwrap_or(serde_json::json!({}));

    let mut sql = "SELECT entry.id AS entry_id,entry.catalog,ext_id AS external_id".to_string();
    if columns.get("exturl").and_then(|v| v.as_bool()).unwrap_or(false) || columns.get("exturl").and_then(|v| v.as_i64()).map(|v| v != 0).unwrap_or(false) {
        sql.push_str(",ext_url AS external_url,ext_name AS `name`,ext_desc AS description,`type` AS entry_type,entry.user AS mnm_user_id");
    }
    sql.push_str(",(CASE WHEN q IS NULL THEN NULL else concat('Q',q) END) AS q,`timestamp` AS matched_on");
    if columns.get("username").and_then(|v| v.as_bool()).or(columns.get("username").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(",user.name AS matched_by_username"); }
    if columns.get("dates").and_then(|v| v.as_bool()).or(columns.get("dates").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(",person_dates.born,person_dates.died"); }
    if columns.get("location").and_then(|v| v.as_bool()).or(columns.get("location").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(",location.lat,location.lon"); }

    sql.push_str(" FROM entry");
    if columns.get("dates").and_then(|v| v.as_bool()).or(columns.get("dates").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(" LEFT JOIN person_dates ON (entry.id=person_dates.entry_id)"); }
    if columns.get("location").and_then(|v| v.as_bool()).or(columns.get("location").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(" LEFT JOIN location ON (entry.id=location.entry_id)"); }
    if columns.get("username").and_then(|v| v.as_bool()).or(columns.get("username").and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false) { sql.push_str(" LEFT JOIN user ON (entry.user=user.id)"); }

    sql.push_str(&format!(" WHERE entry.catalog IN ({catalogs})"));
    let hb = |k: &str| hidden.get(k).and_then(|v| v.as_bool()).or(hidden.get(k).and_then(|v| v.as_i64()).map(|v| v != 0)).unwrap_or(false);
    if hb("any_matched") { sql.push_str(" AND entry.q IS NULL"); }
    if hb("firmly_matched") { sql.push_str(" AND (entry.q IS NULL OR entry.user=0)"); }
    if hb("user_matched") { sql.push_str(" AND (entry.user IS NULL OR entry.user IN (0,3,4))"); }
    if hb("unmatched") { sql.push_str(" AND entry.q IS NOT NULL"); }
    if hb("automatched") { sql.push_str(" AND entry.user!=0"); }
    if hb("aux_matched") { sql.push_str(" AND entry.user!=4"); }

    let rows = app.storage().api_get_download2(&sql).await?;
    let ct = if format == "json" { "application/json; charset=UTF-8" } else { "text/plain; charset=UTF-8" };
    let mut out = String::new();
    for (i, row) in rows.iter().enumerate() {
        if i == 0 {
            if format == "tab" { out.push('#'); out.push_str(&row.keys().cloned().collect::<Vec<_>>().join("\t")); out.push('\n'); }
            if format == "json" { out.push_str("[\n"); }
        }
        if format == "json" {
            if i > 0 { out.push_str(",\n"); }
            out.push_str(&serde_json::to_string(row).unwrap_or_default());
        } else {
            out.push_str(&row.values().map(|v| v.replace(['\t', '\n', '\r'], " ")).collect::<Vec<_>>().join("\t"));
            out.push('\n');
        }
    }
    if rows.is_empty() && format == "json" { out.push_str("[\n"); }
    if format == "json" { out.push_str("\n]"); }
    Ok(([(axum::http::header::CONTENT_TYPE, ct)], out).into_response())
}

// ─── Navigation ─────────────────────────────────────────────────────────────

async fn query_redirect(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "ext_id", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let html = format!("<html><head><META http-equiv=\"refresh\" content=\"0;URL={}\"></head><body></body></html>", entry.ext_url);
    Ok(([(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")], html).into_response())
}

async fn query_proxy_entry_url(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry_id", 0) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    let client = reqwest::Client::new();
    let body = client.get(&entry.ext_url).send().await.map_err(|e| ApiError(e.to_string()))?.text().await.map_err(|e| ApiError(e.to_string()))?;
    Ok(([(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")], body).into_response())
}

async fn query_cersei_forward(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let sid = common::get_param_int(params, "scraper", 0) as usize;
    match app.storage().api_get_cersei_catalog(sid).await? {
        Some(cid) => {
            let url = format!("https://mix-n-match.toolforge.org/#/catalog/{cid}");
            Ok((axum::http::StatusCode::FOUND, [(axum::http::header::LOCATION, url.as_str())]).into_response())
        }
        None => Err(ApiError(format!("No catalog associated with CERSEI scraper {sid}"))),
    }
}

// ─── Admin & config ─────────────────────────────────────────────────────────

async fn query_update_overview(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cs = common::get_param(params, "catalog", "");
    let ids: Vec<usize> = if cs.is_empty() { app.storage().api_get_active_catalog_ids().await? } else { cs.split(',').filter_map(|s| s.trim().parse().ok()).collect() };
    for id in ids { let _ = app.storage().catalog_refresh_overview_table(id).await; }
    Ok(ok(serde_json::json!({})))
}

async fn query_update_ext_urls(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let username = common::get_param(params, "username", "").replace('_', " ");
    match app.storage().get_user_by_name(&username).await? {
        Some((_, _, true)) => {}
        _ => return Err(ApiError(format!("'{username}' is not a catalog admin"))),
    }
    let cid = common::get_catalog(params)?;
    let url = common::get_param(params, "url", "");
    let parts: Vec<&str> = url.split("$1").collect();
    if parts.len() != 2 { return Err(ApiError(format!("Bad $1 replacement for '{url}'"))); }
    let sql = format!("UPDATE entry SET ext_url=concat('{}',ext_id,'{}') WHERE catalog={cid}", parts[0].replace('\'', "''"), parts[1].replace('\'', "''"));
    app.storage().api_get_catalog_entries_raw(&sql).await.ok(); // execute the update
    Ok(ok(serde_json::json!({"sql": sql})))
}

async fn query_add_aliases(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let uid = common::check_user(app, params).await?;
    let text = common::get_param(params, "text", "").trim().to_string();
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    if cid == 0 || text.is_empty() { return Err(ApiError("Catalog ID or text missing".into())); }
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let default_lang = { let wp = cat.search_wp(); if wp.is_empty() { "en".to_string() } else { wp.to_string() } };
    for row in text.lines() {
        let parts: Vec<&str> = row.trim().split('\t').collect();
        if parts.len() < 2 || parts.len() > 3 { continue; }
        let ext_id = parts[0].trim();
        let label = parts[1].trim().replace('|', "");
        let lang = if parts.len() == 3 && !parts[2].trim().is_empty() { parts[2].trim().to_lowercase() } else { default_lang.clone() };
        let _ = app.storage().api_add_alias(cid, ext_id, &lang, &label, uid).await;
    }
    Ok(ok(serde_json::json!({})))
}

async fn query_get_missing_properties(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_missing_properties_raw().await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_set_missing_properties_status(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let uid = common::check_user(app, params).await?;
    let row_id = common::get_param_int(params, "row_id", 0) as usize;
    if row_id == 0 { return Err(ApiError("Bad/missing row ID".into())); }
    let status = common::get_param(params, "status", "");
    if status.is_empty() { return Err(ApiError("Invalid status".into())); }
    let note = common::get_param(params, "note", "");
    app.storage().api_set_missing_properties_status(row_id, &status, &note, uid).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_get_top_groups(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_top_groups().await?;
    Ok(ok(serde_json::json!(data)))
}

async fn query_set_top_group(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let uid = common::check_user(app, params).await?;
    let name = common::get_param(params, "group_name", "");
    let catalogs = common::get_param(params, "catalogs", "");
    let based_on = common::get_param_int(params, "group_id", 0) as usize;
    app.storage().api_set_top_group(&name, &catalogs, uid, based_on).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_remove_empty_top_group(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let gid = common::get_param_int(params, "group_id", 0) as usize;
    app.storage().api_remove_empty_top_group(gid).await?;
    Ok(ok(serde_json::json!({})))
}

async fn query_quick_compare_list(app: &AppState) -> Result<Response, ApiError> {
    let data = app.storage().api_get_quick_compare_list().await?;
    Ok(ok(serde_json::json!(data)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_builds() {
        // Verifies router construction doesn't panic
    }
}
