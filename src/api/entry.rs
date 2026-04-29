//! Entry-related read endpoints: lookup, search, random, queries, and the
//! "entries by Q or aux value" composite endpoint.

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::ExternalServicesContext;
use axum::response::Response;
use futures::stream::{self, StreamExt};
use std::sync::OnceLock;

/// Fan-out concurrency for per-entry DB lookups. Capped well under the RO pool
/// `max_connections` (8) so we don't starve other handlers running at the same time.
const ENTRY_LOOKUP_CONCURRENCY: usize = 6;

fn re_q_match() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^\s*[Qq]?(\d+)\s*$").expect("valid regex"))
}

pub async fn query_get_entry(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let entry_ids_str = common::get_param(params, "entry", "");
    let ext_ids_str = common::get_param(params, "ext_ids", "");
    let entries = if !ext_ids_str.is_empty() {
        if catalog == 0 {
            return Err(ApiError("catalog is required when using ext_ids".into()));
        }
        let ext_ids: Vec<String> = serde_json::from_str(&ext_ids_str).unwrap_or_default();
        // The original implementation did `for eid in ext_ids` and awaited
        // each Entry::from_ext_id sequentially. Run them concurrently with
        // bounded fan-out so 100-element ext_id lists don't stack 100 RTTs.
        stream::iter(ext_ids)
            .map(|eid| async move {
                crate::entry::Entry::from_ext_id(catalog, &eid, app).await.ok()
            })
            .buffer_unordered(ENTRY_LOOKUP_CONCURRENCY)
            .filter_map(|opt| async move { opt })
            .collect::<Vec<_>>()
            .await
    } else {
        let ids: Vec<usize> = entry_ids_str
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        if ids.is_empty() {
            return Err(ApiError("entry is required".into()));
        }
        crate::entry::Entry::multiple_from_ids(&ids, app)
            .await?
            .into_values()
            .collect()
    };
    Ok(ok(common::entries_with_extended_data(&entries, app).await?))
}

pub async fn query_get_entry_by_extid(
    app: &dyn ExternalServicesContext,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "extid", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    Ok(ok(common::entries_with_extended_data(&[entry], app).await?))
}

pub async fn query_search(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    let what = common::get_param(params, "what", "");
    let max_results = common::get_param_int(params, "max", 100) as usize;
    let desc_search = common::get_param_int(params, "description_search", 0) != 0;
    let no_label = common::get_param_int(params, "no_label_search", 0) != 0;
    let user_exclude: Vec<usize> = common::get_param(params, "exclude", "")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let include: Vec<usize> = common::get_param(params, "include", "")
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    // Mirror PHP: the effective exclude list is the user-provided one plus every
    // inactive catalog, so disabled catalogs never leak into text or Q-number search.
    let mut exclude = user_exclude;
    exclude.extend(app.storage().api_get_inactive_catalog_ids().await?);
    exclude.sort_unstable();
    exclude.dedup();

    let what_clean = what.replace('-', " ");
    let q_match = re_q_match()
        .captures(&what_clean)
        .map(|c| c[1].parse::<isize>().unwrap_or(0));
    let entries = if let Some(q) = q_match.filter(|q| *q > 0) {
        app.storage().api_search_by_q(q, &exclude).await?
    } else {
        let words: Vec<String> = what_clean
            .split_whitespace()
            .filter(|w| {
                w.len() >= 3 && w.len() <= 84 && !["the", "a"].contains(&w.to_lowercase().as_str())
            })
            .map(|s| s.to_string())
            .collect();
        if words.is_empty() {
            vec![]
        } else {
            app.storage()
                .api_search_entries(
                    &words,
                    desc_search,
                    no_label,
                    &exclude,
                    &include,
                    max_results,
                )
                .await?
        }
    };
    let data = common::entries_to_json_data(&entries, app).await?;
    Ok(ok(data))
}

pub async fn query_random(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let submode = common::get_param(params, "submode", "");
    let entry_type = common::get_param(params, "type", "");
    let id = common::get_param_int(params, "id", 0) as usize;

    // Find the candidate entry. Mirrors query_random() in PHP API.php:
    //   id != 0            → direct lookup by id (test hook in PHP)
    //   catalog > 0        → catalog-specific random pick (catalog_q_random index)
    //   catalog == 0       → global random pick across active catalogs (the
    //                        active list is filtered Rust-side after the SQL
    //                        scan; pushing the filter into SQL via EXISTS made
    //                        the planner walk huge numbers of `random_2` rows).
    let entry_opt = if id != 0 {
        crate::entry::Entry::from_id(id, app).await.ok()
    } else if catalog > 0 {
        app.storage()
            .api_get_random_entry(catalog, &submode, &entry_type, &[])
            .await?
    } else {
        let active = app.storage().api_get_active_catalog_ids().await?;
        app.storage()
            .api_get_random_entry(0, &submode, &entry_type, &active)
            .await?
    };

    let Some(entry) = entry_opt else {
        return Ok(ok(serde_json::Value::Null));
    };

    // Augment with person dates if we have them.
    let eid = entry.id.unwrap_or(0);
    let mut data = serde_json::json!(entry);
    let pd = app
        .storage()
        .api_get_person_dates_for_entries(&[eid])
        .await?;
    if let Some((born, died)) = pd.get(&eid) {
        if !born.is_empty() {
            data["born"] = serde_json::json!(born);
        }
        if !died.is_empty() {
            data["died"] = serde_json::json!(died);
        }
    }
    Ok(ok(data))
}

pub async fn query_entries_query(app: &dyn ExternalServicesContext, params: &Params) -> Result<Response, ApiError> {
    use crate::match_state::MatchState;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let ms = MatchState {
        unmatched: common::get_param_int(params, "unmatched", 0) != 0,
        partially_matched: common::get_param_int(params, "prelim_matched", 0) != 0,
        fully_matched: common::get_param_int(params, "fully_matched", 0) != 0,
    };
    let eq = crate::entry_query::EntryQuery::default()
        .with_match_state(ms)
        .with_limit(50)
        .with_offset(offset);
    let entries = app.storage().entry_query(&eq).await?;
    Ok(ok(common::entries_with_extended_data(&entries, app).await?))
}

pub async fn query_entries_via_property_value(
    app: &dyn ExternalServicesContext,
    params: &Params,
) -> Result<Response, ApiError> {
    let property: usize = common::get_param(params, "property", "")
        .replace(|c: char| !c.is_ascii_digit(), "")
        .parse()
        .unwrap_or(0);
    let value = common::get_param(params, "value", "").trim().to_string();
    if property == 0 || value.is_empty() {
        return Err(ApiError("property and value required".into()));
    }
    let ids = app.storage().get_entry_ids_by_aux(property, &value).await?;
    let entries: Vec<_> = if ids.is_empty() {
        vec![]
    } else {
        crate::entry::Entry::multiple_from_ids(&ids, app)
            .await?
            .into_values()
            .collect()
    };
    Ok(ok(common::entries_with_extended_data(&entries, app).await?))
}

pub async fn query_get_entries_by_q_or_value(
    app: &dyn ExternalServicesContext,
    params: &Params,
) -> Result<Response, ApiError> {
    let q_str = common::get_param(params, "q", "");
    let q: isize = q_str
        .replace(|c: char| !c.is_ascii_digit() && c != '-', "")
        .parse()
        .unwrap_or(0);
    let json_str = common::get_param(params, "json", "{}");
    let json_val: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or(serde_json::json!({}));

    let mut prop_values: std::collections::HashMap<usize, Vec<String>> =
        std::collections::HashMap::new();
    let mut props: Vec<usize> = vec![];
    if let Some(obj) = json_val.as_object() {
        for (k, v) in obj {
            let p: usize = k.replace('P', "").parse().unwrap_or(0);
            if p == 0 {
                continue;
            }
            props.push(p);
            let vals: Vec<String> = v
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            if !vals.is_empty() {
                prop_values.insert(p, vals);
            }
        }
    }
    let prop_catalog_map = if props.is_empty() {
        std::collections::HashMap::new()
    } else {
        app.storage().api_get_prop2catalog(&props).await?
    };
    let entries = app
        .storage()
        .api_get_entries_by_q_or_value(q, &prop_catalog_map, &prop_values)
        .await?;

    // The catalog-overview lookup only depends on the entries' catalog ids.
    // Run it concurrently with `add_extended_entry_data`, which fans out 8
    // separate DB queries — letting the catalog overview share that wave
    // saves one full RTT on the response path.
    let cat_ids: Vec<usize> = entries
        .iter()
        .map(|e| e.catalog)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut data = common::entries_to_json_data(&entries, app).await?;
    let (extended_res, overview_rows) = tokio::join!(
        common::add_extended_entry_data(app, &mut data),
        async {
            if cat_ids.is_empty() {
                Vec::new()
            } else {
                app.storage()
                    .api_get_catalog_overview_for_ids(&cat_ids)
                    .await
                    .unwrap_or_default()
            }
        },
    );
    extended_res?;

    let mut catalogs = serde_json::Map::new();
    for item in overview_rows {
        if let Some(id) = item.get("id").and_then(|v| v.as_u64()) {
            catalogs.insert(id.to_string(), item);
        }
    }
    data["catalogs"] = serde_json::Value::Object(catalogs);
    Ok(ok(data))
}
