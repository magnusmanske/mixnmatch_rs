//! `creation_candidates`: pick a random catalog "name group" (a few entries
//! sharing an `ext_name`, optionally with shared birth/death years or aux
//! values) so the frontend can offer them as a candidate for a new Wikidata
//! item. This is the largest single legacy handler in the API surface.

use crate::api::common::{ApiError, Params};
use crate::app_state::AppState;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// How many random picks we'll attempt before giving up. Mirrors the PHP
/// "give up" fallback once the random walk fails to land on a usable group.
const MAX_TRIES: usize = 250;

fn re_year() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^\d{1,4}$").expect("valid regex"))
}

fn re_name_variants() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^(\S+) (.+) (\S+)$").expect("valid regex"))
}

/// Validates that a table name contains only safe characters
/// (alphanumerics + underscore). Guards against SQL injection via `mode`.
pub fn is_safe_table_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[allow(clippy::cognitive_complexity)]
pub async fn run(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let opts = ParsedParams::from(params);

    let table = match opts.mode.as_str() {
        "aux" => "common_aux".to_string(),
        "" => "common_names".to_string(),
        m => {
            let t = format!("common_names_{m}");
            if !is_safe_table_name(&t) {
                return Err(ApiError(format!("invalid mode: {m}")));
            }
            t
        }
    };

    let mut result_data = json!({"entries": []});
    let mut result_name: Option<String> = None;
    let mut user_ids: Vec<usize> = vec![];
    let mut completed = false;

    for _attempt in 0..MAX_TRIES {
        let pick_sql = if !opts.ext_name_required.is_empty() {
            // Test/diagnostic shortcut: skip the random-pick scan and pin the
            // candidate name directly. Validates the rest of the pipeline
            // against an indexed `ext_name` lookup.
            let safe = opts.ext_name_required.replace('\'', "''");
            format!("SELECT '{safe}' AS ext_name, 20 AS cnt")
        } else {
            cc_mode_sql(&opts.mode, &table, opts.min, &opts.prop, &opts.require_catalogs)?
        };

        let picks = app
            .storage()
            .cc_random_pick(&pick_sql)
            .await
            .map_err(|e| ApiError(format!("pick query failed: {e}")))?;

        if picks.is_empty() {
            continue;
        }

        let pick = &picks[0];
        // The pick column is `ext_name` for most modes, `aux_name` for `random_prop`.
        let ext_name = pick["ext_name"]
            .as_str()
            .or_else(|| pick["aux_name"].as_str())
            .unwrap_or("")
            .to_string();
        if !ext_name.is_empty() {
            result_name = Some(ext_name.clone());
        }

        let entries = match load_entries_for_pick(app, &opts, pick, &ext_name).await? {
            Some(e) => e,
            None => continue,
        };

        // Required-counts gating (skip when the caller pinned an `ext_name`).
        if opts.ext_name_required.is_empty() {
            let (found_unset, required_found) =
                tally_constraints(&entries, &opts.require_catalogs, &mut user_ids);
            if found_unset < opts.require_unset {
                continue;
            }
            if required_found.len() < opts.catalogs_required {
                continue;
            }
        } else {
            // Even when we skip the gate, we still want user_ids accumulated
            // for the trailing user-resolution lookup.
            for e in &entries {
                if let Some(uid) = e.user {
                    user_ids.push(uid);
                }
            }
        }

        if opts.min > 0 && entries.len() < opts.min && opts.ext_name_required.is_empty() {
            continue;
        }

        let entries_json: Vec<Value> = entries
            .iter()
            .map(|e| serde_json::to_value(e).unwrap_or(json!(null)))
            .collect();
        result_data = json!({"entries": entries_json});
        completed = true;
        break;
    }

    if !completed {
        return Err(ApiError(format!(
            "No results after {MAX_TRIES} attempts, giving up"
        )));
    }

    if let Some(name) = &result_name {
        result_data["name"] = json!(name);
    }

    // Resolve collected uids → user objects (matches PHP `$out['data']['users']`).
    let unique_ids: Vec<usize> = user_ids.iter().copied().collect::<HashSet<_>>().into_iter().collect();
    let users_map = if unique_ids.is_empty() {
        json!({})
    } else {
        let rows = app
            .storage()
            .get_users_by_ids(&unique_ids)
            .await
            .unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (id, val) in rows {
            obj.insert(id.to_string(), val);
        }
        Value::Object(obj)
    };
    result_data["users"] = users_map;

    Ok(result_data)
}

struct ParsedParams {
    min: usize,
    mode: String,
    ext_name_required: String,
    birth_year: Option<String>,
    death_year: Option<String>,
    prop: String,
    require_unset: usize,
    require_catalogs: String,
    catalogs_required: usize,
}

impl ParsedParams {
    fn from(params: &Params) -> Self {
        let opt = |k: &str| -> Option<&str> {
            params.get(k).filter(|v| !v.is_empty()).map(String::as_str)
        };
        let opt_usize = |k: &str| -> Option<usize> { opt(k).and_then(|v| v.parse().ok()) };
        let opt_year = |k: &str| -> Option<String> {
            opt(k)
                .filter(|s| re_year().is_match(s))
                .map(|s| s.to_string())
        };

        Self {
            min: opt_usize("min").unwrap_or(3),
            mode: opt("mode").unwrap_or("").to_string(),
            ext_name_required: opt("ext_name").unwrap_or("").trim().to_string(),
            birth_year: opt_year("birth_year"),
            death_year: opt_year("death_year"),
            prop: opt("prop").unwrap_or("").to_string(),
            require_unset: opt_usize("require_unset").unwrap_or(0),
            require_catalogs: opt("require_catalogs").unwrap_or("").to_string(),
            catalogs_required: opt_usize("min_catalogs_required").unwrap_or(0),
        }
    }
}

/// Returns `Some(entries)` if the pick yielded usable rows, `None` if the
/// caller should retry (bad/empty entry_ids, no rows from name lookup, etc.).
async fn load_entries_for_pick(
    app: &AppState,
    opts: &ParsedParams,
    pick: &Value,
    ext_name: &str,
) -> Result<Option<Vec<crate::entry::Entry>>, ApiError> {
    let uses_entry_ids = matches!(
        opts.mode.as_str(),
        "dates" | "birth_year" | "random_prop" | "artwork" | "aux"
    );

    if uses_entry_ids {
        let entry_ids = pick["entry_ids"].as_str().unwrap_or("");
        if entry_ids.is_empty() {
            return Ok(None);
        }
        // Defence-in-depth: the storage call interpolates this string —
        // reject anything that isn't `\d+(?:,\d+)*`.
        if !entry_ids.chars().all(|c| c.is_ascii_digit() || c == ',') {
            return Ok(None);
        }
        let res = app
            .storage()
            .cc_get_entries_by_ids_active(entry_ids)
            .await
            .map_err(|e| ApiError(format!("entries query failed: {e}")))?;
        Ok(Some(res))
    } else {
        let mut names = vec![ext_name.to_string()];
        // Generate name variants:
        //   "First Middle Last" → "First-Middle Last", "First Middle-Last"
        if let Some(caps) = re_name_variants().captures(ext_name) {
            let (a, b, c) = (&caps[1], &caps[2], &caps[3]);
            names.push(format!("{a}-{b} {c}"));
            names.push(format!("{a} {b}-{c}"));
        }
        let type_filter = if opts.mode == "taxon" { Some("Q16521") } else { None };
        let res = app
            .storage()
            .cc_get_entries_by_names_active(
                &names,
                type_filter,
                opts.birth_year.as_deref(),
                opts.death_year.as_deref(),
            )
            .await
            .map_err(|e| ApiError(format!("entries query failed: {e}")))?;
        Ok(Some(res))
    }
}

/// Count "unset" (auto-matched / unmatched) entries and tally how many
/// required catalogs were hit. Side-effect: appends each entry's `user` to
/// the running `user_ids` list so the caller can resolve them at the end.
fn tally_constraints(
    entries: &[crate::entry::Entry],
    require_catalogs: &str,
    user_ids: &mut Vec<usize>,
) -> (usize, HashMap<String, usize>) {
    let mut found_unset = 0_usize;
    let mut required_found: HashMap<String, usize> = HashMap::new();
    let req_cats: Vec<String> = require_catalogs
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    for e in entries {
        if e.user == Some(0) || e.q.is_none() {
            found_unset += 1;
        }
        let cat_str = e.catalog.to_string();
        if req_cats.contains(&cat_str) {
            *required_found.entry(cat_str).or_default() += 1;
        }
        if let Some(uid) = e.user {
            user_ids.push(uid);
        }
    }
    (found_unset, required_found)
}

/// Build the candidate-picking SQL for a specific creation_candidates mode.
pub fn cc_mode_sql(
    mode: &str,
    table: &str,
    min: usize,
    prop: &str,
    require_catalogs: &str,
) -> Result<String, ApiError> {
    let min_where = if min > 0 {
        format!("cnt>={min}")
    } else {
        "1=1".to_string()
    };
    let random_pick = format!("FROM {table} WHERE {min_where} ORDER BY rand() LIMIT 1");

    match mode {
        "artwork" | "dates" | "birth_year" => {
            Ok(format!("SELECT name AS ext_name, cnt, entry_ids {random_pick}"))
        }
        "taxon" => Ok(format!("SELECT name AS ext_name, cnt {random_pick}")),
        "aux" => Ok(format!(
            "SELECT aux_name AS ext_name, entry_ids, cnt {random_pick}"
        )),
        "random_prop" => {
            let min_rp = if min < 2 { 2 } else { min };
            let mut sql =
                format!("SELECT aux_name, entry_ids, cnt FROM aux_candidates WHERE cnt>={min_rp}");
            if !prop.is_empty() {
                if let Ok(p) = prop.parse::<usize>() {
                    sql += &format!(" AND aux_p={p}");
                }
            }
            Ok(sql + " ORDER BY rand() LIMIT 1")
        }
        "dynamic_name_year_birth" => {
            let r: f64 = rand::random();
            Ok(format!(
                "SELECT ext_name, year_born, count(*) AS cnt, group_concat(entry_id) AS ids \
                 FROM vw_dates \
                 WHERE ext_name=(SELECT ext_name FROM entry WHERE random>={r} AND `type`='Q5' AND q IS NULL ORDER BY random LIMIT 1) \
                 GROUP BY year_born, ext_name HAVING cnt>=2"
            ))
        }
        "" => {
            if !require_catalogs.is_empty() {
                if !require_catalogs.chars().all(|c| c.is_ascii_digit() || c == ',') {
                    return Err(ApiError("invalid require_catalogs".into()));
                }
                return Ok(format!(
                    "SELECT ext_name, count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({require_catalogs}) AND (q IS NULL OR user=0) GROUP BY ext_name HAVING cnt>=3 ORDER BY rand() LIMIT 1"
                ));
            }
            let extra = if min > 0 {
                format!(" cnt>={min} AND")
            } else {
                String::new()
            };
            Ok(format!(
                "SELECT name AS ext_name, cnt FROM {table} WHERE{extra} cnt<15 ORDER BY rand() LIMIT 1"
            ))
        }
        other => Err(ApiError(format!("unknown mode: {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_table_names_are_accepted() {
        assert!(is_safe_table_name("common_names"));
        assert!(is_safe_table_name("common_names_dates"));
        assert!(!is_safe_table_name(""));
        assert!(!is_safe_table_name("table; DROP TABLE"));
    }

    #[test]
    fn cc_mode_sql_default_includes_min() {
        let sql = cc_mode_sql("", "common_names", 3, "", "").unwrap();
        assert!(sql.contains("common_names"));
        assert!(sql.contains("cnt>=3"));
        assert!(sql.contains("cnt<15"));
    }

    #[test]
    fn cc_mode_sql_dates_returns_entry_ids_column() {
        let sql = cc_mode_sql("dates", "common_names_dates", 2, "", "").unwrap();
        assert!(sql.contains("entry_ids"));
        assert!(sql.contains("cnt>=2"));
    }

    #[test]
    fn cc_mode_sql_taxon_omits_entry_ids() {
        let sql = cc_mode_sql("taxon", "common_names_taxon", 3, "", "").unwrap();
        assert!(sql.contains("ext_name"));
        assert!(!sql.contains("entry_ids"));
    }

    #[test]
    fn cc_mode_sql_random_prop_includes_aux_p() {
        let sql = cc_mode_sql("random_prop", "common_names", 1, "227", "").unwrap();
        assert!(sql.contains("aux_candidates"));
        assert!(sql.contains("aux_p=227"));
    }

    #[test]
    fn cc_mode_sql_unknown_mode_errors() {
        assert!(cc_mode_sql("bogus_mode", "t", 3, "", "").is_err());
    }
}
