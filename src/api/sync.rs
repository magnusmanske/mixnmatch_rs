//! `get_sync`: compare a catalog's external IDs as held in MnM against the
//! same property's values on Wikidata. Reports four buckets:
//!
//! - `mm_dupes`: same ext_id mapped to multiple Qs in MnM
//! - `different`: ext_id maps to Q_a in WD and Q_b in MnM
//! - `wd_no_mm`: ext_id present on WD, absent (matched) in MnM
//! - `mm_no_wd`: ext_id matched in MnM, absent on WD
//! - `mm_double`: Q values that map to multiple MnM entries (returns entry IDs)

use crate::api::common::{self, ApiError, Params, ok};
use crate::app_state::AppState;
use crate::wdqs;
use axum::response::Response;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Axum-shape entry point for `?query=get_sync&catalog=…`.
pub async fn query_get_sync(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    Ok(ok(get(app, catalog).await?))
}

fn re_q() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"(Q\d+)$").expect("valid regex"))
}

pub async fn get(app: &AppState, catalog_id: usize) -> Result<Value, ApiError> {
    if catalog_id == 0 {
        return Err(ApiError("missing required parameter: catalog".into()));
    }

    let (wd_prop, wd_qual) = app
        .storage()
        .get_catalog_wd_prop(catalog_id)
        .await
        .map_err(|e| ApiError(format!("database error: {e}")))?;

    let wd_prop = wd_prop
        .ok_or_else(|| ApiError(format!("catalog {catalog_id} has no wd_prop set")))?;

    if wd_qual.is_some() {
        return Err(ApiError(format!(
            "catalog {catalog_id} uses wd_qual (qualifier-based sync not supported)"
        )));
    }

    // The SPARQL fetch hits Wikidata; the MnM-side reads/dupes queries
    // hit our DB. They don't depend on each other, so fan them out
    // concurrently.
    let s = app.storage();
    let (wd_ext2q_res, mnm_entries_res, mm_double_res) = tokio::join!(
        fetch_wd_ext2q(wd_prop),
        s.get_mnm_matched_entries_for_sync(catalog_id),
        s.get_mnm_double_matches(catalog_id),
    );
    let wd_ext2q = wd_ext2q_res?;
    let mnm_entries =
        mnm_entries_res.map_err(|e| ApiError(format!("database error: {e}")))?;
    let mm_double = mm_double_res.map_err(|e| ApiError(format!("database error: {e}")))?;

    let (mnm_ext2q, mm_dupes) = build_mnm_maps(&mnm_entries);
    let (different, wd_no_mm, mm_no_wd) = compare_maps(&wd_ext2q, &mnm_ext2q);

    let mm_double_json: HashMap<String, Value> = mm_double
        .into_iter()
        .map(|(q, entry_ids)| (q, json!(entry_ids)))
        .collect();

    Ok(json!({
        "mm_dupes": mm_dupes,
        "different": different,
        "wd_no_mm": wd_no_mm,
        "mm_no_wd": mm_no_wd,
        "mm_double": mm_double_json,
    }))
}

/// Fetch Wikidata's view of P{wd_prop} as an `ext_id → Qid` map.
///
/// Two-tier strategy, picked because catalogs whose property has many
/// hundreds of thousands of statements (e.g. P396 with ~290 k) used to
/// fail with `error decoding response body` — the JSON payload was big
/// enough that WDQS would close the chunked stream early and reqwest
/// surfaced the truncation as a hard error.
///
/// 1. Ask for the whole result set in one go, but request TSV rather
///    than JSON — TSV is roughly 3× more compact and streams cleanly,
///    enough to unblock the vast majority of properties.
/// 2. If that still fails (timeout, decode error, transient 5xx), split
///    the query into nine chunks keyed by the leading digit of each
///    item's numeric id (`Q1…`, `Q2…`, …, `Q9…`) using `STRSTARTS`. Each
///    chunk gets its own retry budget; partial results are merged. This
///    is the resilient path for genuinely large properties.
async fn fetch_wd_ext2q(wd_prop: usize) -> Result<HashMap<String, String>, ApiError> {
    let client = wdqs::build_client().map_err(|e| ApiError(e.to_string()))?;
    let single = format!("SELECT ?q ?prop {{ ?q wdt:P{wd_prop} ?prop }}");
    if let Ok(rows) = wdqs::run_tsv_query(&client, &single).await {
        return Ok(rows_to_ext2q(rows));
    }

    let mut out: HashMap<String, String> = HashMap::new();
    let mut last_err: Option<String> = None;
    for digit in 1..=9 {
        let chunk = format!(
            "SELECT ?q ?prop {{ ?q wdt:P{wd_prop} ?prop . \
             FILTER(STRSTARTS(STR(?q), \"http://www.wikidata.org/entity/Q{digit}\")) }}"
        );
        match wdqs::run_tsv_query(&client, &chunk).await {
            Ok(rows) => {
                for (ext_id, q) in rows_to_ext2q(rows) {
                    out.insert(ext_id, q);
                }
            }
            Err(e) => last_err = Some(format!("chunk Q{digit}: {e}")),
        }
    }

    // If every chunk failed, surface the last error rather than a
    // suspiciously-empty success.
    if out.is_empty() {
        return Err(ApiError(format!(
            "SPARQL query failed: {}",
            last_err.unwrap_or_else(|| "unknown error".into())
        )));
    }
    Ok(out)
}

/// Project the parsed two-column `(q-iri, prop-literal)` rows into the
/// `ext_id → Qid` map the comparison code expects. Rows whose `q-iri`
/// doesn't end in a Q-number are silently dropped — WDQS occasionally
/// emits redirect IRIs that don't fit the pattern.
fn rows_to_ext2q(rows: Vec<Vec<String>>) -> HashMap<String, String> {
    let re = re_q();
    let mut out: HashMap<String, String> = HashMap::new();
    for row in rows {
        let mut iter = row.into_iter();
        let (Some(q_url), Some(prop_value)) = (iter.next(), iter.next()) else {
            continue;
        };
        if let Some(caps) = re.captures(&q_url) {
            out.insert(prop_value, caps[1].to_string());
        }
    }
    out
}

fn build_mnm_maps(
    mnm_entries: &[(isize, String)],
) -> (HashMap<String, String>, HashMap<String, Vec<String>>) {
    let mut mnm_ext2q: HashMap<String, String> = HashMap::new();
    let mut mm_dupes: HashMap<String, Vec<String>> = HashMap::new();
    for (q, ext_id) in mnm_entries {
        let q_str = format!("Q{q}");
        if let Some(existing_ext) = mnm_ext2q.get(ext_id) {
            mm_dupes
                .entry(ext_id.clone())
                .or_insert_with(|| vec![existing_ext.clone()])
                .push(q_str.clone());
        }
        mnm_ext2q.insert(ext_id.clone(), q_str);
    }
    (mnm_ext2q, mm_dupes)
}

/// Strip the leading "Q" from a QID and return the numeric part. Returns 0 if
/// the input doesn't parse — the callers upstream only ever hand us values
/// that already matched `Q\d+`, so this is a belt-and-braces fallback.
fn q_to_num(q: &str) -> u64 {
    q.trim_start_matches('Q').parse().unwrap_or(0)
}

fn compare_maps(
    wd_ext2q: &HashMap<String, String>,
    mnm_ext2q: &HashMap<String, String>,
) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let mut different: Vec<Value> = Vec::new();
    // `wd_no_mm` is consumed by `match_q_multi`, which reads each row as
    // `[q_numeric, ext_id]`; `mm_no_wd` is fed to the QuickStatements form
    // which reads the same shape. Keep the tuple layout the JS expects —
    // objects would silently produce "Qundefined / undefined" output.
    let mut wd_no_mm: Vec<Value> = Vec::new();
    let mut mm_no_wd: Vec<Value> = Vec::new();

    for (ext_id, wd_q) in wd_ext2q {
        match mnm_ext2q.get(ext_id) {
            Some(mnm_q) if wd_q != mnm_q => {
                different.push(json!({"ext_id": ext_id, "wd_q": wd_q, "mnm_q": mnm_q}));
            }
            None => {
                wd_no_mm.push(json!([q_to_num(wd_q), ext_id]));
            }
            Some(_) => {} // matching → nothing to report
        }
    }

    for (ext_id, mnm_q) in mnm_ext2q {
        if !wd_ext2q.contains_key(ext_id) {
            mm_no_wd.push(json!([q_to_num(mnm_q), ext_id]));
        }
    }
    (different, wd_no_mm, mm_no_wd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_ext2q_extracts_qid_and_drops_malformed() {
        // Generic TSV unescaping is exercised in the `wdqs` module's
        // own tests; here we just verify the projection from raw rows
        // to the `ext_id → Qid` map this endpoint depends on.
        let rows = vec![
            vec![
                "http://www.wikidata.org/entity/Q42".to_string(),
                "ext-A".to_string(),
            ],
            vec![
                "http://www.wikidata.org/entity/Q9999".to_string(),
                "ext-B".to_string(),
            ],
            // Missing trailing Q-number → must be silently dropped.
            vec!["http://example.com/no-qid".to_string(), "ext-C".to_string()],
            // Single-column row → can't be projected; dropped.
            vec!["only-one".to_string()],
        ];
        let map = rows_to_ext2q(rows);
        assert_eq!(map.get("ext-A").map(String::as_str), Some("Q42"));
        assert_eq!(map.get("ext-B").map(String::as_str), Some("Q9999"));
        assert!(!map.contains_key("ext-C"));
    }

    #[test]
    fn test_q_to_num() {
        assert_eq!(q_to_num("Q42"), 42);
        assert_eq!(q_to_num("Q1"), 1);
        assert_eq!(q_to_num("Q0"), 0);
        assert_eq!(q_to_num(""), 0);
        assert_eq!(q_to_num("not-a-qid"), 0);
    }

    #[test]
    fn test_compare_maps_emits_tuple_rows() {
        // The JS frontend reads both mm_no_wd and wd_no_mm as [q_num, ext_id]
        // tuples — match_q_multi and the QuickStatements form both break if
        // these arrive as objects.
        let mut wd: HashMap<String, String> = HashMap::new();
        wd.insert("wd-only".into(), "Q10".into());
        wd.insert("shared".into(), "Q1".into());
        let mut mnm: HashMap<String, String> = HashMap::new();
        mnm.insert("mm-only".into(), "Q20".into());
        mnm.insert("shared".into(), "Q1".into());

        let (different, wd_no_mm, mm_no_wd) = compare_maps(&wd, &mnm);
        assert!(different.is_empty());
        assert_eq!(wd_no_mm, vec![json!([10_u64, "wd-only"])]);
        assert_eq!(mm_no_wd, vec![json!([20_u64, "mm-only"])]);
    }

    #[test]
    fn test_compare_maps_different_reports_object() {
        // `different` is not consumed by the tuple-eating call sites, so the
        // richer object shape is preserved for future consumers.
        let mut wd: HashMap<String, String> = HashMap::new();
        wd.insert("x".into(), "Q10".into());
        let mut mnm: HashMap<String, String> = HashMap::new();
        mnm.insert("x".into(), "Q20".into());
        let (different, wd_no_mm, mm_no_wd) = compare_maps(&wd, &mnm);
        assert_eq!(
            different,
            vec![json!({"ext_id": "x", "wd_q": "Q10", "mnm_q": "Q20"})]
        );
        assert!(wd_no_mm.is_empty());
        assert!(mm_no_wd.is_empty());
    }
}
