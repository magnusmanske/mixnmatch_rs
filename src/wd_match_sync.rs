//! Reconciliation between MnM's confirmed matches and Wikidata's view of
//! the world. Two passes, one direction each:
//!
//! - [`classify_pending`] reads `wd_matches` rows in `UNKNOWN` / `DIFFERENT`
//!   status, fetches the corresponding Wikidata items, and reclassifies
//!   each row by comparing the catalog property's values against the
//!   entry's `ext_id`.
//! - [`push_wd_missing`] (in `wd_match_push`) takes rows that landed in
//!   `WD_MISSING` and writes the `Pn=ext_id` statement back to Wikidata
//!   over OAuth1.0a.
//!
//! Together these mirror PHP `RecentChangesWatcher::syncWdMatches` and
//! `syncMatchesToWikidata`, but with a few cleanups:
//!
//! - The "populate `wd_matches` queue" pass (PHP `addUnknownBatch`) isn't
//!   needed: every confirmed match already inserts its row inline via
//!   `entry_set_match_cleanup`, so the queue stays current without a
//!   periodic sweep.
//! - The PHP `hasPropertyEverBeenRemovedFromItem` paranoia check is
//!   replaced by re-verifying the live values just before writing —
//!   simpler and equally protective against re-adding deleted statements.
//! - Issue rows for `MULTIPLE` carry the live WD values *and* the MnM
//!   ext_id, so the issues page has everything it needs without a second
//!   query.

use crate::app_state::AppState;
use crate::issue::{Issue, IssueType};
use crate::storage::WdMatchRow;
use anyhow::Result;
use log::{debug, warn};
use serde_json::json;
use std::collections::HashSet;
use wikimisc::wikibase::entity_container::EntityContainer;

/// Default classifier batch size — matches PHP's `sync_wd_matches_limit = 1000`.
/// Picked to keep `wbgetentities` responses comfortably under MediaWiki's
/// 50-item-per-request soft cap (the EntityContainer chunks internally) while
/// still making meaningful progress per invocation.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Properties whose schema is single-valued: a Wikidata item should only
/// ever carry one statement of these. If the live item has *any* value for
/// such a property and it doesn't match the MnM `ext_id`, we don't try to
/// "fix" it — we mark the row `MULTIPLE` and surface an issue for human
/// review. Everything else is treated as multi-valued so a `DIFFERENT`
/// outcome just becomes another statement candidate later.
///
/// Source: `RecentChangesWatcher::$respect_single_value_for_property`.
const SINGLE_VALUE_PROPS: &[usize] = &[
    1015, // Bibsys
    1871, // Bach digital ID
    1580, // UB Mannheim
    535,  // Find a Grave memorial ID
];

/// Status string the row should transition to.
///
/// Kept as a small enum (rather than passing strings around) so the
/// classifier and the storage layer can't drift on spelling — every
/// status that appears in `wd_matches.status` is here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WdMatchStatus {
    /// `wbgetentities` couldn't find the item (deleted, malformed, etc.).
    NotApplicable,
    /// WD already carries our `ext_id` for the catalog property.
    Same,
    /// WD has a different value (or values) for the catalog property.
    Different,
    /// WD doesn't list this property at all — candidate for write-back.
    WdMissing,
    /// Single-valued property already has another value; raises an issue.
    Multiple,
}

impl WdMatchStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "N/A",
            Self::Same => "SAME",
            Self::Different => "DIFFERENT",
            Self::WdMissing => "WD_MISSING",
            Self::Multiple => "MULTIPLE",
        }
    }
}

/// Per-status counters returned to callers (CLI, job runner) so they can
/// log progress in a single line.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClassifyStats {
    pub processed: usize,
    pub same: usize,
    pub different: usize,
    pub wd_missing: usize,
    pub multiple: usize,
    pub not_applicable: usize,
}

impl ClassifyStats {
    fn record(&mut self, status: WdMatchStatus) {
        self.processed += 1;
        match status {
            WdMatchStatus::Same => self.same += 1,
            WdMatchStatus::Different => self.different += 1,
            WdMatchStatus::WdMissing => self.wd_missing += 1,
            WdMatchStatus::Multiple => self.multiple += 1,
            WdMatchStatus::NotApplicable => self.not_applicable += 1,
        }
    }
}

impl std::fmt::Display for ClassifyStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} processed (same={}, different={}, wd_missing={}, multiple={}, n/a={})",
            self.processed,
            self.same,
            self.different,
            self.wd_missing,
            self.multiple,
            self.not_applicable,
        )
    }
}

/// Walk the `UNKNOWN` and `DIFFERENT` buckets of `wd_matches`, classify
/// each row against live Wikidata, and write the new status back.
///
/// Both buckets need re-checking: `UNKNOWN` is the freshly-queued bucket
/// every confirmed match lands in; `DIFFERENT` re-runs because a previous
/// classification may have happened before WD picked up the value.
pub async fn classify_pending(app: &AppState, batch_size: usize) -> Result<ClassifyStats> {
    let mut stats = ClassifyStats::default();
    for status in ["UNKNOWN", "DIFFERENT"] {
        let rows = app.storage().wd_matches_get_batch(status, batch_size).await?;
        if rows.is_empty() {
            debug!("wd_matches: no rows in {status}");
            continue;
        }
        debug!("wd_matches: classifying {} rows from {status}", rows.len());
        let batch_stats = classify_batch(app, &rows).await?;
        stats.processed += batch_stats.processed;
        stats.same += batch_stats.same;
        stats.different += batch_stats.different;
        stats.wd_missing += batch_stats.wd_missing;
        stats.multiple += batch_stats.multiple;
        stats.not_applicable += batch_stats.not_applicable;
    }
    Ok(stats)
}

/// Classify a single batch of rows. Loads the corresponding items from
/// Wikidata in one `wbgetentities` round-trip, then transitions each
/// row's status without further network traffic.
async fn classify_batch(app: &AppState, rows: &[WdMatchRow]) -> Result<ClassifyStats> {
    let entities = load_items_for_rows(app, rows).await?;
    let mut stats = ClassifyStats::default();
    for row in rows {
        let q_label = format!("Q{}", row.q_numeric);
        let item = entities.get_entity(q_label.clone());
        let outcome = classify_row(row, item.as_ref());

        if let Outcome::Multiple { wd_values } = &outcome {
            // Best-effort: classification still proceeds even if the issue
            // can't be inserted — the status flip is the load-bearing part,
            // the issue row is a UI hint.
            let payload = json!({ "wd": wd_values, "mnm": row.ext_id });
            match Issue::new(row.entry_id, IssueType::Multiple, payload, app).await {
                Ok(issue) => {
                    if let Err(e) = issue.insert().await {
                        warn!("wd_matches: issue insert failed for entry {}: {e}", row.entry_id);
                    }
                }
                Err(e) => warn!(
                    "wd_matches: issue construction failed for entry {}: {e}",
                    row.entry_id
                ),
            }
        }

        let status = outcome.status();
        if let Err(e) = app
            .storage()
            .wd_matches_set_status(row.entry_id, status.as_str())
            .await
        {
            warn!(
                "wd_matches: status update failed for entry {} ({}): {e}",
                row.entry_id,
                status.as_str()
            );
            continue;
        }
        stats.record(status);
    }
    Ok(stats)
}

async fn load_items_for_rows(app: &AppState, rows: &[WdMatchRow]) -> Result<EntityContainer> {
    // Dedup before hitting WD — a single Q can legitimately appear in
    // multiple rows when several catalogs share the same target item.
    let qs: Vec<String> = rows
        .iter()
        .map(|r| format!("Q{}", r.q_numeric))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let api = app.wikidata().get_mw_api().await?;
    let entities = EntityContainer::new();
    // load_entities returns Err if any QID is malformed, but a missing
    // item (deleted/redirect) just yields no entry — exactly what we
    // want, since `Outcome::NotApplicable` covers that case.
    if let Err(e) = entities.load_entities(&api, &qs).await {
        warn!("wd_matches: load_entities had errors (continuing): {e}");
    }
    Ok(entities)
}

/// Internal classification result. Splits `Multiple` from the plain
/// status enum because the multiple case carries extra payload (the
/// live WD values) that the issue row needs.
enum Outcome {
    Status(WdMatchStatus),
    Multiple { wd_values: Vec<String> },
}

impl Outcome {
    fn status(&self) -> WdMatchStatus {
        match self {
            Self::Status(s) => *s,
            Self::Multiple { .. } => WdMatchStatus::Multiple,
        }
    }
}

fn classify_row(row: &WdMatchRow, item: Option<&wikimisc::wikibase::Entity>) -> Outcome {
    let Some(item) = item else {
        return Outcome::Status(WdMatchStatus::NotApplicable);
    };
    let values = string_values_for_property(item, row.wd_prop);
    decide(&row.ext_id, row.wd_prop, &values)
}

/// Pure decision function — takes the raw inputs and returns the
/// outcome, no IO. Lets the classifier be tested without standing up
/// any Wikidata or DB stubs.
fn decide(ext_id: &str, wd_prop: usize, values: &[String]) -> Outcome {
    if values.iter().any(|v| v == ext_id) {
        return Outcome::Status(WdMatchStatus::Same);
    }
    if values.is_empty() {
        return Outcome::Status(WdMatchStatus::WdMissing);
    }
    if SINGLE_VALUE_PROPS.contains(&wd_prop) {
        return Outcome::Multiple {
            wd_values: values.to_vec(),
        };
    }
    Outcome::Status(WdMatchStatus::Different)
}

/// Extract every string-typed datavalue for `Pnnn` from `item`. Same
/// shape as `wdrc::sync_property_propval2item_get_prop_values` — kept
/// local rather than shared because the wdrc one walks an `Entity` by
/// value and this one wants to borrow.
fn string_values_for_property(item: &wikimisc::wikibase::Entity, property: usize) -> Vec<String> {
    item.claims_with_property(format!("P{property}"))
        .iter()
        .filter_map(|s| s.main_snak().data_value().to_owned())
        .filter_map(|dv| match dv.value() {
            wikimisc::wikibase::Value::StringValue(v) => Some(v.to_owned()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ext_id: &str, prop: usize) -> WdMatchRow {
        WdMatchRow {
            entry_id: 1,
            catalog_id: 1,
            ext_id: ext_id.to_string(),
            q_numeric: 42,
            wd_prop: prop,
        }
    }

    #[test]
    fn decide_same_when_ext_id_present() {
        let r = row("abc", 213);
        let out = decide(&r.ext_id, r.wd_prop, &["abc".to_string()]);
        assert!(matches!(out, Outcome::Status(WdMatchStatus::Same)));
    }

    #[test]
    fn decide_same_when_ext_id_among_many_values() {
        let r = row("abc", 213); // P213 (ISNI) is multi-valued in our list
        let out = decide(&r.ext_id, r.wd_prop, &["xyz".into(), "abc".into()]);
        assert!(matches!(out, Outcome::Status(WdMatchStatus::Same)));
    }

    #[test]
    fn decide_wd_missing_when_no_values() {
        let r = row("abc", 213);
        let out = decide(&r.ext_id, r.wd_prop, &[]);
        assert!(matches!(out, Outcome::Status(WdMatchStatus::WdMissing)));
    }

    #[test]
    fn decide_different_for_multi_valued_prop() {
        let r = row("abc", 213); // P213: multi-valued
        let out = decide(&r.ext_id, r.wd_prop, &["xyz".into()]);
        assert!(matches!(out, Outcome::Status(WdMatchStatus::Different)));
    }

    #[test]
    fn decide_multiple_for_single_valued_prop() {
        // P535 (Find a Grave) is single-valued — divergent value → MULTIPLE.
        let r = row("12345", 535);
        let out = decide(&r.ext_id, r.wd_prop, &["67890".into()]);
        match out {
            Outcome::Multiple { wd_values } => assert_eq!(wd_values, vec!["67890".to_string()]),
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn decide_multiple_carries_all_wd_values() {
        // Even when WD has several values for a single-valued property,
        // include every one in the issue payload so the human reviewer
        // can see the full picture without re-querying.
        let r = row("99", 535);
        let out = decide(&r.ext_id, r.wd_prop, &["a".into(), "b".into(), "c".into()]);
        match out {
            Outcome::Multiple { wd_values } => {
                assert_eq!(wd_values, vec!["a".to_string(), "b".into(), "c".into()]);
            }
            _ => panic!("expected Multiple"),
        }
    }

    #[test]
    fn decide_same_takes_precedence_over_single_value_check() {
        // If our ext_id is the only value WD lists, that's SAME — never
        // MULTIPLE, even though the property is single-valued.
        let r = row("12345", 535);
        let out = decide(&r.ext_id, r.wd_prop, &["12345".into()]);
        assert!(matches!(out, Outcome::Status(WdMatchStatus::Same)));
    }

    #[test]
    fn status_strings_match_php() {
        // These literals must match what PHP writes to wd_matches.status —
        // the same database row is seen by both implementations during
        // the migration window. Drift here silently breaks the sync.
        assert_eq!(WdMatchStatus::NotApplicable.as_str(), "N/A");
        assert_eq!(WdMatchStatus::Same.as_str(), "SAME");
        assert_eq!(WdMatchStatus::Different.as_str(), "DIFFERENT");
        assert_eq!(WdMatchStatus::WdMissing.as_str(), "WD_MISSING");
        assert_eq!(WdMatchStatus::Multiple.as_str(), "MULTIPLE");
    }

    #[test]
    fn stats_records_each_status_correctly() {
        let mut s = ClassifyStats::default();
        s.record(WdMatchStatus::Same);
        s.record(WdMatchStatus::Same);
        s.record(WdMatchStatus::Different);
        s.record(WdMatchStatus::WdMissing);
        s.record(WdMatchStatus::Multiple);
        s.record(WdMatchStatus::NotApplicable);
        assert_eq!(s.processed, 6);
        assert_eq!(s.same, 2);
        assert_eq!(s.different, 1);
        assert_eq!(s.wd_missing, 1);
        assert_eq!(s.multiple, 1);
        assert_eq!(s.not_applicable, 1);
    }
}
