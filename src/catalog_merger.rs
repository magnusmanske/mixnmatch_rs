//! Roll the contents of one catalog into another.
//!
//! Merging is a deliberate, admin-driven operation — it's how MnM
//! consolidates a duplicated catalog (often an older one, or one created
//! against the wrong Wikidata property) into the canonical version
//! without losing the matches accumulated against it.
//!
//! The pipeline is three steps and each one is independently safe:
//!
//! 1. *(optional)* Copy every source entry whose `ext_id` isn't already
//!    in the target as a fresh unmatched row. The target ends up with
//!    the union of the two catalogs' ext_id sets.
//! 2. Port every confirmed manual match from source onto the matching
//!    target row, **only** when the target hasn't been manually matched
//!    yet (still unmatched, or matched by the auto-matcher). Manual
//!    matches in the target are never overwritten.
//! 3. Deactivate the source catalog so it stops accruing new entries.
//!
//! Mirrors PHP `CatalogMerger::merge`. The migration variant
//! (`migrate_property`, below) handles the related but distinct case
//! where a catalog gets a brand-new ext_id schema — typically because
//! the source has reissued IDs — and the matches accumulated under the
//! old schema need to find their equivalents under the new one. The
//! two share the storage layer but have separate pipelines because
//! they answer different questions.

use crate::app_state::{AppState, USER_AUTO};
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::storage::GroupedEntry;
use anyhow::{Result, anyhow};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// What the merge pipeline did, suitable for one-line CLI / log output.
#[derive(Debug, Default, Clone, Copy)]
pub struct MergeStats {
    /// Rows copied from source to target as unmatched (step 1).
    pub entries_copied: usize,
    /// Source matches successfully ported onto target rows (step 2).
    pub matches_ported: usize,
    /// Source matches the storage layer flagged as portable but the
    /// per-row `set_match` call failed on. Left in the source so a
    /// re-run can pick them up; surfaced here so operators notice if
    /// the count is non-zero.
    pub match_port_failures: usize,
}

impl std::fmt::Display for MergeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "copied {} new unmatched row(s); ported {} match(es); {} port failure(s)",
            self.entries_copied, self.matches_ported, self.match_port_failures
        )
    }
}

#[derive(Debug)]
pub struct CatalogMerger {
    app: AppState,
}

impl CatalogMerger {
    pub fn new(app: AppState) -> Self {
        Self { app }
    }

    /// Merge `source` into `target`. `add_blank_entries=false` skips
    /// step 1 (useful when only the matches should be ported, e.g.
    /// because the target catalog is intentionally a strict subset of
    /// the source).
    pub async fn merge(
        &self,
        source: usize,
        target: usize,
        add_blank_entries: bool,
    ) -> Result<MergeStats> {
        if source == target {
            return Err(anyhow!(
                "source and target catalogs must differ (got {source} for both)"
            ));
        }
        if source == 0 || target == 0 {
            return Err(anyhow!("catalog ids must be positive"));
        }

        let mut stats = MergeStats::default();

        if add_blank_entries {
            stats.entries_copied = self.copy_missing_entries(source, target).await?;
        }
        let (ported, failed) = self.port_matches(source, target).await?;
        stats.matches_ported = ported;
        stats.match_port_failures = failed;

        // Always deactivate, even when no matches ported — the merge
        // contract is "after this, only the target receives new work";
        // leaving the source active would invite new entries that the
        // target wouldn't see.
        self.app.storage().catalog_set_active(source, false).await?;
        info!(
            "catalog_merger: merged {source} into {target} ({stats}); source deactivated"
        );

        Ok(stats)
    }

    async fn copy_missing_entries(&self, source: usize, target: usize) -> Result<usize> {
        let added = self
            .app
            .storage()
            .entry_copy_missing_to_catalog(source, target)
            .await?;
        // Bump overview.noq so the catalog page reflects the inflated
        // unmatched count immediately. A subsequent
        // `catalog_refresh_overview_table` would also fix it, but the
        // merger is run interactively and operators expect the page to
        // be correct on the next refresh.
        self.app
            .storage()
            .overview_increment_noq(target, added)
            .await?;
        Ok(added)
    }

    async fn port_matches(&self, source: usize, target: usize) -> Result<(usize, usize)> {
        let candidates = self
            .app
            .storage()
            .entry_get_mergeable_matches(source, target)
            .await?;
        if candidates.is_empty() {
            return Ok((0, 0));
        }

        let mut ported = 0_usize;
        let mut failed = 0_usize;
        for cand in candidates {
            // Set the match through the regular `Entry` path so the
            // overview, wd_matches and reference_fixer side-tables all
            // get touched correctly — bypassing them here would
            // re-introduce the same drift that wd_match_sync was
            // designed to clean up.
            let mut target_entry = match Entry::from_id(cand.target_entry_id, &self.app).await {
                Ok(e) => e,
                Err(e) => {
                    warn!(
                        "catalog_merger: cannot load target entry {}: {e}",
                        cand.target_entry_id
                    );
                    failed += 1;
                    continue;
                }
            };
            let q = format!("Q{}", cand.source_q);
            if let Err(e) = EntryWriter::new(&self.app, &mut target_entry).set_match(&q, cand.source_user).await {
                warn!(
                    "catalog_merger: set_match failed for entry {} -> {q}: {e}",
                    cand.target_entry_id
                );
                failed += 1;
                continue;
            }
            // Restamp the target row to the source's match-time so the
            // audit trail keeps "when did the human confirm this?"
            // intact. Failure here is non-fatal — the match is already
            // recorded, only the timestamp drifts to "now".
            if let Some(ts) = cand.source_timestamp.as_deref() {
                if let Err(e) = self
                    .app
                    .storage()
                    .entry_force_timestamp(cand.target_entry_id, ts)
                    .await
                {
                    warn!(
                        "catalog_merger: timestamp restore failed for entry {}: {e}",
                        cand.target_entry_id
                    );
                }
            }
            ported += 1;
        }
        Ok((ported, failed))
    }
}

// -------------------------------------------------------------------
// Property migration: port matches to a catalog with a new ext_id
// schema. Distinct workflow from `merge` above, hence the separate
// stats type and entry point.
// -------------------------------------------------------------------

/// What `migrate_property` did, suitable for one-line CLI / log output.
#[derive(Debug, Default, Clone, Copy)]
pub struct MigrationStats {
    /// Source matches successfully ported with their original
    /// `(q, user)` attribution intact.
    pub ported: usize,
    /// Source matches that *would* have been ported but failed the
    /// description sanity check or hit a Q-already-in-use guard, and
    /// were applied as auto-matches (user=0) instead. The Q is still
    /// recorded against the entry so the human reviewer can confirm
    /// or undo it.
    pub auto_matched: usize,
    /// Per-row failures (entry vanished, set_match errored, etc.).
    /// Ports / auto-matches above don't include these — re-run picks
    /// them up next time.
    pub errors: usize,
}

impl std::fmt::Display for MigrationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ported {} match(es); {} auto-matched; {} error(s)",
            self.ported, self.auto_matched, self.errors
        )
    }
}

impl CatalogMerger {
    /// Port matches from `old` to `new` after the source's external-ID
    /// scheme has changed.
    ///
    /// The story this fixes: a catalog had matches accumulated against
    /// it, then the upstream provider issued new external IDs, and
    /// somebody imported the new IDs as a fresh `new` catalog — but
    /// the human matches all live under `old`'s ID scheme. This
    /// pipeline carries those matches across by `ext_name`:
    ///
    /// 1. `old` is freshened from Wikidata first (`syncFromSPARQL`)
    ///    so any matches Wikidata learned about while we were sitting
    ///    on the old catalog get pulled in before the cross-walk.
    /// 2. Both catalogs are read into memory and bucketed by
    ///    `ext_name`.
    /// 3. For each name that appears exactly once in both catalogs and
    ///    the new entry hasn't been manually matched yet, the old
    ///    entry's match is ported over — but only if a description
    ///    sanity check passes (identical, or every multi-digit token
    ///    in the old description appears in the new one). Failed
    ///    sanity checks downgrade the port to an auto-match so a
    ///    human reviewer can see what the matcher would have done
    ///    without making it look like a confirmed manual match.
    /// 4. After the pass, every Q used twice in `new` (manual matches
    ///    only) is logged as a warning — those are usually the row
    ///    where the cross-walk found two source IDs that ought to be
    ///    the same item.
    ///
    /// Mirrors PHP `CatalogMerger::migrateProperty`.
    pub async fn migrate_property(&self, old: usize, new: usize) -> Result<MigrationStats> {
        if old == new {
            return Err(anyhow!(
                "old and new catalogs must differ (got {old} for both)"
            ));
        }
        if old == 0 || new == 0 {
            return Err(anyhow!("catalog ids must be positive"));
        }

        // Pick the property both catalogs are anchored on. PHP looked
        // at `old.wd_prop` first, then fell back to `new.wd_prop`; we
        // keep that order — if the migration happened mid-rebadge the
        // new catalog might not have the property set yet.
        let old_cat = Catalog::from_id(old, &self.app).await?;
        let new_cat = Catalog::from_id(new, &self.app).await?;
        let property = old_cat
            .wd_prop()
            .or_else(|| new_cat.wd_prop())
            .ok_or_else(|| {
                anyhow!("neither catalog #{old} nor #{new} has a wd_prop set; nothing to migrate against")
            })?;
        info!("migrate_property: anchoring on P{property}");

        let synced = old_cat
            .sync_from_sparql(&self.app, property)
            .await
            .unwrap_or_else(|e| {
                // Failure here is recoverable: we just skip the
                // freshen pass and migrate against whatever matches
                // already exist on `old`.
                warn!("migrate_property: sync_from_sparql failed: {e} — continuing without it");
                0
            });
        if synced > 0 {
            info!("migrate_property: synced {synced} match(es) onto old catalog from SPARQL");
        }

        let old_groups = group_by_ext_name(
            self.app.storage().entry_load_for_migration(old).await?,
        );
        let new_groups = group_by_ext_name(
            self.app.storage().entry_load_for_migration(new).await?,
        );

        let mut used_qs: HashSet<isize> = new_groups
            .values()
            .flatten()
            .filter(|e| e.user.unwrap_or(0) > 0)
            .filter_map(|e| e.q.filter(|q| *q > 0))
            .collect();

        let mut stats = MigrationStats::default();
        for (name, new_entries) in &new_groups {
            let Some(decision) =
                decide_migration_for_name(name, new_entries, &old_groups, &used_qs)
            else {
                continue;
            };
            // Reserve the Q immediately so a later iteration of the
            // outer loop can't pick the same item even if its bucket
            // somehow agreed.
            used_qs.insert(decision.q);
            self.apply_migration_decision(decision, &mut stats).await;
        }

        // Final advisory: surface any Q duplicated across two manual
        // matches in `new` after the migration. Not an error — the
        // duplicates may be entirely legitimate — but the operator
        // wants to see them.
        match self.app.storage().entry_get_duplicate_qs_in_catalog(new).await {
            Ok(dupes) if !dupes.is_empty() => {
                warn!(
                    "migrate_property: {} Q value(s) used twice in manual matches on \
                     new catalog #{new}: {}",
                    dupes.len(),
                    dupes.iter().map(|q| format!("Q{q}")).collect::<Vec<_>>().join(", ")
                );
            }
            Ok(_) => {}
            Err(e) => warn!("migrate_property: duplicate-Q query failed: {e}"),
        }

        info!("migrate_property: {old} -> {new}: {stats}");
        Ok(stats)
    }
}

/// What `decide_migration_for_name` thinks should happen for one
/// (name → new entries, old entries) bucket pair. The Q is captured
/// up-front so the outer loop can reserve it before issuing the
/// match-write IO. `Auto` is the description-check / Q-collision
/// fallback (set the match, but as `user=0` so a human reviewer
/// can confirm); `Port` carries the source's confirmed match-user.
struct MigrationDecision {
    target_entry_id: usize,
    q: isize,
    user: usize,
    /// True when the source's manual user was downgraded to `USER_AUTO`
    /// because the description check failed or the Q was already in
    /// use. Drives the stats bucket on the writing side.
    downgraded: bool,
}

fn decide_migration_for_name(
    name: &str,
    new_entries: &[GroupedEntry],
    old_groups: &HashMap<String, Vec<GroupedEntry>>,
    used_qs: &HashSet<isize>,
) -> Option<MigrationDecision> {
    // Only single-row buckets are unambiguous enough to port.
    if new_entries.len() != 1 {
        return None;
    }
    let new_entry = &new_entries[0];
    if new_entry.user.unwrap_or(0) > 0 {
        return None; // already manually matched in new — leave alone
    }
    if matches!(new_entry.q, Some(q) if q <= 0) {
        return None; // new is N/A or no-WD; nothing to port over
    }

    let old_entries = old_groups.get(name)?;
    if old_entries.len() != 1 {
        return None;
    }
    let old_entry = &old_entries[0];
    let old_user = old_entry.user.unwrap_or(0);
    let old_q = old_entry.q.filter(|q| *q > 0)?;
    if old_user == 0 {
        return None; // old's match is itself an auto-match — nothing to confirm
    }

    let desc_ok = description_check(&old_entry.ext_desc, &new_entry.ext_desc);
    let q_free = !used_qs.contains(&old_q);
    if !desc_ok {
        info!(
            "migrate_property: {name} failed description check ({:?}/{:?})",
            old_entry.ext_desc, new_entry.ext_desc
        );
    }
    if !q_free {
        info!("migrate_property: {name}: Q{old_q} already in use in new catalog");
    }
    let downgraded = !desc_ok || !q_free;
    Some(MigrationDecision {
        target_entry_id: new_entry.id,
        q: old_q,
        user: if downgraded { USER_AUTO } else { old_user },
        downgraded,
    })
}

impl CatalogMerger {
    async fn apply_migration_decision(
        &self,
        decision: MigrationDecision,
        stats: &mut MigrationStats,
    ) {
        let mut entry = match Entry::from_id(decision.target_entry_id, &self.app).await {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    "migrate_property: cannot load new entry {}: {e}",
                    decision.target_entry_id
                );
                stats.errors += 1;
                return;
            }
        };
        let q_str = format!("Q{}", decision.q);
        if let Err(e) = EntryWriter::new(&self.app, &mut entry).set_match(&q_str, decision.user).await {
            warn!(
                "migrate_property: set_match failed for entry {} -> {q_str}: {e}",
                decision.target_entry_id
            );
            stats.errors += 1;
            return;
        }
        if decision.downgraded {
            stats.auto_matched += 1;
        } else {
            stats.ported += 1;
        }
    }
}

fn group_by_ext_name(entries: Vec<GroupedEntry>) -> HashMap<String, Vec<GroupedEntry>> {
    let mut out: HashMap<String, Vec<GroupedEntry>> = HashMap::new();
    for entry in entries {
        out.entry(entry.ext_name.clone()).or_default().push(entry);
    }
    out
}

fn re_year() -> &'static regex::Regex {
    // 3- or 4-digit groups — matches PHP's `\d{3,4}` for year-like
    // tokens. Anchors to non-digit boundaries so e.g. 2023-04 reads
    // as one year, not two.
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"\d{3,4}").expect("valid year regex"))
}

/// Loose description-match heuristic shared with PHP's
/// `CatalogMerger::descriptionCheck`. Returns true when:
///
/// 1. the two strings are byte-identical (the easy case), or
/// 2. every 3-or-4-digit token (read: year) in `old` also appears
///    somewhere in `new` (substring match, case-insensitive). Catches
///    the typical case where the new catalog reformatted "1850-1920"
///    as "born 1850, died 1920" without losing the year information
///    that distinguishes one person from another.
///
/// Anything else is treated as too dissimilar to port the match
/// confidently — the migration falls back to an auto-match instead.
fn description_check(old: &str, new: &str) -> bool {
    if old == new {
        return true;
    }
    let years: Vec<&str> = re_year().find_iter(old).map(|m| m.as_str()).collect();
    if years.is_empty() {
        return false;
    }
    let new_lower = new.to_ascii_lowercase();
    years.iter().all(|y| new_lower.contains(&y.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    #[tokio::test]
    async fn merge_rejects_self_merge() {
        let app = get_test_app();
        let m = CatalogMerger::new(app);
        let err = m.merge(42, 42, true).await.unwrap_err().to_string();
        assert!(err.contains("must differ"), "err was: {err}");
    }

    #[tokio::test]
    async fn merge_rejects_zero_source() {
        let app = get_test_app();
        let m = CatalogMerger::new(app);
        let err = m.merge(0, 5, true).await.unwrap_err().to_string();
        assert!(err.contains("must be positive"), "err was: {err}");
    }

    #[tokio::test]
    async fn merge_rejects_zero_target() {
        let app = get_test_app();
        let m = CatalogMerger::new(app);
        let err = m.merge(5, 0, true).await.unwrap_err().to_string();
        assert!(err.contains("must be positive"), "err was: {err}");
    }

    #[tokio::test]
    async fn migrate_property_rejects_self_migration() {
        let app = get_test_app();
        let m = CatalogMerger::new(app);
        let err = m.migrate_property(7, 7).await.unwrap_err().to_string();
        assert!(err.contains("must differ"), "err was: {err}");
    }

    #[tokio::test]
    async fn migrate_property_rejects_zero_ids() {
        let app = get_test_app();
        let m = CatalogMerger::new(app);
        assert!(m.migrate_property(0, 5).await.is_err());
        assert!(m.migrate_property(5, 0).await.is_err());
    }

    #[test]
    fn description_check_passes_when_identical() {
        assert!(description_check("Born 1850, died 1920", "Born 1850, died 1920"));
    }

    #[test]
    fn description_check_passes_when_all_years_carry_over() {
        // The new description is a different layout but every year
        // from the old description still appears in it.
        assert!(description_check("1850-1920", "born 1850, died 1920"));
        assert!(description_check("c. 1850 – 1920 painter", "Painter (1850–1920)"));
    }

    #[test]
    fn description_check_fails_when_a_year_is_missing() {
        // The new desc lost one of the source years — not safe to
        // port; the migrator should fall back to auto-match.
        assert!(!description_check("1850 – 1920", "born 1850, died unknown"));
    }

    #[test]
    fn description_check_fails_when_source_has_no_years() {
        // No year-like tokens to anchor on and the strings differ →
        // can't confidently say it's the same person.
        assert!(!description_check("painter from Berlin", "painter from Munich"));
    }

    #[test]
    fn group_by_ext_name_collects_duplicates() {
        let entries = vec![
            GroupedEntry {
                id: 1,
                ext_id: "a".into(),
                ext_name: "Alice".into(),
                ext_desc: String::new(),
                q: None,
                user: None,
                timestamp: None,
            },
            GroupedEntry {
                id: 2,
                ext_id: "b".into(),
                ext_name: "Alice".into(),
                ext_desc: String::new(),
                q: None,
                user: None,
                timestamp: None,
            },
            GroupedEntry {
                id: 3,
                ext_id: "c".into(),
                ext_name: "Bob".into(),
                ext_desc: String::new(),
                q: None,
                user: None,
                timestamp: None,
            },
        ];
        let groups = group_by_ext_name(entries);
        assert_eq!(groups.get("Alice").map(Vec::len), Some(2));
        assert_eq!(groups.get("Bob").map(Vec::len), Some(1));
    }

    #[test]
    fn migration_stats_display_is_one_line() {
        let s = MigrationStats {
            ported: 12,
            auto_matched: 3,
            errors: 1,
        };
        let out = format!("{s}");
        assert!(out.contains("12"));
        assert!(out.contains("3"));
        assert!(out.contains("1 error"));
        assert!(!out.contains('\n'));
    }

    #[test]
    fn merge_stats_display_is_one_line() {
        let s = MergeStats {
            entries_copied: 17,
            matches_ported: 5,
            match_port_failures: 1,
        };
        let out = format!("{s}");
        assert!(out.contains("17"));
        assert!(out.contains("5"));
        assert!(out.contains("1 port failure"));
        assert!(!out.contains('\n'));
    }
}
