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
//! (`migrateProperty` in PHP) is a sibling operation we haven't ported
//! yet — it solves a different problem (ext_id schema changes) and can
//! be added alongside without changing anything here.

use crate::app_state::AppState;
use crate::entry::Entry;
use anyhow::{Result, anyhow};
use log::{info, warn};

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
            if let Err(e) = target_entry.set_match(&q, cand.source_user).await {
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
