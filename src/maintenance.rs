//! Catalog/match maintenance jobs.
//!
//! Two large topical groups have been moved into submodules to
//! reduce file size and improve cohesion:
//!
//! - [`cleanup`] — match-state cleanups (redirects, meta items,
//!   deleted items, GND undifferentiated persons, multi-match
//!   sweeps, sanity checks, inventory-number matches, crossmatch
//!   via aux, html-entity decoding).
//! - [`wikidata_sync`] — jobs that read from or write to Wikidata
//!   (property cache, props_todo, aux candidates, ISO codes,
//!   overwrite_from_wikidata, fixup_wd_matches, ext-url pattern
//!   rewrites, description-aux, fix-aux-item-values).
//!
//! What stays here are the small, mostly-delegating wrappers
//! (taxa/artwork/automatch/common-names/...), the struct
//! definition, and the integration tests.

mod cleanup;
mod wikidata_sync;

use crate::app_state::{AppContext, USER_DATE_MATCH};
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::job::Job;
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Maintenance {
    pub(super) app: Arc<dyn AppContext>,
}

impl Maintenance {
    pub fn new(app: Arc<dyn AppContext>) -> Self {
        Self { app }
    }

    pub async fn common_names_birth_year(&self) -> Result<()> {
        self.app
            .storage()
            .maintenance_common_names_birth_year()
            .await
    }

    pub async fn taxa(&self) -> Result<()> {
        self.app.storage().maintenance_taxa().await
    }

    pub async fn common_aux(&self) -> Result<()> {
        self.app.storage().maintenance_common_aux().await
    }

    pub async fn artwork(&self) -> Result<()> {
        self.app.storage().maintenance_artwork().await
    }

    /// Various small&cheap maintenance tasks
    pub async fn misc_catalog_things(&self) -> Result<()> {
        // Replace all NOWD entries with NOQ (unmatched) entries.
        // This should never happen anymore, but who knows, it's cheap...
        self.app.storage().replace_nowd_with_noq().await?;

        // Remove inactive catalogs from overview table
        // self.app
        //     .storage()
        //     .remove_inactive_catalogs_from_overview()
        //     .await?;

        // Fix overview rows with weird (<0) numbers
        for otr in self.app.storage().get_overview_table().await? {
            if otr.has_weird_numbers() {
                self.app
                    .storage()
                    .catalog_refresh_overview_table(otr.catalog_id())
                    .await?;
            }
        }
        Ok(())
    }

    /// For unmatched entries with day-precision birth and death dates,
    /// finds other, matched entries with the same name and full dates,
    /// then matches them.
    pub async fn match_by_name_and_full_dates(&self) -> Result<()> {
        const BATCH_SIZE: usize = 100;
        let mut results = self
            .app
            .storage()
            .maintenance_match_people_via_name_and_full_dates(BATCH_SIZE)
            .await?;
        results.sort();
        results.dedup();
        for (entry_id, q) in results {
            if let Ok(mut entry) = Entry::from_id(entry_id, self.app.as_ref()).await {
                let _ = EntryWriter::new(self.app.as_ref(), &mut entry)
                    .set_match(&format!("Q{q}"), USER_DATE_MATCH)
                    .await;
            };
        }
        Ok(())
    }

    pub async fn common_names_dates(&self) -> Result<()> {
        self.app.storage().maintenance_common_names_dates().await
    }

    pub async fn common_names_human(&self) -> Result<()> {
        self.app.storage().maintenance_common_names_human().await
    }

    pub async fn create_match_person_dates_jobs_for_catalogs(&self) -> Result<()> {
        self.app
            .storage()
            .create_match_person_dates_jobs_for_catalogs()
            .await?;
        Ok(())
    }

    pub async fn update_has_person_date(&self) -> Result<()> {
        let catalog_ids = self
            .app
            .storage()
            .get_catalogs_with_person_dates_without_flag()
            .await?;
        for catalog_id in catalog_ids {
            let mut catalog = Catalog::from_id(catalog_id, self.app.as_ref()).await?;
            catalog
                .set_has_person_date(self.app.as_ref(), "yes")
                .await?;
            Job::queue_simple_job(self.app.as_ref(), catalog_id, "match_person_dates", None)
                .await?;
            Job::queue_simple_job(self.app.as_ref(), catalog_id, "match_on_birthdate", None)
                .await?;
            Job::queue_simple_job(self.app.as_ref(), catalog_id, "match_on_deathdate", None)
                .await?;
        }
        Ok(())
    }

    pub async fn automatch_people_via_year_born(&self) -> Result<()> {
        self.app
            .storage()
            .maintenance_automatch_people_via_year_born()
            .await
    }

    /// Removes P17 auxiliary values for entryies of type Q5 (human)
    pub async fn remove_p17_for_humans(&self) -> Result<()> {
        self.app.storage().remove_p17_for_humans().await
    }

    pub async fn cleanup_mnm_relations(&self) -> Result<()> {
        self.app.storage().cleanup_mnm_relations().await
    }

    /// Finds some unmatched (Q5) entries where there is a (unique) full match for that name,
    /// and uses it as an auto-match
    pub async fn automatch(&self) -> Result<()> {
        self.app.storage().maintenance_automatch().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_state::MatchState;
    use crate::{
        app_state::get_test_app,
        entry::{Entry, EntryWriter},
        test_support,
    };

    #[test]
    fn property_cache_row_parses_canonical_uris() {
        let row = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://www.wikidata.org/entity/Q5".to_string(),
            "human".to_string(),
        ];
        let parsed =
            super::wikidata_sync::parse_property_cache_row(31, &row).expect("row should parse");
        assert_eq!(parsed.prop_group, 31);
        assert_eq!(parsed.property, 31);
        assert_eq!(parsed.item, 5);
        assert_eq!(parsed.label, "human");
    }

    #[test]
    fn property_cache_row_drops_when_entity_id_missing() {
        // Redirect IRIs occasionally come back from WDQS without a
        // numeric Q on the end; those rows must be silently ignored
        // rather than poisoning the cache with property=0/item=0
        // entries.
        let bad_p = vec![
            "http://www.wikidata.org/entity/redirect-only".to_string(),
            "http://www.wikidata.org/entity/Q5".to_string(),
            "human".to_string(),
        ];
        assert!(super::wikidata_sync::parse_property_cache_row(31, &bad_p).is_none());

        let bad_v = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://example.com/no-qid".to_string(),
            "x".to_string(),
        ];
        assert!(super::wikidata_sync::parse_property_cache_row(31, &bad_v).is_none());
    }

    #[test]
    fn property_cache_row_tolerates_missing_label() {
        // Label is best-effort — wikibase:label can produce a row
        // without it (no English label). Don't drop the row; an empty
        // label is what the cache will hold for it.
        let row = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://www.wikidata.org/entity/Q42".to_string(),
        ];
        let parsed =
            super::wikidata_sync::parse_property_cache_row(31, &row).expect("row should parse");
        assert_eq!(parsed.label, "");
    }

    // The detection-side of unlink_meta_items (P31 → meta-class check)
    // is exercised against a mocked Wikidata API in
    // `wikidata::tests::test_remove_meta_items`. The storage-layer SQL
    // that performs the actual UPDATE is what we test below: catalog
    // scoping (fix #1) and manual-match preservation (fix #2). Using
    // `maintenance_unlink_item_matches` directly skips the API and
    // keeps these regression tests hermetic.

    #[tokio::test]
    async fn test_unlink_item_matches_wipes_auto_match() {
        // Baseline: an auto-match (`user=0`) gets wiped by the unlink
        // SQL — the case the cleanup is *supposed* to handle.
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q16456", 0)
            .await
            .unwrap();
        app.storage()
            .maintenance_unlink_item_matches(catalog_id, vec!["16456".to_string()])
            .await
            .unwrap();
        assert_eq!(Entry::from_id(entry_id, &app).await.unwrap().q, None);
    }

    #[tokio::test]
    async fn test_unlink_item_matches_preserves_manual_match() {
        // Regression for GitHub #6: when a curator has manually
        // confirmed a match (`user > 0`), the unlink SQL must leave it
        // alone — the detection upstream of this call can misfire and
        // we'd rather keep a stale Q-pointer than wipe curation.
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q16456", 2)
            .await
            .unwrap();
        app.storage()
            .maintenance_unlink_item_matches(catalog_id, vec!["16456".to_string()])
            .await
            .unwrap();
        let after = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(after.q, Some(16456), "manual match must be preserved");
        assert_eq!(after.user, Some(2));
    }

    #[tokio::test]
    async fn test_unlink_item_matches_only_affects_target_catalog() {
        // Regression for GitHub #6: a per-catalog microsync run must
        // not wipe matches in *other* catalogs. The pre-fix SQL had no
        // catalog filter, so one catalog's transient blip wiped the
        // same Q tool-wide.
        let app = test_support::test_app().await;
        let (catalog_a, entry_a) = test_support::seed_minimal_entry(&app).await.unwrap();
        let (_catalog_b, entry_b) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut e_a = Entry::from_id(entry_a, &app).await.unwrap();
        EntryWriter::new(&app, &mut e_a)
            .set_match("Q16456", 0)
            .await
            .unwrap();
        let mut e_b = Entry::from_id(entry_b, &app).await.unwrap();
        EntryWriter::new(&app, &mut e_b)
            .set_match("Q16456", 0)
            .await
            .unwrap();
        app.storage()
            .maintenance_unlink_item_matches(catalog_a, vec!["16456".to_string()])
            .await
            .unwrap();
        assert_eq!(
            Entry::from_id(entry_a, &app).await.unwrap().q,
            None,
            "target catalog should be unlinked"
        );
        assert_eq!(
            Entry::from_id(entry_b, &app).await.unwrap().q,
            Some(16456),
            "other catalog's match must survive — no global wipe"
        );
    }

    #[tokio::test]
    async fn test_fix_redirects() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q85756032", 2)
            .await
            .unwrap();
        test_support::seed_wdt_redirect("Q85756032", "Q3819700")
            .await
            .unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.fix_redirects(catalog_id, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry_after = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(entry_after.q, Some(3819700));
    }

    #[tokio::test]
    async fn test_unlink_deleted_items() {
        // Auto-match (user=0) pointing at a Q absent from `page` (treated as
        // deleted) gets unlinked.
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q115205673", 0)
            .await
            .unwrap();
        // Q115205673 intentionally not seeded in `page` → treated as deleted
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.unlink_deleted_items(catalog_id, &MatchState::any_matched())
            .await
            .unwrap();
        let entry_after = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(entry_after.q, None);
    }

    #[tokio::test]
    async fn test_apply_deletions_preserves_manual_match() {
        // Regression for GitHub #6: the global WDRC `apply_deletions` cron
        // walks every entry whose Q was reported deleted and historically
        // wiped them all in one UPDATE — including manual matches. With the
        // user filter, manual matches survive even when the deletion signal
        // is a false positive (Codeberg #124).
        let app = test_support::test_app().await;
        let (_catalog_auto, entry_auto) = test_support::seed_minimal_entry(&app).await.unwrap();
        let (_catalog_manual, entry_manual) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut e_auto = Entry::from_id(entry_auto, &app).await.unwrap();
        EntryWriter::new(&app, &mut e_auto)
            .set_match("Q999000111", 0)
            .await
            .unwrap();
        let mut e_manual = Entry::from_id(entry_manual, &app).await.unwrap();
        EntryWriter::new(&app, &mut e_manual)
            .set_match("Q999000111", 7)
            .await
            .unwrap();

        app.storage()
            .maintenance_apply_deletions(vec![999_000_111])
            .await
            .unwrap();

        assert_eq!(
            Entry::from_id(entry_auto, &app).await.unwrap().q,
            None,
            "auto match should be wiped"
        );
        let after_manual = Entry::from_id(entry_manual, &app).await.unwrap();
        assert_eq!(
            after_manual.q,
            Some(999_000_111),
            "manual match must survive a deletion sweep"
        );
        assert_eq!(after_manual.user, Some(7));
    }

    #[tokio::test]
    async fn test_unlink_deleted_items_preserves_manual_match() {
        // Regression for GitHub #6: a manually-confirmed match must survive
        // the "Q is missing from `page`" check — that signal is not strong
        // enough to override a curator's decision.
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q115205673", 2)
            .await
            .unwrap();
        // Q115205673 intentionally not seeded in `page` → would be flagged as deleted
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.unlink_deleted_items(catalog_id, &MatchState::any_matched())
            .await
            .unwrap();
        let after = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(after.q, Some(115205673), "manual match must be preserved");
        assert_eq!(after.user, Some(2));
    }

    // ── wikidata_sync: update_ext_urls_from_pattern ──────────────────────────

    #[tokio::test]
    async fn test_update_ext_urls_rejects_zero_catalog() {
        let app = test_support::test_app().await;
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms
            .update_ext_urls_from_pattern(0, "https://example.com/$1")
            .await;
        assert!(err.is_err(), "catalog_id=0 must be rejected");
    }

    #[tokio::test]
    async fn test_update_ext_urls_rejects_missing_dollar_one() {
        let app = test_support::test_app().await;
        let (catalog_id, _entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms
            .update_ext_urls_from_pattern(catalog_id, "https://example.com/NOREPLACE")
            .await;
        assert!(err.is_err(), "pattern without '$1' must be rejected");
    }

    #[tokio::test]
    async fn test_update_ext_urls_rewrites_urls() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.update_ext_urls_from_pattern(catalog_id, "https://example.com/$1/view")
            .await
            .unwrap();
        let entry = Entry::from_id(entry_id, &app).await.unwrap();
        let expected = format!("https://example.com/ext_{catalog_id}/view");
        assert_eq!(entry.ext_url, expected);
    }

    // ── wikidata_sync: overwrite_from_wikidata (validation paths) ────────────

    #[tokio::test]
    async fn test_overwrite_from_wikidata_rejects_zero_catalog() {
        let app = test_support::test_app().await;
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms.overwrite_from_wikidata(0).await;
        assert!(err.is_err(), "catalog_id=0 must be rejected");
    }

    #[tokio::test]
    async fn test_overwrite_from_wikidata_rejects_missing_wd_prop() {
        // seed_minimal_entry creates a catalog with wd_prop=NULL
        let app = test_support::test_app().await;
        let (catalog_id, _entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms.overwrite_from_wikidata(catalog_id).await;
        assert!(err.is_err(), "catalog without wd_prop must be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("wd_prop"),
            "error should mention wd_prop; got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_from_wikidata_rejects_inactive_catalog() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::seed_inactive_catalog().await.unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms.overwrite_from_wikidata(catalog_id).await;
        assert!(err.is_err(), "inactive catalog must be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("not active"),
            "error should mention 'not active'; got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_from_wikidata_rejects_wd_qual_catalog() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::seed_catalog_with_wd_qual(214, 813)
            .await
            .unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms.overwrite_from_wikidata(catalog_id).await;
        assert!(err.is_err(), "catalog with wd_qual must be rejected");
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("wd_qual"),
            "error should mention wd_qual; got: {msg}"
        );
    }

    // ── wikidata_sync: delete_multi_match_for_fully_matched ──────────────────

    #[tokio::test]
    async fn test_delete_multi_match_removes_fully_matched() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        // Fully match the entry (user > 0)
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q42", 2)
            .await
            .unwrap();
        // Seed a multi_match row for the now-fully-matched entry
        test_support::seed_multi_match(entry_id, catalog_id)
            .await
            .unwrap();
        assert!(
            test_support::multi_match_entry_exists(entry_id)
                .await
                .unwrap()
        );
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.delete_multi_match_for_fully_matched().await.unwrap();
        assert!(
            !test_support::multi_match_entry_exists(entry_id)
                .await
                .unwrap(),
            "fully-matched entry's multi_match row must be deleted"
        );
    }

    #[tokio::test]
    async fn test_delete_multi_match_spares_unmatched_entry() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        // Entry remains unmatched (q=NULL, user=NULL)
        test_support::seed_multi_match(entry_id, catalog_id)
            .await
            .unwrap();
        assert!(
            test_support::multi_match_entry_exists(entry_id)
                .await
                .unwrap()
        );
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.delete_multi_match_for_fully_matched().await.unwrap();
        assert!(
            test_support::multi_match_entry_exists(entry_id)
                .await
                .unwrap(),
            "unmatched entry's multi_match row must be preserved"
        );
    }

    // ── wikidata_sync: fixup_wd_matches ──────────────────────────────────────

    #[tokio::test]
    async fn test_fixup_wd_matches_removes_inactive_catalog_rows() {
        let app = test_support::test_app().await;
        let inactive_catalog_id = test_support::seed_inactive_catalog().await.unwrap();
        // Use a synthetic entry_id that won't clash with seeded entries
        let synthetic_entry_id = inactive_catalog_id + 9_000_000;
        test_support::seed_wd_match(synthetic_entry_id, inactive_catalog_id)
            .await
            .unwrap();
        assert!(
            test_support::wd_match_entry_exists(synthetic_entry_id)
                .await
                .unwrap()
        );
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.fixup_wd_matches().await.unwrap();
        assert!(
            !test_support::wd_match_entry_exists(synthetic_entry_id)
                .await
                .unwrap(),
            "wd_matches row for an inactive catalog must be deleted"
        );
    }

    // ── wikidata_sync: apply_description_aux ─────────────────────────────────

    #[tokio::test]
    async fn test_apply_description_aux_rejects_zero_catalog() {
        let app = test_support::test_app().await;
        let ms = Maintenance::new(Arc::new(app.clone()));
        let err = ms.apply_description_aux(0).await;
        assert!(err.is_err(), "catalog_id=0 must be rejected");
    }

    #[tokio::test]
    async fn test_apply_description_aux_noop_when_no_rules() {
        // The testcontainer schema ships with an empty description_aux table.
        let app = test_support::test_app().await;
        let (catalog_id, _entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let ms = Maintenance::new(Arc::new(app.clone()));
        ms.apply_description_aux(catalog_id).await.unwrap();
        // No assertion on side effects — success here is "no panic/error"
    }
}
