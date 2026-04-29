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

use crate::app_state::{AppState, USER_DATE_MATCH};
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::job::Job;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Maintenance {
    pub(super) app: AppState,
}

impl Maintenance {
    pub fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
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
            if let Ok(mut entry) = Entry::from_id(entry_id, &self.app).await {
                let _ = EntryWriter::new(&self.app, &mut entry)
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
            Catalog::from_id(catalog_id, &self.app)
                .await?
                .set_has_person_date("yes")
                .await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_person_dates", None).await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_on_birthdate", None).await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_on_deathdate", None).await?;
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
        app_state::{TEST_MUTEX, get_test_app},
        entry::{Entry, EntryWriter},
    };

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

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

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_unlink_meta_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Set a match to a disambiguation item
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q16456", 2)
            .await
            .unwrap();

        // Remove matches to disambiguation items
        let maintenance = Maintenance::new(&app);
        maintenance
            .unlink_meta_items(TEST_CATALOG_ID, &MatchState::any_matched())
            .await
            .unwrap();

        // Check that removal was successful
        assert_eq!(Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap().q, None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q85756032", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.fix_redirects(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(3819700));
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_unlink_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q115205673", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.unlink_deleted_items(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_update_auxiliary_fix_table() {
        let app = get_test_app();
        let ms = Maintenance::new(&app);
        let prop2type = ms.get_sparql_prop2type().await.unwrap();
        assert!(prop2type.len() > 12000);
        assert!(prop2type.iter().any(|(prop, _)| prop == "P31"));
    }
}
