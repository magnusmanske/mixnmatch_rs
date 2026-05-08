//! `impl TaxonQueries for StorageMySQL`. Two reads driving the
//! taxon-matcher subsystem.

use super::StorageMySQL;
use crate::taxon_matcher::{RankedNames, TAXON_RANKS, TaxonMatcher, TaxonNameField};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{params, prelude::*};
use std::collections::HashMap;

#[async_trait]
impl crate::storage::TaxonQueries for StorageMySQL {
    async fn set_catalog_taxon_run(&self, catalog_id: usize, taxon_run: bool) -> Result<()> {
        let taxon_run = taxon_run as u16;
        let sql =
            "UPDATE `catalog` SET `taxon_run`=1 WHERE `id`=:catalog_id AND `taxon_run`=:taxon_run";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id, taxon_run}).await?;
        Ok(())
    }

    async fn match_taxa_get_ranked_names_batch(
        &self,
        ranks: &[&str],
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<(usize, RankedNames)> {
        let results = self
            .match_taxa_get_ranked_names_batch_get_results(
                ranks, field, catalog_id, batch_size, offset,
            )
            .await?;
        let mut ranked_names: RankedNames = HashMap::new();
        for result in &results {
            let entry_id = result.0;
            let taxon_name = match TaxonMatcher::rewrite_taxon_name(catalog_id, &result.1) {
                Some(s) => s,
                None => continue,
            };
            let type_name = &result.2;
            let rank = match TAXON_RANKS.get(type_name.as_str()) {
                Some(rank) => format!(" ; wdt:P105 {rank}"),
                None => "".to_string(),
            };
            ranked_names
                .entry(rank)
                .or_default()
                .push((entry_id, taxon_name));
        }
        Ok((results.len(), ranked_names))
    }
}

#[cfg(test)]
mod tests {
    use crate::taxon_matcher::TaxonNameField;
    use crate::test_support;
    use mysql_async::prelude::*;

    async fn fetch_taxon_run(catalog_id: usize) -> u16 {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: (u16,) = conn
            .exec_first(
                "SELECT `taxon_run` FROM `catalog` WHERE `id`=:id",
                params! { "id" => catalog_id },
            )
            .await
            .unwrap()
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.0
    }

    // ── set_catalog_taxon_run ───────────────────────────────────────────────
    //
    // The SQL is a CAS-style flip: it always *sets* `taxon_run=1` and the
    // bool argument is the *expected current value*. Verify both branches.

    /// Catalog seeded with `taxon_run=0` + bool argument `false`
    /// (i.e. "I expect taxon_run to currently be 0"): WHERE matches, the
    /// row is flipped to 1.
    #[tokio::test]
    async fn set_catalog_taxon_run_flips_zero_to_one_when_expected_zero() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        assert_eq!(fetch_taxon_run(catalog_id).await, 0, "fresh catalog must start at 0");

        app.storage().set_catalog_taxon_run(catalog_id, false).await.unwrap();

        assert_eq!(fetch_taxon_run(catalog_id).await, 1, "WHERE matched → row flipped to 1");
    }

    /// Catalog seeded with `taxon_run=0` + bool argument `true`
    /// (expected current value 1): WHERE doesn't match, no-op.
    #[tokio::test]
    async fn set_catalog_taxon_run_no_op_when_expected_value_disagrees() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();

        app.storage().set_catalog_taxon_run(catalog_id, true).await.unwrap();

        assert_eq!(fetch_taxon_run(catalog_id).await, 0, "WHERE didn't match → row unchanged");
    }

    // ── match_taxa_get_ranked_names_batch ───────────────────────────────────

    /// With no entries seeded for the supplied catalog, the batch query
    /// returns `(0, empty_map)` — exercises the SQL path and the empty
    /// rank-mapping branch.
    #[tokio::test]
    async fn match_taxa_get_ranked_names_batch_empty_for_unseeded_catalog() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();

        let (count, ranked) = app
            .storage()
            .match_taxa_get_ranked_names_batch(
                &["species"],
                &TaxonNameField::Name,
                catalog_id,
                100,
                0,
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert!(ranked.is_empty());
    }
}
