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
