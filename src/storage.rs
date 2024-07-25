use std::collections::HashMap;

use crate::{
    catalog::Catalog,
    coordinate_matcher::LocationRow,
    taxon_matcher::{RankedNames, TaxonNameField},
    update_catalog::UpdateInfo,
};
use anyhow::Result;
use async_trait::async_trait;
// use serde_json::Value;

#[async_trait]
pub trait Storage {
    // fn new(j: &Value) -> impl Storage;
    async fn disconnect(&self) -> Result<()>;

    // Taxon matcher

    async fn set_catalog_taxon_run(&self, catalog_id: usize, taxon_run: bool) -> Result<()>;
    async fn match_taxa_get_ranked_names_batch(
        &self,
        ranks: &Vec<&str>,
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<(usize, RankedNames)>;

    // Coordinate matcher

    async fn get_coordinate_matcher_rows(
        &self,
        catalog_id: &Option<usize>,
        bad_catalogs: &Vec<usize>,
        max_results: usize,
    ) -> Result<Vec<LocationRow>>;
    async fn get_coordinate_matcher_permissions(&self) -> Result<Vec<(usize, String, String)>>;

    // Data source

    async fn get_data_source_type_for_uuid(&self, uuid: &str) -> Result<Vec<String>>;
    async fn get_existing_ext_ids(
        &self,
        placeholders: String,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<String>>;
    async fn update_catalog_get_update_info(&self, catalog_id: usize) -> Result<Vec<UpdateInfo>>;

    // Catalog

    async fn number_of_entries_in_catalog(&self, catalog_id: usize) -> Result<usize>;
    async fn get_catalog_from_id(&self, catalog_id: usize) -> Result<Catalog>;
    async fn get_catalog_key_value_pairs(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, String>>;
    async fn catalog_refresh_overview_table(&self, catalog_id: usize) -> Result<()>;

    // Microsync

    async fn microsync_load_entry_names(
        &self,
        entry_ids: &Vec<usize>,
    ) -> Result<HashMap<usize, String>>;
    async fn microsync_get_multiple_q_in_mnm(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(isize, String, String)>>;
    async fn microsync_get_entries_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &Vec<&String>,
    ) -> Result<Vec<(usize, Option<isize>, Option<usize>, String, String)>>;
}
