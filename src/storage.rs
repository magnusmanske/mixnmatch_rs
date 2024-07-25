use crate::{
    coordinate_matcher::LocationRow,
    taxon_matcher::{RankedNames, TaxonNameField},
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
}
