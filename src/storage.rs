use std::collections::HashMap;

use crate::{
    automatch::{ResultInOriginalCatalog, ResultInOtherCatalog},
    auxiliary_matcher::AuxiliaryResults,
    catalog::Catalog,
    coordinate_matcher::LocationRow,
    entry::Entry,
    issue::Issue,
    job::{JobRow, JobStatus, TaskSize},
    mixnmatch::MatchState,
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
        ranks: &[&str],
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<(usize, RankedNames)>;

    // Coordinate matcher

    async fn get_coordinate_matcher_rows(
        &self,
        catalog_id: &Option<usize>,
        bad_catalogs: &[usize],
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

    // MixNMatch
    //
    async fn update_overview_table(
        &self,
        old_entry: &Entry,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()>;
    async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<()>;
    async fn avoid_auto_match(&self, entry_id: usize, q_numeric: Option<isize>) -> Result<bool>;
    async fn get_random_active_catalog_id_with_property(&self) -> Option<usize>;
    async fn get_kv_value(&self, key: &str) -> Result<Option<String>>;
    async fn set_kv_value(&self, key: &str, value: &str) -> Result<()>;

    // Issue

    async fn issue_insert(&self, issue: &Issue) -> Result<()>;

    // Autoscrape

    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>>;
    async fn autoscrape_get_entry_ids_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &Vec<String>,
    ) -> Result<Vec<(String, usize)>>;
    async fn autoscrape_start(&self, autoscrape_id: usize) -> Result<()>;
    async fn autoscrape_finish(&self, autoscrape_id: usize, last_run_urls: usize) -> Result<()>;

    // Auxiliary matcher

    async fn auxiliary_matcher_match_via_aux(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
        extid_props: &Vec<String>,
        blacklisted_catalogs: &Vec<String>,
    ) -> Result<Vec<AuxiliaryResults>>;
    async fn auxiliary_matcher_add_auxiliary_to_wikidata(
        &self,
        blacklisted_properties: &Vec<String>,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AuxiliaryResults>>;

    // Maintenance

    async fn remove_p17_for_humans(&self) -> Result<()>;
    async fn cleanup_mnm_relations(&self) -> Result<()>;
    async fn maintenance_sync_redirects(&self, redirects: HashMap<isize, isize>) -> Result<()>;
    async fn maintenance_apply_deletions(&self, deletions: Vec<isize>) -> Result<Vec<usize>>;
    async fn maintenance_get_prop2catalog_ids(&self) -> Result<Vec<(usize, usize)>>;
    async fn maintenance_sync_property(
        &self,
        catalogs: &Vec<usize>,
        propval2item: &HashMap<String, isize>,
        params: Vec<String>,
    ) -> Result<Vec<(usize, String, Option<usize>, Option<usize>)>>;
    async fn maintenance_fix_redirects(&self, from: isize, to: isize) -> Result<()>;
    async fn maintenance_unlink_item_matches(&self, items: Vec<String>) -> Result<()>;
    async fn maintenance_automatch(&self) -> Result<()>;
    async fn get_items(
        &self,
        catalog_id: usize,
        offset: usize,
        state: &MatchState,
    ) -> Result<Vec<String>>;

    // Jobs

    async fn jobs_get_tasks(&self) -> Result<HashMap<String, TaskSize>>;
    async fn reset_running_jobs(&self) -> Result<()>;
    async fn reset_failed_jobs(&self) -> Result<()>;
    async fn jobs_queue_simple_job(
        &self,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
        status: &str,
        timestamp: String,
    ) -> Result<usize>;
    async fn jobs_reset_json(&self, job_id: usize, timestamp: String) -> Result<()>;
    async fn jobs_set_json(
        &self,
        job_id: usize,
        json_string: String,
        timestamp: &String,
    ) -> Result<()>;
    async fn jobs_row_from_id(&self, job_id: usize) -> Result<JobRow>;
    async fn jobs_set_status(
        &self,
        status: &JobStatus,
        job_id: usize,
        timestamp: String,
    ) -> Result<()>;
    async fn jobs_set_note(&self, note: Option<String>, job_id: usize) -> Result<Option<String>>;
    async fn jobs_update_next_ts(&self, job_id: usize, next_ts: String) -> Result<()>;
    async fn jobs_get_next_job(
        &self,
        status: JobStatus,
        depends_on: Option<JobStatus>,
        no_actions: &Vec<String>,
        next_ts: Option<String>,
    ) -> Option<usize>;

    // Automatch

    async fn automatch_by_sitelink_get_entries(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String)>>;
    async fn automatch_by_search_get_results(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String, String, String)>>;
    async fn automatch_creations_get_results(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(String, usize, String)>>;
    async fn automatch_simple_get_results(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String, String, String)>>;
    async fn automatch_from_other_catalogs_get_results(
        &self,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<ResultInOriginalCatalog>>;
    async fn automatch_from_other_catalogs_get_results2(
        &self,
        results_in_original_catalog: &Vec<ResultInOriginalCatalog>,
        ext_names: Vec<String>,
    ) -> Result<Vec<ResultInOtherCatalog>>;
    async fn purge_automatches(&self, catalog_id: usize) -> Result<()>;
    async fn match_person_by_dates_get_results(
        &self,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<(usize, String, String, String)>>;
    async fn match_person_by_single_date_get_results(
        &self,
        match_field: &str,
        catalog_id: usize,
        precision: i32,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<(usize, String, String, String)>>;
    async fn automatch_complex_get_el_chunk(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String)>>;
}
