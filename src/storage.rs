use crate::{
    automatch::{ResultInOriginalCatalog, ResultInOtherCatalog},
    auxiliary_matcher::AuxiliaryResults,
    catalog::Catalog,
    cersei::CurrentScraper,
    coordinate_matcher::LocationRow,
    entry::{AuxiliaryRow, CoordinateLocation, Entry},
    issue::Issue,
    job_row::JobRow,
    job_status::JobStatus,
    match_state::MatchState,
    prop_todo::PropTodo,
    task_size::TaskSize,
    taxon_matcher::{RankedNames, TaxonNameField},
    update_catalog::UpdateInfo,
};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use wikimisc::wikibase::LocaleString;

#[async_trait]
pub trait Storage: std::fmt::Debug + Send + Sync {
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
    async fn get_all_catalogs_key_value_pairs(&self) -> Result<Vec<(usize, String, String)>>;

    // Data source

    async fn get_data_source_type_for_uuid(&self, uuid: &str) -> Result<Vec<String>>;
    async fn get_existing_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<String>>;
    async fn update_catalog_get_update_info(&self, catalog_id: usize) -> Result<Vec<UpdateInfo>>;

    // Catalog

    async fn create_catalog(&self, catalog: &Catalog) -> Result<usize>;
    async fn number_of_entries_in_catalog(&self, catalog_id: usize) -> Result<usize>;
    async fn get_catalog_from_id(&self, catalog_id: usize) -> Result<Catalog>;
    async fn get_catalog_from_name(&self, name: &str) -> Result<Catalog>;
    async fn get_catalog_key_value_pairs(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, String>>;
    async fn catalog_refresh_overview_table(&self, catalog_id: usize) -> Result<()>;
    async fn catalog_get_entries_of_people_with_initials(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<Entry>>;

    // Microsync

    async fn microsync_load_entry_names(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, String>>;
    async fn microsync_get_multiple_q_in_mnm(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(isize, String, String)>>;
    async fn microsync_get_entries_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[&String],
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
    async fn do_catalog_entries_have_person_date(&self, catalog_id: usize) -> Result<bool>;
    async fn set_has_person_date(&self, catalog_id: usize, new_has_person_date: &str)
        -> Result<()>;

    // Issue

    async fn issue_insert(&self, issue: &Issue) -> Result<()>;

    // Autoscrape

    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>>;
    async fn autoscrape_get_entry_ids_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<(String, usize)>>;
    async fn autoscrape_start(&self, autoscrape_id: usize) -> Result<()>;
    async fn autoscrape_finish(&self, autoscrape_id: usize, last_run_urls: usize) -> Result<()>;

    // Auxiliary matcher

    async fn auxiliary_matcher_match_via_aux(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
        extid_props: &[String],
        blacklisted_catalogs: &[String],
    ) -> Result<Vec<AuxiliaryResults>>;
    async fn auxiliary_matcher_add_auxiliary_to_wikidata(
        &self,
        blacklisted_properties: &[String],
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AuxiliaryResults>>;

    // Maintenance

    async fn get_props_todo(&self) -> Result<Vec<PropTodo>>;
    async fn add_props_todo(&self, new_props: Vec<PropTodo>) -> Result<()>;
    async fn mark_props_todo_as_has_catalog(&self) -> Result<()>;
    async fn set_props_todo_items_using(&self, prop_numeric: u64, cnt: u64) -> Result<()>;
    async fn remove_p17_for_humans(&self) -> Result<()>;
    async fn cleanup_mnm_relations(&self) -> Result<()>;
    async fn create_match_person_dates_jobs_for_catalogs(&self) -> Result<()>;
    async fn maintenance_sync_redirects(&self, redirects: HashMap<isize, isize>) -> Result<()>;
    async fn maintenance_apply_deletions(&self, deletions: Vec<isize>) -> Result<Vec<usize>>;
    async fn maintenance_get_prop2catalog_ids(&self) -> Result<Vec<(usize, usize)>>;
    async fn maintenance_sync_property(
        &self,
        catalogs: &[usize],
        propval2item: &HashMap<String, isize>,
        params: Vec<String>,
    ) -> Result<Vec<(usize, String, Option<usize>, Option<usize>)>>;
    async fn maintenance_fix_redirects(&self, from: isize, to: isize) -> Result<()>;
    async fn maintenance_unlink_item_matches(&self, items: Vec<String>) -> Result<()>;
    async fn automatch_people_with_birth_year(&self, catalog_id: usize) -> Result<()>;
    async fn maintenance_automatch(&self) -> Result<()>;
    async fn maintenance_automatch_people_via_year_born(&self) -> Result<()>;
    async fn maintenance_match_people_via_name_and_full_dates(
        &self,
        batch_size: usize,
    ) -> Result<Vec<(usize, usize)>>;
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
        timestamp: &str,
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
        no_actions: &[String],
        next_ts: Option<String>,
    ) -> Option<usize>;

    // Automatch

    async fn automatch_entry_by_sparql(
        &self,
        catalog_id: usize,
        q_numeric: usize,
        label: &str,
    ) -> Result<()>;

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
        results_in_original_catalog: &[ResultInOriginalCatalog],
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

    // Entry

    async fn entry_from_id(&self, entry_id: usize) -> Result<Entry>;
    async fn entry_from_ext_id(&self, catalog_id: usize, ext_id: &str) -> Result<Entry>;
    async fn multiple_from_ids(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Entry>>;
    async fn get_entry_batch(
        &self,
        catalog_id: usize,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entry>>;
    async fn entry_insert_as_new(&self, entry: &Entry) -> Result<usize>;
    async fn entry_delete(&self, entry_id: usize) -> Result<()>;
    async fn entry_get_creation_time(&self, entry_id: usize) -> Option<String>;
    async fn entry_set_ext_name(&self, ext_name: &str, entry_id: usize) -> Result<()>;
    async fn entry_set_auxiliary_in_wikidata(&self, in_wikidata: bool, aux_id: usize)
        -> Result<()>;
    async fn entry_set_ext_desc(&self, ext_desc: &str, entry_id: usize) -> Result<()>;
    async fn entry_set_ext_id(&self, ext_id: &str, entry_id: usize) -> Result<()>;
    async fn entry_set_ext_url(&self, ext_url: &str, entry_id: usize) -> Result<()>;
    async fn entry_set_type_name(&self, type_name: Option<String>, entry_id: usize) -> Result<()>;
    async fn entry_delete_person_dates(&self, entry_id: usize) -> Result<()>;
    async fn entry_set_person_dates(
        &self,
        entry_id: usize,
        born: String,
        died: String,
    ) -> Result<()>;
    async fn entry_get_person_dates(
        &self,
        entry_id: usize,
    ) -> Result<(Option<String>, Option<String>)>;
    async fn entry_remove_language_description(
        &self,
        entry_id: usize,
        language: &str,
    ) -> Result<()>;
    async fn entry_set_language_description(
        &self,
        entry_id: usize,
        language: &str,
        text: String,
    ) -> Result<()>;
    async fn entry_get_aliases(&self, entry_id: usize) -> Result<Vec<LocaleString>>;
    async fn entry_add_alias(&self, entry_id: usize, language: &str, label: &str) -> Result<()>;
    async fn entry_get_language_descriptions(
        &self,
        entry_id: usize,
    ) -> Result<HashMap<String, String>>;
    async fn entry_remove_auxiliary(&self, entry_id: usize, prop_numeric: usize) -> Result<()>;
    async fn entry_set_auxiliary(
        &self,
        entry_id: usize,
        prop_numeric: usize,
        value: String,
    ) -> Result<()>;
    async fn entry_remove_coordinate_location(&self, entry_id: usize) -> Result<()>;
    async fn entry_set_coordinate_location(
        &self,
        entry_id: usize,
        lat: f64,
        lon: f64,
    ) -> Result<()>;
    async fn entry_get_coordinate_location(
        &self,
        entry_id: usize,
    ) -> Result<Option<CoordinateLocation>>;
    async fn entry_get_aux(&self, entry_id: usize) -> Result<Vec<AuxiliaryRow>>;
    async fn entry_set_match(
        &self,
        entry: &Entry,
        user_id: usize,
        q_numeric: isize,
        timestamp: &str,
    ) -> Result<bool>;
    async fn entry_set_match_status(
        &self,
        entry_id: usize,
        status: &str,
        is_matched: i32,
    ) -> Result<()>;
    async fn entry_remove_multi_match(&self, entry_id: usize) -> Result<()>;
    async fn entry_unmatch(&self, entry_id: usize) -> Result<()>;
    async fn entry_get_multi_matches(&self, entry_id: usize) -> Result<Vec<String>>;
    async fn entry_set_multi_match(
        &self,
        entry_id: usize,
        candidates: String,
        candidates_count: usize,
    ) -> Result<()>;
    async fn app_state_seppuku_get_running(&self, ts: &str) -> (usize, usize);

    // CERSEI
    async fn get_current_scrapers(&self) -> Result<HashMap<usize, CurrentScraper>>;
    async fn add_cersei_catalog(&self, catalog_id: usize, scraper_id: usize) -> Result<()>;
    async fn update_cersei_last_update(&self, scraper_id: usize, last_sync: &str) -> Result<()>;
}
