use crate::{
    automatch::{AutomatchSearchRow, CandidateDatesRow, PersonDateMatchRow, ResultInOriginalCatalog, ResultInOtherCatalog},
    auxiliary_data::AuxiliaryRow,
    auxiliary_matcher::AuxiliaryResults,
    catalog::Catalog,
    cersei::CurrentScraper,
    coordinates::{CoordinateLocation, LocationRow},
    entry::Entry,
    entry_query::EntryQuery,
    issue::{Issue, IssueStatus},
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
use mysql_async::Row;
use std::collections::HashMap;
use wikimisc::wikibase::LocaleString;

#[derive(Debug, Clone, PartialEq)]
pub struct OverviewTableRow {
    catalog_id: usize,
    total: isize,
    noq: isize,
    autoq: isize,
    na: isize,
    manual: isize,
    nowd: isize,
    multi_match: isize,
    types: String,
}

impl OverviewTableRow {
    pub fn from_row(row: &Row) -> Option<Self> {
        Some(Self {
            catalog_id: row.get("catalog")?,
            total: row.get("total")?,
            noq: row.get("noq")?,
            autoq: row.get("autoq")?,
            na: row.get("na")?,
            manual: row.get("manual")?,
            nowd: row.get("nowd")?,
            multi_match: row.get("multi_match")?,
            types: row.get("types")?,
        })
    }

    pub fn catalog_id(&self) -> usize {
        self.catalog_id
    }

    pub fn has_weird_numbers(&self) -> bool {
        self.total < 0
            || self.noq < 0
            || self.autoq < 0
            || self.na < 0
            || self.manual < 0
            || self.nowd < 0
            || self.multi_match < 0
    }
}

#[async_trait]
pub trait Storage: std::fmt::Debug + Send + Sync {
    // fn new(j: &Value) -> impl Storage;
    async fn disconnect(&self) -> Result<()>;

    async fn entry_query(&self, query: &EntryQuery) -> Result<Vec<Entry>>;
    async fn get_entry_ids_by_aux(&self, prop_numeric: usize, value: &str) -> Result<Vec<usize>>;
    async fn get_user_name_from_id(&self, user_id: usize) -> Option<String>;

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
    /// Returns `(type, user)` for the given import_file UUID, or `None` if not found.
    async fn get_import_file_info(&self, uuid: &str) -> Result<Option<(String, usize)>>;
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
    // async fn remove_inactive_catalogs_from_overview(&self) -> Result<()>;
    async fn replace_nowd_with_noq(&self) -> Result<()>;
    async fn catalog_refresh_overview_table(&self, catalog_id: usize) -> Result<()>;
    async fn catalog_get_entries_of_people_with_initials(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<Entry>>;
    async fn get_all_external_ids(&self, catalog_id: usize) -> Result<HashMap<String, usize>>;
    async fn delete_catalog(&self, catalog_id: usize) -> Result<()>;

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
    async fn get_overview_table(&self) -> Result<Vec<OverviewTableRow>>;
    async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<()>;
    async fn avoid_auto_match(&self, entry_id: usize, q_numeric: Option<isize>) -> Result<bool>;
    async fn get_random_active_catalog_id_with_property(&self) -> Option<usize>;
    async fn get_kv_value(&self, key: &str) -> Result<Option<String>>;
    async fn set_kv_value(&self, key: &str, value: &str) -> Result<()>;
    async fn do_catalog_entries_have_person_date(&self, catalog_id: usize) -> Result<bool>;
    async fn set_has_person_date(&self, catalog_id: usize, new_has_person_date: &str)
    -> Result<()>;

    // Issue

    async fn get_open_wd_duplicates(&self) -> Result<Vec<Issue>>;
    async fn issue_insert(&self, issue: &Issue) -> Result<()>;
    async fn set_issue_status(&self, issue_id: usize, status: IssueStatus) -> Result<()>;

    // Autoscrape

    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>>;
    async fn get_entry_ids_for_ext_ids(
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

    async fn maintenance_update_auxiliary_props(
        &self,
        prop2type: &[(String, String)],
    ) -> Result<()>;
    async fn maintenance_use_auxiliary_broken(&self) -> Result<()>;
    async fn maintenance_common_names_dates(&self) -> Result<()>;
    async fn maintenance_common_names_birth_year(&self) -> Result<()>;
    async fn maintenance_taxa(&self) -> Result<()>;
    async fn maintenance_common_aux(&self) -> Result<()>;
    async fn maintenance_artwork(&self) -> Result<()>;
    async fn import_relations_into_aux(&self) -> Result<()>;
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
        ext_ids: Vec<String>,
    ) -> Result<Vec<(usize, String, Option<usize>)>>;
    async fn maintenance_fix_redirects(&self, from: isize, to: isize) -> Result<()>;
    async fn maintenance_unlink_item_matches(&self, items: Vec<String>) -> Result<()>;
    async fn automatch_people_with_birth_year(&self, catalog_id: usize) -> Result<()>;
    async fn use_automatchers(&self, catalog_id: usize, use_automatchers: u8) -> Result<()>;
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
    async fn get_catalogs_with_person_dates_without_flag(&self) -> Result<Vec<usize>>;
    async fn add_mnm_relation(
        &self,
        entry_id: usize,
        prop_numeric: usize,
        target_entry_id: usize,
    ) -> Result<()>;

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
    ) -> Result<Vec<AutomatchSearchRow>>;
    async fn automatch_creations_get_results(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(String, usize, String)>>;
    async fn automatch_simple_get_results(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AutomatchSearchRow>>;
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
    ) -> Result<Vec<PersonDateMatchRow>>;
    async fn match_person_by_single_date_get_results(
        &self,
        match_field: &str,
        catalog_id: usize,
        precision: i32,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<CandidateDatesRow>>;
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
    async fn entry_insert_as_new(&self, entry: &Entry) -> Result<Option<usize>>;
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
        precision: Option<f64>,
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
    async fn get_cersei_scrapers(&self) -> Result<HashMap<usize, CurrentScraper>>;
    async fn add_cersei_catalog(&self, catalog_id: usize, scraper_id: usize) -> Result<()>;
    async fn update_cersei_last_update(&self, scraper_id: usize, last_sync: &str) -> Result<()>;

    // MetaEntry support
    async fn meta_entry_get_mnm_relations(
        &self,
        entry_id: usize,
    ) -> Result<Vec<crate::meta_entry::MetaMnmRelation>>;
    async fn meta_entry_get_issues(
        &self,
        entry_id: usize,
    ) -> Result<Vec<crate::meta_entry::MetaIssue>>;
    async fn meta_entry_get_kv_entries(
        &self,
        entry_id: usize,
    ) -> Result<Vec<crate::meta_entry::MetaKvEntry>>;
    async fn meta_entry_get_log_entries(
        &self,
        entry_id: usize,
    ) -> Result<Vec<crate::meta_entry::MetaLogEntry>>;
    async fn meta_entry_get_statement_text(
        &self,
        entry_id: usize,
    ) -> Result<Vec<crate::meta_entry::MetaStatementText>>;
    async fn meta_entry_delete_auxiliary(&self, entry_id: usize) -> Result<()>;
    async fn meta_entry_delete_aliases(&self, entry_id: usize) -> Result<()>;
    async fn meta_entry_delete_descriptions(&self, entry_id: usize) -> Result<()>;
    async fn meta_entry_delete_mnm_relations(&self, entry_id: usize) -> Result<()>;
    async fn meta_entry_delete_kv_entries(&self, entry_id: usize) -> Result<()>;
    async fn meta_entry_set_kv_entry(&self, entry_id: usize, key: &str, value: &str) -> Result<()>;

    // API support methods

    async fn get_user_by_name(&self, name: &str) -> Result<Option<(usize, String, bool)>>; // returns (id, name, is_catalog_admin)
    async fn get_or_create_user_id(&self, name: &str) -> Result<usize>;
    async fn get_users_by_ids(&self, user_ids: &[usize]) -> Result<HashMap<usize, serde_json::Value>>;

    // Bulk extended entry data (for add_extended_entry_data equivalent)
    async fn api_get_person_dates_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, (String, String)>>; // entry_id -> (born, died)
    async fn api_get_locations_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, (f64, f64)>>; // entry_id -> (lat, lon)
    async fn api_get_multi_match_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, String>>; // entry_id -> candidates string
    async fn api_get_auxiliary_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Vec<serde_json::Value>>>; // entry_id -> aux rows
    async fn api_get_aliases_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Vec<serde_json::Value>>>; // entry_id -> alias rows
    async fn api_get_descriptions_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Vec<serde_json::Value>>>; // entry_id -> desc rows
    async fn api_get_kv_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Vec<(String, String, u8)>>>; // entry_id -> (key, value, done)
    async fn api_get_mnm_relations_for_entries(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Vec<serde_json::Value>>>; // entry_id -> relation rows

    // Catalog overview
    async fn api_get_catalog_overview(&self) -> Result<Vec<serde_json::Value>>; // Full overview with catalog+overview+user+autoscrape data
    async fn api_get_single_catalog_overview(&self, catalog_id: usize) -> Result<serde_json::Value>;

    // Catalog details (3 aggregate queries)
    async fn api_get_catalog_type_counts(&self, catalog_id: usize) -> Result<Vec<serde_json::Value>>;
    async fn api_get_catalog_match_by_month(&self, catalog_id: usize) -> Result<Vec<serde_json::Value>>;
    async fn api_get_catalog_matcher_by_user(&self, catalog_id: usize) -> Result<Vec<serde_json::Value>>;

    // Jobs
    async fn api_get_jobs(&self, catalog_id: usize, start: usize, max: usize) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>)>; // (stats, jobs)

    // Issues
    async fn api_get_issues_count(&self, issue_type: &str, catalogs: &str) -> Result<usize>;
    async fn api_get_issues(&self, issue_type: &str, catalogs: &str, limit: usize, offset: usize, random_threshold: f64) -> Result<Vec<serde_json::Value>>;
    async fn api_get_all_issues(&self, mode: &str) -> Result<Vec<serde_json::Value>>;

    // Search
    async fn api_search_entries(&self, words: &[String], description_search: bool, no_label_search: bool, exclude: &[usize], include: &[usize], max_results: usize) -> Result<Vec<Entry>>;
    async fn api_search_by_q(&self, q: isize) -> Result<Vec<Entry>>;

    // Recent changes
    async fn api_get_recent_changes(&self, ts: &str, catalog_id: usize, limit: usize) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>)>; // (events from entry, events from log)

    // Catalog entry listing (query=catalog)
    async fn api_get_catalog_entries_raw(&self, sql: &str) -> Result<Vec<Entry>>;

    // Existing job actions
    async fn api_get_existing_job_actions(&self) -> Result<Vec<String>>;

    // Random entry
    async fn api_get_random_entry(&self, catalog_id: usize, submode: &str, entry_type: &str, random: f64, active_catalogs: &[usize]) -> Result<Option<Entry>>;
    async fn api_get_active_catalog_ids(&self) -> Result<Vec<usize>>;

    // Additional API support methods
    async fn api_get_wd_props(&self) -> Result<Vec<usize>>;
    async fn api_get_top_missing(&self, catalogs: &str) -> Result<Vec<serde_json::Value>>;
    async fn api_get_common_names(&self, catalog_id: usize, type_q: &str, other_cats_desc: bool, min: usize, max: usize, limit: usize, offset: usize) -> Result<Vec<serde_json::Value>>;
    async fn api_get_locations_bbox(&self, lon_min: f64, lat_min: f64, lon_max: f64, lat_max: f64) -> Result<Vec<serde_json::Value>>;
    async fn api_get_locations_in_catalog(&self, catalog_id: usize) -> Result<Vec<serde_json::Value>>;
    async fn api_get_download_entries(&self, catalog_id: usize) -> Result<Vec<(isize, String, String, String, Option<usize>)>>; // (q, ext_id, ext_url, ext_name, user_id)
    async fn api_get_download2(&self, sql: &str) -> Result<Vec<HashMap<String, String>>>;
    async fn api_edit_catalog(&self, catalog_id: usize, name: &str, url: &str, desc: &str, type_name: &str, search_wp: &str, wd_prop: Option<usize>, wd_qual: Option<usize>, active: bool) -> Result<()>;
    async fn api_get_catalog_overview_for_ids(&self, catalog_ids: &[usize]) -> Result<Vec<serde_json::Value>>;
    async fn api_match_q_multi(&self, catalog_id: usize, ext_id: &str, q: isize, user_id: usize) -> Result<bool>;
    async fn api_remove_all_q(&self, catalog_id: usize, q: isize) -> Result<()>;
    async fn api_remove_all_multimatches(&self, entry_id: usize) -> Result<()>;
    async fn api_suggest(&self, catalog_id: usize, ext_id: &str, q: isize, overwrite: bool) -> Result<bool>;
    async fn api_add_alias(&self, catalog_id: usize, ext_id: &str, language: &str, label: &str, user_id: usize) -> Result<()>;
    async fn api_get_cersei_catalog(&self, scraper_id: usize) -> Result<Option<usize>>;
    async fn api_get_same_names(&self) -> Result<(String, Vec<Entry>)>;
    async fn api_get_random_person_batch(&self, gender: &str, has_desc: bool) -> Result<Vec<serde_json::Value>>;
    async fn api_get_property_cache(&self) -> Result<(HashMap<String, Vec<(usize, usize)>>, HashMap<String, String>)>;
    async fn api_get_quick_compare_list(&self) -> Result<Vec<serde_json::Value>>;
    async fn api_get_mnm_unmatched_relations(&self, property: usize, offset: usize, limit: usize) -> Result<(Vec<(usize, usize)>, Vec<Entry>)>; // (id, cnt) pairs + entries
    async fn api_get_top_groups(&self) -> Result<Vec<serde_json::Value>>;
    async fn api_set_top_group(&self, name: &str, catalogs: &str, user_id: usize, based_on: usize) -> Result<()>;
    async fn api_remove_empty_top_group(&self, group_id: usize) -> Result<()>;
    async fn api_set_missing_properties_status(&self, row_id: usize, status: &str, note: &str, user_id: usize) -> Result<()>;
    async fn api_get_entries_by_q_or_value(&self, q: isize, prop_catalog_map: &HashMap<usize, Vec<usize>>, prop_values: &HashMap<usize, Vec<String>>) -> Result<Vec<Entry>>;
    async fn api_get_prop2catalog(&self, props: &[usize]) -> Result<HashMap<usize, Vec<usize>>>;
    async fn api_get_missing_properties_raw(&self) -> Result<Vec<serde_json::Value>>;
    async fn api_get_rc_log_events(&self, min_ts: &str, max_ts: &str, catalog_id: usize) -> Result<Vec<serde_json::Value>>;

    // Code fragments
    async fn get_code_fragment_lua(&self, function: &str, catalog_id: usize) -> Result<Option<String>>;
    async fn touch_code_fragment(&self, function: &str, catalog_id: usize) -> Result<()>;
    async fn clear_person_dates_for_catalog(&self, catalog_id: usize) -> Result<()>;
    async fn get_code_fragments_for_catalog(&self, catalog_id: usize) -> Result<Vec<serde_json::Value>>;
    async fn get_all_code_fragment_functions(&self) -> Result<Vec<String>>;
    async fn save_code_fragment(&self, fragment: &serde_json::Value) -> Result<usize>;

    // Jobs
    async fn queue_job(&self, catalog_id: usize, action: &str, depends_on: Option<usize>) -> Result<usize>;

    // Micro-API: sparql_list
    async fn get_entries_by_ext_names_unmatched(&self, names: &[String]) -> Result<Vec<Entry>>;

    // Micro-API: get_sync
    async fn get_catalog_wd_prop(&self, catalog_id: usize) -> Result<(Option<usize>, Option<usize>)>;
    async fn get_mnm_matched_entries_for_sync(&self, catalog_id: usize) -> Result<Vec<(isize, String)>>;
    async fn get_mnm_double_matches(&self, catalog_id: usize) -> Result<HashMap<String, Vec<String>>>;

    // Micro-API: creation_candidates
    async fn cc_random_pick(&self, sql: &str) -> Result<Vec<serde_json::Value>>;
    async fn cc_get_entries_by_ids_active(&self, entry_ids: &str) -> Result<Vec<Entry>>;
    async fn cc_get_entries_by_names_active(&self, names: &[String], type_filter: Option<&str>, birth_year: Option<&str>, death_year: Option<&str>) -> Result<Vec<Entry>>;

    // Micro-API: quick_compare
    async fn qc_get_entries(&self, catalog_id: usize, entry_id: Option<usize>, require_image: bool, require_coordinates: bool, random_threshold: f64, max_results: usize) -> Result<Vec<serde_json::Value>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_overview_row(
        total: isize,
        noq: isize,
        autoq: isize,
        na: isize,
        manual: isize,
        nowd: isize,
        multi_match: isize,
    ) -> OverviewTableRow {
        OverviewTableRow {
            catalog_id: 1,
            total,
            noq,
            autoq,
            na,
            manual,
            nowd,
            multi_match,
            types: String::new(),
        }
    }

    #[test]
    fn test_has_weird_numbers_all_positive() {
        let row = make_overview_row(100, 50, 20, 5, 10, 3, 2);
        assert!(!row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_all_zero() {
        let row = make_overview_row(0, 0, 0, 0, 0, 0, 0);
        assert!(!row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_total() {
        let row = make_overview_row(-1, 0, 0, 0, 0, 0, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_noq() {
        let row = make_overview_row(0, -1, 0, 0, 0, 0, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_autoq() {
        let row = make_overview_row(0, 0, -1, 0, 0, 0, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_na() {
        let row = make_overview_row(0, 0, 0, -1, 0, 0, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_manual() {
        let row = make_overview_row(0, 0, 0, 0, -1, 0, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_nowd() {
        let row = make_overview_row(0, 0, 0, 0, 0, -1, 0);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_has_weird_numbers_negative_multi_match() {
        let row = make_overview_row(0, 0, 0, 0, 0, 0, -1);
        assert!(row.has_weird_numbers());
    }

    #[test]
    fn test_catalog_id_accessor() {
        let row = make_overview_row(0, 0, 0, 0, 0, 0, 0);
        assert_eq!(row.catalog_id(), 1);
    }
}
