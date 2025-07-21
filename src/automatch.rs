use crate::app_state::AppState;
use crate::app_state::USER_AUTO;
use crate::app_state::USER_DATE_MATCH;
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::entry_query::EntryQuery;
use crate::issue::Issue;
use crate::issue::IssueType;
use crate::job::Job;
use crate::job::Jobbable;
use crate::match_state::MatchState;
use crate::person::Person;
use anyhow::{anyhow, Result};
use chrono::prelude::*;
use chrono::{NaiveDateTime, Utc};
use futures::future::join_all;
use futures::StreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use mediawiki::api::Api;
use regex::Regex;
use serde_json::json;
use std::collections::HashMap;

lazy_static! {
    static ref RE_YEAR: Regex = Regex::new(r"(\d{3,4})").expect("Regexp error");
}

#[derive(Debug, Clone, Copy)]
pub enum DateMatchField {
    Born,
    Died,
}

impl DateMatchField {
    const fn get_field_name(&self) -> &'static str {
        match self {
            DateMatchField::Born => "born",
            DateMatchField::Died => "died",
        }
    }

    const fn get_property(&self) -> &'static str {
        match self {
            DateMatchField::Born => "P569",
            DateMatchField::Died => "P570",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DatePrecision {
    Day,
    Year,
}

impl DatePrecision {
    const fn as_i32(&self) -> i32 {
        match self {
            DatePrecision::Day => 10,
            DatePrecision::Year => 4,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ResultInOriginalCatalog {
    pub entry_id: usize,
    pub ext_name: String,
    pub type_name: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ResultInOtherCatalog {
    pub entry_id: usize,
    pub ext_name: String,
    pub type_name: String,
    pub q: Option<isize>,
}

#[derive(Debug, Clone)]
struct CandidateDates {
    pub entry_id: usize,
    pub born: String,
    pub died: String,
    pub matches: Vec<String>,
}

impl CandidateDates {
    //TODO test
    fn from_row(r: &(usize, String, String, String)) -> Self {
        Self {
            entry_id: r.0,
            born: r.1.clone(),
            died: r.2.clone(),
            matches: r
                .3
                .split(',')
                .filter(|q| !q.is_empty())
                .map(|q| format!("Q{q}"))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoMatch {
    app: AppState,
    job: Option<Job>,
}

impl Jobbable for AutoMatch {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }

    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl AutoMatch {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            job: None,
        }
    }

    pub async fn automatch_with_sparql(&mut self, catalog_id: usize) -> Result<()> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let kv_pairs = catalog.get_key_value_pairs().await?;
        let sparql_part = kv_pairs
            .iter()
            .filter(|(k, _)| *k == "automatch_sparql")
            .map(|(_, v)| v)
            .next()
            .ok_or_else(|| anyhow!("No automatch_sparql key in catalog"))?;
        let sparql = format!("SELECT ?q ?qLabel WHERE {{ {sparql_part} }}");
        let mut reader = self.app.wikidata().load_sparql_csv(&sparql).await?;
        let api = self.app.wikidata().get_mw_api().await?;
        let mut label2q = HashMap::new();
        for row in reader.records().filter_map(|r| r.ok()) {
            let q = api.extract_entity_from_uri(&row[0])?;
            let q_label = row[1].to_string();
            if let Ok(q_numeric) = q[1..].parse::<usize>() {
                let q_label = q_label.to_lowercase();
                label2q.insert(q_label, q_numeric);
                if label2q.len() >= 100000 {
                    self.process_automatch_with_sparql(catalog_id, &label2q)
                        .await?;
                    label2q.clear();
                }
            }
        }
        self.process_automatch_with_sparql(catalog_id, &label2q)
            .await?;
        let _ = self.app.storage().use_automatchers(catalog_id, 0).await;
        Ok(())
    }

    async fn process_automatch_with_sparql(
        &self,
        catalog_id: usize,
        label2q: &HashMap<String, usize>,
    ) -> Result<()> {
        if label2q.is_empty() {
            return Ok(());
        }
        let mut offset = 0;
        let batch_size = 50000;
        loop {
            let query = EntryQuery::default()
                .with_catalog_id(catalog_id)
                .with_match_state(MatchState::unmatched())
                .with_limit(batch_size)
                .with_offset(offset);
            let mut entry_batch = self.app.storage().entry_query(&query).await?;
            for entry in &mut entry_batch {
                if let Some(q) = label2q.get(&entry.ext_name.to_lowercase()) {
                    // println!("Found {q} for {}", entry.ext_name);
                    entry.set_app(&self.app);
                    let _ = entry.set_match(&format!("Q{q}"), USER_AUTO).await;
                }
            }
            if entry_batch.len() < batch_size {
                break;
            }
            offset += entry_batch.len();
        }
        Ok(())
    }

    pub async fn automatch_by_sitelink(&mut self, catalog_id: usize) -> Result<()> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let language = catalog.search_wp();
        let site = format!("{}wiki", &language);
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        loop {
            let entries = self
                .app
                .storage()
                .automatch_by_sitelink_get_entries(catalog_id, offset, batch_size)
                .await?;
            if entries.is_empty() {
                break; // Done
            }
            let name2entries = Self::automatch_by_sitelink_name2entries(&entries);
            let wd_matches = self
                .automatch_by_sitelink_get_wd_matches(&name2entries, &site)
                .await?;
            self.automatch_by_sitelink_process_wd_matches(wd_matches, name2entries)
                .await;
            if entries.len() < batch_size {
                break;
            }
            offset += entries.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_by_sitelink_process_wd_matches(
        &mut self,
        wd_matches: Vec<(usize, String)>,
        name2entries: HashMap<String, Vec<usize>>,
    ) {
        for (q, title) in wd_matches {
            if let Some(v) = name2entries.get(&title) {
                for entry_id in v {
                    if let Ok(mut entry) = Entry::from_id(*entry_id, &self.app).await {
                        let _ = entry.set_match(&format!("Q{q}"), USER_AUTO).await;
                    }
                }
            }
        }
    }

    async fn automatch_by_sitelink_get_wd_matches(
        &mut self,
        name2entries: &HashMap<String, Vec<usize>>,
        site: &String,
    ) -> Result<Vec<(usize, String)>> {
        let params: Vec<String> = name2entries.keys().map(|s| s.to_owned()).collect();
        let wd_matches = self
            .app
            .wikidata()
            .get_items_for_pages_on_wiki(params, site)
            .await?;
        Ok(wd_matches)
    }

    async fn search_with_type_and_entity_id(
        &self,
        entry_id: usize,
        name: &str,
        type_q: &str,
    ) -> Option<(usize, Vec<String>)> {
        let mut items = match self.app.wikidata().search_with_type_api(name, type_q).await {
            Ok(items) => items,
            Err(_e) => {
                // error!("search_with_type_and_entity_id: {e}");
                return None;
            }
        };
        if items.is_empty() {
            return None;
        }
        items.sort();
        items.dedup();
        Some((entry_id, items))
    }

    async fn match_entries_to_items(
        &self,
        entry_id2items: &HashMap<usize, Vec<String>>,
    ) -> Result<()> {
        let entry_ids: Vec<usize> = entry_id2items.keys().copied().collect();
        let mut entries = Entry::multiple_from_ids(&entry_ids, &self.app).await?;
        let mut futures = vec![];

        for (entry_id, entry) in &mut entries {
            let items = match entry_id2items.get(entry_id) {
                Some(items) => items,
                None => continue,
            };
            let future = entry.set_auto_and_multi_match(items);
            futures.push(future);
        }

        let _ = join_all(futures).await; // Ignore errors
        Ok(())
    }

    // async fn match_entry_to_items(&self, entry_id: usize, mut items: Vec<String>) -> Result<(),GenericError> {
    //     items.sort();
    //     items.dedup();
    //     let mut entry= Entry::from_id(entry_id, &self.app).await?;
    //     if entry.q!=AppState::item2numeric(&items[0]) {
    //         entry.set_match(&items[0],USER_AUTO).await?;
    //         if items.len()>1 { // Multi-match
    //             entry.set_multi_match(&items).await?;
    //         }
    //     }
    //     Ok(())
    // }

    pub async fn automatch_by_search(&mut self, catalog_id: usize) -> Result<()> {
        let mut offset = self.get_last_job_offset().await;
        let batch_size = *self
            .app
            .task_specific_usize()
            .get("automatch_by_search_batch_size")
            .unwrap_or(&5000);
        let search_batch_size = *self
            .app
            .task_specific_usize()
            .get("automatch_by_search_search_batch_size")
            .unwrap_or(&100);

        loop {
            let results = self
                .app
                .storage()
                .automatch_by_search_get_results(catalog_id, offset, batch_size)
                .await?;
            // println!("automatch_by_search [{catalog_id}]:Done.");

            for result_batch in results.chunks(search_batch_size) {
                self.automatch_by_search_process_results_batch(result_batch)
                    .await;
            }
            // println!("automatch_by_search [{catalog_id}]: Batch completed.");

            if results.len() < batch_size {
                break;
            }
            // println!("automatch_by_search [{catalog_id}]: Another batch...");
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        // println!("automatch_by_search [{catalog_id}]: All batches completed.");
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_by_search_process_results_batch(
        &mut self,
        result_batch: &[(usize, String, String, String)],
    ) {
        let mut search_results = self
            .automatch_by_search_process_results_batch_process_futures(result_batch)
            .await;
        if search_results.is_empty() {
            return;
        }
        self.automatch_by_search_process_results_batch_filter_search_results(&mut search_results)
            .await;
        let mut entry_id2items: HashMap<usize, Vec<String>> = HashMap::new();
        for (entry_id, q) in search_results {
            entry_id2items.entry(entry_id).or_default().push(q);
        }
        let _ = self.match_entries_to_items(&entry_id2items).await;
    }

    async fn automatch_by_search_process_results_batch_filter_search_results(
        &mut self,
        search_results: &mut Vec<(usize, String)>,
    ) {
        let mut no_meta_items = search_results
            .iter()
            .map(|(_entry_id, q)| q)
            .cloned()
            .collect_vec();
        let _ = self
            .app
            .wikidata()
            .remove_meta_items(&mut no_meta_items)
            .await;
        search_results.retain(|(_entry_id, q)| no_meta_items.contains(q));
    }

    pub async fn automatch_creations(&mut self, catalog_id: usize) -> Result<()> {
        let results = self
            .app
            .storage()
            .automatch_creations_get_results(catalog_id)
            .await?;

        for result in &results {
            let object_title = &result.0;
            let object_entry_id = result.1;
            let search_query = &result.2;

            if !object_title.contains(' ') {
                // Skip single-word titles
                continue;
            }

            let items = match self.app.wikidata().search_api(search_query).await {
                Ok(items) => items,
                Err(_e) => continue,
            };
            if items.is_empty() {
                // No search results
                continue;
            }
            if let Ok(mut entry) = Entry::from_id(object_entry_id, &self.app).await {
                let _ = entry.set_auto_and_multi_match(&items).await;
            };
        }
        Ok(())
    }

    pub async fn automatch_simple(&mut self, catalog_id: usize) -> Result<()> {
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        loop {
            // TODO make this more efficient, too many wd replica queries
            let results = self
                .app
                .storage()
                .automatch_simple_get_results(catalog_id, offset, batch_size)
                .await?;

            for result in &results {
                let (entry_id, items) = match self.automatch_simple_items_from_result(result).await
                {
                    Some(value) => value,
                    None => continue,
                };
                self.automatch_simple_set_matches(items, entry_id).await;
            }

            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_simple_set_matches(&mut self, items: Vec<String>, entry_id: usize) {
        let item = match items.first() {
            Some(item) => item,
            None => return,
        };
        let mut entry = match Entry::from_id(entry_id, &self.app).await {
            Ok(entry) => entry,
            _ => return, // Ignore error
        };
        if entry.set_match(item, USER_AUTO).await.is_err() {
            return; // Ignore error
        }
        if items.len() > 1 {
            // Multi-match
            let _ = entry.set_multi_match(&items).await.is_err(); // Ignore error
        }
    }

    async fn automatch_simple_items_from_result(
        &mut self,
        result: &(usize, String, String, String),
    ) -> Option<(usize, Vec<String>)> {
        let entry_id = result.0;
        let label = &result.1;
        let type_q = &result.2;
        let aliases: Vec<&str> = result.3.split('|').collect();
        let mut items = match self.app.wikidata().search_db_with_type(label, type_q).await {
            Ok(items) => items,
            _ => return None, // Ignore error
        };
        for alias in &aliases {
            let mut tmp = match self.app.wikidata().search_db_with_type(alias, type_q).await {
                Ok(tmp) => tmp,
                _ => continue, // Ignore error
            };
            items.append(&mut tmp);
        }
        items.sort();
        items.dedup();
        if self
            .app
            .wikidata()
            .remove_meta_items(&mut items)
            .await
            .is_err()
        {
            return None; // Ignore error
        }
        Some((entry_id, items))
    }

    //TODO test
    pub async fn automatch_from_other_catalogs(&mut self, catalog_id: usize) -> Result<()> {
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 500;
        loop {
            let results_in_original_catalog = self
                .app
                .storage()
                .automatch_from_other_catalogs_get_results(catalog_id, batch_size, offset)
                .await?;
            if results_in_original_catalog.is_empty() {
                break;
            }

            let ext_names: Vec<String> = results_in_original_catalog
                .iter()
                .map(|r| r.ext_name.to_owned())
                .collect();

            let name_type2id =
                Self::automatch_from_other_catalogs_name_type2id(&results_in_original_catalog);

            let results_in_other_catalogs = self
                .app
                .storage()
                .automatch_from_other_catalogs_get_results2(&results_in_original_catalog, ext_names)
                .await?;
            for r in &results_in_other_catalogs {
                self.automatch_from_other_catalogs_process_result(r, &name_type2id)
                    .await;
            }
            if results_in_original_catalog.len() < batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results_in_original_catalog.len();
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_from_other_catalogs_process_result(
        &mut self,
        r: &ResultInOtherCatalog,
        name_type2id: &HashMap<(String, String), Vec<usize>>,
    ) {
        let q = match r.q {
            Some(q) => format!("Q{q}"),
            None => return,
        };
        let key = (r.ext_name.to_owned(), r.type_name.to_owned());
        if let Some(v) = name_type2id.get(&key) {
            for entry_id in v {
                if let Ok(mut entry) = Entry::from_id(*entry_id, &self.app).await {
                    let _ = entry.set_match(&q, USER_AUTO).await;
                };
            }
        }
    }

    pub async fn purge_automatches(&self, catalog_id: usize) -> Result<()> {
        self.app.storage().purge_automatches(catalog_id).await
    }

    async fn match_person_by_dates_process_result(
        &self,
        result: &(usize, String, String, String),
        mw_api: &Api,
    ) -> Result<()> {
        let entry_id = result.0;
        let candidate_items = match self
            .match_person_by_dates_process_result_get_candidate_items(result, mw_api)
            .await
        {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        match candidate_items.len() {
            0 => {} // No results
            1 => {
                let q = &candidate_items[0];
                let _ = Entry::from_id(entry_id, &self.app)
                    .await?
                    .set_match(q, USER_DATE_MATCH)
                    .await;
            }
            _ => {
                Issue::new(
                    entry_id,
                    IssueType::WdDuplicate,
                    json!(candidate_items),
                    &self.app,
                )
                .await?
                .insert()
                .await?;
            }
        }
        Ok(())
    }

    async fn match_person_by_dates_process_result_get_candidate_items(
        &self,
        result: &(usize, String, String, String),
        mw_api: &Api,
    ) -> Result<Vec<String>, ()> {
        let ext_name = &result.1;
        let birth_year = match Self::extract_sane_year_from_date(&result.2) {
            Some(year) => year,
            None => return Err(()),
        };
        let death_year = match Self::extract_sane_year_from_date(&result.3) {
            Some(year) => year,
            None => return Err(()),
        };
        let candidate_items = match self.search_person(ext_name).await {
            Ok(c) => c,
            _ => return Err(()), // Ignore error
        };
        if candidate_items.is_empty() {
            return Err(()); // No candidate items
        }
        let candidate_items = match self
            .subset_items_by_birth_death_year(&candidate_items, birth_year, death_year, mw_api)
            .await
        {
            Ok(ci) => ci,
            _ => return Err(()), // Ignore error
        };
        Ok(candidate_items)
    }

    pub async fn match_person_by_dates(&mut self, catalog_id: usize) -> Result<()> {
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        loop {
            let results = self
                .app
                .storage()
                .match_person_by_dates_get_results(catalog_id, batch_size, offset)
                .await?;
            for result in &results {
                // Ignore error
                let _ = self
                    .match_person_by_dates_process_result(result, &mw_api)
                    .await;
            }
            if results.len() < batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results.len();
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    pub async fn match_person_by_single_date(
        &mut self,
        catalog_id: usize,
        match_field: DateMatchField,
        precision: DatePrecision,
    ) -> Result<()> {
        // let (match_field, match_prop) = match_field.get_field_and_prop();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        // CAUTION: Do NOT use views in the SQL statement, it will/might throw an "Prepared statement needs to be re-prepared" error
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 100;
        loop {
            let (results, items) = self
                .match_person_by_single_date_get_results(
                    &match_field,
                    catalog_id,
                    precision.as_i32(),
                    batch_size,
                    offset,
                    &mw_api,
                )
                .await?;
            for result in &results {
                self.match_person_by_single_date_check_candidates(
                    result,
                    &items,
                    precision.as_i32(),
                    &match_field,
                )
                .await?;
            }
            if results.len() < batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results.len();
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn match_person_by_single_date_get_results(
        &mut self,
        match_field: &DateMatchField,
        catalog_id: usize,
        precision: i32,
        batch_size: usize,
        offset: usize,
        mw_api: &mediawiki::api::Api,
    ) -> Result<(
        Vec<CandidateDates>,
        wikimisc::wikibase::entity_container::EntityContainer,
    )> {
        let results = self
            .app
            .storage()
            .match_person_by_single_date_get_results(
                match_field.get_field_name(),
                catalog_id,
                precision,
                batch_size,
                offset,
            )
            .await?;
        let results: Vec<CandidateDates> = results.iter().map(CandidateDates::from_row).collect();
        let items_to_load: Vec<String> = results.iter().flat_map(|r| r.matches.clone()).collect();
        let items = wikimisc::wikibase::entity_container::EntityContainer::new();
        let _ = items.load_entities(mw_api, &items_to_load).await;
        Ok((results, items))
    }

    async fn match_person_by_single_date_check_candidates(
        &mut self,
        result: &CandidateDates,
        items: &wikimisc::wikibase::entity_container::EntityContainer,
        precision: i32,
        match_field: &DateMatchField,
    ) -> Result<()> {
        let mut candidates = vec![];
        for q in &result.matches {
            let item = match items.get_entity(q.to_owned()) {
                Some(item) => item,
                None => continue,
            };
            let statements = item.claims_with_property(match_field.get_property());
            for statement in &statements {
                Self::match_person_by_single_date_check_statement(
                    statement,
                    precision,
                    match_field.get_field_name(),
                    result,
                    &mut candidates,
                    q,
                );
            }
        }
        if candidates.len() == 1 {
            // TODO >1
            if let Some(q) = candidates.first() {
                let _ = Entry::from_id(result.entry_id, &self.app)
                    .await?
                    .set_match(q, USER_DATE_MATCH)
                    .await;
            }
        };
        Ok(())
    }

    //TODO test
    async fn search_person(&self, name: &str) -> Result<Vec<String>> {
        let name = Person::sanitize_simplify_name(name);
        self.app.wikidata().search_with_type_api(&name, "Q5").await
    }

    //TODO test
    async fn subset_items_by_birth_death_year(
        &self,
        all_items: &[String],
        birth_year: i32,
        death_year: i32,
        mw_api: &mediawiki::api::Api,
    ) -> Result<Vec<String>> {
        let mut ret = vec![];
        for items in all_items.chunks(100) {
            let item_str = items.join(" wd:");
            let sparql = format!("SELECT DISTINCT ?q {{ VALUES ?q {{ wd:{} }} . ?q wdt:P569 ?born ; wdt:P570 ?died. FILTER ( year(?born)={}).FILTER ( year(?died)={} ) }}",&item_str,birth_year,death_year);
            if let Ok(results) = mw_api.sparql_query(&sparql).await {
                let mut candidates = mw_api.entities_from_sparql_result(&results, "q");
                ret.append(&mut candidates);
            }
        }
        Ok(ret)
    }

    //TODO test
    fn extract_sane_year_from_date(date: &str) -> Option<i32> {
        let captures = RE_YEAR.captures(date)?;
        if captures.len() != 2 {
            return None;
        }
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        if year < 0 || year > Utc::now().year() {
            None
        } else {
            Some(year)
        }
    }

    async fn automatch_complex_batch(
        &self,
        el_chunk: &[(usize, String)],
        sparql_parts: &str,
        language: &str,
    ) -> Result<()> {
        let search_results = self.automatch_complex_batch_search(el_chunk).await?;
        let api = self.app.wikidata().get_mw_api().await?;
        let entry_ids = el_chunk.iter().map(|(entry_id, _)| *entry_id).collect_vec();
        let mut entries = Entry::multiple_from_ids(&entry_ids, &self.app).await?;

        for sr in search_results.chunks(50) {
            let sr = sr.join(" wd:");
            let sparql_subquery =
                format!("SELECT DISTINCT ?q {{ {sparql_parts} . VALUES ?q {{ wd:{sr} }} }}");
            let sparql = format!("SELECT ?q ?qLabel {{ {{ {sparql_subquery} }} SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"{language},[AUTO_LANGUAGE],en\" }} }}");
            let mut reader = match self.app.wikidata().load_sparql_csv(&sparql).await {
                Ok(result) => result,
                Err(_) => continue, // Ignore error
            };
            for row in reader.records().filter_map(|r| r.ok()) {
                Self::automatch_complex_batch_process_row(&api, row, el_chunk, &mut entries).await;
            }
        }
        Ok(())
    }

    async fn automatch_complex_batch_search(
        &self,
        el_chunk: &[(usize, String)],
    ) -> Result<Vec<String>> {
        let query: Vec<String> = el_chunk
            .iter()
            .map(|(_, label)| format!("\"{}\"", label.replace('"', "")))
            .collect();
        let query = query.join(" OR ");
        let mut search_results = self
            .app
            .wikidata()
            .search_with_limit(&query, Some(500))
            .await?;
        if search_results.is_empty() {
            return Ok(vec![]);
        }
        search_results.sort();
        search_results.dedup();
        Ok(search_results)
    }

    async fn automatch_complex_get_sparql_parts(&self, catalog: &Catalog) -> Result<String> {
        let key_value_pairs = catalog.get_key_value_pairs().await?;
        let property_roots = key_value_pairs
            .get("automatch_complex")
            .ok_or_else(|| anyhow!("No automatch_complex key in catalog"))?;
        let property_roots = serde_json::from_str::<Vec<(usize, usize)>>(property_roots)?;
        let sparql_parts: Vec<String> = property_roots
            .iter()
            .map(|(p, q)| match *p {
                31 => format!("?q wdt:P31/wdt:P279* wd:Q{q}"),
                131 => format!("?q wdt:P131* wd:Q{q}"),
                prop => format!("?q wdt:P{prop} wd:Q{q}"),
            })
            .collect();
        let sparql_parts = sparql_parts.join(" . ");
        Ok(sparql_parts)
    }

    pub async fn automatch_complex(&mut self, catalog_id: usize) -> Result<()> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let sparql_parts = self.automatch_complex_get_sparql_parts(&catalog).await?;
        let mut language = catalog.search_wp().to_string();
        if language.is_empty() {
            language = "en".to_string();
        }

        let mut offset = self.get_last_job_offset().await;
        let batch_size = 10;
        loop {
            let el_chunk = self
                .app
                .storage()
                .automatch_complex_get_el_chunk(catalog_id, offset, batch_size)
                .await?;

            if el_chunk.is_empty() {
                break; // Done
            }
            let _ = self
                .automatch_complex_batch(&el_chunk, &sparql_parts, &language)
                .await; // Ignore error

            if el_chunk.len() < batch_size {
                break;
            }
            offset += el_chunk.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    fn automatch_from_other_catalogs_name_type2id(
        results_in_original_catalog: &[ResultInOriginalCatalog],
    ) -> HashMap<(String, String), Vec<usize>> {
        let mut name_type2id: HashMap<(String, String), Vec<usize>> = HashMap::new();
        results_in_original_catalog.iter().for_each(|r| {
            name_type2id
                .entry((r.ext_name.to_owned(), r.type_name.to_owned()))
                .and_modify(|v| v.push(r.entry_id))
                .or_insert(vec![r.entry_id]);
        });
        name_type2id
    }

    fn match_person_by_single_date_check_statement(
        statement: &&wikimisc::wikibase::Statement,
        precision: i32,
        match_field: &str,
        result: &CandidateDates,
        candidates: &mut Vec<String>,
        q: &str,
    ) {
        let main_snak = statement.main_snak();
        let data_value = match main_snak.data_value() {
            Some(dv) => dv,
            None => return,
        };
        let time = match data_value.value() {
            wikimisc::wikibase::value::Value::Time(tv) => tv,
            _ => return,
        };
        let dt = match NaiveDateTime::parse_from_str(time.time(), "+%Y-%m-%dT%H:%M:%SZ") {
            Ok(dt) => dt,
            _ => return, // Could not parse date
        };
        let date = match precision {
            4 => format!("{}", dt.format("%Y")),
            10 => format!("{}", dt.format("%Y-%m-%d")),
            other => panic!("Bad precision '{other}'"), // Should never happen
        };
        if (match_field == "born" && date == result.born)
            || (match_field == "died" && date == result.died)
        {
            candidates.push(q.to_string());
        }
    }

    async fn automatch_complex_batch_process_row(
        api: &Api,
        row: csv::StringRecord,
        el_chunk: &[(usize, String)],
        entries: &mut HashMap<usize, Entry>,
    ) {
        let q = api.extract_entity_from_uri(&row[0]).unwrap();
        let q_label = &row[1];
        let entry_candidates: Vec<usize> = el_chunk
            .iter()
            .filter(|(_, label)| label.contains(q_label) || q_label.contains(label))
            .map(|(entry_id, _)| *entry_id)
            .collect();
        if entry_candidates.len() != 1 {
            // No match, or multiple matches, not touching this one
            return;
        }

        if let Some(entry) = entries.get_mut(&entry_candidates[0]) {
            // println!("{q} {q_label} => {}",entry.id);
            let _ = entry.set_auto_and_multi_match(&[q]).await; // Ignore error
        }
    }

    fn automatch_by_sitelink_name2entries(
        entries: &[(usize, String)],
    ) -> HashMap<String, Vec<usize>> {
        let mut name2entries: HashMap<String, Vec<usize>> = HashMap::new();
        entries.iter().for_each(|(id, name)| {
            name2entries
                .entry(name.to_owned())
                .and_modify(|n2e| n2e.push(*id))
                .or_insert(vec![*id]);
        });
        name2entries
    }

    async fn automatch_by_search_process_results_batch_process_futures(
        &self,
        result_batch: &[(usize, String, String, String)],
    ) -> Vec<(usize, String)> {
        let mut futures = vec![];
        for result in result_batch {
            let entry_id = result.0;
            let label = &result.1;
            let type_q = &result.2;
            let aliases: Vec<&str> = result
                .3
                .split('|')
                .filter(|alias| !alias.is_empty())
                .collect();
            let future = self.search_with_type_and_entity_id(entry_id, label, type_q);
            futures.push(future);
            for alias in &aliases {
                let future_tmp = self.search_with_type_and_entity_id(entry_id, alias, type_q);
                futures.push(future_tmp);
            }
        }

        let mut search_results = join_all(futures)
            .await
            .into_iter()
            .flatten()
            .flat_map(|(entry_id, items)| items.into_iter().map(move |q| (entry_id, q.to_string())))
            .collect_vec();
        search_results.sort();
        search_results.dedup();
        search_results
    }

    async fn get_json_from_url_and_entry(
        client: &reqwest::Client,
        url: String,
        entry: Entry,
    ) -> Result<(serde_json::Value, Entry)> {
        let result = client
            .get(url)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        Ok((result, entry))
    }

    fn json_array_of_strings_to_vec_item_ids(json: &serde_json::Value) -> Vec<usize> {
        match json.as_array() {
            Some(array) => array
                .iter()
                .map(|item| item.as_str().unwrap()[1..].parse().unwrap())
                .collect(),
            None => Vec::new(),
        }
    }

    pub async fn automatch_people_with_birth_year(&self, catalog_id: usize) -> Result<()> {
        self.app
            .storage()
            .automatch_people_with_birth_year(catalog_id)
            .await?;
        Ok(())
    }

    pub async fn automatch_people_with_initials(&self, catalog_id: usize) -> Result<()> {
        let client = crate::autoscrape::Autoscrape::reqwest_client_external()?;
        let all_entries = self
            .app
            .storage()
            .catalog_get_entries_of_people_with_initials(catalog_id)
            .await?;
        for entries in all_entries.chunks(50) {
            let futures: Vec<_> = entries
                .iter()
                .map(|entry| {
                    let url = format!(
                        "https://wd-infernal.toolforge.org/initial_search/{}",
                        urlencoding::encode(&entry.ext_name)
                    );
                    Self::get_json_from_url_and_entry(&client, url, entry.to_owned())
                })
                .collect();

            let stream = futures::stream::iter(futures).buffer_unordered(5);
            let mut results = stream.collect::<Vec<_>>().await;
            for (json, entry) in results.iter_mut().flatten() {
                let items = Self::json_array_of_strings_to_vec_item_ids(json);
                match items.len() {
                    0 => {
                        if !entry.is_unmatched() {
                            let _ = entry.unmatch().await;
                        }
                    }
                    1 => {
                        let _ = entry.set_match(&format!("{}", items[0]), USER_AUTO).await;
                    }
                    _ => {
                        let items = items
                            .iter()
                            .map(|q| format!("Q{q}"))
                            .collect::<Vec<String>>();
                        let _ = entry.set_multi_match(&items).await;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{get_test_app, TEST_MUTEX};

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;
    const TEST_ENTRY_ID2: usize = 144000954;

    // TODO finish test
    // #[tokio::test]
    // async fn test_automatch_complex() {
    //     let _test_lock = TEST_MUTEX.lock();
    //     let app = get_test_app();
    //     let mut am = AutoMatch::new(&app);
    //     let result = am.automatch_complex(3663).await.unwrap();
    //     println!("{result:?}");
    // }

    #[tokio::test]
    async fn test_match_person_by_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        Entry::from_id(TEST_ENTRY_ID2, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();

        // Match by date
        let mut am = AutoMatch::new(&app);
        am.match_person_by_dates(TEST_CATALOG_ID).await.unwrap();

        // Check if set
        let entry = Entry::from_id(TEST_ENTRY_ID2, &app).await.unwrap();
        assert!(entry.is_fully_matched());
        assert_eq!(1035, entry.q.unwrap());
    }

    #[tokio::test]
    async fn test_automatch_by_search() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();

        assert!(Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .is_unmatched());

        // Run automatch
        let mut am = AutoMatch::new(&app);
        am.automatch_by_search(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(467402));
        assert_eq!(entry.user, Some(USER_AUTO));

        // Clear
        entry.unmatch().await.unwrap();
    }

    #[tokio::test]
    async fn test_automatch_by_sitelink() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();

        let mut am = AutoMatch::new(&app);

        // am.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Run automatch
        am.automatch_by_sitelink(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry2.q, Some(13520818));
        assert_eq!(entry2.user, Some(USER_AUTO));

        // Clear
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }

    #[tokio::test]
    async fn test_purge_automatches() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Set a full match
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();
        entry.set_match("Q1", 4).await.unwrap();
        assert!(entry.is_fully_matched());

        // Purge catalog
        let am2 = AutoMatch::new(&app);
        am2.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is still fully matched
        let entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry2.is_fully_matched());

        // Set an automatch
        let mut entry3 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry3.unmatch().await.unwrap();
        entry3.set_match("Q1", 0).await.unwrap();
        assert!(entry3.is_partially_matched());

        // Purge catalog
        let am4 = AutoMatch::new(&app);
        am4.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is now unmatched
        let entry4 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry4.is_unmatched());
    }

    #[tokio::test]
    async fn test_match_person_by_single_date() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();

        let mut am = AutoMatch::new(&app);

        // Set prelim match
        let mut entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry2.set_match("Q13520818", 0).await.unwrap();

        // Run automatch
        am.match_person_by_single_date(TEST_CATALOG_ID, DateMatchField::Born, DatePrecision::Day)
            .await
            .unwrap();

        // Check match
        let mut entry3 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry3.q, Some(13520818));
        assert_eq!(entry3.user, Some(USER_DATE_MATCH));

        // Cleanup
        entry3.unmatch().await.unwrap();
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }
}
