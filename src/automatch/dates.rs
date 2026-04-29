//! Person-date matching and the property-based "complex" strategy.
//!
//! `match_person_by_dates` and `match_person_by_single_date` confirm
//! candidate matches by reading the candidate's birth/death year
//! statements from Wikidata. `automatch_complex` evaluates a
//! catalog-supplied property/item conjunction in SPARQL (instance-of
//! chains, location chains, free properties).

use super::{AutoMatch, CandidateDates, PersonDateMatchRow, RE_YEAR};
use crate::app_state::USER_DATE_MATCH;
use crate::catalog::Catalog;
use crate::app_state::AppState;
use crate::entry::{Entry, EntryWriter};
use crate::issue::Issue;
use crate::issue::IssueType;
use crate::job::Jobbable;
use crate::person::Person;
use anyhow::{Result, anyhow};
use chrono::prelude::*;
use chrono::{NaiveDateTime, Utc};
use itertools::Itertools;
use mediawiki::api::Api;
use serde_json::json;

#[derive(Debug, Clone, Copy)]
pub enum DateMatchField {
    Born,
    Died,
}

impl DateMatchField {
    pub(super) const fn get_field_name(&self) -> &'static str {
        match self {
            DateMatchField::Born => "born",
            DateMatchField::Died => "died",
        }
    }

    pub(super) const fn get_property(&self) -> &'static str {
        match self {
            DateMatchField::Born => crate::util::wikidata_props::P_DATE_OF_BIRTH,
            DateMatchField::Died => crate::util::wikidata_props::P_DATE_OF_DEATH,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DateStringLength {
    Day,
    Year,
}

impl DateStringLength {
    pub(super) const fn as_i32(&self) -> i32 {
        match self {
            DateStringLength::Day => 10,
            DateStringLength::Year => 4,
        }
    }
}

impl AutoMatch {
    /// Helper method to handle match candidates based on their count:
    /// - 0 candidates: do nothing
    /// - 1 candidate: set as match with given user_id
    /// - >1 candidates: create duplicate issue
    async fn handle_match_candidates(
        &self,
        entry_id: usize,
        candidates: Vec<String>,
        user_id: usize,
    ) -> Result<()> {
        match candidates.len() {
            0 => Ok(()),
            1 => {
                let mut entry = Entry::from_id(entry_id, &self.app).await?;
                let _ = EntryWriter::new(&self.app, &mut entry).set_match(&candidates[0], user_id).await?;
                Ok(())
            }
            _ => {
                Issue::new(entry_id, IssueType::WdDuplicate, json!(candidates))
                    .insert(self.app.storage().as_ref().as_ref())
                    .await?;
                Ok(())
            }
        }
    }

    async fn match_person_by_dates_process_result(
        &self,
        result: &PersonDateMatchRow,
        mw_api: &Api,
    ) -> Result<()> {
        let entry_id = result.entry_id;
        let candidate_items = match self
            .match_person_by_dates_process_result_get_candidate_items(result, mw_api)
            .await
        {
            Ok(value) => value,
            Err(value) => return Ok(value),
        };
        self.handle_match_candidates(entry_id, candidate_items, USER_DATE_MATCH)
            .await
    }

    async fn match_person_by_dates_process_result_get_candidate_items(
        &self,
        result: &PersonDateMatchRow,
        mw_api: &Api,
    ) -> Result<Vec<String>, ()> {
        let ext_name = &result.ext_name;
        let birth_year = match Self::extract_sane_year_from_date(&result.born) {
            Some(year) => year,
            None => return Err(()),
        };
        let death_year = match Self::extract_sane_year_from_date(&result.died) {
            Some(year) => year,
            None => return Err(()),
        };
        let candidate_items = match self.search_person(ext_name).await {
            Ok(c) => c,
            _ => return Err(()),
        };
        if candidate_items.is_empty() {
            return Err(());
        }
        let candidate_items = match self
            .subset_items_by_birth_death_year(
                &candidate_items,
                birth_year,
                death_year,
                mw_api,
                ext_name,
            )
            .await
        {
            Ok(ci) => ci,
            _ => return Err(()),
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
        precision: DateStringLength,
    ) -> Result<()> {
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
        self.handle_match_candidates(result.entry_id, candidates, USER_DATE_MATCH)
            .await
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
            _ => return,
        };
        let date = match precision {
            4 => format!("{}", dt.format("%Y")),
            10 => format!("{}", dt.format("%Y-%m-%d")),
            other => panic!("Bad precision '{other}'"),
        };
        if (match_field == "born" && date == result.born)
            || (match_field == "died" && date == result.died)
        {
            candidates.push(q.to_string());
        }
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
        name: &str,
    ) -> Result<Vec<String>> {
        // Extract the family name (last word of the simplified name) for candidate filtering.
        // This prevents false positives where birth/death years coincidentally match but the
        // names are clearly different (e.g. "A. G. Thomas" matching "Thomas Douglas Forsyth").
        let simplified = Person::sanitize_simplify_name(name);
        let family_name = simplified
            .split_whitespace()
            .last()
            .unwrap_or("")
            .to_lowercase();

        let mut ret = vec![];
        for items in all_items.chunks(100) {
            let item_str = items.join(" wd:");
            let sparql = format!(
                "SELECT DISTINCT ?q ?qLabel {{ VALUES ?q {{ wd:{} }} . ?q wdt:P569 ?born ; wdt:P570 ?died. FILTER ( year(?born)={}).FILTER ( year(?died)={} ) SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"en\" }} }}",
                &item_str, birth_year, death_year
            );
            if let Ok(results) = mw_api.sparql_query(&sparql).await {
                if let Some(bindings) = results["results"]["bindings"].as_array() {
                    for b in bindings {
                        let entity_url = match b["q"]["value"].as_str() {
                            Some(u) => u,
                            None => continue,
                        };
                        let q = match mw_api.extract_entity_from_uri(entity_url) {
                            Ok(q) => q,
                            Err(_) => continue,
                        };
                        // If we have a family name, require the candidate's English label to
                        // contain it (case-insensitive). Skip candidates with no label.
                        if !family_name.is_empty() {
                            let label =
                                b["qLabel"]["value"].as_str().unwrap_or("").to_lowercase();
                            if label.is_empty() || !label.contains(&family_name) {
                                continue;
                            }
                        }
                        ret.push(q);
                    }
                }
            }
        }
        Ok(ret)
    }

    //TODO test
    pub(super) fn extract_sane_year_from_date(date: &str) -> Option<i32> {
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
            let sparql = format!(
                "SELECT ?q ?qLabel {{ {{ {sparql_subquery} }} SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"{language},[AUTO_LANGUAGE],en\" }} }}"
            );
            let mut reader = match self.app.wikidata().load_sparql_csv(&sparql).await {
                Ok(result) => result,
                Err(_) => continue,
            };
            for row in reader.records().filter_map(|r| r.ok()) {
                Self::automatch_complex_batch_process_row(
                    &api,
                    row,
                    el_chunk,
                    &mut entries,
                    &self.app,
                )
                .await;
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
        Self::sort_and_dedup(&mut search_results);
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
                break;
            }
            let _ = self
                .automatch_complex_batch(&el_chunk, &sparql_parts, &language)
                .await;

            if el_chunk.len() < batch_size {
                break;
            }
            offset += el_chunk.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_complex_batch_process_row(
        api: &Api,
        row: csv::StringRecord,
        el_chunk: &[(usize, String)],
        entries: &mut std::collections::HashMap<usize, Entry>,
        app: &AppState,
    ) {
        let q = match api.extract_entity_from_uri(&row[0]) {
            Ok(q) => q,
            Err(_) => return,
        };
        let q_label = &row[1];
        let entry_candidates: Vec<usize> = el_chunk
            .iter()
            .filter(|(_, label)| label.contains(q_label) || q_label.contains(label))
            .map(|(entry_id, _)| *entry_id)
            .collect();
        if entry_candidates.len() != 1 {
            return;
        }

        if let Some(entry) = entries.get_mut(&entry_candidates[0]) {
            let _ = EntryWriter::new(app, entry)
                .set_auto_and_multi_match(&[q])
                .await;
        }
    }
}
