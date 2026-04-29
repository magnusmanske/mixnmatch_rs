//! Search-based and label-based matching strategies.
//!
//! Each `automatch_*` method here implements one matching approach
//! against Wikidata. They share the AutoMatch struct's app/job
//! state but are otherwise independent — re-ordering, swapping in,
//! or removing a strategy doesn't affect the others.

use super::{
    AutoMatch, AutomatchSearchRow, ResultInOriginalCatalog, ResultInOtherCatalog,
    SPARQL_FALLBACK_BATCH_SIZE, SPARQL_PROCESS_CHUNK_SIZE,
};
use crate::app_state::USER_AUTO;
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::entry_query::EntryQuery;
use crate::job::Jobbable;
use crate::match_state::MatchState;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use futures::future::join_all;
use itertools::Itertools;
use mediawiki::api::Api;
use std::collections::HashMap;

impl AutoMatch {
    /// Helper method to sort and deduplicate a vector of strings
    pub(super) fn sort_and_dedup(items: &mut Vec<String>) {
        items.sort();
        items.dedup();
    }

    pub async fn automatch_with_sparql(&mut self, catalog_id: usize) -> Result<()> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let sparql = Self::read_automatch_sparql(&catalog).await?;

        // Try the unbatched query first — fast path when WDQS is healthy
        // and the result is small enough to stream. If the streaming query
        // fails for any reason (WDQS timeout, dropped TCP connection,
        // partial transfer), fall back to LIMIT/OFFSET batching so the
        // job still completes the whole result set.
        if let Err(e) = self.run_sparql_streaming(catalog_id, &sparql).await {
            log::warn!(
                "automatch_with_sparql cat={catalog_id}: streaming run failed ({e}); falling back to batched query"
            );
            self.run_sparql_batched(catalog_id, &sparql).await?;
        }

        let _ = self.app.storage().use_automatchers(catalog_id, 0).await;
        Ok(())
    }

    /// Pull the user-supplied SPARQL fragment out of the catalog's kv_pairs
    /// and shape it into a complete `SELECT ?q ?qLabel WHERE { ... }` query
    /// when the user only supplied the WHERE body.
    async fn read_automatch_sparql(catalog: &Catalog) -> Result<String> {
        let kv_pairs = catalog.get_key_value_pairs().await?;
        let sparql_part = kv_pairs
            .get("automatch_sparql")
            .ok_or_else(|| anyhow!("No automatch_sparql key in catalog"))?;
        Ok(if sparql_part.starts_with("SELECT ") {
            sparql_part.clone()
        } else {
            format!("SELECT ?q ?qLabel WHERE {{ {sparql_part} }}")
        })
    }

    /// Stream the query in one go, processing matches in fixed-size chunks.
    /// Returns Err on any stream/parse failure so the caller can fall back
    /// to batched paging.
    async fn run_sparql_streaming(&self, catalog_id: usize, sparql: &str) -> Result<()> {
        let mut reader = self.app.wikidata().load_sparql_csv(sparql).await?;
        let api = self.app.wikidata().get_mw_api().await?;
        let mut label2q = HashMap::new();
        for row in reader.records() {
            // Propagate row-level errors instead of `filter_map(Result::ok)`-ing
            // them away — silently dropping rows after a broken transfer is
            // exactly what we want to detect and recover from.
            let row = row?;
            if let Some((label, q_numeric)) = Self::parse_sparql_row(&api, &row) {
                label2q.insert(label, q_numeric);
                if label2q.len() >= SPARQL_PROCESS_CHUNK_SIZE {
                    self.process_automatch_with_sparql(catalog_id, &label2q)
                        .await?;
                    label2q.clear();
                }
            }
        }
        self.process_automatch_with_sparql(catalog_id, &label2q)
            .await?;
        Ok(())
    }

    /// Fallback: wrap the user query as a sub-select and page through it
    /// with deterministic ordering on `?q`. Each batch is its own HTTP
    /// request, so a single failure costs at most one batch's worth of work.
    async fn run_sparql_batched(&self, catalog_id: usize, sparql: &str) -> Result<()> {
        let batch_size = *self
            .app
            .task_specific_usize()
            .get("automatch_sparql_batch_size")
            .unwrap_or(&SPARQL_FALLBACK_BATCH_SIZE);
        let api = self.app.wikidata().get_mw_api().await?;
        let mut offset = 0_usize;
        loop {
            // Trying to avoid the sub-select wrapper and use `LIMIT/OFFSET` directly
            let paged = format!("{sparql} LIMIT {batch_size} OFFSET {offset}");
            let mut reader = match self.app.wikidata().load_sparql_csv(&paged).await {
                Ok(r) => r,
                Err(e) => {
                    return Err(anyhow!("batched SPARQL failed at offset {offset}: {e}"));
                }
            };
            let mut label2q = HashMap::new();
            let mut row_count = 0_usize;
            for row in reader.records() {
                // Inside a batch, skip the occasional bad row instead of
                // aborting — we'll still surface a clean stop-condition via
                // `row_count < batch_size`.
                let Ok(row) = row else { continue };
                row_count += 1;
                if let Some((label, q_numeric)) = Self::parse_sparql_row(&api, &row) {
                    label2q.insert(label, q_numeric);
                }
            }
            if !label2q.is_empty() {
                self.process_automatch_with_sparql(catalog_id, &label2q)
                    .await?;
            }
            if row_count < batch_size {
                break;
            }
            offset += batch_size;
        }
        Ok(())
    }

    fn parse_sparql_row(api: &Api, row: &csv::StringRecord) -> Option<(String, usize)> {
        let q = api.extract_entity_from_uri(row.get(0)?).ok()?;
        let q_numeric = q.get(1..)?.parse::<usize>().ok()?;
        let label = row.get(1)?.to_lowercase();
        Some((label, q_numeric))
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
                    let _ = EntryWriter::new(&self.app, entry)
                        .set_match(&format!("Q{q}"), USER_AUTO)
                        .await;
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
                        let _ = EntryWriter::new(&self.app, &mut entry)
                            .set_match(&format!("Q{q}"), USER_AUTO)
                            .await;
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

    async fn search_with_type_and_entity_id(
        &self,
        entry_id: usize,
        name: &str,
        type_q: &str,
    ) -> Option<(usize, Vec<String>)> {
        let mut items = match self.app.wikidata().search_with_type_api(name, type_q).await {
            Ok(items) => items,
            Err(_e) => return None,
        };
        if items.is_empty() {
            return None;
        }
        Self::sort_and_dedup(&mut items);
        Some((entry_id, items))
    }

    async fn match_entries_to_items(
        &self,
        entry_id2items: &HashMap<usize, Vec<String>>,
    ) -> Result<()> {
        let entry_ids: Vec<usize> = entry_id2items.keys().copied().collect();
        let mut entries = Entry::multiple_from_ids(&entry_ids, &self.app).await?;

        for (entry_id, entry) in &mut entries {
            let items = match entry_id2items.get(entry_id) {
                Some(items) => items,
                None => continue,
            };
            let _ = EntryWriter::new(&self.app, entry)
                .set_auto_and_multi_match(items)
                .await;
        }
        Ok(())
    }

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

            for result_batch in results.chunks(search_batch_size) {
                self.automatch_by_search_process_results_batch(result_batch)
                    .await;
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

    async fn automatch_by_search_process_results_batch(
        &mut self,
        result_batch: &[AutomatchSearchRow],
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
        let mut no_meta_items: Vec<String> = search_results
            .iter()
            .map(|(_entry_id, q)| q.clone())
            .collect();
        let _ = self
            .app
            .wikidata()
            .remove_meta_items(&mut no_meta_items)
            .await;
        // Avoid an O(N·M) scan across the candidate list for every search
        // result — batches routinely run into the thousands.
        let keep: std::collections::HashSet<String> = no_meta_items.into_iter().collect();
        search_results.retain(|(_entry_id, q)| keep.contains(q));
    }

    async fn automatch_by_search_process_results_batch_process_futures(
        &self,
        result_batch: &[AutomatchSearchRow],
    ) -> Vec<(usize, String)> {
        let mut futures = vec![];
        for result in result_batch {
            let entry_id = result.entry_id;
            let label = &result.ext_name;
            let type_q = &result.type_name;
            let aliases: Vec<&str> = result
                .aliases
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
                continue;
            }
            if let Ok(mut entry) = Entry::from_id(object_entry_id, &self.app).await {
                let _ = EntryWriter::new(&self.app, &mut entry)
                    .set_auto_and_multi_match(&items)
                    .await;
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
        if items.is_empty() {
            return;
        }
        let mut entry = match Entry::from_id(entry_id, &self.app).await {
            Ok(entry) => entry,
            _ => return, // Ignore error
        };
        let _ = EntryWriter::new(&self.app, &mut entry)
            .set_auto_and_multi_match(&items)
            .await;
    }

    async fn automatch_simple_items_from_result(
        &mut self,
        result: &AutomatchSearchRow,
    ) -> Option<(usize, Vec<String>)> {
        let entry_id = result.entry_id;
        let label = &result.ext_name;
        let type_q = &result.type_name;
        let aliases: Vec<&str> = result.aliases.split('|').collect();
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
        Self::sort_and_dedup(&mut items);
        if self
            .app
            .wikidata()
            .remove_meta_items(&mut items)
            .await
            .is_err()
        {
            return None;
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
                    let _ = EntryWriter::new(&self.app, &mut entry)
                        .set_match(&q, USER_AUTO)
                        .await;
                };
            }
        }
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

    pub async fn purge_automatches(&self, catalog_id: usize) -> Result<()> {
        self.app.storage().purge_automatches(catalog_id).await
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
                    get_json_from_url_and_entry(&client, url, entry.to_owned())
                })
                .collect();

            let stream = futures::stream::iter(futures).buffer_unordered(5);
            let mut results = stream.collect::<Vec<_>>().await;
            for (json, entry) in results.iter_mut().flatten() {
                let items = json_array_of_strings_to_vec_item_ids(json);
                match items.len() {
                    0 => {
                        if !entry.is_unmatched() {
                            let _ = EntryWriter::new(&self.app, entry).unmatch().await;
                        }
                    }
                    1 => {
                        let _ = EntryWriter::new(&self.app, entry)
                            .set_match(&format!("{}", items[0]), USER_AUTO)
                            .await;
                    }
                    _ => {
                        let items = items
                            .iter()
                            .map(|q| format!("Q{q}"))
                            .collect::<Vec<String>>();
                        let _ = EntryWriter::new(&self.app, entry)
                            .set_multi_match(&items)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }
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
            .filter_map(|item| item.as_str()?.get(1..)?.parse().ok())
            .collect(),
        None => Vec::new(),
    }
}
