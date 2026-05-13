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
use crate::app_state::{USER_AUTO, item2numeric};
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::entry_query::EntryQuery;
use crate::job::Jobbable;
use crate::match_state::MatchState;
use anyhow::{Result, anyhow};
use futures::StreamExt;
use mediawiki::api::Api;
use std::collections::HashMap;

/// Map from `(label_text, type_q)` to the list of entry IDs that share that key.
type TextTypeMap = HashMap<(String, String), Vec<usize>>;

impl AutoMatch {
    /// Sort by numeric Q-id and deduplicate. Used by callers whose item
    /// list has no inherent relevance order (e.g. raw term-store SQL
    /// results); numeric sort puts the smallest Q first, which acts as a
    /// notability heuristic since lower Q-ids tend to be older/more
    /// established items. Downstream `set_auto_and_multi_match` then picks
    /// items[0] as the auto-match.
    pub(super) fn sort_and_dedup(items: &mut Vec<String>) {
        items.sort_by_key(|s| item2numeric(s).unwrap_or(0));
        items.dedup();
    }

    /// Deduplicate keeping the first occurrence of each item; preserves
    /// the input order. Used by callers whose list already encodes a
    /// preference order (e.g. Wikidata search-API results, which return
    /// exact-title matches first). Codeberg #90.
    pub(super) fn dedup_preserving_order(items: &mut Vec<String>) {
        let mut seen = std::collections::HashSet::new();
        items.retain(|item| seen.insert(item.clone()));
    }

    pub async fn automatch_with_sparql(&mut self, catalog_id: usize) -> Result<()> {
        let catalog = Catalog::from_id(catalog_id, self.app.as_ref()).await?;
        let sparql = Self::read_automatch_sparql(&catalog, self.app.as_ref()).await?;

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
    async fn read_automatch_sparql(catalog: &Catalog, app: &dyn crate::app_state::AppContext) -> Result<String> {
        let kv_pairs = catalog.get_key_value_pairs(app).await?;
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
        let q_str = api.extract_entity_from_uri(row.get(0)?).ok()?;
        let q_numeric = q_str.get(1..)?.parse::<usize>().ok()?;
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
                    let _ = EntryWriter::new(self.app.as_ref(), entry)
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
        let catalog = Catalog::from_id(catalog_id, self.app.as_ref()).await?;
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
        let entry_id2q = Self::build_sitelink_entry_id2q(&wd_matches, &name2entries);
        if entry_id2q.is_empty() {
            return;
        }
        let entry_ids: Vec<usize> = entry_id2q.keys().copied().collect();
        let Ok(mut entries) = Entry::multiple_from_ids(&entry_ids, self.app.as_ref()).await else {
            return;
        };
        for (entry_id, entry) in &mut entries {
            if let Some(q_value) = entry_id2q.get(entry_id) {
                let _ = EntryWriter::new(self.app.as_ref(), entry)
                    .set_match(q_value, USER_AUTO)
                    .await;
            }
        }
    }

    /// Builds an `entry_id → "Q{n}"` map from Wikidata sitelink query results.
    /// When the same `entry_id` is reached via multiple titles, the first match
    /// in `wd_matches` order wins (preserves prior behaviour).
    pub(super) fn build_sitelink_entry_id2q(
        wd_matches: &[(usize, String)],
        name2entries: &HashMap<String, Vec<usize>>,
    ) -> HashMap<usize, String> {
        let mut entry_id2q: HashMap<usize, String> = HashMap::new();
        for (q, title) in wd_matches {
            if let Some(ids) = name2entries.get(title) {
                let q_value = format!("Q{q}");
                for entry_id in ids {
                    entry_id2q.entry(*entry_id).or_insert(q_value.clone());
                }
            }
        }
        entry_id2q
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

    async fn match_entries_to_items(
        &self,
        entry_id2items: &HashMap<usize, Vec<String>>,
    ) -> Result<()> {
        let entry_ids: Vec<usize> = entry_id2items.keys().copied().collect();
        let mut entries = Entry::multiple_from_ids(&entry_ids, self.app.as_ref()).await?;

        for (entry_id, entry) in &mut entries {
            let items = match entry_id2items.get(entry_id) {
                Some(items) => items,
                None => continue,
            };
            let _ = EntryWriter::new(self.app.as_ref(), entry)
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
        let concurrent = *self
            .app
            .task_specific_usize()
            .get("automatch_by_search_search_batch_size")
            .unwrap_or(&50);

        // One COUNT at the start so the UI can render a real percent
        // alongside the resume cursor. Total may drift while the job runs
        // (entries get matched, possibly added) — accept that as cheaper
        // than re-querying every batch. `from_counts` clamps overshoot to
        // 100%.
        let total = self
            .app
            .storage()
            .number_of_entries_in_catalog_filtered(
                catalog_id,
                &MatchState::not_fully_matched(),
            )
            .await
            .ok()
            .map(|n| n as u64);

        loop {
            let results = self
                .app
                .storage()
                .automatch_by_search_get_results(catalog_id, offset, batch_size)
                .await?;

            self.automatch_by_search_process_results_batch(&results, concurrent)
                .await;

            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.report_progress(offset as u64, total).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_by_search_process_results_batch(
        &mut self,
        result_batch: &[AutomatchSearchRow],
        concurrent: usize,
    ) {
        let mut search_results = self
            .automatch_by_search_process_results_batch_process_futures(result_batch, concurrent)
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
        concurrent: usize,
    ) -> Vec<(usize, String)> {
        if result_batch.is_empty() {
            return vec![];
        }
        let (label_map, alias_map) = Self::group_searches_by_text(result_batch);

        // Two-phase search: labels first, aliases second.
        // Running phases sequentially guarantees that each entry's label
        // candidates are inserted before its alias candidates, preserving
        // the relevance ordering required by Codeberg #90.
        let label_results = run_search_phase(self, label_map, concurrent).await;
        let alias_results = run_search_phase(self, alias_map, concurrent).await;

        // Flatten: for each entry, label candidates first then alias extras,
        // with per-entry order-preserving dedup.
        let all_entry_ids: std::collections::HashSet<usize> = label_results
            .keys()
            .chain(alias_results.keys())
            .copied()
            .collect();
        let mut flat: Vec<(usize, String)> = Vec::new();
        for entry_id in all_entry_ids {
            let mut seen = std::collections::HashSet::new();
            let label_items = label_results.get(&entry_id).map_or(&[][..], |v| v.as_slice());
            let alias_items = alias_results.get(&entry_id).map_or(&[][..], |v| v.as_slice());
            for q in label_items.iter().chain(alias_items.iter()) {
                if seen.insert(q.as_str()) {
                    flat.push((entry_id, q.clone()));
                }
            }
        }
        flat
    }

    /// Groups entries in `result_batch` by their unique `(text, type_q)` search keys,
    /// returning separate maps for label searches and alias searches.
    /// Entries sharing the same label+type produce a single shared search key,
    /// which eliminates duplicate Wikidata API calls.
    pub(super) fn group_searches_by_text(
        result_batch: &[AutomatchSearchRow],
    ) -> (TextTypeMap, TextTypeMap) {
        let mut label_map: TextTypeMap = HashMap::new();
        let mut alias_map: TextTypeMap = HashMap::new();
        for row in result_batch {
            label_map
                .entry((row.ext_name.clone(), row.type_name.clone()))
                .or_default()
                .push(row.entry_id);
            for alias in row.aliases.split('|').filter(|a| !a.is_empty()) {
                alias_map
                    .entry((alias.to_string(), row.type_name.clone()))
                    .or_default()
                    .push(row.entry_id);
            }
        }
        (label_map, alias_map)
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
            if let Ok(mut entry) = Entry::from_id(object_entry_id, self.app.as_ref()).await {
                let _ = EntryWriter::new(self.app.as_ref(), &mut entry)
                    .set_auto_and_multi_match(&items)
                    .await;
            };
        }
        Ok(())
    }

    pub async fn automatch_simple(&mut self, catalog_id: usize) -> Result<()> {
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        // Total over the same `not_fully_matched` filter the row-fetch uses
        // (see `automatch_simple_get_results`), so processed/total agree.
        let total = self
            .app
            .storage()
            .number_of_entries_in_catalog_filtered(
                catalog_id,
                &MatchState::not_fully_matched(),
            )
            .await
            .ok()
            .map(|n| n as u64);
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
            let _ = self.report_progress(offset as u64, total).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_simple_set_matches(&mut self, items: Vec<String>, entry_id: usize) {
        if items.is_empty() {
            return;
        }
        let mut entry = match Entry::from_id(entry_id, self.app.as_ref()).await {
            Ok(entry) => entry,
            _ => return, // Ignore error
        };
        let _ = EntryWriter::new(self.app.as_ref(), &mut entry)
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

    pub async fn automatch_from_other_catalogs(&mut self, catalog_id: usize) -> Result<()> {
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 500;
        // Row-fetch filters by bare `q IS NULL` (see
        // `automatch_from_other_catalogs_get_results`); use the same filter
        // for the total so processed/total agree on the same population.
        let total = self
            .app
            .storage()
            .number_of_entries_in_catalog_filtered(catalog_id, &MatchState::unmatched())
            .await
            .ok()
            .map(|n| n as u64);
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
            self.automatch_from_other_catalogs_apply_matches(
                &results_in_other_catalogs,
                &name_type2id,
            )
            .await;
            if results_in_original_catalog.len() < batch_size {
                break;
            }
            // Preserve the existing flush ordering: persist `offset` (the
            // start of the just-processed batch) as the safe resume point
            // before advancing. The progress bar therefore lags by one
            // batch but the resume cursor is honest.
            let _ = self.report_progress(offset as u64, total).await;
            offset += results_in_original_catalog.len();
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn automatch_from_other_catalogs_apply_matches(
        &self,
        results_in_other_catalogs: &[ResultInOtherCatalog],
        name_type2id: &HashMap<(String, String), Vec<usize>>,
    ) {
        // Build entry_id → q_value in one pass (no I/O).
        // If an entry_id appears more than once (same name matched in multiple
        // other catalogs) the first match wins, which is fine — a second pass
        // would just try to re-match an already-matched entry.
        let mut entry_id2q: HashMap<usize, String> = HashMap::new();
        for r in results_in_other_catalogs {
            let Some(q) = r.q else { continue };
            let key = (r.ext_name.clone(), r.type_name.clone());
            if let Some(ids) = name_type2id.get(&key) {
                let q_value = format!("Q{q}");
                for entry_id in ids {
                    entry_id2q.entry(*entry_id).or_insert(q_value.clone());
                }
            }
        }
        if entry_id2q.is_empty() {
            return;
        }
        // One batch load instead of N individual queries.
        let entry_ids: Vec<usize> = entry_id2q.keys().copied().collect();
        let Ok(mut entries) = Entry::multiple_from_ids(&entry_ids, self.app.as_ref()).await else {
            return;
        };
        for (entry_id, entry) in &mut entries {
            if let Some(q_value) = entry_id2q.get(entry_id) {
                let _ = EntryWriter::new(self.app.as_ref(), entry)
                    .set_match(q_value, USER_AUTO)
                    .await;
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
                            let _ = EntryWriter::new(self.app.as_ref(), entry).unmatch().await;
                        }
                    }
                    1 => {
                        let _ = EntryWriter::new(self.app.as_ref(), entry)
                            .set_match(&format!("{}", items[0]), USER_AUTO)
                            .await;
                    }
                    _ => {
                        let items = items
                            .iter()
                            .map(|q| format!("Q{q}"))
                            .collect::<Vec<String>>();
                        let _ = EntryWriter::new(self.app.as_ref(), entry)
                            .set_multi_match(&items)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Executes a single Wikidata text search and attributes the results to all
/// `entry_ids` that share this `(name, type_q)` key. Returns the same
/// `entry_ids` alongside the de-ordered-deduped candidate Q-strings so the
/// caller can fan results out to multiple entries at once.
async fn search_unique_text(
    am: &AutoMatch,
    name: String,
    type_q: String,
    entry_ids: Vec<usize>,
) -> (Vec<usize>, Vec<String>) {
    let mut items = match am.app.wikidata().search_with_type_api(&name, &type_q).await {
        Ok(items) => items,
        Err(_) => return (entry_ids, vec![]),
    };
    AutoMatch::dedup_preserving_order(&mut items);
    (entry_ids, items)
}

/// Runs all searches in `search_map` with up to `concurrent` in-flight
/// at once. Returns a map of `entry_id → candidate Q-strings` built by
/// fanning each search result out to every entry that shared the search key.
async fn run_search_phase(
    am: &AutoMatch,
    search_map: HashMap<(String, String), Vec<usize>>,
    concurrent: usize,
) -> HashMap<usize, Vec<String>> {
    if search_map.is_empty() {
        return HashMap::new();
    }
    let futures: Vec<_> = search_map
        .into_iter()
        .map(|((name, type_q), entry_ids)| search_unique_text(am, name, type_q, entry_ids))
        .collect();
    let search_results = futures::stream::iter(futures)
        .buffer_unordered(concurrent)
        .collect::<Vec<_>>()
        .await;
    let mut entry2items: HashMap<usize, Vec<String>> = HashMap::new();
    for (entry_ids, items) in search_results {
        for entry_id in entry_ids {
            entry2items
                .entry(entry_id)
                .or_default()
                .extend_from_slice(&items);
        }
    }
    entry2items
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AutomatchSearchRow, ResultInOriginalCatalog};
    use serde_json::json;

    // ── group_searches_by_text ────────────────────────────────────────────

    fn make_row(id: usize, label: &str, type_q: &str, aliases: &str) -> AutomatchSearchRow {
        AutomatchSearchRow::new(id, label.into(), type_q.into(), aliases.into())
    }

    #[test]
    fn group_searches_deduplicates_shared_labels() {
        let rows = vec![
            make_row(1, "John Smith", "Q5", ""),
            make_row(2, "John Smith", "Q5", ""),
            make_row(3, "Jane Doe", "Q5", ""),
        ];
        let (label_map, alias_map) = AutoMatch::group_searches_by_text(&rows);
        assert_eq!(label_map.len(), 2);
        let john_entries = &label_map[&("John Smith".to_string(), "Q5".to_string())];
        assert_eq!(john_entries.len(), 2);
        assert!(john_entries.contains(&1));
        assert!(john_entries.contains(&2));
        assert_eq!(label_map[&("Jane Doe".to_string(), "Q5".to_string())], vec![3]);
        assert!(alias_map.is_empty());
    }

    #[test]
    fn group_searches_separates_labels_and_aliases() {
        let rows = vec![make_row(1, "John Smith", "Q5", "J. Smith|Smithy")];
        let (label_map, alias_map) = AutoMatch::group_searches_by_text(&rows);
        assert_eq!(label_map.len(), 1);
        assert_eq!(alias_map.len(), 2);
        assert_eq!(alias_map[&("J. Smith".to_string(), "Q5".to_string())], vec![1]);
        assert_eq!(alias_map[&("Smithy".to_string(), "Q5".to_string())], vec![1]);
    }

    #[test]
    fn group_searches_deduplicates_shared_aliases() {
        let rows = vec![
            make_row(1, "Alpha", "Q5", "Beta"),
            make_row(2, "Gamma", "Q5", "Beta"),
        ];
        let (_, alias_map) = AutoMatch::group_searches_by_text(&rows);
        assert_eq!(alias_map.len(), 1);
        let beta_entries = &alias_map[&("Beta".to_string(), "Q5".to_string())];
        assert_eq!(beta_entries.len(), 2);
        assert!(beta_entries.contains(&1));
        assert!(beta_entries.contains(&2));
    }

    #[test]
    fn group_searches_empty_batch_gives_empty_maps() {
        let (label_map, alias_map) = AutoMatch::group_searches_by_text(&[]);
        assert!(label_map.is_empty());
        assert!(alias_map.is_empty());
    }

    #[test]
    fn group_searches_skips_empty_aliases() {
        let rows = vec![make_row(1, "Alpha", "Q5", "||")];
        let (label_map, alias_map) = AutoMatch::group_searches_by_text(&rows);
        assert_eq!(label_map.len(), 1);
        assert!(alias_map.is_empty(), "empty alias tokens must be skipped");
    }

    #[test]
    fn group_searches_different_types_are_separate_keys() {
        let rows = vec![
            make_row(1, "River Thames", "Q4022", ""),
            make_row(2, "River Thames", "Q355304", ""),
        ];
        let (label_map, _) = AutoMatch::group_searches_by_text(&rows);
        assert_eq!(label_map.len(), 2, "same label but different types must be separate search keys");
    }

    // ── build_sitelink_entry_id2q ─────────────────────────────────────────

    #[test]
    fn sitelink_entry_id2q_basic_match() {
        let wd_matches = vec![(42_usize, "Albert Einstein".to_string())];
        let name2entries: HashMap<String, Vec<usize>> =
            [("Albert Einstein".to_string(), vec![1_usize])].into();
        let map = AutoMatch::build_sitelink_entry_id2q(&wd_matches, &name2entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map[&1], "Q42");
    }

    #[test]
    fn sitelink_entry_id2q_shared_title_maps_all_entries() {
        let wd_matches = vec![(7_usize, "Paris".to_string())];
        let name2entries: HashMap<String, Vec<usize>> =
            [("Paris".to_string(), vec![10_usize, 20_usize])].into();
        let map = AutoMatch::build_sitelink_entry_id2q(&wd_matches, &name2entries);
        assert_eq!(map.len(), 2);
        assert_eq!(map[&10], "Q7");
        assert_eq!(map[&20], "Q7");
    }

    #[test]
    fn sitelink_entry_id2q_first_match_wins_on_collision() {
        // Same entry_id reached by two different titles → first insertion wins.
        let wd_matches = vec![
            (1_usize, "Rome".to_string()),
            (2_usize, "Roma".to_string()),
        ];
        let name2entries: HashMap<String, Vec<usize>> = [
            ("Rome".to_string(), vec![99_usize]),
            ("Roma".to_string(), vec![99_usize]),
        ]
        .into();
        let map = AutoMatch::build_sitelink_entry_id2q(&wd_matches, &name2entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map[&99], "Q1", "first match in wd_matches order must win");
    }

    #[test]
    fn sitelink_entry_id2q_unrecognised_title_is_ignored() {
        let wd_matches = vec![(5_usize, "Unknown Page".to_string())];
        let name2entries: HashMap<String, Vec<usize>> =
            [("Known Page".to_string(), vec![1_usize])].into();
        let map = AutoMatch::build_sitelink_entry_id2q(&wd_matches, &name2entries);
        assert!(map.is_empty());
    }

    #[test]
    fn sitelink_entry_id2q_empty_inputs_give_empty_map() {
        let map = AutoMatch::build_sitelink_entry_id2q(&[], &HashMap::new());
        assert!(map.is_empty());
    }

    // ── automatch_by_sitelink_name2entries ────────────────────────────────

    #[test]
    fn name2entries_groups_by_name() {
        let entries = vec![
            (1_usize, "Caesar".to_string()),
            (2_usize, "Caesar".to_string()),
            (3_usize, "Brutus".to_string()),
        ];
        let map = AutoMatch::automatch_by_sitelink_name2entries(&entries);
        assert_eq!(map["Caesar"], vec![1, 2]);
        assert_eq!(map["Brutus"], vec![3]);
    }

    #[test]
    fn name2entries_empty_input_gives_empty_map() {
        let map = AutoMatch::automatch_by_sitelink_name2entries(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn name2entries_preserves_case() {
        let entries = vec![
            (1, "caesar".to_string()),
            (2, "Caesar".to_string()),
        ];
        let map = AutoMatch::automatch_by_sitelink_name2entries(&entries);
        assert_eq!(map.len(), 2, "different cases must be separate keys");
    }

    // ── automatch_from_other_catalogs_name_type2id ────────────────────────

    fn make_original(entry_id: usize, name: &str, type_name: &str) -> ResultInOriginalCatalog {
        ResultInOriginalCatalog {
            entry_id,
            ext_name: name.to_string(),
            type_name: type_name.to_string(),
        }
    }

    #[test]
    fn name_type2id_groups_by_name_and_type() {
        let rows = vec![
            make_original(1, "Caesar", "Q5"),
            make_original(2, "Caesar", "Q5"),
            make_original(3, "Caesar", "Q167037"),
        ];
        let map = AutoMatch::automatch_from_other_catalogs_name_type2id(&rows);
        assert_eq!(map[&("Caesar".to_string(), "Q5".to_string())], vec![1, 2]);
        assert_eq!(map[&("Caesar".to_string(), "Q167037".to_string())], vec![3]);
    }

    #[test]
    fn name_type2id_empty_input_gives_empty_map() {
        let map = AutoMatch::automatch_from_other_catalogs_name_type2id(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn name_type2id_different_names_are_separate_keys() {
        let rows = vec![
            make_original(10, "Alpha", "Q5"),
            make_original(20, "Beta", "Q5"),
        ];
        let map = AutoMatch::automatch_from_other_catalogs_name_type2id(&rows);
        assert_eq!(map.len(), 2);
    }

    // ── json_array_of_strings_to_vec_item_ids ─────────────────────────────

    #[test]
    fn json_item_ids_parses_q_strings() {
        let json = json!(["Q1", "Q42", "Q999"]);
        let ids = json_array_of_strings_to_vec_item_ids(&json);
        assert_eq!(ids, vec![1, 42, 999]);
    }

    #[test]
    fn json_item_ids_skips_malformed_entries() {
        let json = json!(["Q1", "NotAnId", "Q", "Q999", 42]);
        let ids = json_array_of_strings_to_vec_item_ids(&json);
        // Only Q1 and Q999 parse correctly; "NotAnId" starts with 'N',
        // "Q" has empty tail, and 42 is not a string.
        assert_eq!(ids, vec![1, 999]);
    }

    #[test]
    fn json_item_ids_empty_array_gives_empty_vec() {
        let json = json!([]);
        assert!(json_array_of_strings_to_vec_item_ids(&json).is_empty());
    }

    #[test]
    fn json_item_ids_non_array_gives_empty_vec() {
        assert!(json_array_of_strings_to_vec_item_ids(&json!(null)).is_empty());
        assert!(json_array_of_strings_to_vec_item_ids(&json!({"q": "Q1"})).is_empty());
    }
}
