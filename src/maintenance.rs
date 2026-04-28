use crate::app_state::{AppState, USER_AUX_MATCH, USER_DATE_MATCH};
use crate::auxiliary_matcher::AuxiliaryMatcher;
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::job::Job;
use crate::match_state::MatchState;
use crate::prop_todo::PropTodo;
use anyhow::{Result, anyhow};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct Maintenance {
    app: AppState,
}

impl Maintenance {
    pub fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and replaces redirects with their targets.
    pub async fn fix_redirects(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
        }
    }

    pub async fn common_names_birth_year(&self) -> Result<()> {
        self.app
            .storage()
            .maintenance_common_names_birth_year()
            .await
    }

    pub async fn taxa(&self) -> Result<()> {
        self.app.storage().maintenance_taxa().await
    }

    pub async fn common_aux(&self) -> Result<()> {
        self.app.storage().maintenance_common_aux().await
    }

    pub async fn artwork(&self) -> Result<()> {
        self.app.storage().maintenance_artwork().await
    }

    /// Various small&cheap maintenance tasks
    pub async fn misc_catalog_things(&self) -> Result<()> {
        // Replace all NOWD entries with NOQ (unmatched) entries.
        // This should never happen anymore, but who knows, it's cheap...
        self.app.storage().replace_nowd_with_noq().await?;

        // Remove inactive catalogs from overview table
        // self.app
        //     .storage()
        //     .remove_inactive_catalogs_from_overview()
        //     .await?;

        // Fix overview rows with weird (<0) numbers
        for otr in self.app.storage().get_overview_table().await? {
            if otr.has_weird_numbers() {
                self.app
                    .storage()
                    .catalog_refresh_overview_table(otr.catalog_id())
                    .await?;
            }
        }
        Ok(())
    }

    /// For unmatched entries with day-precision birth and death dates,
    /// finds other, matched entries with the same name and full dates,
    /// then matches them.
    pub async fn match_by_name_and_full_dates(&self) -> Result<()> {
        const BATCH_SIZE: usize = 100;
        let mut results = self
            .app
            .storage()
            .maintenance_match_people_via_name_and_full_dates(BATCH_SIZE)
            .await?;
        results.sort();
        results.dedup();
        for (entry_id, q) in results {
            if let Ok(mut entry) = Entry::from_id(entry_id, &self.app).await {
                // Ignore error
                let _ = entry.set_match(&format!("Q{q}"), USER_DATE_MATCH).await;
            };
        }
        Ok(())
    }

    pub async fn common_names_dates(&self) -> Result<()> {
        self.app.storage().maintenance_common_names_dates().await
    }

    pub async fn common_names_human(&self) -> Result<()> {
        self.app.storage().maintenance_common_names_human().await
    }

    /// Rebuild the `property_cache` table from Wikidata's authoritative
    /// view of which items can serve as values for a handful of
    /// "schema-pointing" properties (currently `P17` and `P31`). The
    /// cache backs property-aware UIs and is read on hot paths, so we
    /// can't afford a per-pageload SPARQL query.
    ///
    /// Mirrors PHP `Maintenance::updatePropertyCache`: pull the same
    /// SPARQL twice (once per "prop-group" property), accumulate, gate
    /// on the result count to defend against a partial outage wiping
    /// the table, then `TRUNCATE` + chunked-`INSERT`.
    pub async fn update_property_cache(&self) -> Result<()> {
        // Tracks which Wikidata properties act as our "what kind of
        // values does this property take?" pointers. P17 = country,
        // P31 = instance of — both are richly populated and the labels
        // they yield drive the property-cache-aware editors.
        const PROP_GROUPS: &[usize] = &[17, 31];
        // Sanity floor: a partial WDQS outage produces a small but
        // non-empty result, which would silently shrink the cache.
        // PHP picks 20 000; the same threshold has held up in
        // practice across both implementations.
        const MIN_EXPECTED_ROWS: usize = 20_000;

        let client = crate::wdqs::build_client()?;
        let mut rows: Vec<crate::storage::PropertyCacheRow> = Vec::new();
        for &group in PROP_GROUPS {
            let sparql = format!(
                "SELECT ?p ?v ?vLabel {{ \
                    ?p rdf:type wikibase:Property ; wdt:P{group} ?v . \
                    SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'en' }} \
                }}"
            );
            let tsv_rows = crate::wdqs::run_tsv_query(&client, &sparql).await?;
            log::info!(
                "update_property_cache: P{group} returned {} row(s)",
                tsv_rows.len()
            );
            for row in tsv_rows {
                if let Some(parsed) = parse_property_cache_row(group, &row) {
                    rows.push(parsed);
                }
            }
        }

        if rows.len() < MIN_EXPECTED_ROWS {
            return Err(anyhow!(
                "update_property_cache: only {} row(s) parsed (< {} expected); \
                 refusing to truncate property_cache — likely a partial WDQS outage",
                rows.len(),
                MIN_EXPECTED_ROWS,
            ));
        }

        self.app.storage().property_cache_replace(&rows).await?;
        log::info!("update_property_cache: replaced cache with {} row(s)", rows.len());
        Ok(())
    }

    pub async fn create_match_person_dates_jobs_for_catalogs(&self) -> Result<()> {
        self.app
            .storage()
            .create_match_person_dates_jobs_for_catalogs()
            .await?;
        Ok(())
    }

    pub async fn update_has_person_date(&self) -> Result<()> {
        let catalog_ids = self
            .app
            .storage()
            .get_catalogs_with_person_dates_without_flag()
            .await?;
        for catalog_id in catalog_ids {
            Catalog::from_id(catalog_id, &self.app)
                .await?
                .set_has_person_date("yes")
                .await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_person_dates", None).await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_on_birthdate", None).await?;
            Job::queue_simple_job(&self.app, catalog_id, "match_on_deathdate", None).await?;
        }
        Ok(())
    }

    pub async fn update_props_todo(&self) -> Result<()> {
        // We don't really care if one of these fails occasionally
        let _ = self.update_props_todo_add_new_properties().await;
        let _ = self.update_props_todo_update_items_using().await;
        let _ = self.app.storage().mark_props_todo_as_has_catalog().await;
        // TODO add default_type?
        Ok(())
    }

    async fn update_props_todo_update_items_using_get_props_todo(&self) -> Result<HashSet<u64>> {
        let props_todo: HashSet<u64> = self
            .app
            .storage()
            .get_props_todo()
            .await?
            .into_iter()
            .filter(|p| p.items_using.is_none()) // Only those without number; comment out to update everything
            .map(|p| p.prop_num)
            .collect();
        Ok(props_todo)
    }

    async fn update_props_todo_update_items_using_get_bindings(
        &self,
    ) -> Result<Vec<serde_json::Value>> {
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let sparql = r#"select ?p (count(?q) AS ?cnt) { ?q ?p [] } group by ?p"#;
        let results = match mw_api.sparql_query(sparql).await {
            Ok(results) => results,
            Err(_) => return Ok(vec![]),
        };
        let bindings = match results["results"]["bindings"].as_array() {
            Some(bindings) => bindings,
            None => return Ok(vec![]),
        };
        Ok(bindings.to_owned())
    }

    async fn update_props_todo_update_items_using(&self) -> Result<()> {
        let props_todo = self
            .update_props_todo_update_items_using_get_props_todo()
            .await?;
        let bindings = self
            .update_props_todo_update_items_using_get_bindings()
            .await?;
        for b in bindings {
            self.update_props_todo_update_items_using_process_binding(b, &props_todo)
                .await;
        }
        Ok(())
    }

    async fn update_props_todo_update_items_using_process_binding(
        &self,
        b: serde_json::Value,
        props_todo: &HashSet<u64>,
    ) {
        let (prop_url, cnt) = match (b["p"]["value"].as_str(), b["cnt"]["value"].as_str()) {
            (Some(prop_url), Some(cnt)) => (prop_url, cnt),
            _ => return,
        };
        let (url, prop) = match prop_url.rsplit_once('/') {
            Some((url, prop)) => (url, prop),
            _ => return,
        };
        if prop.chars().nth(0) != Some('P') || url != "http://www.wikidata.org/prop/direct" {
            return;
        }
        let prop_num = match prop[1..].parse::<u64>() {
            Ok(prop_num) => prop_num,
            Err(_) => return,
        };
        if !props_todo.contains(&prop_num) {
            return;
        }
        let cnt = match cnt.parse::<u64>() {
            Ok(cnt) => cnt,
            Err(_) => return,
        };
        let _ = self
            .app
            .storage()
            .set_props_todo_items_using(prop_num, cnt)
            .await;
    }

    async fn update_props_todo_add_new_properties_get_bindings(
        &self,
        mw_api: &mediawiki::Api,
    ) -> Result<Vec<serde_json::Value>> {
        let sparql = r#"SELECT ?p ?pLabel {
    	VALUES ?auth { wd:Q19595382 wd:Q62589316 wd:Q42396390 } .
     	?p rdf:type wikibase:Property ; wdt:P31/wdt:P279* ?auth .
      	MINUS { ?p wdt:P2264 [] } .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
    }"#;
        let results = match mw_api.sparql_query(sparql).await {
            Ok(results) => results,
            Err(_) => return Ok(vec![]),
        };
        let bindings = match results["results"]["bindings"].as_array() {
            Some(bindings) => bindings,
            None => return Ok(vec![]),
        };
        Ok(bindings.to_owned())
    }

    async fn update_props_todo_add_new_properties(&self) -> Result<()> {
        let (properties, prop_names) = self
            .update_props_todo_add_new_properties_get_props()
            .await?;
        let extisting_props = self.app.storage().get_props_todo().await?;
        let existing_hash: HashSet<u64> = extisting_props.iter().map(|p| p.prop_num).collect();
        let new_props: Vec<PropTodo> = properties
            .iter()
            .filter(|prop_num| !existing_hash.contains(prop_num))
            .map(|prop_num| {
                let name = prop_names
                    .get(prop_num)
                    .map(|s| s.to_string())
                    .unwrap_or(format!("P{prop_num}"));
                PropTodo::new(*prop_num, name)
            })
            .collect();
        self.app.storage().add_props_todo(new_props).await?;
        Ok(())
    }

    async fn update_props_todo_add_new_properties_get_props(
        &self,
    ) -> Result<(Vec<u64>, HashMap<u64, String>)> {
        let mut properties = vec![];
        let mut prop_names = HashMap::new();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let bindings = self
            .update_props_todo_add_new_properties_get_bindings(&mw_api)
            .await?;
        for b in bindings {
            if let Some(entity_url) = b["p"]["value"].as_str() {
                if let Ok(entity) = mw_api.extract_entity_from_uri(entity_url) {
                    if let Ok(prop_num) = entity[1..].parse::<u64>() {
                        properties.push(prop_num);
                        if let Some(prop_name) = b["pLabel"]["value"].as_str() {
                            let prop_name = prop_name.to_string();
                            prop_names.insert(prop_num, prop_name);
                        }
                    }
                }
            }
        }
        Ok((properties, prop_names))
    }

    pub async fn fix_auxiliary_item_values(&self) -> Result<()> {
        self.update_auxiliary_fix_table().await?;
        self.app
            .storage()
            .maintenance_use_auxiliary_broken()
            .await?;
        Ok(())
    }

    async fn update_auxiliary_fix_table(&self) -> Result<()> {
        let prop2type = self.get_sparql_prop2type().await?;
        self.app
            .storage()
            .maintenance_update_auxiliary_props(&prop2type)
            .await?;
        Ok(())
    }

    async fn get_sparql_prop2type(&self) -> Result<Vec<(String, String)>> {
        let sparql = "SELECT ?p ?type { ?p a wikibase:Property; wikibase:propertyType ?type }";
        let mut reader = self.app.wikidata().load_sparql_csv(sparql).await?;
        let api = self.app.wikidata().get_mw_api().await?;
        let mut prop2type = vec![];
        for row in reader.records().filter_map(|r| r.ok()) {
            let q = api.extract_entity_from_uri(&row[0])?;
            let property_type = row[1].to_string();
            if let Some(property_type) = property_type.split('#').next_back() {
                prop2type.push((q, property_type.to_string()));
            };
        }
        Ok(prop2type)
    }

    pub async fn automatch_people_via_year_born(&self) -> Result<()> {
        self.app
            .storage()
            .maintenance_automatch_people_via_year_born()
            .await
    }

    pub async fn fully_match_via_collection_inventory_number(&self) -> Result<()> {
        let catalog_ids: Vec<usize> = self
            .app
            .storage()
            .get_all_catalogs_key_value_pairs()
            .await?
            .iter()
            .filter(|(_catalog_id, key, _value)| key == "collection")
            .map(|(catalog_id, _key, _value)| *catalog_id)
            .collect();
        let mut futures = vec![];
        for catalog_id in catalog_ids {
            let future = self.fully_match_via_collection_inventory_number_for_catalog(catalog_id);
            futures.push(future);
        }
        let _ = join_all(futures).await;
        Ok(())
    }

    async fn fully_match_via_collection_inventory_number_for_catalog(
        &self,
        catalog_id: usize,
    ) -> Result<()> {
        // println!("Starting {catalog_id}");
        let inventory_number2entry_id = self.get_inventory_numbers_to_entry_id(catalog_id).await?;
        if inventory_number2entry_id.is_empty() {
            return Ok(());
        }

        // println!("Running {catalog_id}");
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let results = self
            .get_items_and_inventory_numbers_for_catalog(catalog_id, &mw_api)
            .await?;

        // Match via aux to inventory numbers
        for binding in results {
            self.fully_match_via_collection_inventory_number_for_catalog_process_binding(
                binding,
                &mw_api,
                &inventory_number2entry_id,
            )
            .await;
        }
        Ok(())
    }

    async fn fully_match_via_collection_inventory_number_for_catalog_process_binding(
        &self,
        binding: serde_json::Value,
        mw_api: &mediawiki::Api,
        inventory_number2entry_id: &HashMap<String, usize>,
    ) {
        let q = binding["q"]["value"].as_str();
        let id = binding["id"]["value"].as_str();
        let (q, id) = match (q, id) {
            (Some(q), Some(id)) => (q.to_string(), id.to_string()),
            _ => return,
        };
        let q = match mw_api.extract_entity_from_uri(&q) {
            Ok(q) => q,
            Err(_) => return,
        };
        if let Some(entry_id) = inventory_number2entry_id.get(&id) {
            if let Ok(mut entry) = Entry::from_id(*entry_id, &self.app).await {
                if !entry.is_fully_matched() {
                    // println!("Matching https://mix-n-match.toolforge.org/#/entry/{entry_id} to https://www.wikidata.org/wiki/{q}");
                    let _ = entry.set_match(&q, USER_AUX_MATCH).await;
                }
            }
        }
    }

    async fn get_items_and_inventory_numbers_for_catalog(
        &self,
        catalog_id: usize,
        mw_api: &mediawiki::Api,
    ) -> Result<Vec<serde_json::Value>> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let kv_catalog = catalog.get_key_value_pairs().await?;
        let collection_q = kv_catalog
            .get("collection")
            .ok_or_else(|| anyhow!("Catalog {catalog_id} does not have a 'collection' key"))?;
        let sparql = format!(
            "SELECT ?q ?id {{ ?q p:P217 ?statement . ?statement pq:P195 wd:{collection_q}; ps:P217 ?id }}"
        );
        let results = mw_api.sparql_query(&sparql).await?;
        let results = results["results"]["bindings"]
            .as_array()
            .ok_or_else(|| anyhow!("SPARQL failed"))?;
        Ok(results.to_owned())
    }

    async fn get_inventory_numbers_to_entry_id(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, usize>> {
        let inventory_number2entry_id: HashMap<String, usize> = self
            .app
            .storage()
            .auxiliary_matcher_match_via_aux(
                catalog_id,
                0,
                usize::MAX,
                &["217".to_string()],
                &AuxiliaryMatcher::get_blacklisted_catalogs(),
            )
            .await?
            .iter()
            .map(|a| (a.value.to_owned(), a.entry_id))
            .collect();
        Ok(inventory_number2entry_id)
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks meta items, such as disambiguation pages.
    pub async fn unlink_meta_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks deleted pages
    pub async fn unlink_deleted_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Fixes redirected items, and unlinks deleted and meta items.
    /// This is more efficient than calling the functions individually, because it uses the same batching run.
    pub async fn fix_matched_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Removes P17 auxiliary values for entryies of type Q5 (human)
    pub async fn remove_p17_for_humans(&self) -> Result<()> {
        self.app.storage().remove_p17_for_humans().await
    }

    pub async fn cleanup_mnm_relations(&self) -> Result<()> {
        self.app.storage().cleanup_mnm_relations().await
    }

    /// Finds redirects in a batch of items, and changes app matches to their respective targets.
    async fn fix_redirected_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let page2rd = self.app.wikidata().get_redirected_items(unique_qs).await?;
        for (from, to) in &page2rd {
            if let (Some(from), Some(to)) =
                (AppState::item2numeric(from), AppState::item2numeric(to))
            {
                if from > 0 && to > 0 {
                    self.app
                        .storage()
                        .maintenance_fix_redirects(from, to)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Finds deleted items in a batch of items, and unlinks app matches to them.
    async fn unlink_deleted_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let not_found = self.app.wikidata().get_deleted_items(unique_qs).await?;
        self.unlink_item_matches(&not_found).await?;
        Ok(())
    }

    /// Finds meta items (disambig etc) in a batch of items, and unlinks app matches to them.
    async fn unlink_meta_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let meta_items = self.app.wikidata().get_meta_items(unique_qs).await?;
        self.unlink_item_matches(&meta_items).await?;
        Ok(())
    }

    /// Unlinks app matches to items in a list.
    pub async fn unlink_item_matches(&self, items: &[String]) -> Result<()> {
        let items: Vec<isize> = items
            .iter()
            .filter_map(|q| AppState::item2numeric(q))
            .collect();

        if !items.is_empty() {
            let items: Vec<String> = items.iter().map(|q| q.to_string()).collect();
            self.app
                .storage()
                .maintenance_unlink_item_matches(items)
                .await?;
        }
        Ok(())
    }

    /// Finds some unmatched (Q5) entries where there is a (unique) full match for that name,
    /// and uses it as an auto-match
    pub async fn automatch(&self) -> Result<()> {
        self.app.storage().maintenance_automatch().await
    }

    /// Sweep every `multi_match` row whose entry has since been fully
    /// matched. Per-row cleanup happens inline in `Entry::set_match`
    /// already, but legacy data paths and ad-hoc fixes leave behind
    /// rows that the inline cleanup never saw — this is the broom.
    /// Mirrors PHP `Maintenance::deleteMultimatchesForFullyMatchedEntries`.
    pub async fn delete_multi_match_for_fully_matched(&self) -> Result<()> {
        let n = self
            .app
            .storage()
            .maintenance_delete_multi_match_for_fully_matched()
            .await?;
        log::info!("delete_multi_match_for_fully_matched: removed {n} row(s)");
        Ok(())
    }

    /// Tidy up `wd_matches` against the catalogs they reference:
    /// orphan deletes, catalog back-fills, and N/A flips for catalogs
    /// `wd_match_sync` can't do anything with. Mirrors PHP
    /// `Maintenance::fixupWdMatches`.
    pub async fn fixup_wd_matches(&self) -> Result<()> {
        let (deleted, recatalogued, marked_na) =
            self.app.storage().maintenance_fixup_wd_matches().await?;
        log::info!(
            "fixup_wd_matches: deleted {deleted}, back-filled {recatalogued}, \
             marked {marked_na} N/A"
        );
        Ok(())
    }

    /// Walk every Q5 entry whose match was set by the date matcher
    /// (`user=3`) or aux matcher (`user=4`) and verify the matched
    /// Wikidata item is actually `instance of` Q5. Items that aren't
    /// get unmatched — those are the false positives that the
    /// algorithmic matchers occasionally produce when a name+date
    /// pair coincidentally matches a non-human Wikidata item (a
    /// fictional character with year-of-creation = "1850", a
    /// disambiguation page, …).
    ///
    /// Items are fetched from Wikidata in batches via `wbgetentities`
    /// (one network round-trip per batch through the
    /// `EntityContainer`); the `P31=Q5` check is then a pure
    /// in-memory walk of the loaded claims. A single Q can show up
    /// against many entries (the same item is the "match" for many
    /// catalogs), so the check is implicitly cached by the
    /// container's dedup.
    ///
    /// Mirrors PHP `Maintenance::sanityCheckDateMatchesAreHuman`,
    /// trading PHP's per-row `pagelinks` SELECT against the
    /// Wikidata replica for an item-bulk-load — same result, fewer
    /// network/DB round-trips. Returns the number of matches it
    /// removed.
    pub async fn sanity_check_date_matches_are_human(&self) -> Result<usize> {
        // wbgetentities caps at 50 ids per request; the
        // EntityContainer chunks internally so any batch this size
        // or below maps to one round-trip. 200 keeps the per-batch
        // memory footprint bounded for very large match sets.
        const BATCH_SIZE: usize = 200;

        let candidates = self
            .app
            .storage()
            .entry_get_algorithmic_human_matches()
            .await?;
        if candidates.is_empty() {
            log::info!("sanity_check_date_matches_are_human: no algorithmic Q5 matches");
            return Ok(0);
        }
        log::info!(
            "sanity_check_date_matches_are_human: checking {} algorithmic match(es)",
            candidates.len()
        );

        // Group by Q so each item is loaded once regardless of how
        // many entries point at it.
        let mut q_to_entries: HashMap<isize, Vec<usize>> = HashMap::new();
        for (entry_id, q) in candidates {
            q_to_entries.entry(q).or_default().push(entry_id);
        }
        let qs: Vec<String> = q_to_entries.keys().map(|q| format!("Q{q}")).collect();

        let api = self.app.wikidata().get_mw_api().await?;
        let mut removed = 0_usize;
        for batch in qs.chunks(BATCH_SIZE) {
            let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
            if let Err(e) = entities.load_entities(&api, &batch.to_vec()).await {
                log::warn!(
                    "sanity_check_date_matches_are_human: batch load failed: {e} \
                     — continuing"
                );
                continue;
            }
            for q_label in batch {
                let q_num: isize = match q_label
                    .strip_prefix('Q')
                    .and_then(|s| s.parse().ok())
                {
                    Some(n) => n,
                    None => continue,
                };
                let Some(entry_ids) = q_to_entries.get(&q_num) else {
                    continue;
                };
                let item = entities.get_entity(q_label.clone());
                if item_is_human(item.as_ref()) {
                    continue;
                }
                // Item is not Q5 (or didn't load — treat as
                // non-human; the mismatch is real and the next
                // run can re-check after WD data updates).
                for &entry_id in entry_ids {
                    let mut entry = match Entry::from_id(entry_id, &self.app).await {
                        Ok(e) => e,
                        Err(e) => {
                            log::warn!(
                                "sanity_check_date_matches_are_human: \
                                 cannot load entry {entry_id}: {e}"
                            );
                            continue;
                        }
                    };
                    if let Err(e) = entry.unmatch().await {
                        log::warn!(
                            "sanity_check_date_matches_are_human: \
                             unmatch failed for entry {entry_id}: {e}"
                        );
                        continue;
                    }
                    removed += 1;
                }
            }
        }
        log::info!(
            "sanity_check_date_matches_are_human: removed {removed} non-human match(es)"
        );
        Ok(removed)
    }

    /// Apply every regex rule from `description_aux` to one
    /// catalog's `entry.ext_desc` column, materialising matched
    /// `(property, value)` pairs as new `auxiliary` rows. Idempotent:
    /// the SQL already skips entries that have the corresponding
    /// auxiliary row, so re-runs are no-ops on stable data.
    /// Mirrors PHP `Maintenance::applyDescriptionAux`.
    pub async fn apply_description_aux(&self, catalog_id: usize) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let rules = self.app.storage().description_aux_get_all().await?;
        if rules.is_empty() {
            log::info!("apply_description_aux: description_aux table is empty");
            return Ok(());
        }
        let mut total = 0_usize;
        for rule in &rules {
            match self
                .app
                .storage()
                .apply_description_aux_to_catalog(catalog_id, rule)
                .await
            {
                Ok(n) => total += n,
                Err(e) => log::warn!(
                    "apply_description_aux: rule P{}={} failed for catalog {catalog_id}: {e}",
                    rule.property, rule.value
                ),
            }
        }
        log::info!(
            "apply_description_aux: catalog {catalog_id} added {total} aux row(s) across {} rule(s)",
            rules.len()
        );
        Ok(())
    }

    /// Propagate a confirmed Wikidata match across entries that
    /// share a strong authority identifier — ISNI (P214) or GND
    /// (P227). When two or more catalog entries hold the same value
    /// for one of those properties and exactly one of them has a
    /// human-confirmed match, the others are auto-matched to the
    /// same Q with attribution `USER_AUX_MATCH`. After the pass,
    /// every catalog that received a new match gets a `microsync`
    /// job queued so the cross-walk between MnM and WD reflects
    /// the new state.
    ///
    /// Mirrors PHP `Maintenance::crossmatchViaAux` but builds the
    /// candidate group set in one storage call rather than walking
    /// per-row, and skips the optional `entry_is_matched=1`
    /// pre-flight UPDATE — the storage HAVING clause already does
    /// the right comparison without needing the column to be fresh.
    ///
    /// Returns total auto-matches set; useful both for cron logging
    /// and for callers that want to know whether to run a follow-up
    /// pass straight away.
    pub async fn crossmatch_via_aux(&self) -> Result<usize> {
        // Authority properties strong enough to imply same-person.
        // Matches PHP's hardcoded list — keeping them in lock-step
        // means cross-tool data quality stays consistent.
        const AUX_PROPS: &[usize] = &[214, 227];

        let groups = self
            .app
            .storage()
            .auxiliary_get_crossmatch_groups(AUX_PROPS)
            .await?;
        if groups.is_empty() {
            log::info!("crossmatch_via_aux: no candidate groups");
            return Ok(0);
        }

        // Cache active catalogs once — looking each catalog up
        // per-entry would dominate the runtime when groups are large
        // (P214/P227 alone routinely have tens of thousands of rows).
        let active_catalogs: HashSet<usize> = self
            .app
            .storage()
            .api_get_active_catalog_ids()
            .await?
            .into_iter()
            .collect();

        let mut total_matched = 0_usize;
        let mut catalogs_to_microsync: HashSet<usize> = HashSet::new();
        for (prop, _aux_name, entry_ids) in groups {
            let entries = match Entry::multiple_from_ids(&entry_ids, &self.app).await {
                Ok(map) => map,
                Err(e) => {
                    log::warn!(
                        "crossmatch_via_aux: cannot load entries for P{prop} group: {e}"
                    );
                    continue;
                }
            };

            // Bucket the loaded entries by match state. Inactive
            // catalogs are dropped here rather than at the SQL level —
            // mirrors PHP's `JOIN catalog ... WHERE catalog.active=1`.
            let mut manual_qs: HashSet<isize> = HashSet::new();
            let mut unmatched_entries: Vec<(usize, usize)> = Vec::new(); // (entry_id, catalog_id)
            for (_id, entry) in entries {
                if !active_catalogs.contains(&entry.catalog) {
                    continue;
                }
                let user = entry.user.unwrap_or(0);
                let q = entry.q.unwrap_or(0);
                if user > 0 && q > 0 {
                    manual_qs.insert(q);
                } else if let Some(entry_id) = entry.id {
                    unmatched_entries.push((entry_id, entry.catalog));
                }
            }

            // Need exactly one human-confirmed Q to know what to
            // propagate. Zero → nothing to share. More than one →
            // ambiguous; PHP logs and skips, we do the same.
            if manual_qs.len() != 1 || unmatched_entries.is_empty() {
                continue;
            }
            let q = *manual_qs.iter().next().expect("len == 1");
            let q_str = format!("Q{q}");

            for (entry_id, catalog_id) in unmatched_entries {
                let mut entry = match Entry::from_id(entry_id, &self.app).await {
                    Ok(e) => e,
                    Err(e) => {
                        log::warn!(
                            "crossmatch_via_aux: re-load failed for entry {entry_id}: {e}"
                        );
                        continue;
                    }
                };
                if entry.set_match(&q_str, USER_AUX_MATCH).await.is_ok() {
                    total_matched += 1;
                    catalogs_to_microsync.insert(catalog_id);
                }
            }
        }

        // Queue follow-up microsync per touched catalog so the
        // wd_matches / external state reflects the new matches in
        // the next sweep. Failure to queue is logged but doesn't
        // unwind the matches — the wd_matches inline insert in
        // entry_set_match_cleanup already covers the most important
        // bookkeeping.
        for catalog_id in catalogs_to_microsync {
            if let Err(e) = Job::queue_simple_job(&self.app, catalog_id, "microsync", None).await
            {
                log::warn!(
                    "crossmatch_via_aux: failed to queue microsync for catalog {catalog_id}: {e}"
                );
            }
        }

        log::info!("crossmatch_via_aux: matched {total_matched} entry(ies)");
        Ok(total_matched)
    }

    /// Rewrite every `entry.ext_url` in the catalog by substituting
    /// `$1` in `url_pattern` with the row's `ext_id`. Useful when a
    /// catalog's source moves to a new URL scheme and the existing
    /// rows need their cached external URLs refreshed; also a one-
    /// off fix when an importer wrote the wrong URLs to begin with.
    /// Mirrors PHP `Maintenance::updateExternalUrlsFromPattern`.
    ///
    /// The bulk SQL lives in `storage::api_update_catalog_ext_urls`
    /// (also reached by the web `update_ext_urls` API endpoint); this
    /// is the matching Rust-callable wrapper that splits the pattern
    /// on `$1` and validates the input.
    pub async fn update_ext_urls_from_pattern(
        &self,
        catalog_id: usize,
        url_pattern: &str,
    ) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let parts: Vec<&str> = url_pattern.splitn(2, "$1").collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "url_pattern '{url_pattern}' does not contain '$1'"
            ));
        }
        self.app
            .storage()
            .api_update_catalog_ext_urls(catalog_id, parts[0], parts[1])
            .await?;
        log::info!(
            "update_ext_urls_from_pattern: catalog {catalog_id} rewritten with '{url_pattern}'"
        );
        Ok(())
    }

    /// Walk every `auxiliary` row for P227 (GND ID) and delete the
    /// rows whose GND target is an "Undifferentiated Person" — the
    /// GND placeholder used when several real-world people share a
    /// name and DNB hasn't yet split them. Treating those as a single
    /// person poisons matches downstream, so the rule is to drop them
    /// from MnM rather than ever match against them.
    ///
    /// Each row triggers an HTTP fetch to `d-nb.info` for its
    /// `…/about/lds` RDF representation; presence of the
    /// `gndo:UndifferentiatedPerson` literal flags the row for
    /// deletion. 404s and other read errors are treated as "not
    /// undifferentiated" — same as PHP — to keep a transient DNB
    /// outage from sweeping live data.
    ///
    /// Mirrors PHP `Maintenance::fixGndUndifferentiatedPersons`.
    pub async fn fix_gnd_undifferentiated_persons(&self) -> Result<()> {
        let rows = self.app.storage().auxiliary_select_for_prop(227).await?;
        if rows.is_empty() {
            log::info!("fix_gnd_undifferentiated_persons: no GND aux rows");
            return Ok(());
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("mix-n-match (https://mix-n-match.toolforge.org)")
            .build()?;

        let mut checked = 0_usize;
        let mut removed = 0_usize;
        for (id, gnd) in rows {
            checked += 1;
            if !is_gnd_undifferentiated_person(&client, &gnd).await {
                continue;
            }
            if let Err(e) = self.app.storage().auxiliary_delete_row(id).await {
                log::warn!(
                    "fix_gnd_undifferentiated_persons: delete failed for aux row {id}: {e}"
                );
                continue;
            }
            removed += 1;
        }
        log::info!(
            "fix_gnd_undifferentiated_persons: checked {checked}, removed {removed}"
        );
        Ok(())
    }

    /// Decode HTML entities (`&amp;`, `&eacute;`, …) in every
    /// `entry.ext_name` of the given catalog and rewrite the row when
    /// the decoded form differs. Imports occasionally pick names up
    /// pre-decoded by the source HTML; this is the post-hoc fix.
    /// Mirrors PHP `Maintenance::fixHTMLentitiesForNamesInCatalog`.
    pub async fn fix_html_entities_in_catalog(&self, catalog_id: usize) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let candidates = self
            .app
            .storage()
            .entry_select_with_html_entities_in_name(catalog_id)
            .await?;
        let mut rewritten = 0_usize;
        for (id, raw) in candidates {
            let decoded = html_escape::decode_html_entities(&raw).trim().to_string();
            if decoded == raw {
                // LIKE '%&%;%' is approximate: `&` and `;` can show
                // up in text that isn't an entity reference. Skipping
                // unchanged rows keeps a noisy false-positive set
                // from generating no-op writes.
                continue;
            }
            // Existing storage helper takes (name, id) — note the
            // arg order. Truncates to 127 chars at the SQL layer to
            // match the column width.
            if let Err(e) = self.app.storage().entry_set_ext_name(&decoded, id).await {
                log::warn!(
                    "fix_html_entities_in_catalog: update failed for entry {id}: {e}"
                );
                continue;
            }
            rewritten += 1;
        }
        log::info!(
            "fix_html_entities_in_catalog: catalog {catalog_id} rewrote {rewritten} name(s)"
        );
        Ok(())
    }

    /// Rebuild `aux_candidates` — the cache the
    /// `creation_candidates?mode=random_prop` picker reads from.
    ///
    /// Three steps mirroring PHP `Maintenance::updateAuxCandidates`:
    ///
    /// 1. Pull every distinct `aux_p` we currently know about from
    ///    `auxiliary`.
    /// 2. Intersect that with Wikidata's list of external-ID
    ///    properties via SPARQL — only properties that actually take
    ///    string values can power the picker, and external-ID is the
    ///    canonical type for those.
    /// 3. Hand the filtered allowlist to the storage layer's
    ///    `aux_candidates` rebuild.
    ///
    /// Refuses to wipe the table when the allowlist is empty (a
    /// transient SPARQL outage shouldn't break the picker until the
    /// next successful run). The 3-row floor matches the PHP
    /// constant.
    pub async fn update_aux_candidates(&self) -> Result<()> {
        const MIN_COUNT: usize = 3;
        let props_in_aux = self.app.storage().auxiliary_distinct_props().await?;
        if props_in_aux.is_empty() {
            log::info!("update_aux_candidates: auxiliary is empty, nothing to do");
            return Ok(());
        }

        // Wikidata-side allowlist via SPARQL. The matcher uses the
        // same query, so it lives there as the single source of truth.
        let ext_id_props: HashSet<usize> =
            crate::auxiliary_matcher::AuxiliaryMatcher::get_properties_that_have_external_ids(
                &self.app,
            )
            .await?
            .into_iter()
            .filter_map(|s| s.trim_start_matches('P').parse::<usize>().ok())
            .collect();
        let allowlist: Vec<usize> = props_in_aux
            .into_iter()
            .filter(|p| ext_id_props.contains(p))
            .collect();
        if allowlist.is_empty() {
            return Err(anyhow!(
                "update_aux_candidates: no aux property survived the external-ID \
                 filter — refusing to truncate aux_candidates"
            ));
        }

        let n = self
            .app
            .storage()
            .maintenance_update_aux_candidates(&allowlist, MIN_COUNT)
            .await?;
        log::info!(
            "update_aux_candidates: rebuilt with {n} row(s) across {} prop(s)",
            allowlist.len()
        );
        Ok(())
    }
}

/// True iff the loaded item carries at least one `P31 = Q5`
/// statement. Treats a missing item (None) as not-human — the
/// match either points at a deleted/redirected Q (which
/// `fix_redirected_items` and `unlink_deleted_items` handle
/// separately) or at something else entirely; either way, the
/// algorithmic match shouldn't keep claiming it's a person.
fn item_is_human(item: Option<&wikimisc::wikibase::Entity>) -> bool {
    let Some(item) = item else { return false };
    item.claims_with_property("P31".to_string())
        .iter()
        .filter_map(|s| s.main_snak().data_value().clone())
        .any(|dv| match dv.value() {
            wikimisc::wikibase::Value::Entity(e) => e.id() == "Q5",
            _ => false,
        })
}

/// Look up a GND identifier in DNB's RDF representation and return
/// `true` iff the resource is flagged `gndo:UndifferentiatedPerson`.
/// Network / parse failures count as "not undifferentiated" so a
/// transient DNB outage can't silently delete live aux rows. Mirrors
/// PHP `MixNMatch::isGNDundifferentiatedPerson`.
async fn is_gnd_undifferentiated_person(client: &reqwest::Client, gnd: &str) -> bool {
    // The `/about/lds` endpoint returns a small Turtle/RDF fragment;
    // a substring match is enough — the property literal is unique
    // to this exact concept in DNB's vocabulary.
    let url = format!("https://d-nb.info/gnd/{gnd}/about/lds");
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => body.contains("gndo:UndifferentiatedPerson"),
            Err(_) => false,
        },
        _ => false,
    }
}

/// Decode one TSV row of `?p ?v ?vLabel` into the storage row.
/// Returns `None` if either entity URI doesn't end with the expected
/// numeric id — defensive against redirect IRIs and the occasional
/// nonsense row.
fn parse_property_cache_row(
    prop_group: usize,
    row: &[String],
) -> Option<crate::storage::PropertyCacheRow> {
    let p_uri = row.first()?;
    let v_uri = row.get(1)?;
    let label = row.get(2).cloned().unwrap_or_default();
    let property = crate::wdqs::entity_id_from_uri(p_uri, 'P')?;
    let item = crate::wdqs::entity_id_from_uri(v_uri, 'Q')?;
    Some(crate::storage::PropertyCacheRow {
        prop_group,
        property,
        item,
        label,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_state::{TEST_MUTEX, get_test_app},
        entry::Entry,
    };

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[test]
    fn property_cache_row_parses_canonical_uris() {
        let row = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://www.wikidata.org/entity/Q5".to_string(),
            "human".to_string(),
        ];
        let parsed = parse_property_cache_row(31, &row).expect("row should parse");
        assert_eq!(parsed.prop_group, 31);
        assert_eq!(parsed.property, 31);
        assert_eq!(parsed.item, 5);
        assert_eq!(parsed.label, "human");
    }

    #[test]
    fn property_cache_row_drops_when_entity_id_missing() {
        // Redirect IRIs occasionally come back from WDQS without a
        // numeric Q on the end; those rows must be silently ignored
        // rather than poisoning the cache with property=0/item=0
        // entries.
        let bad_p = vec![
            "http://www.wikidata.org/entity/redirect-only".to_string(),
            "http://www.wikidata.org/entity/Q5".to_string(),
            "human".to_string(),
        ];
        assert!(parse_property_cache_row(31, &bad_p).is_none());

        let bad_v = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://example.com/no-qid".to_string(),
            "x".to_string(),
        ];
        assert!(parse_property_cache_row(31, &bad_v).is_none());
    }

    #[test]
    fn property_cache_row_tolerates_missing_label() {
        // Label is best-effort — wikibase:label can produce a row
        // without it (no English label). Don't drop the row; an empty
        // label is what the cache will hold for it.
        let row = vec![
            "http://www.wikidata.org/entity/P31".to_string(),
            "http://www.wikidata.org/entity/Q42".to_string(),
        ];
        let parsed = parse_property_cache_row(31, &row).expect("row should parse");
        assert_eq!(parsed.label, "");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_unlink_meta_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Set a match to a disambiguation item
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.set_match("Q16456", 2).await.unwrap();

        // Remove matches to disambiguation items
        let maintenance = Maintenance::new(&app);
        maintenance
            .unlink_meta_items(TEST_CATALOG_ID, &MatchState::any_matched())
            .await
            .unwrap();

        // Check that removal was successful
        assert_eq!(Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap().q, None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .set_match("Q85756032", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.fix_redirects(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(3819700));
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_unlink_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .set_match("Q115205673", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.unlink_deleted_items(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_update_auxiliary_fix_table() {
        let app = get_test_app();
        let ms = Maintenance::new(&app);
        let prop2type = ms.get_sparql_prop2type().await.unwrap();
        assert!(prop2type.len() > 12000);
        assert!(prop2type.iter().any(|(prop, _)| prop == "P31"));
    }
}
