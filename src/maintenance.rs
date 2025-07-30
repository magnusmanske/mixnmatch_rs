use crate::app_state::{AppState, USER_AUX_MATCH, USER_DATE_MATCH};
use crate::auxiliary_matcher::AuxiliaryMatcher;
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::job::Job;
use crate::match_state::MatchState;
use crate::prop_todo::PropTodo;
use anyhow::{anyhow, Result};
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
        let sparql = format!("SELECT ?q ?id {{ ?q p:P217 ?statement . ?statement pq:P195 wd:{collection_q}; ps:P217 ?id }}");
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
    async fn fix_redirected_items_batch(&self, unique_qs: &Vec<String>) -> Result<()> {
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
    async fn unlink_meta_items_batch(&self, unique_qs: &Vec<String>) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_state::{get_test_app, TEST_MUTEX},
        entry::Entry,
    };

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
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
    async fn test_update_auxiliary_fix_table() {
        let app = get_test_app();
        let ms = Maintenance::new(&app);
        let prop2type = ms.get_sparql_prop2type().await.unwrap();
        assert!(prop2type.len() > 12000);
        assert!(prop2type.iter().any(|(prop, _)| prop == "P31"));
    }
}
