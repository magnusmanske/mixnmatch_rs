//! Discover Wikidata candidates for unmatched entries by searching for
//! `auxiliary` external-id values (`haswbstatement:"P_id=value"`).
//!
//! Driven by the `auxiliary_matcher` job. Walks the Mix'n'match auxiliary
//! table for a catalog, fans out parallel `wbsearchentities`-style queries,
//! confirms hits by re-loading the candidate entity, and either records a
//! match (`USER_AUX_MATCH`) or files a `WdDuplicate` issue.

use super::{AUX_BLACKLISTED_PROPERTIES, AuxiliaryMatcher, AuxiliaryResults};
use crate::app_state::USER_AUX_MATCH;
use crate::entry::Entry;
use crate::issue::{Issue, IssueType};
use crate::job::{Job, Jobbable};
use anyhow::Result;
use futures::future::join_all;
use serde_json::json;
use wikimisc::wikibase::entity_container::EntityContainer;

impl AuxiliaryMatcher {
    pub(super) async fn search_property_value(
        &self,
        aux: AuxiliaryResults,
    ) -> Option<(AuxiliaryResults, Vec<String>)> {
        let query = format!("haswbstatement:\"{}={}\"", aux.prop(), aux.value);
        (self.app.wikidata().search_api(&query).await).map_or(None, |results| Some((aux, results)))
    }

    //TODO test
    pub async fn match_via_auxiliary(&mut self, catalog_id: usize) -> Result<()> {
        let blacklisted_catalogs = Self::get_blacklisted_catalogs();
        let extid_props = self.get_extid_props().await?;
        let mut offset = self.get_last_job_offset().await;
        let batch_size = self.get_batch_size();
        let search_batch_size = self.get_search_batch_size();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        loop {
            let results = self
                .app
                .storage()
                .auxiliary_matcher_match_via_aux(
                    catalog_id,
                    offset,
                    batch_size,
                    &extid_props,
                    &blacklisted_catalogs,
                )
                .await?;
            let items_to_check = self
                .match_via_auxiliary_parallel(&results, search_batch_size, catalog_id)
                .await?;
            self.match_via_auxiliary_check_items(items_to_check, &mw_api)
                .await;
            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        let _ = Job::queue_simple_job(&self.app, catalog_id, "aux2wd", None).await;
        Ok(())
    }

    async fn match_via_auxiliary_check_items(
        &mut self,
        items_to_check: Vec<(String, AuxiliaryResults)>,
        mw_api: &mediawiki::api::Api,
    ) {
        // Load the actual entities, don't trust the search results
        let items_to_load = items_to_check
            .iter()
            .map(|(q, _aux)| q.to_owned())
            .collect();
        let entities = EntityContainer::new();
        let _ = entities.load_entities(mw_api, &items_to_load).await;
        for (q, aux) in &items_to_check {
            if let Some(entity) = &entities.get_entity(q.to_owned()) {
                if aux.entity_has_statement(entity) {
                    if let Ok(mut entry) = Entry::from_id(aux.entry_id, &self.app).await {
                        let _ = entry.set_match(q, USER_AUX_MATCH).await;
                    }
                }
            }
        }
    }

    // DEPRECATED
    #[allow(dead_code)]
    async fn _match_via_auxiliary_serially(
        &mut self,
        results: &[AuxiliaryResults],
        catalog_id: usize,
        items_to_check: &mut Vec<(String, AuxiliaryResults)>,
    ) -> Result<()> {
        for aux in results {
            if Self::is_catalog_property_combination_suspect(catalog_id, aux.property) {
                continue;
            }
            let query = format!("haswbstatement:\"{}={}\"", aux.prop(), aux.value);
            let search_results = match self.app.wikidata().search_api(&query).await {
                Ok(result) => result,
                Err(_) => continue, // Something went wrong, just skip this one
            };
            match search_results.len().cmp(&1) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    if let Some(q) = search_results.first() {
                        items_to_check.push((q.to_owned(), aux.to_owned()));
                    }
                }
                std::cmp::Ordering::Greater => {
                    Issue::new(aux.entry_id, IssueType::WdDuplicate, json!(search_results))
                        .insert(self.app.storage().as_ref().as_ref())
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn match_via_auxiliary_parallel(
        &mut self,
        results: &[AuxiliaryResults],
        search_batch_size: usize,
        catalog_id: usize,
    ) -> Result<Vec<(String, AuxiliaryResults)>> {
        let mut items_to_check: Vec<(String, AuxiliaryResults)> = vec![];
        for results_chunk in results.chunks(search_batch_size) {
            let mut futures = vec![];
            for aux in results_chunk {
                if !Self::is_catalog_property_combination_suspect(catalog_id, aux.property) {
                    let future = self.search_property_value(aux.to_owned());
                    futures.push(future);
                }
            }
            let futures_results = join_all(futures).await.into_iter().flatten();
            for (aux, items) in futures_results {
                match items.len().cmp(&1) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => items_to_check.push((items[0].to_owned(), aux)),
                    std::cmp::Ordering::Greater => {
                        Issue::new(aux.entry_id, IssueType::WdDuplicate, json!(items))
                            .insert(self.app.storage().as_ref().as_ref())
                            .await?;
                    }
                }
            }
        }
        Ok(items_to_check)
    }

    fn get_search_batch_size(&mut self) -> usize {
        *self
            .app
            .task_specific_usize()
            .get("auxiliary_matcher_search_batch_size")
            .unwrap_or(&50)
    }

    fn get_batch_size(&mut self) -> usize {
        *self
            .app
            .task_specific_usize()
            .get("auxiliary_matcher_batch_size")
            .unwrap_or(&500)
    }

    async fn get_extid_props(&mut self) -> Result<Vec<String>> {
        self.properties_that_have_external_ids =
            Self::get_properties_that_have_external_ids(&self.app).await?;
        let extid_props: Vec<String> = self
            .properties_that_have_external_ids
            .iter()
            .filter_map(|s| s.replace('P', "").parse::<usize>().ok())
            .filter(|i| !AUX_BLACKLISTED_PROPERTIES.contains(i))
            .map(|i| i.to_string())
            .collect();
        Ok(extid_props)
    }
}
