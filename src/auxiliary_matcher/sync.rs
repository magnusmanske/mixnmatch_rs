//! Push existing auxiliary values to Wikidata as new statements.
//!
//! Driven by the `aux2wd` job. Walks Mix'n'match entries that already have a
//! confirmed Q-match, looks up the target entity on Wikidata to make sure the
//! statement isn't already there, then issues `WikidataCommand`s to add the
//! missing statements with appropriate `stated in` references.

use super::{
    AUX_BLACKLISTED_PROPERTIES, AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA, AuxiliaryMatcher,
    AuxiliaryMatcherError, AuxiliaryResults,
};
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::issue::{Issue, IssueType};
use crate::job::Jobbable;
use crate::util::wikidata_props as wp;
use crate::wikidata::META_ITEMS;
use crate::wikidata_commands::{
    WikidataCommand, WikidataCommandPropertyValue, WikidataCommandPropertyValueGroup,
    WikidataCommandValue, WikidataCommandWhat,
};
use anyhow::Result;
use mediawiki::Api;
use serde_json::json;
use std::collections::HashMap;
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::Value;
use wikimisc::wikibase::entity_container::EntityContainer;

impl AuxiliaryMatcher {
    //TODO test
    pub async fn add_auxiliary_to_wikidata(&mut self, catalog_id: usize) -> Result<()> {
        if AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA.contains(&catalog_id) {
            return Err(AuxiliaryMatcherError::BlacklistedCatalog.into());
        }
        self.properties_using_items = Self::get_properties_using_items(&self.app).await?;
        self.properties_that_have_external_ids =
            Self::get_properties_that_have_external_ids(&self.app).await?;
        let blacklisted_properties: Vec<String> = AUX_BLACKLISTED_PROPERTIES
            .iter()
            .map(|u| u.to_string())
            .collect();

        let mut offset = self.get_last_job_offset().await;
        let batch_size = 500;
        let mw_api = self.app.wikidata().get_mw_api().await?;

        loop {
            let results = self
                .app
                .storage()
                .auxiliary_matcher_add_auxiliary_to_wikidata(
                    &blacklisted_properties,
                    catalog_id,
                    offset,
                    batch_size,
                )
                .await?;
            let (aux, sources) = self.aux2wd_remap_results(catalog_id, &results).await;

            self.add_auxiliary_to_wikidata_run_commands(aux, sources, &mw_api)
                .await?;

            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn add_auxiliary_to_wikidata_run_commands(
        &mut self,
        aux: HashMap<usize, Vec<AuxiliaryResults>>,
        sources: HashMap<String, Vec<WikidataCommandPropertyValue>>,
        mw_api: &Api,
    ) -> Result<()> {
        let entities = EntityContainer::new();
        if self.aux2wd_skip_existing_property {
            let entity_ids: Vec<String> = aux.keys().map(|q| format!("Q{q}")).collect();
            if entities.load_entities(mw_api, &entity_ids).await.is_err() {
                return Ok(()); // We can't know which items already have specific properties, so skip this batch
            }
        }

        let mut commands: Vec<WikidataCommand> = vec![];
        for data in aux.values() {
            commands.append(&mut self.aux2wd_process_item(data, &sources, &entities).await);
        }
        self.app.wikidata_mut().execute_commands(commands).await?;
        Ok(())
    }

    //TODO test
    pub(super) fn is_statement_in_entity(entity: &Entity, property: &str, value: &str) -> bool {
        entity
            .claims_with_property(property)
            .iter()
            .filter_map(|claim| {
                claim
                    .main_snak()
                    .data_value()
                    .as_ref()
                    .and_then(|datavalue| match datavalue.value() {
                        Value::StringValue(s) => Some(s.to_string()),
                        Value::Entity(e) => Some(e.id().to_string()),
                        Value::Coordinate(c) => {
                            Some(format!("@{}/{}", c.latitude(), c.longitude()))
                        }
                        _ => None, // TODO more types?
                    })
            })
            .any(|simplified_value| value == simplified_value)
    }

    //TODO test
    pub(super) async fn entity_already_has_property(
        &self,
        aux: &AuxiliaryResults,
        entity: &Entity,
    ) -> bool {
        if !entity.has_claims_with_property(aux.prop()) {
            return false;
        }
        // Is that specific value in the entity?
        if Self::is_statement_in_entity(entity, &aux.prop(), &aux.value) {
            if let Ok(entry) = Entry::from_id(aux.entry_id, &self.app).await {
                let _ = entry.set_auxiliary_in_wikidata(aux.aux_id, true).await;
            };
        }
        true
    }

    //TODO test
    async fn aux2wd_process_item(
        &self,
        aux_data: &[AuxiliaryResults],
        sources: &HashMap<String, WikidataCommandPropertyValueGroup>,
        entities: &EntityContainer,
    ) -> Vec<WikidataCommand> {
        let q = match aux_data.first() {
            Some(aux) => aux.q(),
            None => {
                return vec![];
            } // Empty input
        };
        let source: WikidataCommandPropertyValueGroup =
            sources.get(&q).unwrap_or(&vec![]).to_owned();
        let mut commands: Vec<WikidataCommand> = vec![];
        for aux in aux_data {
            self.aux2wd_process_item_aux(aux, entities, &mut commands, &source)
                .await;
        }
        commands
    }

    async fn aux2wd_process_item_aux(
        &self,
        aux: &AuxiliaryResults,
        entities: &EntityContainer,
        commands: &mut Vec<WikidataCommand>,
        source: &[WikidataCommandPropertyValue],
    ) {
        if !self.aux2wd_process_item_aux_check_aux(aux, entities).await {
            return;
        }
        match self
            .app
            .storage()
            .avoid_auto_match(aux.entry_id, Some(aux.q_numeric as isize))
            .await
        {
            Ok(false) => {}
            _ => return,
        }
        self.aux2wd_process_item_aux_add_command(aux, commands, source);
    }

    async fn aux2wd_process_item_aux_check_aux(
        &self,
        aux: &AuxiliaryResults,
        entities: &EntityContainer,
    ) -> bool {
        if AUX_BLACKLISTED_PROPERTIES.contains(&aux.property) {
            // No blacklisted properties
            return false;
        }
        if let Some(entity) = entities.get_entity(aux.q()) {
            if META_ITEMS
                .iter()
                .any(|q| entity.has_target_entity(wp::P_INSTANCE_OF, q))
            {
                return false; // Don't edit items that are META items
            }
            if self.entity_already_has_property(aux, &entity).await {
                return false; // Don't add anything if item already has a statement with that property
            }
        }
        if self
            .aux2wd_check_if_property_value_is_on_wikidata(aux)
            .await
        {
            // Search Wikidata for other occurrences
            return false;
        }
        true
    }

    fn aux2wd_process_item_aux_add_command(
        &self,
        aux: &AuxiliaryResults,
        commands: &mut Vec<WikidataCommand>,
        source: &[WikidataCommandPropertyValue],
    ) {
        let command_value: Option<WikidataCommandValue> =
            if self.properties_using_items.contains(&aux.prop()) {
                aux.value_as_item_id()
            } else if self.properties_with_coordinates.contains(&aux.prop()) {
                aux.value_as_item_location()
            } else {
                Some(WikidataCommandValue::String(aux.value.to_owned()))
            };

        if let Some(value) = command_value {
            commands.push(WikidataCommand {
                item_id: aux.q_numeric,
                what: WikidataCommandWhat::Property(aux.property),
                value: value.to_owned(),
                references: vec![source.to_vec()],
                qualifiers: vec![],
                comment: Some(aux.entry_comment_link()),
                rank: None,
            });
        }
    }

    /// Check if that property/value combination is on Wikidata. Returns true if something was found.
    //TODO test
    async fn aux2wd_check_if_property_value_is_on_wikidata(&self, aux: &AuxiliaryResults) -> bool {
        if !self.properties_that_have_external_ids.contains(&aux.prop()) {
            return false;
        }
        let query = format!("haswbstatement:\"{}={}\"", aux.prop(), aux.value);
        let search_results = match self.app.wikidata().search_api(&query).await {
            Ok(result) => result,
            Err(_) => return true, // Something went wrong, just skip this one
        };

        match search_results.len().cmp(&1) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                if search_results[0] == aux.q() {
                    if let Ok(entry) = Entry::from_id(aux.entry_id, &self.app).await {
                        let _ = entry.set_auxiliary_in_wikidata(aux.aux_id, true).await;
                    }
                } else {
                    let issue = Issue::new(
                        aux.entry_id,
                        IssueType::Mismatch,
                        json!([search_results[0], aux.q()]),
                    );
                    let _ = issue.insert(self.app.storage().as_ref().as_ref()).await;
                };
            }
            std::cmp::Ordering::Greater => {
                let issue = Issue::new(
                    aux.entry_id,
                    IssueType::Multiple,
                    json!({"wd": search_results,"app": aux.value,}),
                );
                let _ = issue.insert(self.app.storage().as_ref().as_ref()).await;
            }
        }
        true
    }

    //TODO test
    async fn aux2wd_remap_results(
        &mut self,
        catalog_id: usize,
        results: &[AuxiliaryResults],
    ) -> (
        HashMap<usize, Vec<AuxiliaryResults>>,
        HashMap<String, WikidataCommandPropertyValueGroup>,
    ) {
        let mut aux: HashMap<usize, Vec<AuxiliaryResults>> = HashMap::new();
        let mut sources: HashMap<String, WikidataCommandPropertyValueGroup> = HashMap::new();

        let entry_ids: Vec<usize> = results.iter().map(|r| r.entry_id).collect();
        let entries = Entry::multiple_from_ids(&entry_ids, &self.app)
            .await
            .unwrap_or_default();

        for result in results {
            if Self::is_catalog_property_combination_suspect(catalog_id, result.property) {
                continue;
            }
            aux.entry(result.q_numeric)
                .and_modify(|v| v.push(result.to_owned()))
                .or_insert(vec![result.to_owned()]);
            if let Some(entry) = entries.get(&result.entry_id) {
                if let Some(s) = self.get_source_for_entry(entry).await {
                    sources.insert(result.q(), s.to_owned());
                }
            }
        }
        (aux, sources)
    }

    //TODO test
    pub(super) async fn get_source_for_entry(
        &mut self,
        entry: &Entry,
    ) -> Option<WikidataCommandPropertyValueGroup> {
        let (catalog, mut stated_in) = match self.get_source_for_entry_init(entry).await {
            Ok(value) => value,
            Err(value) => return value,
        };

        // Source via catalog property
        if let Some(wd_prop) = catalog.wd_prop() {
            self.get_source_for_entry_via_catalog_property(&mut stated_in, wd_prop, entry)
                .await?;
            return Some(stated_in);
        }

        // Source via external URL of the entry
        if !entry.ext_url.is_empty() {
            stated_in.push(WikidataCommandPropertyValue {
                property: 854,
                value: WikidataCommandValue::String(entry.ext_url.to_string()),
            });
            return Some(stated_in);
        }

        // Fallback: Source via Mix'n'match entry URL
        let mnm_entry_url = format!(
            "https://mix-n-match.toolforge.org/#/entry/{}",
            entry.id.unwrap_or(0)
        );
        stated_in.push(WikidataCommandPropertyValue {
            property: 854,
            value: WikidataCommandValue::String(mnm_entry_url),
        });
        Some(stated_in)
    }

    async fn get_source_for_entry_init(
        &mut self,
        entry: &Entry,
    ) -> Result<
        (&Catalog, Vec<WikidataCommandPropertyValue>),
        Option<Vec<WikidataCommandPropertyValue>>,
    > {
        self.catalogs
            .entry(entry.catalog)
            .or_insert(Catalog::from_id(entry.catalog, &self.app).await.ok());
        let catalog = match self.catalogs.get(&entry.catalog) {
            Some(catalog) => catalog,
            None => return Err(None), // No catalog, no source
        };
        let catalog = match catalog {
            Some(catalog) => catalog,
            None => return Err(None), // No catalog, no source
        };
        let mut stated_in: WikidataCommandPropertyValueGroup = vec![];
        if let Some(q) = catalog.source_item() {
            stated_in.push(WikidataCommandPropertyValue {
                property: 248,
                value: WikidataCommandValue::Item(q),
            });
        }
        Ok((catalog, stated_in))
    }

    async fn get_source_for_entry_via_catalog_property(
        &mut self,
        stated_in: &mut Vec<WikidataCommandPropertyValue>,
        wd_prop: usize,
        entry: &Entry,
    ) -> Option<()> {
        if stated_in.is_empty() {
            let prop = format!("P{wd_prop}");
            if !self.properties.has_entity(prop.to_owned()) {
                let mw_api = self.app.wikidata().get_mw_api().await.ok()?;
                let _ = self.properties.load_entity(&mw_api, prop.to_owned()).await;
            }
            if let Some(prop_entity) = self.properties.get_entity(prop) {
                let p9073 = prop_entity.values_for_property(wp::P_APPLICABLE_STATED_IN);
                #[allow(clippy::collapsible_match)]
                if let Some(value) = p9073.first() {
                    /* trunk-ignore(clippy/collapsible_match) */
                    if let Value::Entity(entity_value) = value {
                        if let Ok(q) = entity_value.id().replace('Q', "").parse::<usize>() {
                            stated_in.push(WikidataCommandPropertyValue {
                                property: 248,
                                value: WikidataCommandValue::Item(q),
                            });
                        }
                    }
                }
            }
        }
        stated_in.push(WikidataCommandPropertyValue {
            property: wd_prop,
            value: WikidataCommandValue::String(entry.ext_id.to_string()),
        });
        Some(())
    }
}
