use crate::app_state::AppState;
use crate::app_state::USER_AUX_MATCH;
use crate::catalog::*;
use crate::entry::*;
use crate::issue::*;
use crate::job::*;
use crate::wikidata::META_ITEMS;
use crate::wikidata_commands::*;
use anyhow::Result;
use futures::future::join_all;
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::Value;

pub const AUX_BLACKLISTED_CATALOGS: &[usize] = &[506];
pub const AUX_BLACKLISTED_CATALOGS_PROPERTIES: &[(usize, usize)] = &[(2099, 428)];
pub const AUX_BLACKLISTED_PROPERTIES: &[usize] = &[
    233, 235, // See https://www.wikidata.org/wiki/Topic:Ue8t23abchlw716q
    846, 2528, 4511,
];
pub const AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA: &[usize] = &[655];
pub const AUX_PROPERTIES_ALSO_USING_LOWERCASE: &[usize] = &[2002];

lazy_static! {
    static ref RE_COORDINATE_PATTERN: Regex =
        Regex::new(r"^\@{0,1}([0-9\.\-]+)[,/]([0-9\.\-]+)$").expect("Regex error");
}

#[derive(Debug, Clone)]
pub struct AuxiliaryResults {
    pub aux_id: usize,
    pub entry_id: usize,
    pub q_numeric: usize,
    pub property: usize,
    pub value: String,
}

impl AuxiliaryResults {
    //TODO test
    pub fn from_result(result: &(usize, usize, usize, usize, String)) -> Self {
        Self {
            aux_id: result.0,
            entry_id: result.1,
            q_numeric: result.2,
            property: result.3,
            value: result.4.to_owned(),
        }
    }

    //TODO test
    fn value_as_item_id(&self) -> Option<WikidataCommandValue> {
        self.value
            .replace('Q', "")
            .parse::<usize>()
            .map(WikidataCommandValue::Item)
            .ok()
    }

    //TODO test
    fn value_as_item_location(&self) -> Option<WikidataCommandValue> {
        let captures = RE_COORDINATE_PATTERN.captures(&self.value)?;
        if captures.len() == 3 {
            let lat = captures.get(1)?.as_str().parse::<f64>().ok()?;
            let lon = captures.get(2)?.as_str().parse::<f64>().ok()?;
            return Some(WikidataCommandValue::Location(CoordinateLocation {
                lat,
                lon,
            }));
        }
        None
    }

    //TODO test
    fn q(&self) -> String {
        format!("Q{}", self.q_numeric)
    }

    //TODO test
    fn prop(&self) -> String {
        format!("P{}", self.property)
    }

    //TODO test
    fn entry_comment_link(&self) -> String {
        format!(
            "via https://mix-n-match.toolforge.org/#/entry/{} ;",
            self.entry_id
        )
    }

    //TODO test
    fn entity_has_statement(&self, entity: &Entity) -> bool {
        entity
            .claims_with_property(self.prop())
            .iter()
            .filter_map(|statement| statement.main_snak().data_value().to_owned())
            .map(|datavalue| datavalue.value().to_owned())
            .any(|v| {
                if let Value::StringValue(s) = v {
                    if AUX_PROPERTIES_ALSO_USING_LOWERCASE.contains(&self.property) {
                        return s.to_lowercase() == self.value.to_lowercase();
                    }
                    return *s == self.value;
                }
                false
            })
    }
}

#[derive(Debug, Clone)]
enum AuxiliaryMatcherError {
    BlacklistedCatalog,
}

impl Error for AuxiliaryMatcherError {}

impl fmt::Display for AuxiliaryMatcherError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuxiliaryMatcherError::BlacklistedCatalog => write!(f, "Blacklisted catalog"),
        }
    }
}

#[derive(Debug)]
pub struct AuxiliaryMatcher {
    properties_using_items: Vec<String>,
    properties_that_have_external_ids: Vec<String>,
    properties_with_coordinates: Vec<String>,
    app: AppState,
    catalogs: HashMap<usize, Option<Catalog>>,
    properties: EntityContainer,
    aux2wd_skip_existing_property: bool,
    job: Option<Job>,
}

impl Jobbable for AuxiliaryMatcher {
    //TODO test
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    //TODO test
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl AuxiliaryMatcher {
    //TODO test
    pub fn new(app: &AppState) -> Self {
        Self {
            properties_using_items: vec![],
            properties_that_have_external_ids: vec![],
            properties_with_coordinates: vec!["P625".to_string()], // TODO load dynamically like the ones above
            app: app.clone(),
            catalogs: HashMap::new(),
            properties: EntityContainer::new(),
            aux2wd_skip_existing_property: true,
            job: None,
        }
    }

    //TODO test
    async fn get_properties_using_items(app: &AppState) -> Result<Vec<String>> {
        let mw_api = app.wikidata().get_mw_api().await?;
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:WikibaseItem }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results, "p"))
    }

    //TODO test
    async fn get_properties_that_have_external_ids(app: &AppState) -> Result<Vec<String>> {
        let mw_api = app.wikidata().get_mw_api().await?;
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:ExternalId }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results, "p"))
    }

    async fn search_property_value(
        &self,
        aux: AuxiliaryResults,
    ) -> Option<(AuxiliaryResults, Vec<String>)> {
        let query = format!("haswbstatement:\"{}={}\"", aux.prop(), aux.value);
        match self.app.wikidata().search_api(&query).await {
            Ok(results) => Some((aux, results)),
            Err(_) => None,
        }
    }

    //TODO test
    pub async fn match_via_auxiliary(&mut self, catalog_id: usize) -> Result<()> {
        let blacklisted_catalogs = get_blacklisted_catalogs();
        let extid_props = self.get_extid_props().await?;
        let mut offset = self.get_last_job_offset().await;
        let batch_size = self.get_batch_size();
        let search_batch_size = self.get_search_batch_size();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        loop {
            // println!("Catalog {catalog_id} running {batch_size} entries from {offset}");
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
    async fn _match_via_auxiliary_serially(
        &mut self,
        results: &Vec<AuxiliaryResults>,
        catalog_id: usize,
        items_to_check: &mut Vec<(String, AuxiliaryResults)>,
    ) -> Result<()> {
        for aux in results {
            if self.is_catalog_property_combination_suspect(catalog_id, aux.property) {
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
                    Issue::new(
                        aux.entry_id,
                        IssueType::WdDuplicate,
                        json!(search_results),
                        &self.app,
                    )
                    .await?
                    .insert()
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
                if !self.is_catalog_property_combination_suspect(catalog_id, aux.property) {
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
                        Issue::new(
                            aux.entry_id,
                            IssueType::WdDuplicate,
                            json!(items),
                            &self.app,
                        )
                        .await?
                        .insert()
                        .await?;
                    }
                }
            }
        }
        Ok(items_to_check)
    }

    fn get_search_batch_size(&mut self) -> usize {
        let search_batch_size = *self
            .app
            .task_specific_usize()
            .get("auxiliary_matcher_search_batch_size")
            .unwrap_or(&50);
        search_batch_size
    }

    fn get_batch_size(&mut self) -> usize {
        let batch_size = *self
            .app
            .task_specific_usize()
            .get("auxiliary_matcher_batch_size")
            .unwrap_or(&500);
        batch_size
    }

    async fn get_extid_props(&mut self) -> Result<Vec<String>> {
        self.properties_that_have_external_ids =
            Self::get_properties_that_have_external_ids(&self.app).await?;
        let extid_props: Vec<String> = self
            .properties_that_have_external_ids
            .iter()
            .filter_map(|s| s.replace('P', "").parse::<usize>().ok())
            .filter(|i| !AUX_BLACKLISTED_PROPERTIES.contains(i))
            .map(|i| format!("{}", i))
            .collect();
        Ok(extid_props)
    }

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
            .map(|u| format!("{}", u))
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

            let entities = EntityContainer::new();
            if self.aux2wd_skip_existing_property {
                let entity_ids: Vec<String> = aux.keys().map(|q| format!("Q{}", q)).collect();
                if entities.load_entities(&mw_api, &entity_ids).await.is_err() {
                    continue; // We can't know which items already have specific properties, so skip this batch
                }
            }

            let mut commands: Vec<WikidataCommand> = vec![];
            for data in aux.values() {
                commands.append(&mut self.aux2wd_process_item(data, &sources, &entities).await);
            }
            self.app.wikidata_mut().execute_commands(commands).await?;

            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    //TODO test
    fn is_statement_in_entity(&self, entity: &Entity, property: &str, value: &str) -> bool {
        entity
            .claims_with_property(property)
            .iter()
            .filter_map(|claim| {
                match &claim.main_snak().data_value() {
                    Some(datavalue) => {
                        match datavalue.value() {
                            Value::StringValue(s) => Some(s.to_string()),
                            Value::Entity(e) => Some(e.id().to_string()),
                            Value::Coordinate(c) => {
                                Some(format!("@{}/{}", c.latitude(), c.longitude()))
                            }
                            _ => None, // TODO more types?
                        }
                    }
                    _ => None,
                }
            })
            .any(|simplified_value| value == simplified_value)
    }

    //TODO test
    async fn entity_already_has_property(&self, aux: &AuxiliaryResults, entity: &Entity) -> bool {
        if !entity.has_claims_with_property(aux.prop()) {
            return false;
        }
        // Is that specific value in the entity?
        if self.is_statement_in_entity(entity, &aux.prop(), &aux.value) {
            if let Ok(entry) = Entry::from_id(aux.entry_id, &self.app).await {
                let _ = entry.set_auxiliary_in_wikidata(aux.aux_id, true).await;
            };
        }
        true
    }

    //TODO test
    async fn aux2wd_process_item(
        &self,
        aux_data: &Vec<AuxiliaryResults>,
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
                .any(|q| entity.has_target_entity("P31", q))
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
                } else if let Ok(issue) = Issue::new(
                    aux.entry_id,
                    IssueType::Mismatch,
                    json!([search_results[0], aux.q()]),
                    &self.app,
                )
                .await
                {
                    let _ = issue.insert().await;
                };
            }
            std::cmp::Ordering::Greater => {
                if let Ok(issue) = Issue::new(
                    aux.entry_id,
                    IssueType::Multiple,
                    json!({"wd": search_results,"app": aux.value,}),
                    &self.app,
                )
                .await
                {
                    let _ = issue.insert().await;
                };
            }
        }
        true
    }

    //TODO test
    async fn aux2wd_remap_results(
        &mut self,
        catalog_id: usize,
        results: &Vec<AuxiliaryResults>,
    ) -> (
        HashMap<usize, Vec<AuxiliaryResults>>,
        HashMap<String, WikidataCommandPropertyValueGroup>,
    ) {
        let mut aux: HashMap<usize, Vec<AuxiliaryResults>> = HashMap::new();
        let mut sources: HashMap<String, WikidataCommandPropertyValueGroup> = HashMap::new();

        let entry_ids: Vec<usize> = results.iter().map(|r| r.entry_id).collect();
        let entries = match Entry::multiple_from_ids(&entry_ids, &self.app).await {
            Ok(entries) => entries,
            Err(_) => HashMap::new(), // Something went wrong, ignore
        };

        for result in results {
            if self.is_catalog_property_combination_suspect(catalog_id, result.property) {
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
    async fn get_source_for_entry(
        &mut self,
        entry: &Entry,
    ) -> Option<WikidataCommandPropertyValueGroup> {
        let (catalog, mut stated_in) = match self.get_source_for_entry_init(entry).await {
            Ok(value) => value,
            Err(value) => return value,
        };

        // Source via catalog property
        if let Some(wd_prop) = catalog.wd_prop {
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
        let mnm_entry_url = format!("https://mix-n-match.toolforge.org/#/entry/{}", entry.id);
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
        if let Some(q) = catalog.source_item {
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
            let prop = format!("P{}", wd_prop);
            if !self.properties.has_entity(prop.to_owned()) {
                let mw_api = self.app.wikidata().get_mw_api().await.ok()?;
                let _ = self.properties.load_entity(&mw_api, prop.to_owned()).await;
            }
            if let Some(prop_entity) = self.properties.get_entity(prop) {
                let p9073 = prop_entity.values_for_property("P9073");
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

    //TODO test
    fn is_catalog_property_combination_suspect(&self, catalog_id: usize, prop: usize) -> bool {
        AUX_BLACKLISTED_CATALOGS_PROPERTIES.contains(&(catalog_id, prop))
    }
}

fn get_blacklisted_catalogs() -> Vec<String> {
    let blacklisted_catalogs: Vec<String> = AUX_BLACKLISTED_CATALOGS
        .iter()
        .map(|u| format!("{}", u))
        .collect();
    blacklisted_catalogs
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
    const TEST_ITEM_ID: usize = 13520818; // Q13520818

    #[tokio::test]
    async fn test_is_statement_in_entity() {
        let app = get_test_app();
        let mw_api = app.wikidata().get_mw_api().await.unwrap();
        let entities = EntityContainer::new();
        let entity = entities.load_entity(&mw_api, "Q13520818").await.unwrap();
        let am = AuxiliaryMatcher::new(&app);
        assert!(am.is_statement_in_entity(&entity, "P31", "Q5"));
        assert!(am.is_statement_in_entity(&entity, "P214", "30701597"));
        assert!(!am.is_statement_in_entity(&entity, "P214", "30701596"));
    }

    #[tokio::test]
    async fn test_entity_already_has_property() {
        let app = get_test_app();
        let mw_api = app.wikidata().get_mw_api().await.unwrap();
        let entities = EntityContainer::new();
        let entity = entities.load_entity(&mw_api, "Q13520818").await.unwrap();
        let aux = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 214,
            value: "30701597".to_string(),
        };
        let am = AuxiliaryMatcher::new(&app);
        assert!(am.entity_already_has_property(&aux, &entity).await);
        let aux = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 214,
            value: "foobar".to_string(),
        };
        assert!(am.entity_already_has_property(&aux, &entity).await);
        let aux = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 212,
            value: "foobar".to_string(),
        };
        assert!(!am.entity_already_has_property(&aux, &entity).await);
    }

    #[tokio::test]
    async fn test_add_auxiliary_to_wikidata() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry
            .set_auxiliary(214, Some("30701597".to_string()))
            .await
            .unwrap();
        entry
            .set_auxiliary(370, Some("foobar".to_string()))
            .await
            .unwrap(); // Sandbox string property
        entry.set_match("Q13520818", 2).await.unwrap();

        // Run matcher
        let mut am = AuxiliaryMatcher::new(&app);
        am.add_auxiliary_to_wikidata(TEST_CATALOG_ID).await.unwrap();

        // Check
        let aux = entry.get_aux().await.unwrap();
        assert!(aux.iter().any(|x| x.prop_numeric == 214 && x.in_wikidata));
        assert!(aux.iter().any(|x| x.prop_numeric == 370 && !x.in_wikidata));

        // Cleanup
        entry.set_auxiliary(214, None).await.unwrap();
        entry.set_auxiliary(370, None).await.unwrap();
        entry.unmatch().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_source_for_entry() {
        let app = get_test_app();
        let mut am = AuxiliaryMatcher::new(&app);
        let entry = Entry::from_id(144507016, &app).await.unwrap();
        let res = am.get_source_for_entry(&entry).await;
        let x1 = WikidataCommandPropertyValue {
            property: 248,
            value: WikidataCommandValue::Item(97032597),
        };
        let x2 = WikidataCommandPropertyValue {
            property: 3124,
            value: WikidataCommandValue::String("38084".to_string()),
        };
        assert_eq!(res, Some(vec![x1, x2]));
    }
}
