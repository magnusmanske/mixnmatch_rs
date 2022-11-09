use lazy_static::lazy_static;
use regex::Regex;
use wikibase::entity_container::EntityContainer;
use std::error::Error;
use std::fmt;
use mysql_async::prelude::*;
use std::collections::HashMap;
use mysql_async::from_row;
use crate::mixnmatch::*;
use crate::entry::*;
use crate::catalog::*;
use crate::job::*;
use crate::app_state::*;
use crate::wikidata_commands::*;

const AUX_BLACKLISTED_CATALOGS_PROPERTIES: &'static [(usize,usize)] = &[
    (2099,428)
];
const AUX_BLACKLISTED_PROPERTIES: &'static [usize] = &[
    233 ,
    235 , // See https://www.wikidata.org/wiki/Topic:Ue8t23abchlw716q
    846 ,
    2528 ,
    4511
];
const AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA: &'static [usize] = &[
    655
];

lazy_static!{
    static ref RE_COORDINATE_PATTERN : Regex = Regex::new(r"^\@{0,1}([0-9\.\-]+)[,/]([0-9\.\-]+)$").unwrap();
}

#[derive(Debug, Clone)]
struct AuxiliaryResults {
    pub aux_id: usize,
    pub entry_id: usize,
    pub q_numeric: usize,
    pub property: usize,
    pub value: String
}

impl AuxiliaryResults {
    fn from_result(result: &(usize,usize,usize,usize,String)) -> Self {
        Self {
            aux_id: result.0,
            entry_id: result.1,
            q_numeric: result.2,
            property: result.3,
            value: result.4.to_owned(),
        }
    }

    fn value_as_item_id(&self) -> Option<WikidataCommandValue> {
        self.value
            .replace("Q","")
            .parse::<usize>()
            .map(|numeric|WikidataCommandValue::Item(numeric))
            .ok()
    }

    fn value_as_item_location(&self) -> Option<WikidataCommandValue> {
        let captures = RE_COORDINATE_PATTERN.captures(&self.value)?;
        if captures.len()==3 {
            let lat = captures.get(1)?.as_str().parse::<f64>().ok()?;
            let lon = captures.get(2)?.as_str().parse::<f64>().ok()?;
            return Some(WikidataCommandValue::Location(CoordinateLocation{lat,lon}));
        }
        None
    }

    fn q(&self) -> String {
        format!("Q{}",self.q_numeric)
    }

    fn prop(&self) -> String {
        format!("P{}",self.property)
    }

    fn entry_comment_link(&self) -> String {
        format!("via https://mix-n-match.toolforge.org/#/entry/{} ;",self.entry_id)
    }
}

#[derive(Debug, Clone)]
enum AuxiliaryMatcherError {
    BlacklistedCatalog,
}

impl Error for AuxiliaryMatcherError {}

impl fmt::Display for AuxiliaryMatcherError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}

#[derive(Debug)]
pub struct AuxiliaryMatcher {
    properties_using_items: Vec<String>,
    properties_that_have_external_ids: Vec<String>,
    properties_with_coordinates: Vec<String>,
    mnm: MixNMatch,
    catalogs: HashMap<usize,Option<Catalog>>,
    properties: wikibase::entity_container::EntityContainer,
    aux2wd_skip_existing_property: bool,
    job: Option<Job>
}

impl Jobbable for AuxiliaryMatcher {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}

impl AuxiliaryMatcher {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            properties_using_items: vec![],
            properties_that_have_external_ids: vec![],
            properties_with_coordinates: vec!["P625".to_string()], // TODO load dynamically like the ones above
            mnm: mnm.clone(),
            catalogs: HashMap::new(),
            properties: wikibase::entity_container::EntityContainer::new(),
            aux2wd_skip_existing_property: true,
            job: None
        }
    }

    async fn get_properties_using_items(mnm: &MixNMatch) -> Result<Vec<String>,GenericError> {
        let mw_api = mnm.get_mw_api().await.unwrap();
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:WikibaseItem }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results,"p"))
    }

    async fn get_properties_that_have_external_ids(mnm: &MixNMatch) -> Result<Vec<String>,GenericError> {
        let mw_api = mnm.get_mw_api().await.unwrap();
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:ExternalId }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results,"p"))
    }

    pub async fn add_auxiliary_to_wikidata(&mut self, catalog_id: usize) -> Result<(),GenericError> {
        if AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA.contains(&catalog_id) {
            return Err(Box::new(AuxiliaryMatcherError::BlacklistedCatalog));
        }
        self.properties_using_items = Self::get_properties_using_items(&self.mnm).await?;
        self.properties_that_have_external_ids = Self::get_properties_that_have_external_ids(&self.mnm).await?;
        let blacklisted_properties: Vec<String> = AUX_BLACKLISTED_PROPERTIES.iter().map(|u|format!("{}",u)).collect();
        let sql = format!("SELECT auxiliary.id,entry_id,q,aux_p,aux_name FROM entry,auxiliary 
            WHERE entry_id=entry.id AND catalog=:catalog_id 
            {}
            AND in_wikidata=0 
            AND aux_p NOT IN ({})
            ORDER BY auxiliary.id LIMIT :batch_size OFFSET :offset"
            ,MatchState::fully_matched().get_sql()
            ,blacklisted_properties.join(","));
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 500 ;
        let mw_api = self.mnm.get_mw_api().await?;
        
        loop {
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,usize,usize,usize,String)>).await?;
            let results: Vec<AuxiliaryResults> = results.iter().map(|r|AuxiliaryResults::from_result(r)).collect();
            let (aux,sources) = self.aux2wd_remap_results(catalog_id, &results).await;
            
            let entity_ids: Vec<String> = aux.keys().map(|q|format!("Q{}",q)).collect();
            let entities = wikibase::entity_container::EntityContainer::new();
            if self.aux2wd_skip_existing_property {
                if let Err(_) = entities.load_entities(&mw_api, &entity_ids).await {
                    continue // We can't know which items already have specific properties, so skip this batch
                }
            }
            
            let mut commands: Vec<WikidataCommand> = vec![];
            for (_,data) in &aux {
                commands.append(&mut self.aux2wd_process_item(data, &sources, &entities).await);

            }
            let _ = self.mnm.execute_commands(commands).await;

            // ________________________________________________________________________________
            if results.len()<batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn aux2wd_process_item(&self, aux_data: &Vec<AuxiliaryResults>, sources: &HashMap<String,WikidataCommandPropertyValueGroups>, entities: &EntityContainer) -> Vec<WikidataCommand> {
        let q = match aux_data.get(0) {
            Some(aux) => aux.q(),
            None => {return vec![];} // Empty input
        };
        let source: WikidataCommandPropertyValueGroups = sources.get(&q).unwrap_or(&vec![]).to_owned();
        let mut commands: Vec<WikidataCommand> = vec![];
        for aux in aux_data {
            if AUX_BLACKLISTED_PROPERTIES.contains(&aux.property) { // No blacklisted properties
                continue;
            }
            if let Some(entity) = entities.get_entity(aux.q()) { // Don't add anything if item already has a statement with that property
                if entity.has_claims_with_property(aux.prop()) {
                    // Is that specific value on Wikidata?
                    if entity
                        .claims_with_property(aux.prop())
                        .iter()
                        .filter_map(|claim|{
                            match &claim.main_snak().data_value() {
                                Some(datavalue) => {
                                    match datavalue.value() {
                                        wikibase::Value::StringValue(s) => Some(s.to_string()),
                                        wikibase::Value::Entity(e) => Some(e.id().to_string()),
                                        wikibase::Value::Coordinate(c) => Some(format!("@{}/{}",c.latitude(),c.longitude())),
                                        _ => None // TODO more
                                    }
                                }
                                _ => None
                            }
                        })
                        .any(|value|value==aux.value)
                        {
                            if let Ok(entry) = Entry::from_id(aux.entry_id,&self.mnm).await {
                                let _ = entry.set_auxiliary_in_wikidata(aux.aux_id,true).await;
                            };
                        }
                        
                    continue
                }
            }
            if self.aux2wd_check_if_property_value_is_on_wikidata(aux).await { // Search Wikidata for other occurrences
                continue
            }
            if let Ok(b) = self.mnm.avoid_auto_match(aux.entry_id,Some(aux.q_numeric as isize)).await {
                if b { continue }
            } else { // Something went wrong, ignore this one
                continue
            }

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
                    references: source.clone(),
                    qualifiers: vec![],
                    comment: Some(aux.entry_comment_link()),
                    rank: None
                });        
            }
        }
        commands
    }

    /// Check if that property/value combination is on Wikidata. Returns true if something was found.
    async fn aux2wd_check_if_property_value_is_on_wikidata(&self, aux: &AuxiliaryResults) -> bool {
        if !self.properties_that_have_external_ids.contains(&aux.prop()) {
            return false;
        }
        let query = format!("haswbstatement:\"{}={}\"",aux.prop(),aux.value);
        let search_results = match self.mnm.wd_search(&query).await {
            Ok(result) => result,
            Err(_) => return true // Something went wrong, just skip this one
        };
        if search_results.len()==1 {
            if search_results[0]==aux.q() {
                if let Ok(entry) = Entry::from_id(aux.entry_id, &self.mnm).await {
                    let _ = entry.set_auxiliary_in_wikidata(aux.aux_id,true).await;
                }
            } else {
                // TODO Mismatch issue
            }
        } else if search_results.len()>1 {
            // TODO Multiple items with the same extid issue
        }
        true
    }

    async fn aux2wd_remap_results(&mut self, catalog_id: usize, results: &Vec<AuxiliaryResults>) -> (HashMap<usize,Vec<AuxiliaryResults>>,HashMap<String,WikidataCommandPropertyValueGroups>) {
        let mut aux: HashMap<usize,Vec<AuxiliaryResults>> = HashMap::new();
        let mut sources: HashMap<String,WikidataCommandPropertyValueGroups> = HashMap::new();
        for result in results {
            if self.is_catalog_property_combination_suspect(catalog_id,result.property) {
                continue
            }
            aux.entry(result.q_numeric)
                .and_modify(|v| v.push(result.to_owned()))
                .or_insert(vec![result.to_owned()]);
            if let Some(s) = self.get_source_for_entry(result.entry_id,catalog_id,&result.value).await {
                sources.insert(result.q(),s.to_owned());
            }
        }
        (aux,sources)
    }

    async fn get_source_for_entry(&mut self, entry_id: usize, catalog_id: usize, ext_id: &str) -> Option<WikidataCommandPropertyValueGroups> {
        if !self.catalogs.contains_key(&catalog_id) {
            let catalog = Catalog::from_id(catalog_id, &self.mnm).await.ok();
            self.catalogs.insert(catalog_id,catalog);
        }
        let catalog = match self.catalogs.get(&catalog_id).unwrap() {
            Some(catalog) => catalog,
            None => { return None } // No catalog, no source
        };
        let mut stated_in: WikidataCommandPropertyValueGroup = vec![];
        match catalog.source_item {
            Some(q) => {
                stated_in.push(WikidataCommandPropertyValue{property:248,value: WikidataCommandValue::Item(q)});
            }
            None => {}
        }

        // Source via catalog property
        if let Some(wd_prop) = catalog.wd_prop {
            if stated_in.is_empty() {
                let prop = format!("P{}",wd_prop);
                if !self.properties.has_entity(prop.to_owned()) {
                    let mw_api = self.mnm.get_mw_api().await.unwrap();
                    let _ = self.properties.load_entity(&mw_api, prop.to_owned()).await;
                }
                if let Some(prop_entity) = self.properties.get_entity(prop) {
                    let p9073 = prop_entity.values_for_property("P9073");
                    if let Some(value) = p9073.get(0) {
                        if let wikibase::Value::Entity(entity_value) = value {
                            if let Ok(q) = entity_value.id().replace("Q","").parse::<usize>() {
                                stated_in.push(WikidataCommandPropertyValue{property:248,value: WikidataCommandValue::Item(q)});
                            }
                        }
                    }
                }
            }

            return Some(vec![
                stated_in,
                vec![WikidataCommandPropertyValue{property:wd_prop,value:WikidataCommandValue::String(ext_id.to_string())}]
            ]);
        }

        // Source via external URL of the entry
        if let Ok(entry) = Entry::from_id(entry_id, &self.mnm).await {
            if !entry.ext_url.is_empty() {
                return Some(vec![
                    stated_in,
                    vec![WikidataCommandPropertyValue{property:854,value:WikidataCommandValue::String(entry.ext_url.to_string())}]
                ]);
            }
        }

        // Fallback: Source via Mix'n'match entry URL
        let mnm_entry_url = format!("https://mix-n-match.toolforge.org/#/entry/{}",entry_id);
        return Some(vec![
            stated_in,
            vec![WikidataCommandPropertyValue{property:854,value:WikidataCommandValue::String(mnm_entry_url)}]
        ]);
    }

    fn is_catalog_property_combination_suspect(&self,catalog_id: usize,prop: usize) -> bool {
        AUX_BLACKLISTED_CATALOGS_PROPERTIES.contains(&(catalog_id,prop))
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_add_auxiliary_to_wikidata() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let mut entry = Entry::from_id(TEST_ENTRY_ID,&mnm).await.unwrap();
        entry.set_auxiliary(214,Some("30701597".to_string())).await.unwrap();
        entry.set_auxiliary(370,Some("foobar".to_string())).await.unwrap(); // Sandbox string property
        entry.set_match("Q13520818",2).await.unwrap();

        // Run matcher
        let mut am = AuxiliaryMatcher::new(&mnm);
        am.add_auxiliary_to_wikidata(TEST_CATALOG_ID).await.unwrap();

        // Check
        let aux  = entry.get_aux().await.unwrap();
        assert!(aux.iter().any(|x|x.prop_numeric==214&&x.in_wikidata));
        assert!(aux.iter().any(|x|x.prop_numeric==370&&!x.in_wikidata));

        // Cleanup
        entry.set_auxiliary(214,None).await.unwrap();
        entry.set_auxiliary(370,None).await.unwrap();
        entry.unmatch().await.unwrap();
    }
}