//! Auxiliary-data driven matching and Wikidata sync.
//!
//! Two distinct workflows live here, separated into submodules:
//! - [`finder`] — discovers candidate Wikidata items for unmatched
//!   Mix'n'match entries by searching `haswbstatement:"P_id=value"` (job
//!   action `auxiliary_matcher`).
//! - [`sync`] — pushes already-matched entries' auxiliary values to
//!   Wikidata as new statements (job action `aux2wd`).
//!
//! Both share the [`AuxiliaryMatcher`] type defined in this module — it
//! holds the property metadata and per-batch caches that both workflows
//! consume — plus the [`AuxiliaryResults`] row type that the storage layer
//! returns.

mod finder;
mod sync;

use crate::app_state::{AppContext, AppState, WikidataContext};
use crate::catalog::Catalog;
use crate::coordinates::CoordinateLocation;
use crate::job::Job;
use crate::job::Jobbable;
use crate::util::wikidata_props as wp;
use crate::wikidata_commands::WikidataCommandValue;
use crate::wikidata_writer::WikidataWriter;
use anyhow::Result;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use wikimisc::wikibase::Entity;
use wikimisc::wikibase::Value;
use wikimisc::wikibase::entity_container::EntityContainer;

pub const AUX_BLACKLISTED_CATALOGS: &[usize] = &[506];
pub const AUX_BLACKLISTED_CATALOGS_PROPERTIES: &[(usize, usize)] = &[(2099, 428)];
pub const AUX_BLACKLISTED_PROPERTIES: &[usize] = &[
    233, 235, // See https://www.wikidata.org/wiki/Topic:Ue8t23abchlw716q
    846, 2528, 4511,
];
pub const AUX_DO_NOT_SYNC_CATALOG_TO_WIKIDATA: &[usize] = &[655];
pub const AUX_PROPERTIES_ALSO_USING_LOWERCASE: &[usize] = &[2002];

#[derive(Debug, Clone)]
pub struct AuxiliaryResults {
    pub aux_id: usize,
    pub entry_id: usize,
    pub q_numeric: usize,
    pub property: usize,
    pub value: String,
}

impl AuxiliaryResults {
    pub fn new(
        aux_id: usize,
        entry_id: usize,
        q_numeric: usize,
        property: usize,
        value: String,
    ) -> Self {
        Self {
            aux_id,
            entry_id,
            q_numeric,
            property,
            value,
        }
    }

    //TODO test
    pub(super) fn value_as_item_id(&self) -> Option<WikidataCommandValue> {
        self.value
            .replace('Q', "")
            .parse::<usize>()
            .map(WikidataCommandValue::Item)
            .ok()
    }

    //TODO test
    pub(super) fn value_as_item_location(&self) -> Option<WikidataCommandValue> {
        CoordinateLocation::parse(&self.value).map(WikidataCommandValue::Location)
    }

    //TODO test
    pub(super) fn q(&self) -> String {
        format!("Q{}", self.q_numeric)
    }

    //TODO test
    pub(super) fn prop(&self) -> String {
        format!("P{}", self.property)
    }

    //TODO test
    pub(super) fn entry_comment_link(&self) -> String {
        format!(
            "via https://mix-n-match.toolforge.org/#/entry/{} ;",
            self.entry_id
        )
    }

    //TODO test
    pub(super) fn entity_has_statement(&self, entity: &Entity) -> bool {
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
pub(super) enum AuxiliaryMatcherError {
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
    pub(super) properties_using_items: Vec<String>,
    pub(super) properties_that_have_external_ids: Vec<String>,
    pub(super) properties_with_coordinates: Vec<String>,
    pub(super) app: Arc<dyn AppContext>,
    /// Wikidata write session. Production code holds a real `Wikidata`
    /// (boxed); tests substitute `MockWikidataWriter` via `new_with_writer`.
    pub(super) wikidata: Box<dyn WikidataWriter>,
    pub(super) catalogs: HashMap<usize, Option<Catalog>>,
    pub(super) properties: EntityContainer,
    pub(super) aux2wd_skip_existing_property: bool,
    pub(super) job: Option<Job>,
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
        Self::new_with_writer(app, Box::new(app.wikidata().clone()))
    }

    pub(crate) fn new_with_writer(app: &AppState, wikidata: Box<dyn WikidataWriter>) -> Self {
        let app: Arc<dyn AppContext> = Arc::new(app.clone());
        Self {
            properties_using_items: vec![],
            properties_that_have_external_ids: vec![],
            // TODO load dynamically like the ones above
            properties_with_coordinates: vec![wp::P_COORDINATES.to_string()],
            app,
            wikidata,
            catalogs: HashMap::new(),
            properties: EntityContainer::new(),
            aux2wd_skip_existing_property: true,
            job: None,
        }
    }

    //TODO test
    pub(super) async fn get_properties_using_items(app: &dyn WikidataContext) -> Result<Vec<String>> {
        let mw_api = app.wikidata().get_mw_api().await?;
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:WikibaseItem }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results, "p"))
    }

    //TODO test
    /// SPARQL list of every property whose `wikibase:propertyType` is
    /// `wikibase:ExternalId`. Pub-crate so the maintenance jobs that
    /// only care about external-ID props (e.g. `update_aux_candidates`)
    /// can reuse the same query without going through the matcher.
    pub(crate) async fn get_properties_that_have_external_ids(
        app: &dyn WikidataContext,
    ) -> Result<Vec<String>> {
        let mw_api = app.wikidata().get_mw_api().await?;
        let sparql = "SELECT ?p WHERE { ?p rdf:type wikibase:Property; wikibase:propertyType wikibase:ExternalId }";
        let sparql_results = mw_api.sparql_query(sparql).await?;
        Ok(mw_api.entities_from_sparql_result(&sparql_results, "p"))
    }

    //TODO test
    pub(super) fn is_catalog_property_combination_suspect(catalog_id: usize, prop: usize) -> bool {
        AUX_BLACKLISTED_CATALOGS_PROPERTIES.contains(&(catalog_id, prop))
    }

    pub fn get_blacklisted_catalogs() -> Vec<String> {
        let blacklisted_catalogs: Vec<String> = AUX_BLACKLISTED_CATALOGS
            .iter()
            .map(|u| u.to_string())
            .collect();
        blacklisted_catalogs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata_commands::WikidataCommandPropertyValue;
    use crate::wikidata_writer::MockWikidataWriter;
    use crate::{
        app_state::get_test_app,
        entry::{Entry, EntryWriter},
        test_support,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use wiremock::matchers::{method, path, query_param_contains};

    const TEST_ITEM_ID: usize = 13520818; // Q13520818

    const SITEINFO_JSON: &str = include_str!("../../tests/fixtures/wikidata/siteinfo.json");
    const Q13520818_JSON: &str = include_str!("../../tests/fixtures/wikidata/Q13520818.json");

    #[tokio::test]
    async fn test_is_statement_in_entity() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SITEINFO_JSON))
            .mount(&server).await;
        Mock::given(method("GET")).and(query_param_contains("action", "wbgetentities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(Q13520818_JSON))
            .mount(&server).await;
        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let mw_api = app.wikidata().get_mw_api().await.unwrap();
        let entities = EntityContainer::new();
        let entity = entities.load_entity(&mw_api, "Q13520818").await.unwrap();
        assert!(AuxiliaryMatcher::is_statement_in_entity(&entity, "P31", "Q5"));
        assert!(AuxiliaryMatcher::is_statement_in_entity(&entity, "P214", "30701597"));
        assert!(!AuxiliaryMatcher::is_statement_in_entity(&entity, "P214", "30701596"));
    }

    #[tokio::test]
    async fn test_entity_already_has_property() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SITEINFO_JSON))
            .mount(&server).await;
        Mock::given(method("GET")).and(query_param_contains("action", "wbgetentities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(Q13520818_JSON))
            .mount(&server).await;
        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let mw_api = app.wikidata().get_mw_api().await.unwrap();
        let entities = EntityContainer::new();
        let entity = entities.load_entity(&mw_api, "Q13520818").await.unwrap();
        let am = AuxiliaryMatcher::new(&app);
        // P214 is present in entity → returns true regardless of value
        let aux_present = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 214,
            value: "30701597".to_string(),
        };
        assert!(am.entity_already_has_property(&aux_present, &entity).await);
        let aux_any_val = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 214,
            value: "foobar".to_string(),
        };
        assert!(am.entity_already_has_property(&aux_any_val, &entity).await);
        // P212 is absent → returns false
        let aux_absent = AuxiliaryResults {
            aux_id: 0,
            entry_id: 0,
            q_numeric: TEST_ITEM_ID,
            property: 212,
            value: "foobar".to_string(),
        };
        assert!(!am.entity_already_has_property(&aux_absent, &entity).await);
    }

    #[tokio::test]
    async fn test_add_auxiliary_to_wikidata() {
        // Mock server provides siteinfo (with SPARQL URL), wbgetentities, and SPARQL.
        // MockWikidataWriter records execute_commands calls without touching Wikidata.
        let server = MockServer::start().await;

        // Siteinfo must point wikibase-sparql at the mock server so sparql_query()
        // hits our mock instead of query.wikidata.org.
        let sparql_url = format!("{}/sparql", server.uri());
        let siteinfo = format!(
            r#"{{"batchcomplete":"","query":{{"general":{{"sitename":"Wikidata","dbname":"wikidatawiki","wikibase-conceptbaseuri":"http://www.wikidata.org/entity/","wikibase-sparql":"{sparql_url}","mainpage":"Wikidata","base":"https://www.wikidata.org/wiki/Wikidata:Main_Page","generator":"MediaWiki 1.38.0","phpversion":"8.1.0","phpsapi":"cli","dbtype":"mysql","dbversion":"10.6","lang":"en","fallback":[],"fallback8bitEncoding":"windows-1252","writeapi":"","maxarticlesize":2097152,"timezone":"UTC","timeoffset":0,"articlepath":"/wiki/$1","scriptpath":"/w","script":"/w/index.php","variantarticlepath":false,"server":"https://www.wikidata.org","servername":"www.wikidata.org","wikiid":"wikidatawiki","time":"2024-01-01T00:00:00Z","case":"first-letter"}},"namespaces":{{"0":{{"id":0,"case":"first-letter","content":"","*":""}},"1":{{"id":1,"case":"first-letter","*":"Talk"}}}},"namespacealiases":[],"interwikimap":[]}}}}"#
        );

        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&siteinfo))
            .mount(&server).await;
        Mock::given(method("GET"))
            .and(query_param_contains("action", "wbgetentities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(Q13520818_JSON))
            .mount(&server).await;
        // Both get_properties_using_items and get_properties_that_have_external_ids
        // POST here. Empty bindings → both lists stay empty, which is fine: the
        // P214 in_wikidata path depends on entity_already_has_property, not SPARQL.
        let empty_sparql = r#"{"results":{"bindings":[]},"head":{"vars":["p"]}}"#;
        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(ResponseTemplate::new(200).set_body_string(empty_sparql))
            .mount(&server).await;

        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;

        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        {
            let mut ew = EntryWriter::new(&app, &mut entry);
            ew.set_match("Q13520818", 2).await.unwrap();
            ew.set_auxiliary(214, Some("30701597".to_string())).await.unwrap();
            ew.set_auxiliary(370, Some("foobar".to_string())).await.unwrap();
        }

        let mut am = AuxiliaryMatcher::new_with_writer(&app, Box::new(MockWikidataWriter::new()));
        am.add_auxiliary_to_wikidata(catalog_id).await.unwrap();

        // P214=30701597 is already in Q13520818 → entity_already_has_property marks it
        // P370=foobar is absent from the entity → in_wikidata stays false
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        let aux = EntryWriter::new(&app, &mut entry).get_aux().await.unwrap();
        assert!(aux.iter().any(|x| x.prop_numeric() == 214 && x.in_wikidata()),
            "P214 should be marked in_wikidata after entity check");
        assert!(aux.iter().any(|x| x.prop_numeric() == 370 && !x.in_wikidata()),
            "P370 should not be marked in_wikidata (absent from entity)");
    }

    #[tokio::test]
    async fn test_new_with_writer_and_as_any() {
        use crate::wikidata_writer::MockWikidataWriter;
        let app = test_support::test_app().await;
        let am = AuxiliaryMatcher::new_with_writer(&app, Box::new(MockWikidataWriter::new()));
        // Verify the mock is stored and accessible via as_any.
        am.wikidata
            .as_any()
            .downcast_ref::<MockWikidataWriter>()
            .expect("downcast to MockWikidataWriter should succeed");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
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
