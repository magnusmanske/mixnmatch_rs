//! Discover Wikidata candidates for unmatched entries by searching for
//! `auxiliary` external-id values.
//!
//! Driven by the `auxiliary_matcher` job. Two paths, tried in order:
//!
//! 1. **SPARQL batch lookup** (primary): groups aux records by property, then
//!    runs one `SELECT ?item ?id WHERE { VALUES … }` query per batch. Faster
//!    by orders of magnitude — 200 lookups → 1 SPARQL call instead of 200
//!    CirrusSearch calls. Falls back to path 2 only when SPARQL errors out.
//!
//! 2. **`haswbstatement` search** (fallback): fans out individual
//!    `haswbstatement:"P_id=value"` queries in parallel. Used when SPARQL is
//!    temporarily unavailable for a batch.
//!
//! Matches from both paths are applied directly (`USER_AUX_MATCH`) without a
//! second entity-reload pass: all records reaching this code are pre-filtered
//! to `wikibase:ExternalId` properties by the SQL query that produced them,
//! so the SPARQL result or `haswbstatement` hit is already sufficient proof.

use super::{AUX_BLACKLISTED_PROPERTIES, AuxiliaryMatcher, AuxiliaryResults};
use crate::app_state::USER_AUX_MATCH;
use crate::entry::{Entry, EntryWriter};
use crate::issue::{Issue, IssueType};
use crate::job::{Job, Jobbable};
use anyhow::Result;
use futures::future::join_all;
use serde_json::json;
use std::collections::HashMap;

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
        let sparql_batch_size = self.get_sparql_batch_size();
        let search_batch_size = self.get_search_batch_size();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        // COUNT matching the same WHERE clause the paged fetch uses. One
        // query at job start; small overhead vs hours-long body of work.
        let total = self
            .app
            .storage()
            .auxiliary_matcher_match_via_aux_count(
                catalog_id,
                &extid_props,
                &blacklisted_catalogs,
            )
            .await
            .ok()
            .map(|n| n as u64);
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
            // Primary: SPARQL batch lookup — many values → one query per property per batch.
            // Returns only the records whose SPARQL batch errored, for fallback below.
            let fallback = self
                .match_via_sparql(&results, sparql_batch_size, catalog_id, &mw_api)
                .await;
            // Fallback: haswbstatement search for records where SPARQL was unavailable.
            let items_to_check = self
                .match_via_auxiliary_parallel(&fallback, search_batch_size, catalog_id)
                .await?;
            // Apply matches without entity re-load. All aux_p values reaching here are
            // wikibase:ExternalId properties (enforced by the SQL filter), so a single
            // search hit is sufficient proof — no second round-trip needed.
            self.match_via_auxiliary_apply_matches(items_to_check).await;
            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.report_progress(offset as u64, total).await;
            if self.should_yield() {
                let _ = self.mark_yielded().await;
                return Ok(());
            }
        }
        let _ = self.clear_offset().await;
        let _ = Job::queue_simple_job(self.app.as_ref(), catalog_id, "aux2wd", None).await;
        Ok(())
    }

    /// Build and run one SPARQL `VALUES` batch query per property in `results`.
    ///
    /// On success the query result is authoritative: values present → matched,
    /// values absent → no match exists. Only entries whose entire SPARQL batch
    /// returned an error are added to the returned fallback list.
    async fn match_via_sparql(
        &self,
        results: &[AuxiliaryResults],
        sparql_batch_size: usize,
        catalog_id: usize,
        mw_api: &mediawiki::api::Api,
    ) -> Vec<AuxiliaryResults> {
        if results.is_empty() {
            return vec![];
        }

        // Group by property, honouring the per-catalog blacklist.
        let mut by_property: HashMap<usize, Vec<&AuxiliaryResults>> = HashMap::new();
        for aux in results {
            if !Self::is_catalog_property_combination_suspect(catalog_id, aux.property) {
                by_property.entry(aux.property).or_default().push(aux);
            }
        }

        let mut fallback: Vec<AuxiliaryResults> = vec![];
        for (property, aux_list) in &by_property {
            for chunk in aux_list.chunks(sparql_batch_size) {
                match self
                    .sparql_lookup_property_values(*property, chunk, mw_api)
                    .await
                {
                    Ok(value_to_qs) => {
                        for aux in chunk {
                            let qs = match value_to_qs.get(&aux.value) {
                                Some(v) => v,
                                None => continue, // Definitive no-match from SPARQL
                            };
                            match qs.len().cmp(&1) {
                                std::cmp::Ordering::Equal => {
                                    if let Ok(mut entry) =
                                        Entry::from_id(aux.entry_id, self.app.as_ref()).await
                                    {
                                        let _ = EntryWriter::new(self.app.as_ref(), &mut entry)
                                            .set_match(&qs[0], USER_AUX_MATCH)
                                            .await;
                                    }
                                }
                                std::cmp::Ordering::Greater => {
                                    let _ =
                                        Issue::new(aux.entry_id, IssueType::WdDuplicate, json!(qs))
                                            .insert(self.app.storage().as_ref().as_ref())
                                            .await;
                                }
                                std::cmp::Ordering::Less => {}
                            }
                        }
                    }
                    Err(_) => {
                        // SPARQL unavailable for this batch — hand off to haswbstatement fallback.
                        fallback.extend(chunk.iter().copied().cloned());
                    }
                }
            }
        }
        fallback
    }

    /// Execute `SELECT ?item ?id WHERE { VALUES (?id) { … } ?item wdt:P<N> ?id. }`
    /// and return a map of `external-id value → Vec<Q-string>`.
    ///
    /// An empty `aux_slice` returns an empty map without a network call.
    pub(crate) async fn sparql_lookup_property_values(
        &self,
        property: usize,
        aux_slice: &[&AuxiliaryResults],
        mw_api: &mediawiki::api::Api,
    ) -> Result<HashMap<String, Vec<String>>> {
        if aux_slice.is_empty() {
            return Ok(HashMap::new());
        }
        let values_str: String = aux_slice
            .iter()
            .map(|a| {
                let escaped = a.value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("(\"{escaped}\")")
            })
            .collect::<Vec<_>>()
            .join(" ");
        let sparql = format!(
            "SELECT ?item ?id WHERE {{ VALUES (?id) {{ {values_str} }} ?item wdt:P{property} ?id. }}"
        );
        let sparql_results = mw_api.sparql_query(&sparql).await?;
        let mut result_map: HashMap<String, Vec<String>> = HashMap::new();
        if let Some(bindings) = sparql_results["results"]["bindings"].as_array() {
            for binding in bindings {
                if let (Some(item_uri), Some(id_value)) = (
                    binding["item"]["value"].as_str(),
                    binding["id"]["value"].as_str(),
                ) {
                    if let Some(q) = item_uri.split('/').next_back() {
                        if q.starts_with('Q') {
                            result_map
                                .entry(id_value.to_string())
                                .or_default()
                                .push(q.to_string());
                        }
                    }
                }
            }
        }
        Ok(result_map)
    }

    /// Apply a list of (Q, AuxiliaryResults) matches without loading the Wikidata
    /// entity for re-verification.
    ///
    /// This is safe because every record reaching this function was already
    /// pre-filtered by the SQL query to `wikibase:ExternalId` properties only,
    /// making the search or SPARQL result a sufficient match guarantee.
    async fn match_via_auxiliary_apply_matches(&self, matches: Vec<(String, AuxiliaryResults)>) {
        for (q, aux) in &matches {
            if let Ok(mut entry) = Entry::from_id(aux.entry_id, self.app.as_ref()).await {
                let _ = EntryWriter::new(self.app.as_ref(), &mut entry)
                    .set_match(q, USER_AUX_MATCH)
                    .await;
            }
        }
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

    fn get_sparql_batch_size(&mut self) -> usize {
        *self
            .app
            .task_specific_usize()
            .get("auxiliary_matcher_sparql_batch_size")
            .unwrap_or(&200)
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
            Self::get_properties_that_have_external_ids(self.app.as_ref()).await?;
        let extid_props: Vec<String> = self
            .properties_that_have_external_ids
            .iter()
            .filter_map(|s| s.replace('P', "").parse::<usize>().ok())
            .filter(|i| !AUX_BLACKLISTED_PROPERTIES.contains(i))
            .map(|i| i.to_string())
            .collect();
        Ok(extid_props)
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
                Err(_) => continue,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{app_state::USER_AUX_MATCH, test_support};
    use std::sync::Arc;
    use wiremock::matchers::{method, path, query_param_contains};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Build a siteinfo JSON fragment that routes `wikibase-sparql` to `sparql_url`.
    fn siteinfo_with_sparql(sparql_url: &str) -> String {
        format!(
            r#"{{"batchcomplete":"","query":{{"general":{{"sitename":"Wikidata","dbname":"wikidatawiki",
"wikibase-conceptbaseuri":"http://www.wikidata.org/entity/",
"wikibase-sparql":"{sparql_url}",
"mainpage":"Wikidata","base":"https://www.wikidata.org/wiki/Wikidata:Main_Page",
"generator":"MediaWiki 1.38.0","phpversion":"8.1.0","phpsapi":"cli",
"dbtype":"mysql","dbversion":"10.6","lang":"en","fallback":[],
"fallback8bitEncoding":"windows-1252","writeapi":"","maxarticlesize":2097152,
"timezone":"UTC","timeoffset":0,"articlepath":"/wiki/$1","scriptpath":"/w",
"script":"/w/index.php","variantarticlepath":false,
"server":"https://www.wikidata.org","servername":"www.wikidata.org",
"wikiid":"wikidatawiki","time":"2024-01-01T00:00:00Z","case":"first-letter"}},
"namespaces":{{"0":{{"id":0,"case":"first-letter","content":"","*":""}}}},
"namespacealiases":[],"interwikimap":[]}}}}"#
        )
    }

    // ── A: default search-batch-size ─────────────────────────────────────────

    #[tokio::test]
    async fn test_get_search_batch_size_default_is_50() {
        let app = test_support::test_app().await;
        let mut am = AuxiliaryMatcher::new(Arc::new(app));
        assert_eq!(am.get_search_batch_size(), 50);
    }

    // ── B: sparql_lookup_property_values ─────────────────────────────────────

    #[tokio::test]
    async fn test_sparql_lookup_empty_slice_returns_empty_map() {
        let app = test_support::test_app().await;
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let mw_api = app.wikidata().get_mw_api().await.unwrap();
        let result = am
            .sparql_lookup_property_values(214, &[], &mw_api)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_sparql_lookup_parses_single_match() {
        let server = MockServer::start().await;
        let sparql_url = format!("{}/sparql", server.uri());
        let siteinfo = siteinfo_with_sparql(&sparql_url);

        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&siteinfo))
            .mount(&server)
            .await;

        let sparql_resp = r#"{
            "results":{"bindings":[{
                "item":{"type":"uri","value":"http://www.wikidata.org/entity/Q123"},
                "id":{"type":"literal","value":"46552284"}
            }]},
            "head":{"vars":["item","id"]}
        }"#;
        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sparql_resp))
            .mount(&server)
            .await;

        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let mw_api = am.app.wikidata().get_mw_api().await.unwrap();

        let aux = AuxiliaryResults::new(0, 0, 0, 214, "46552284".to_string());
        let result = am
            .sparql_lookup_property_values(214, &[&aux], &mw_api)
            .await
            .unwrap();

        assert_eq!(result.get("46552284"), Some(&vec!["Q123".to_string()]));
    }

    #[tokio::test]
    async fn test_sparql_lookup_parses_duplicate_returns_multiple_qs() {
        let server = MockServer::start().await;
        let sparql_url = format!("{}/sparql", server.uri());
        let siteinfo = siteinfo_with_sparql(&sparql_url);

        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&siteinfo))
            .mount(&server)
            .await;

        let sparql_resp = r#"{
            "results":{"bindings":[
                {"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q1"},
                 "id":{"type":"literal","value":"dup-val"}},
                {"item":{"type":"uri","value":"http://www.wikidata.org/entity/Q2"},
                 "id":{"type":"literal","value":"dup-val"}}
            ]},
            "head":{"vars":["item","id"]}
        }"#;
        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(ResponseTemplate::new(200).set_body_string(sparql_resp))
            .mount(&server)
            .await;

        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let mw_api = am.app.wikidata().get_mw_api().await.unwrap();

        let aux = AuxiliaryResults::new(0, 0, 0, 214, "dup-val".to_string());
        let result = am
            .sparql_lookup_property_values(214, &[&aux], &mw_api)
            .await
            .unwrap();

        let qs = result.get("dup-val").expect("dup-val must be in results");
        assert_eq!(qs.len(), 2);
        assert!(qs.contains(&"Q1".to_string()));
        assert!(qs.contains(&"Q2".to_string()));
    }

    #[tokio::test]
    async fn test_sparql_lookup_returns_err_on_http_failure() {
        let server = MockServer::start().await;
        let sparql_url = format!("{}/sparql", server.uri());
        let siteinfo = siteinfo_with_sparql(&sparql_url);

        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&siteinfo))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let mw_api = am.app.wikidata().get_mw_api().await.unwrap();

        let aux = AuxiliaryResults::new(0, 0, 0, 214, "val".to_string());
        let result = am
            .sparql_lookup_property_values(214, &[&aux], &mw_api)
            .await;

        assert!(result.is_err(), "HTTP 503 should propagate as Err");
    }

    #[tokio::test]
    async fn test_sparql_lookup_values_str_escapes_quotes() {
        // Ensure values containing double-quotes are escaped so the SPARQL is valid.
        // We verify this indirectly: the method must accept the value and build a
        // query without panicking; the mock just returns an empty result set.
        let server = MockServer::start().await;
        let sparql_url = format!("{}/sparql", server.uri());
        let siteinfo = siteinfo_with_sparql(&sparql_url);

        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(&siteinfo))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/sparql"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(
                    r#"{"results":{"bindings":[]},"head":{"vars":["item","id"]}}"#,
                ),
            )
            .mount(&server)
            .await;

        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let mw_api = am.app.wikidata().get_mw_api().await.unwrap();

        let aux = AuxiliaryResults::new(0, 0, 0, 1, r#"val"with"quotes"#.to_string());
        let result = am.sparql_lookup_property_values(1, &[&aux], &mw_api).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ── B: get_sparql_batch_size default ─────────────────────────────────────

    #[tokio::test]
    async fn test_get_sparql_batch_size_default_is_200() {
        let app = test_support::test_app().await;
        let mut am = AuxiliaryMatcher::new(Arc::new(app));
        assert_eq!(am.get_sparql_batch_size(), 200);
    }

    // ── C: match_via_auxiliary_apply_matches calls set_match ─────────────────

    #[tokio::test]
    async fn test_match_via_auxiliary_apply_matches_upgrades_auto_match() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        // Seed an auto-match (user=0) on Q42
        {
            let mut entry = crate::entry::Entry::from_id(entry_id, &app).await.unwrap();
            let _ = EntryWriter::new(&app, &mut entry).set_match("Q42", 0).await;
        }
        let am = AuxiliaryMatcher::new(Arc::new(app.clone()));
        let aux = AuxiliaryResults::new(0, entry_id, 0, 214, "dummy".to_string());
        am.match_via_auxiliary_apply_matches(vec![("Q42".to_string(), aux)])
            .await;
        let entry_after = crate::entry::Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(
            entry_after.user,
            Some(USER_AUX_MATCH),
            "apply_matches must upgrade user=0 to USER_AUX_MATCH"
        );
    }
}
