use crate::app_state::AppState;
use crate::app_state::USER_LOCATION_MATCH;
use crate::entry::Entry;
use crate::job::{Job, Jobbable};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use log::error;
use mediawiki::api::Api;
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;

const DEFAULT_MAX_DISTANCE: &str = "500m";
const MAX_AUTOMATCH_DISTANCE: f64 = 0.1; // km
const MAX_RESULTS_FOR_RANDOM_CATALOG: usize = 5000;

lazy_static! {
    static ref RE_METERS: Regex = RegexBuilder::new(r"^([0-9.]+)m$")
        .build()
        .expect("Regex error");
    static ref RE_KILOMETERS: Regex = RegexBuilder::new(r"^([0-9.]+)km$")
        .build()
        .expect("Regex error");
}

#[derive(Debug, Clone)]
pub struct LocationRow {
    pub lat: f64,
    pub lon: f64,
    pub entry_id: usize,
    pub catalog_id: usize,
    pub ext_name: String,
    pub entry_type: String,
    pub q: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct CoordinateMatcher {
    app: AppState,
    mw_api: Api,
    job: Option<Job>,
    catalog_id: Option<usize>,
    permissions: HashMap<String, HashMap<usize, String>>,
    bad_catalogs: Vec<usize>,
}

impl Jobbable for CoordinateMatcher {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }

    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl CoordinateMatcher {
    pub async fn new(app: &AppState, catalog_id: Option<usize>) -> Result<Self> {
        let mw_api = app.wikidata().get_mw_api().await?;
        let mut ret = Self {
            app: app.clone(),
            mw_api,
            job: None,
            catalog_id, // Specific catalog ID, or None for random catalogs
            permissions: HashMap::new(),
            bad_catalogs: vec![],
        };
        ret.load_permissions().await?;
        Ok(ret)
    }

    pub async fn run(&self) -> Result<()> {
        self.check_bad_catalog()?;
        let rows = self
            .app
            .storage()
            .get_coordinate_matcher_rows(
                &self.catalog_id,
                &self.bad_catalogs,
                MAX_RESULTS_FOR_RANDOM_CATALOG,
            )
            .await?;
        for row in &rows {
            let _ = self.process_row(row).await;
        }
        Ok(())
    }

    async fn process_row(&self, row: &LocationRow) -> Result<()> {
        let (max_distance, max_distance_sparql) = self.get_max_distance_sparql_for_entry(row);
        let ext_name = row
            .ext_name
            .split('(')
            .next()
            .ok_or_else(|| anyhow!("Can't split {}", &row.ext_name))?
            .trim()
            .to_lowercase();

        let mut matches = vec![];
        let results = self.process_row_get_results(row, &max_distance).await?;
        for result in results {
            let q = match result["title"].as_str() {
                Some(q) => q,
                None => continue, // No title, should never happen
            };
            let snippet = result["snippet"].as_str().unwrap_or_default();
            for label in snippet.split('\n') {
                let label = label.trim().to_lowercase();
                if label.starts_with(&ext_name) || label.ends_with(&ext_name) {
                    matches.push(q.to_string());
                    break;
                }
            }
        }

        self.process_row_process_matches(matches, row, max_distance_sparql)
            .await;
        Ok(())
    }

    async fn process_row_process_matches(
        &self,
        matches: Vec<String>,
        row: &LocationRow,
        max_distance_sparql: f64,
    ) {
        if matches.is_empty() {
            if self.is_permission("allow_location_create", row.catalog_id, "yes")
                && self
                    .try_match_via_sparql_query(row, max_distance_sparql)
                    .await
            {
                error!("CoordinateMatcher: TODO create item");
            }
        } else if self.is_permission("allow_location_match", row.catalog_id, "yes")
            && !self.try_match_via_wikidata_search(row, &matches).await
        {
            let _ = self
                .try_match_via_sparql_query(row, max_distance_sparql)
                .await;
        }
    }

    async fn process_row_get_results(
        &self,
        row: &LocationRow,
        max_distance: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let p31 = self.get_entry_type(row).unwrap_or_default();
        let mut query = format!("nearcoord:{max_distance},{},{}", row.lat, row.lon);
        if !p31.is_empty() {
            query += " haswbstatement:P31={p31}";
        }
        let params = vec![
            ("action", "query"),
            ("list", "search"),
            ("titlesnippet", ""),
            ("srnamespace", "0"),
            ("srlimit", "500"),
            ("srsearch", query.as_str()),
        ];
        let params = self.mw_api.params_into(&params);
        let result = self.mw_api.query_api_json(&params, "GET").await?;
        let results = result["query"]["search"]
            .as_array()
            .map(|v| v.to_owned())
            .unwrap_or_default();
        Ok(results)
    }

    // Returns true if there is a match
    async fn try_match_via_wikidata_search(&self, row: &LocationRow, items: &[String]) -> bool {
        if items.is_empty() {
            return false;
        }
        let mut entry = match Entry::from_id(row.entry_id, &self.app).await {
            Ok(entry) => entry,
            Err(_) => return false,
        };
        if items.len() == 1 {
            let q = items.first().unwrap();
            if entry.q == AppState::item2numeric(q) && entry.is_fully_matched() {
                // Already the same match
                return false;
            }
            // println!("Matching https://mix-n-match.toolforge.org/#/entry/{} to https://www.wikidata.org/wiki/{q}", row.entry_id);
            let _ = entry.set_match(q, USER_LOCATION_MATCH).await;
        } else if items.len() > 1 && entry.is_unmatched() {
            // Only set multimatch if entry is unmatched
            // println!("WARNING: https://mix-n-match.toolforge.org/#/entry/{} seems to match: {items:?}", row.entry_id);
            let _ = entry.set_auto_and_multi_match(items).await;
        }
        true // Entry is fully or partially matched
    }

    // Returns true if no results were found
    async fn try_match_via_sparql_query(&self, row: &LocationRow, max_distance: f64) -> bool {
        let type_query = self
            .get_entry_type(row)
            .map_or_else(String::default, |type_q| {
                format!("?place wdt:P31/wdt:P279* wd:{type_q}")
            });
        let sparql = format!(
            "SELECT DISTINCT ?place ?distance WHERE {{
		    SERVICE wikibase:around {{
		      ?place wdt:P625 ?location .
		      bd:serviceParam wikibase:center 'Point({} {})'^^geo:wktLiteral .
		      bd:serviceParam wikibase:radius '{max_distance}' .
		      bd:serviceParam wikibase:distance ?distance .
		    }}
            {type_query}
		}} ORDER BY (?distance) LIMIT 500",
            row.lon, row.lat
        );
        let sparql_result = match self.mw_api.sparql_query(&sparql).await {
            Ok(r) => r,
            Err(_) => return false,
        };
        let mut candidates = vec![];
        if let Some(bindings) = sparql_result["results"]["bindings"].as_array() {
            for b in bindings {
                if b["distance"]["value"].as_f64().unwrap_or(0.0) > max_distance {
                    continue;
                }
                if let Some(place) = b["place"]["value"].as_str() {
                    if let Ok(place) = self.mw_api.extract_entity_from_uri(place) {
                        let q_already_set_to_place =
                            row.q.is_some_and(|q| format!("Q{q}") != place);
                        if !q_already_set_to_place {
                            candidates.push(place);
                        }
                    }
                }
            }
        }
        candidates.is_empty()
    }

    fn check_bad_catalog(&self) -> Result<()> {
        if let Some(catalog_id) = self.catalog_id {
            if self.bad_catalogs.contains(&catalog_id) {
                return Err(anyhow!("CoordinateMatcher: Bad catalog: {catalog_id}"));
            }
        }
        Ok(())
    }

    // (max_distance,max_distance_sparql)
    fn get_max_distance_sparql_for_entry(&self, row: &LocationRow) -> (String, f64) {
        let max_distance = self
            .get_permission_value("location_distance", row.catalog_id)
            .map_or_else(|| DEFAULT_MAX_DISTANCE.to_string(), |s| s.to_owned());
        let mut max_distance_sparql = MAX_AUTOMATCH_DISTANCE; // Default
        if let Some(captures) = RE_KILOMETERS.captures(&max_distance) {
            if let Ok(value) = captures[1].parse::<f64>() {
                max_distance_sparql = value;
            }
        } else if let Some(captures) = RE_METERS.captures(&max_distance) {
            if let Ok(value) = captures[1].parse::<f64>() {
                max_distance_sparql = value / 1000.0;
            }
        }
        (max_distance, max_distance_sparql)
    }

    fn get_entry_type(&self, row: &LocationRow) -> Option<String> {
        if self.is_permission("location_force_same_type", row.catalog_id, "yes")
            && !row.entry_type.is_empty()
        {
            return Some(row.entry_type.to_owned());
        }
        None
    }

    fn get_permission_value(&self, key: &str, catalog_id: usize) -> Option<&String> {
        self.permissions.get(key)?.get(&catalog_id) //.map(|v|v.to_owned())
    }

    fn is_permission(&self, key: &str, catalog_id: usize, value: &str) -> bool {
        self.get_permission_value(key, catalog_id) == Some(&value.to_string())
    }

    async fn load_permissions(&mut self) -> Result<()> {
        let results = self
            .app
            .storage()
            .get_all_catalogs_key_value_pairs()
            .await?;
        for (catalog_id, kv_key, kv_value) in results {
            self.permissions
                .entry(kv_key)
                .or_default()
                .insert(catalog_id, kv_value);
        }
        self.bad_catalogs = self
            .permissions
            .entry("allow_location_operations".to_owned())
            .or_default()
            .iter()
            .filter(|(_catalog_id, value)| *value == "no")
            .map(|(catalog_id, _value)| *catalog_id)
            .collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::app_state::*;

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 157175552;

    #[tokio::test]
    async fn test_match_by_coordinates() {
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();
        let cm = CoordinateMatcher::new(&app, Some(TEST_CATALOG_ID))
            .await
            .unwrap();
        cm.run().await.unwrap();
        let mut entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry2.q, Some(12060465));
        entry2.unmatch().await.unwrap();
    }
}
