use crate::app_state::AppState;
use crate::app_state::USER_AUX_MATCH;
use crate::catalog::Catalog;
use crate::entry::*;
use crate::job::*;
use anyhow::Result;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;

pub type RankedNames = HashMap<String, Vec<(usize, String)>>;

lazy_static! {
    pub static ref TAXON_RANKS: HashMap<&'static str, &'static str> = {
        let mut m = HashMap::new();
        m.insert("variety", "Q767728");
        m.insert("subspecies", "Q68947");
        m.insert("species", "Q7432");
        m.insert("superfamily", "Q2136103");
        m.insert("subfamily", "Q2455704");
        m.insert("class", "Q37517");
        m.insert("suborder", "Q5867959");
        m.insert("genus", "Q34740");
        m.insert("family", "Q35409");
        m.insert("order", "Q36602");
        m
    };
    static ref USE_DESCRIPTIONS_FOR_TAXON_NAME_CATALOGS: Vec<usize> = vec!(169, 827);
    static ref RE_CATALOG_169: Regex = RegexBuilder::new(r"^.*\[([a-z ]+).*$")
        .case_insensitive(true)
        .build()
        .expect("Regex error");
}

pub enum TaxonNameField {
    Name,
    Description,
}

impl Jobbable for TaxonMatcher {
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

#[derive(Debug, Clone)]
pub struct TaxonMatcher {
    app: AppState,
    job: Option<Job>,
}

impl TaxonMatcher {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            job: None,
        }
    }

    /// Bespoke taxon name fixes for specific catalogs
    pub fn rewrite_taxon_name(catalog_id: usize, taxon_name: &str) -> Option<String> {
        let mut taxon_name = taxon_name.to_string();

        // Generic
        taxon_name = taxon_name.replace(" ssp. ", " subsp. ");
        if taxon_name.starts_with("× ") {
            taxon_name = taxon_name.replacen("× ", "×", 1);
        };
        taxon_name = taxon_name
            .replace(" subsp ", " subsp. ")
            .replace(" var ", " var. ");

        // Catalog-specific
        if catalog_id == 169 {
            taxon_name = RE_CATALOG_169.replace_all(&taxon_name, "$1").to_string();
        }
        Some(taxon_name)
    }

    /// Tries to find full matches for entries that are a taxon
    pub async fn match_taxa(&mut self, catalog_id: usize) -> Result<()> {
        let mut catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let use_desc = USE_DESCRIPTIONS_FOR_TAXON_NAME_CATALOGS.contains(&catalog_id);
        let mut ranks: Vec<&str> = TAXON_RANKS.clone().into_values().collect();
        ranks.push("Q16521"); // taxon item
        let taxon_name_field = if use_desc {
            TaxonNameField::Description
        } else {
            TaxonNameField::Name
        };
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        loop {
            let (results_len, ranked_names) = self
                .app
                .storage()
                .match_taxa_get_ranked_names_batch(
                    &ranks,
                    &taxon_name_field,
                    catalog_id,
                    batch_size,
                    offset,
                )
                .await?;

            for (rank, v) in ranked_names.iter() {
                self.match_taxa_name_to_entry(rank, v, &mw_api).await?;
            }

            if results_len < batch_size {
                break;
            }
            offset += results_len;
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;

        // Update catalog as "done at least once" if necessary
        catalog.set_taxon_run(self.app.storage(), true).await?;
        Ok(())
    }

    async fn match_taxa_name_to_entry(
        &mut self,
        rank: &str,
        v: &[(usize, String)],
        mw_api: &mediawiki::api::Api,
    ) -> Result<(), anyhow::Error> {
        let all_names: Vec<String> = v
            .iter()
            .map(|(_entry_id, name)| format!("\"{name}\""))
            .collect();
        let name2entry_id: HashMap<String, usize> = v
            .iter()
            .map(|(entry_id, name)| (name.to_owned(), *entry_id))
            .collect();
        for names in all_names.chunks(50) {
            // Prepare SPARQL
            let mut name2q: HashMap<String, Vec<String>> = HashMap::new();
            let names = names.join(" ");
            let sparql = format!("SELECT DISTINCT ?q ?name {{
                        VALUES ?name {{ {} }} VALUES ?instance {{ wd:Q16521 wd:Q4886 }}
                        {{ {{ SELECT DISTINCT ?q ?name ?instance {{ ?q wdt:P225 ?name ; wdt:P31 ?instance {rank} }} }} UNION
                        {{ SELECT DISTINCT ?q ?name ?instance {{ ?q wdt:P1420 ?name ; wdt:P31 ?instance {rank} }} }} }} }}",names);

            // Run SPARQL
            if let Ok(sparql_result) = mw_api.sparql_query(&sparql).await {
                if let Some(bindings) = sparql_result["results"]["bindings"].as_array() {
                    for b in bindings {
                        if let (Some(entity_url), Some(name)) =
                            (b["q"]["value"].as_str(), b["name"]["value"].as_str())
                        {
                            if let Ok(q) = mw_api.extract_entity_from_uri(entity_url) {
                                name2q.entry(name.to_string()).or_default().push(q);
                            }
                        }
                    }
                }
            }

            self.match_taxa_filter_name2q(name2q, &name2entry_id)
                .await?;
        }
        Ok(())
    }

    async fn match_taxa_filter_name2q(
        &mut self,
        name2q: HashMap<String, Vec<String>>,
        name2entry_id: &HashMap<String, usize>,
    ) -> Result<(), anyhow::Error> {
        for (name, mut qs) in name2q {
            if let Some(entry_id) = name2entry_id.get(&name) {
                qs.sort();
                qs.dedup();

                match qs.len().cmp(&1) {
                    std::cmp::Ordering::Less => {}
                    std::cmp::Ordering::Equal => {
                        if let Some(q) = qs.pop() {
                            let _ = Entry::from_id(*entry_id, &self.app)
                                .await?
                                .set_match(&q, USER_AUX_MATCH)
                                .await;
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        let _ = Entry::from_id(*entry_id, &self.app)
                            .await?
                            .set_multi_match(&qs)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    const TEST_CATALOG_ID: usize = 5526;
    //const _TEST_ENTRY_ID1: usize = 144000951 ; // Britannica-style, akin to catalog 169
    const TEST_ENTRY_ID: usize = 144000952;

    #[tokio::test]
    async fn test_rewrite_taxon_name() {
        assert_eq!(
            "Carphophis amoenus",
            TaxonMatcher::rewrite_taxon_name(0, "Carphophis amoenus").unwrap()
        ); // Pass through
        assert_eq!(
            "Carphophis subsp. amoenus",
            TaxonMatcher::rewrite_taxon_name(0, "Carphophis ssp. amoenus").unwrap()
        ); // Subspecies
        assert_eq!(
            "Carphophis amoenus",
            TaxonMatcher::rewrite_taxon_name(169, "reptile; [Carphophis amoenus, foo bar]")
                .unwrap()
        ); // Britannica desc
    }

    #[tokio::test]
    async fn test_match_taxa() {
        let app = get_test_app();
        let mut tm = TaxonMatcher::new(&app);

        // Clear entry
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();

        // Run matching
        tm.match_taxa(TEST_CATALOG_ID).await.unwrap();

        // Check matching and clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(2940133));
        assert_eq!(entry.user, Some(4));
        entry.unmatch().await.unwrap();
    }
}
