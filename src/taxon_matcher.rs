use crate::app_state::*;
use crate::entry::*;
use crate::job::*;
use crate::mixnmatch::*;
use lazy_static::lazy_static;
use mysql_async::from_row;
use mysql_async::prelude::*;
use regex::{Regex, RegexBuilder};
use std::collections::HashMap;

lazy_static! {
    static ref TAXON_RANKS: HashMap<&'static str, &'static str> = {
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
    mnm: MixNMatch,
    job: Option<Job>,
}

impl TaxonMatcher {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
            job: None,
        }
    }

    /// Bespoke taxon name fixes for specific catalogs
    fn rewrite_taxon_name(&self, catalog_id: usize, taxon_name: &str) -> Option<String> {
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
    pub async fn match_taxa(&mut self, catalog_id: usize) -> Result<(), GenericError> {
        let mw_api = self.mnm.get_mw_api().await?;
        let use_desc = USE_DESCRIPTIONS_FOR_TAXON_NAME_CATALOGS.contains(&catalog_id);
        let taxon_name_column = if use_desc { "ext_desc" } else { "ext_name" };
        let mut ranks: Vec<&str> = TAXON_RANKS.clone().into_values().collect();
        ranks.push("Q16521"); // taxon item
        let sql = format!(
            r"SELECT `id`,`{}` AS taxon_name,`type` FROM `entry`
            WHERE `catalog` IN (:catalog_id) AND (`q` IS NULL OR `user`=0) AND `type` IN ('{}')
            LIMIT :batch_size OFFSET :offset",
            taxon_name_column,
            ranks.join("','")
        );
        let mut offset = self.get_last_job_offset().await;
        let batch_size = 5000;
        loop {
            let results = self
                .mnm
                .app
                .get_mnm_conn()
                .await?
                .exec_iter(sql.clone(), params! {catalog_id,batch_size,offset})
                .await?
                .map_and_drop(from_row::<(usize, String, String)>)
                .await?;
            let mut ranked_names: HashMap<String, Vec<(usize, String)>> = HashMap::new();
            for result in &results {
                let entry_id = result.0;
                let taxon_name = match self.rewrite_taxon_name(catalog_id, &result.1) {
                    Some(s) => s,
                    None => continue,
                };
                let type_name = &result.2;
                let rank = match TAXON_RANKS.get(type_name.as_str()) {
                    Some(rank) => format!(" ; wdt:P105 {rank}"),
                    None => "".to_string(),
                };
                ranked_names
                    .entry(rank)
                    .or_default()
                    .push((entry_id, taxon_name));
            }

            for (rank, v) in ranked_names.iter() {
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

                    // Filter results
                    for (name, mut qs) in name2q {
                        if let Some(entry_id) = name2entry_id.get(&name) {
                            qs.sort();
                            qs.dedup();

                            match qs.len().cmp(&1) {
                                std::cmp::Ordering::Less => {}
                                std::cmp::Ordering::Equal => {
                                    if let Some(q) = qs.pop() {
                                        let _ = Entry::from_id(*entry_id, &self.mnm)
                                            .await?
                                            .set_match(&q, USER_AUX_MATCH)
                                            .await;
                                    }
                                }
                                std::cmp::Ordering::Greater => {
                                    let _ = Entry::from_id(*entry_id, &self.mnm)
                                        .await?
                                        .set_multi_match(&qs)
                                        .await;
                                }
                            }
                        }
                    }
                }
            }

            if results.len() < batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;

        // Update catalog as "done at least once" if necessary
        let sql = "UPDATE `catalog` SET `taxon_run`=1 WHERE `id`=:catalog_id AND `taxon_run`=0";
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params! {catalog_id})
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526;
    //const _TEST_ENTRY_ID1: usize = 144000951 ; // Britannica-style, akin to catalog 169
    const TEST_ENTRY_ID: usize = 144000952;

    #[tokio::test]
    async fn test_rewrite_taxon_name() {
        let mnm = get_test_mnm();
        let tm = TaxonMatcher::new(&mnm);
        assert_eq!(
            "Carphophis amoenus",
            tm.rewrite_taxon_name(0, "Carphophis amoenus").unwrap()
        ); // Pass through
        assert_eq!(
            "Carphophis subsp. amoenus",
            tm.rewrite_taxon_name(0, "Carphophis ssp. amoenus").unwrap()
        ); // Subspecies
        assert_eq!(
            "Carphophis amoenus",
            tm.rewrite_taxon_name(169, "reptile; [Carphophis amoenus, foo bar]")
                .unwrap()
        ); // Britannica desc
    }

    #[tokio::test]
    async fn test_match_taxa() {
        let mnm = get_test_mnm();
        let mut tm = TaxonMatcher::new(&mnm);

        // Clear entry
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.unmatch().await.unwrap();

        // Run matching
        tm.match_taxa(TEST_CATALOG_ID).await.unwrap();

        // Check matching and clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q, Some(2940133));
        assert_eq!(entry.user, Some(4));
        entry.unmatch().await.unwrap();
    }
}
