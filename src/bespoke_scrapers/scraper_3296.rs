use crate::{
    app_state::AppState,
    auxiliary_data::AuxiliaryRow,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// GBIF – Global Biodiversity Information Facility checklist (catalog 3296).
// Fetches all accepted, non-synonym species from the GBIF species API and stores
// their scientific name (P225) and taxon rank (P105).

const BATCH_SIZE: u64 = 1000;

#[derive(Debug)]
pub struct BespokeScraper3296 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper3296 {
    scraper_boilerplate!(3296);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut offset: u64 = 0;
        let mut end_of_records = false;

        while !end_of_records {
            let url = format!(
                "https://api.gbif.org/v1/species?limit={BATCH_SIZE}&offset={offset}"
            );
            let json: serde_json::Value = client.get(&url).send().await?.json().await?;
            end_of_records = json["endOfRecords"].as_bool().unwrap_or(true);

            let results = match json["results"].as_array() {
                Some(r) => r,
                None => break,
            };

            let mut cache: Vec<ExtendedEntry> = results
                .iter()
                .filter_map(|r| Self::parse_result(self.catalog_id(), r))
                .collect();

            self.process_cache(&mut cache).await?;
            offset += BATCH_SIZE;
        }
        Ok(())
    }
}

impl BespokeScraper3296 {
    /// Parse one GBIF species result; return `None` for synonyms and unaccepted names.
    pub(crate) fn parse_result(catalog_id: usize, r: &serde_json::Value) -> Option<ExtendedEntry> {
        // Mirror the PHP filter: skip synonyms and non-accepted records
        if r["synonym"].as_bool().unwrap_or(false) {
            return None;
        }
        if r["taxonomicStatus"].as_str() != Some("ACCEPTED") {
            return None;
        }

        let key = r["key"].as_u64()?;
        let scientific_name = r["scientificName"].as_str()?;

        let mut desc_parts = vec![];
        if let Some(rank) = r["rank"].as_str() {
            desc_parts.push(rank.to_string());
        }
        if let Some(vn) = r["vernacularName"].as_str() {
            if !vn.eq_ignore_ascii_case(scientific_name) {
                desc_parts.push(vn.to_string());
            }
        }
        let ext_desc = desc_parts.join(" | ");

        let rank_q = r["rank"]
            .as_str()
            .and_then(|rank| Self::rank_to_q(rank));

        let mut aux = std::collections::HashSet::new();
        aux.insert(AuxiliaryRow::new(225, scientific_name.to_string()));
        if let Some(q) = rank_q {
            aux.insert(AuxiliaryRow::new(105, q.to_string()));
        }

        Some(ExtendedEntry {
            entry: Entry {
                catalog: catalog_id,
                ext_id: key.to_string(),
                ext_url: format!(
                    "https://www.gbif.org/species/{}",
                    key
                ),
                ext_name: scientific_name.to_string(),
                ext_desc,
                type_name: Some("Q16521".to_string()),
                random: rand::rng().random(),
                ..Default::default()
            },
            aux,
            ..Default::default()
        })
    }

    /// Map GBIF rank string to the corresponding Wikidata Q-item (P105 value).
    pub(crate) fn rank_to_q(rank: &str) -> Option<&'static str> {
        match rank.to_ascii_lowercase().as_str() {
            "kingdom" => Some("Q36732"),
            "phylum" => Some("Q38348"),
            "class" => Some("Q37517"),
            "order" => Some("Q36602"),
            "family" => Some("Q35409"),
            "genus" => Some("Q34740"),
            "subgenus" => Some("Q3238261"),
            "species" => Some("Q7432"),
            "subspecies" => Some("Q68947"),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    fn make_scraper() -> BespokeScraper3296 {
        BespokeScraper3296 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_3296_catalog_id() {
        assert_eq!(make_scraper().catalog_id(), 3296);
    }

    #[test]
    fn test_3296_parse_accepted_species() {
        let r = serde_json::json!({
            "key": 1,
            "scientificName": "Animalia",
            "rank": "KINGDOM",
            "taxonomicStatus": "ACCEPTED",
            "synonym": false,
            "vernacularName": "animals"
        });
        let ee = BespokeScraper3296::parse_result(3296, &r).unwrap();
        assert_eq!(ee.entry.ext_id, "1");
        assert_eq!(ee.entry.ext_name, "Animalia");
        assert_eq!(ee.entry.type_name, Some("Q16521".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Animalia".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(105, "Q36732".to_string())));
        // vernacular differs from scientific, so it's in desc
        assert!(ee.entry.ext_desc.contains("animals"));
    }

    #[test]
    fn test_3296_parse_synonym_returns_none() {
        let r = serde_json::json!({
            "key": 2,
            "scientificName": "OldName",
            "rank": "SPECIES",
            "taxonomicStatus": "SYNONYM",
            "synonym": true
        });
        assert!(BespokeScraper3296::parse_result(3296, &r).is_none());
    }

    #[test]
    fn test_3296_parse_doubtful_returns_none() {
        let r = serde_json::json!({
            "key": 3,
            "scientificName": "Uncertain",
            "rank": "SPECIES",
            "taxonomicStatus": "DOUBTFUL",
            "synonym": false
        });
        assert!(BespokeScraper3296::parse_result(3296, &r).is_none());
    }

    #[test]
    fn test_3296_parse_missing_key_returns_none() {
        let r = serde_json::json!({
            "scientificName": "NoKey",
            "rank": "SPECIES",
            "taxonomicStatus": "ACCEPTED",
            "synonym": false
        });
        assert!(BespokeScraper3296::parse_result(3296, &r).is_none());
    }

    #[test]
    fn test_3296_parse_unknown_rank_no_p105() {
        let r = serde_json::json!({
            "key": 99,
            "scientificName": "Weirdus taxon",
            "rank": "VARIETY",
            "taxonomicStatus": "ACCEPTED",
            "synonym": false
        });
        let ee = BespokeScraper3296::parse_result(3296, &r).unwrap();
        // P225 present, P105 absent (unknown rank)
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Weirdus taxon".to_string())));
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 105));
    }

    #[test]
    fn test_3296_rank_to_q_known_ranks() {
        assert_eq!(BespokeScraper3296::rank_to_q("SPECIES"), Some("Q7432"));
        assert_eq!(BespokeScraper3296::rank_to_q("GENUS"), Some("Q34740"));
        assert_eq!(BespokeScraper3296::rank_to_q("FAMILY"), Some("Q35409"));
        assert_eq!(BespokeScraper3296::rank_to_q("kingdom"), Some("Q36732"));
    }

    #[test]
    fn test_3296_rank_to_q_unknown() {
        assert_eq!(BespokeScraper3296::rank_to_q("VARIETY"), None);
        assert_eq!(BespokeScraper3296::rank_to_q(""), None);
    }

    #[test]
    fn test_3296_vernacular_same_as_scientific_not_in_desc() {
        let r = serde_json::json!({
            "key": 5,
            "scientificName": "Animalia",
            "rank": "KINGDOM",
            "taxonomicStatus": "ACCEPTED",
            "synonym": false,
            "vernacularName": "Animalia"
        });
        let ee = BespokeScraper3296::parse_result(3296, &r).unwrap();
        // Same name should not be duplicated in desc
        assert!(!ee.entry.ext_desc.to_lowercase().contains("animalia"));
    }
}
