use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Plants of the World Online (POWO) (1178)

#[derive(Debug)]
pub struct BespokeScraper1178 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper1178 {

    scraper_boilerplate!(1178);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        let mut page: u64 = 1;
        let mut total_pages: u64 = 3;
        while page < total_pages {
            let url = format!(
                "https://powo.science.kew.org/api/1/search?page.size=480&page={}",
                page
            );
            let json: serde_json::Value = client.get(&url).send().await?.json().await?;
            let results = match json["results"].as_array() {
                Some(r) => r,
                None => break,
            };
            if let Some(tp) = json["totalPages"].as_u64() {
                total_pages = tp;
            }
            for result in results {
                if let Some(ee) = Self::parse_result(self.catalog_id(), result) {
                    entry_cache.push(ee);
                }
            }
            self.maybe_flush_cache(&mut entry_cache).await?;
            page += 1;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper1178 {
    pub(crate) fn parse_result(
        catalog_id: usize,
        result: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        // Only accepted taxa
        if !result["accepted"].as_bool().unwrap_or(false) {
            return None;
        }

        let url_path = result["url"].as_str()?;
        let id = Self::extract_id_from_url(url_path)?;
        if id.is_empty() {
            return None;
        }

        let name = result["name"].as_str().unwrap_or_default();
        if name.is_empty() {
            return None;
        }

        let ext_url = format!("http://www.plantsoftheworldonline.org/taxon/{}", id);

        // Build aux data
        let mut aux = std::collections::HashSet::new();
        // P225 = taxon name
        aux.insert(AuxiliaryRow::new(225, name.to_string()));

        // Build description parts
        let mut desc_parts = vec![];
        if let Some(author) = result["author"].as_str() {
            if !author.is_empty() {
                desc_parts.push(format!("author:{}", author));
            }
        }
        if let Some(rank) = result["rank"].as_str() {
            let rank_lower = rank.to_lowercase();
            if !rank_lower.is_empty() {
                desc_parts.push(format!("rank:{}", rank_lower));
                // P105 = taxon rank
                match rank_lower.as_str() {
                    "species" => {
                        aux.insert(AuxiliaryRow::new(105, "Q7432".to_string()));
                    }
                    "genus" => {
                        aux.insert(AuxiliaryRow::new(105, "Q34740".to_string()));
                    }
                    "family" => {
                        aux.insert(AuxiliaryRow::new(105, "Q35409".to_string()));
                    }
                    _ => {}
                }
            }
        }
        let desc = desc_parts.join("|");

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.to_string(),
            ext_name: name.to_string(),
            ext_desc: desc,
            ext_url,
            random: rand::rng().random(),
            type_name: Some("Q16521".to_string()), // taxon
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            ..Default::default()
        })
    }

    /// Extract the ID from a URL path like "/taxon/urn:lsid:ipni.org:names:12345-1"
    /// by taking everything after the last '/'.
    pub(crate) fn extract_id_from_url(url_path: &str) -> Option<String> {
        let id = url_path.rsplit('/').next()?;
        if id.is_empty() {
            None
        } else {
            Some(id.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper1178 {
        BespokeScraper1178 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_1178_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 1178);
    }

    #[test]
    fn test_1178_extract_id_from_url() {
        assert_eq!(
            BespokeScraper1178::extract_id_from_url("/taxon/urn:lsid:ipni.org:names:12345-1"),
            Some("urn:lsid:ipni.org:names:12345-1".to_string())
        );
        assert_eq!(
            BespokeScraper1178::extract_id_from_url("/something/abc"),
            Some("abc".to_string())
        );
        assert_eq!(
            BespokeScraper1178::extract_id_from_url("simple-id"),
            Some("simple-id".to_string())
        );
    }

    #[test]
    fn test_1178_extract_id_from_url_empty() {
        assert!(BespokeScraper1178::extract_id_from_url("").is_none());
        assert!(BespokeScraper1178::extract_id_from_url("/").is_none());
    }

    #[test]
    fn test_1178_parse_result_accepted_species() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/urn:lsid:ipni.org:names:12345-1",
            "name": "Rosa canina",
            "author": "L.",
            "rank": "Species"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        assert_eq!(ee.entry.ext_id, "urn:lsid:ipni.org:names:12345-1");
        assert_eq!(ee.entry.ext_name, "Rosa canina");
        assert_eq!(ee.entry.ext_desc, "author:L.|rank:species");
        assert_eq!(
            ee.entry.ext_url,
            "http://www.plantsoftheworldonline.org/taxon/urn:lsid:ipni.org:names:12345-1"
        );
        assert_eq!(ee.entry.catalog, 1178);
        assert_eq!(ee.entry.type_name, Some("Q16521".to_string()));
        // Check aux: P225 (taxon name) and P105 (taxon rank = Q7432 for species)
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Rosa canina".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(105, "Q7432".to_string())));
    }

    #[test]
    fn test_1178_parse_result_genus() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/urn:lsid:ipni.org:names:30000-1",
            "name": "Rosa",
            "rank": "Genus"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        assert_eq!(ee.entry.ext_desc, "rank:genus");
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Rosa".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(105, "Q34740".to_string())));
    }

    #[test]
    fn test_1178_parse_result_family() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/urn:lsid:ipni.org:names:40000-1",
            "name": "Rosaceae",
            "rank": "Family"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        assert_eq!(ee.entry.ext_desc, "rank:family");
        assert!(ee.aux.contains(&AuxiliaryRow::new(105, "Q35409".to_string())));
    }

    #[test]
    fn test_1178_parse_result_not_accepted() {
        let result = serde_json::json!({
            "accepted": false,
            "url": "/taxon/urn:lsid:ipni.org:names:99999-1",
            "name": "Some synonym",
            "rank": "Species"
        });
        assert!(BespokeScraper1178::parse_result(1178, &result).is_none());
    }

    #[test]
    fn test_1178_parse_result_missing_accepted() {
        let result = serde_json::json!({
            "url": "/taxon/urn:lsid:ipni.org:names:99999-1",
            "name": "No accepted field"
        });
        // accepted defaults to false
        assert!(BespokeScraper1178::parse_result(1178, &result).is_none());
    }

    #[test]
    fn test_1178_parse_result_no_url() {
        let result = serde_json::json!({
            "accepted": true,
            "name": "No URL"
        });
        assert!(BespokeScraper1178::parse_result(1178, &result).is_none());
    }

    #[test]
    fn test_1178_parse_result_empty_name() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/id123",
            "name": ""
        });
        assert!(BespokeScraper1178::parse_result(1178, &result).is_none());
    }

    #[test]
    fn test_1178_parse_result_no_rank_no_author() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/id456",
            "name": "Minimal Plant"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
        // Only P225 aux, no P105
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Minimal Plant".to_string())));
        assert_eq!(ee.aux.len(), 1);
    }

    #[test]
    fn test_1178_parse_result_unknown_rank() {
        // A rank that is not species/genus/family should not add P105 aux
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/id789",
            "name": "Some Order",
            "rank": "Order"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        assert_eq!(ee.entry.ext_desc, "rank:order");
        // Only P225, no P105 for unknown ranks
        assert!(ee.aux.contains(&AuxiliaryRow::new(225, "Some Order".to_string())));
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 105));
    }

    #[test]
    fn test_1178_parse_result_species_aux_count() {
        let result = serde_json::json!({
            "accepted": true,
            "url": "/taxon/id-species",
            "name": "Quercus robur",
            "author": "L.",
            "rank": "Species"
        });
        let ee = BespokeScraper1178::parse_result(1178, &result).unwrap();
        // Should have P225 (taxon name) and P105 (taxon rank) = 2 aux entries
        assert_eq!(ee.aux.len(), 2);
    }

    #[test]
    fn test_1178_total_pages_parsing() {
        let json = serde_json::json!({
            "totalPages": 150,
            "results": []
        });
        let tp = json["totalPages"].as_u64().unwrap_or(0);
        assert_eq!(tp, 150);
    }
}
