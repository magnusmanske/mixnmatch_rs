use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// laji.fi — Finnish taxa (6545)
//
// Paginated species index restricted to Finnish-occurring taxa under
// `MX.37600`. Each page returns up to 1000 results; we keep paging
// until a page comes back short (PHP `if ($found != 1000) break`).
// Every entry is a taxon (Q16521).

const PAGE_SIZE: usize = 1000;

#[derive(Debug)]
pub struct BespokeScraper6545 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6545 {
    scraper_boilerplate!(6545);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        let mut page: usize = 1;
        loop {
            let url = format!(
                "https://laji.fi/api/taxa/MX.37600/species\
?onlyFinnish=true\
&selectedFields=vernacularName,scientificName,cursiveName,typeOfOccurrenceInFinland,\
latestRedListStatusFinland,administrativeStatuses,*.scientificName,\
*.scientificNameAuthorship,*.cursiveName,id,nonHiddenParentsIncludeSelf\
&lang=multi\
&page={page}\
&pageSize={PAGE_SIZE}\
&sortOrder=taxonomic"
            );
            let response = match client.get(&url).send().await {
                Ok(r) => r,
                Err(_) => break,
            };
            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(_) => break,
            };
            let results = match json["results"].as_array() {
                Some(arr) => arr,
                None => break,
            };
            let found = results.len();
            for r in results {
                if let Some(ee) = Self::parse_item(self.catalog_id(), r) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
            // Stop paging the first time we get a short page — same
            // termination rule as PHP. Note: this means a page
            // returning exactly 0 results also stops the loop.
            if found != PAGE_SIZE {
                break;
            }
            page += 1;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper6545 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        r: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = r.get("id")?.as_str()?.to_string();
        if id.is_empty() {
            return None;
        }
        let scientific_name = r
            .get("scientificName")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if scientific_name.is_empty() {
            return None;
        }
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name: scientific_name,
            ext_url: format!("https://laji.fi/taxon/{id}"),
            random: rand::rng().random(),
            // Q16521 = taxon
            type_name: Some("Q16521".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_6545_parse_item_full() {
        let r = serde_json::json!({
            "id": "MX.41747",
            "scientificName": "Ursus arctos"
        });
        let ee = BespokeScraper6545::parse_item(6545, &r).unwrap();
        assert_eq!(ee.entry.ext_id, "MX.41747");
        assert_eq!(ee.entry.ext_name, "Ursus arctos");
        assert_eq!(ee.entry.ext_url, "https://laji.fi/taxon/MX.41747");
        assert_eq!(ee.entry.type_name, Some("Q16521".to_string()));
    }

    #[test]
    fn test_6545_parse_item_missing_id_skipped() {
        let r = serde_json::json!({"scientificName": "Foo bar"});
        assert!(BespokeScraper6545::parse_item(6545, &r).is_none());
    }

    #[test]
    fn test_6545_parse_item_empty_id_skipped() {
        let r = serde_json::json!({"id": "", "scientificName": "Foo bar"});
        assert!(BespokeScraper6545::parse_item(6545, &r).is_none());
    }

    #[test]
    fn test_6545_parse_item_empty_scientific_name_skipped() {
        let r = serde_json::json!({"id": "x", "scientificName": ""});
        assert!(BespokeScraper6545::parse_item(6545, &r).is_none());
    }

    #[test]
    fn test_6545_parse_item_missing_scientific_name_skipped() {
        let r = serde_json::json!({"id": "x"});
        assert!(BespokeScraper6545::parse_item(6545, &r).is_none());
    }

    #[test]
    fn test_6545_page_size_constant() {
        assert_eq!(PAGE_SIZE, 1000);
    }
}
