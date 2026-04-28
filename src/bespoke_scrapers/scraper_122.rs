use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Kansallisbiografia - Finnish National Biography (122)

#[derive(Debug)]
pub struct BespokeScraper122 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper122 {

    scraper_boilerplate!(122);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        let mut page: u64 = 1;
        loop {
            let url = format!(
                "https://kansallisbiografia.fi/api/data-national-biography?page={}",
                page
            );
            let resp = client.get(&url).send().await?;
            let text = resp.text().await?;
            if text.is_empty() {
                break;
            }
            let json: serde_json::Value = match serde_json::from_str(&text) {
                Ok(j) => j,
                Err(_) => break,
            };
            let entries = Self::parse_page(self.catalog_id(), &json);
            entry_cache.extend(entries);
            self.maybe_flush_cache(&mut entry_cache).await?;
            let page_count = json["pagination"]["pageCount"].as_u64().unwrap_or(0);
            if page_count <= page {
                break;
            }
            page += 1;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper122 {
    pub(crate) fn parse_page(catalog_id: usize, json: &serde_json::Value) -> Vec<ExtendedEntry> {
        let items = match json["items"].as_array() {
            Some(items) => items,
            None => return vec![],
        };
        items
            .iter()
            .filter_map(|item| Self::parse_item(catalog_id, item))
            .collect()
    }

    pub(crate) fn parse_item(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = item["id"].as_i64().or_else(|| {
            item["id"].as_str().and_then(|s| s.parse::<i64>().ok())
        })?;
        let id_str = id.to_string();

        let firstname = item["firstname"].as_str().unwrap_or_default();
        let lastname = item["lastname"].as_str().unwrap_or_default();
        let name = format!("{} {}", firstname, lastname).trim().to_string();
        if name.is_empty() {
            return None;
        }

        let year_of_birth = item["year_of_birth"].as_str().unwrap_or_default();
        let year_of_death = item["year_of_death"].as_str().unwrap_or_default();
        let desc = format!("{} {}", year_of_birth, year_of_death).trim().to_string();

        let ext_url = format!("http://www.kansallisbiografia.fi/kb/artikkeli/{}", id_str);

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id_str,
            ext_name: name,
            ext_desc: desc,
            ext_url,
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
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

    fn make_scraper() -> BespokeScraper122 {
        BespokeScraper122 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_122_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 122);
    }

    #[test]
    fn test_122_parse_page_full() {
        let json = serde_json::json!({
            "pagination": { "pageCount": 5 },
            "items": [
                {
                    "id": 1234,
                    "firstname": "Carl",
                    "lastname": "Mannerheim",
                    "year_of_birth": "1867",
                    "year_of_death": "1951"
                },
                {
                    "id": 5678,
                    "firstname": "Elias",
                    "lastname": "Lönnrot",
                    "year_of_birth": "1802",
                    "year_of_death": "1884"
                }
            ]
        });
        let entries = BespokeScraper122::parse_page(122, &json);
        assert_eq!(entries.len(), 2);

        let e0 = &entries[0].entry;
        assert_eq!(e0.ext_id, "1234");
        assert_eq!(e0.ext_name, "Carl Mannerheim");
        assert_eq!(e0.ext_desc, "1867 1951");
        assert_eq!(
            e0.ext_url,
            "http://www.kansallisbiografia.fi/kb/artikkeli/1234"
        );
        assert_eq!(e0.catalog, 122);
        assert_eq!(e0.type_name, Some("Q5".to_string()));

        let e1 = &entries[1].entry;
        assert_eq!(e1.ext_id, "5678");
        assert_eq!(e1.ext_name, "Elias Lönnrot");
        assert_eq!(e1.ext_desc, "1802 1884");
    }

    #[test]
    fn test_122_parse_page_empty_items() {
        let json = serde_json::json!({
            "pagination": { "pageCount": 1 },
            "items": []
        });
        let entries = BespokeScraper122::parse_page(122, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_122_parse_page_missing_items() {
        let json = serde_json::json!({
            "pagination": { "pageCount": 1 }
        });
        let entries = BespokeScraper122::parse_page(122, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_122_parse_item_missing_names() {
        // Both firstname and lastname missing => empty name => None
        let item = serde_json::json!({
            "id": 999,
            "year_of_birth": "1900",
            "year_of_death": "1980"
        });
        assert!(BespokeScraper122::parse_item(122, &item).is_none());
    }

    #[test]
    fn test_122_parse_item_only_firstname() {
        let item = serde_json::json!({
            "id": 100,
            "firstname": "Väinö",
            "year_of_birth": "1908",
            "year_of_death": "1964"
        });
        let ee = BespokeScraper122::parse_item(122, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Väinö");
        assert_eq!(ee.entry.ext_desc, "1908 1964");
    }

    #[test]
    fn test_122_parse_item_only_lastname() {
        let item = serde_json::json!({
            "id": 101,
            "lastname": "Sibelius",
            "year_of_birth": "1865",
            "year_of_death": "1957"
        });
        let ee = BespokeScraper122::parse_item(122, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Sibelius");
    }

    #[test]
    fn test_122_parse_item_no_dates() {
        let item = serde_json::json!({
            "id": 200,
            "firstname": "Unknown",
            "lastname": "Person"
        });
        let ee = BespokeScraper122::parse_item(122, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Unknown Person");
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_122_parse_item_missing_id() {
        let item = serde_json::json!({
            "firstname": "Test",
            "lastname": "User"
        });
        assert!(BespokeScraper122::parse_item(122, &item).is_none());
    }

    #[test]
    fn test_122_parse_item_string_id() {
        // Some APIs return IDs as strings
        let item = serde_json::json!({
            "id": "42",
            "firstname": "Jean",
            "lastname": "Sibelius"
        });
        let ee = BespokeScraper122::parse_item(122, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "42");
    }

    #[test]
    fn test_122_pagination_page_count() {
        let json = serde_json::json!({
            "pagination": { "pageCount": 10 },
            "items": []
        });
        let page_count = json["pagination"]["pageCount"].as_u64().unwrap_or(0);
        assert_eq!(page_count, 10);
    }

    #[test]
    fn test_122_parse_item_ext_url_format() {
        let item = serde_json::json!({
            "id": 7777,
            "firstname": "Test",
            "lastname": "Person"
        });
        let ee = BespokeScraper122::parse_item(122, &item).unwrap();
        assert!(ee.entry.ext_url.starts_with("http://www.kansallisbiografia.fi/kb/artikkeli/"));
        assert!(ee.entry.ext_url.ends_with("7777"));
    }
}
