use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Litteraturbanken - Swedish Literature Authors (1223)

#[derive(Debug)]
pub struct BespokeScraper1223 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper1223 {

    scraper_boilerplate!(1223);

    async fn run(&self) -> Result<()> {
        let url = "https://litteraturbanken.se/api/get_authors?exclude=intro,db_*,doc_type,corpus,es_id";
        let client = self.http_client();
        let json: serde_json::Value = client.get(url).send().await?.json().await?;
        let data = match json["data"].as_array() {
            Some(data) => data,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for item in data {
            if let Some(ee) = Self::parse_item(self.catalog_id(), item) {
                entry_cache.push(ee);
            self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper1223 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let author_id = item["authorid"].as_str()?;
        if author_id.is_empty() {
            return None;
        }
        let full_name = item["full_name"].as_str().unwrap_or_default();
        if full_name.is_empty() {
            return None;
        }

        let ext_url = format!("https://litteraturbanken.se/forfattare/{}/", author_id);

        let mut desc_parts = vec![];
        if let Some(doc_type) = item["doc_type"].as_str() {
            if !doc_type.is_empty() {
                desc_parts.push(doc_type.to_string());
            }
        }
        if let Some(gender) = item["gender"].as_str() {
            if !gender.is_empty() {
                desc_parts.push(gender.to_string());
            }
        }
        if let Some(birth_date) = item["birth"]["date"].as_str() {
            if !birth_date.is_empty() {
                desc_parts.push(format!("born {}", birth_date));
            }
        }
        if let Some(death_date) = item["death"]["date"].as_str() {
            if !death_date.is_empty() {
                desc_parts.push(format!("died {}", death_date));
            }
        }
        let desc = desc_parts.join("; ");

        let entry = Entry {
            catalog: catalog_id,
            ext_id: author_id.to_string(),
            ext_name: full_name.to_string(),
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

    fn make_scraper() -> BespokeScraper1223 {
        BespokeScraper1223 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_1223_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 1223);
    }

    #[test]
    fn test_1223_parse_item_full() {
        let item = serde_json::json!({
            "authorid": "StrindbergA",
            "full_name": "August Strindberg",
            "doc_type": "author",
            "gender": "male",
            "birth": { "date": "1849-01-22" },
            "death": { "date": "1912-05-14" }
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "StrindbergA");
        assert_eq!(ee.entry.ext_name, "August Strindberg");
        assert_eq!(ee.entry.ext_desc, "author; male; born 1849-01-22; died 1912-05-14");
        assert_eq!(
            ee.entry.ext_url,
            "https://litteraturbanken.se/forfattare/StrindbergA/"
        );
        assert_eq!(ee.entry.catalog, 1223);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_1223_parse_item_minimal() {
        let item = serde_json::json!({
            "authorid": "LagerlofS",
            "full_name": "Selma Lagerlöf"
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "LagerlofS");
        assert_eq!(ee.entry.ext_name, "Selma Lagerlöf");
        assert_eq!(ee.entry.ext_desc, "");
        assert_eq!(
            ee.entry.ext_url,
            "https://litteraturbanken.se/forfattare/LagerlofS/"
        );
    }

    #[test]
    fn test_1223_parse_item_partial_dates() {
        let item = serde_json::json!({
            "authorid": "TestA",
            "full_name": "Test Author",
            "gender": "female",
            "birth": { "date": "1800" }
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "female; born 1800");
    }

    #[test]
    fn test_1223_parse_item_no_authorid() {
        let item = serde_json::json!({
            "full_name": "No Id Author"
        });
        assert!(BespokeScraper1223::parse_item(1223, &item).is_none());
    }

    #[test]
    fn test_1223_parse_item_empty_authorid() {
        let item = serde_json::json!({
            "authorid": "",
            "full_name": "Empty Id"
        });
        assert!(BespokeScraper1223::parse_item(1223, &item).is_none());
    }

    #[test]
    fn test_1223_parse_item_no_full_name() {
        let item = serde_json::json!({
            "authorid": "TestB"
        });
        assert!(BespokeScraper1223::parse_item(1223, &item).is_none());
    }

    #[test]
    fn test_1223_parse_item_empty_full_name() {
        let item = serde_json::json!({
            "authorid": "TestC",
            "full_name": ""
        });
        assert!(BespokeScraper1223::parse_item(1223, &item).is_none());
    }

    #[test]
    fn test_1223_parse_item_death_only() {
        let item = serde_json::json!({
            "authorid": "TestD",
            "full_name": "Died Only",
            "death": { "date": "1950-12-31" }
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "died 1950-12-31");
    }

    #[test]
    fn test_1223_parse_item_birth_missing_date_field() {
        // birth object exists but no date inside
        let item = serde_json::json!({
            "authorid": "TestE",
            "full_name": "No Birth Date",
            "birth": { "place": "Stockholm" }
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_1223_parse_item_all_desc_fields() {
        let item = serde_json::json!({
            "authorid": "AllFields",
            "full_name": "All Fields",
            "doc_type": "editor",
            "gender": "unknown",
            "birth": { "date": "1700" },
            "death": { "date": "1780" }
        });
        let ee = BespokeScraper1223::parse_item(1223, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "editor; unknown; born 1700; died 1780");
    }

    #[test]
    fn test_1223_data_array_parsing() {
        let json = serde_json::json!({
            "data": [
                { "authorid": "A1", "full_name": "Author One" },
                { "authorid": "A2", "full_name": "Author Two" }
            ]
        });
        let data = json["data"].as_array().unwrap();
        let entries: Vec<ExtendedEntry> = data
            .iter()
            .filter_map(|item| BespokeScraper1223::parse_item(1223, item))
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "A1");
        assert_eq!(entries[1].entry.ext_id, "A2");
    }
}
