use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry, person_date::PersonDate};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// Porträttarkiv – Swedish portrait archive (catalog 4837).
// Data is fetched via a POST endpoint that returns paginated Elasticsearch results.

const BATCH_SIZE: u64 = 1000;

#[derive(Debug)]
pub struct BespokeScraper4837 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4837 {
    scraper_boilerplate!(4837);

    async fn run(&self) -> Result<()> {
        let url = "https://portrattarkiv.se/endpoints/latest.php";
        let client = self.http_client();
        let mut from: u64 = 0;
        let mut total: u64 = u64::MAX;

        while from < total {
            let body = format!(r#"{{"size":{BATCH_SIZE},"from":{from}}}"#);
            let json: serde_json::Value = client
                .post(url)
                .header("Content-Type", "text/plain")
                .header("Origin", "https://portrattarkiv.se")
                .header("Referer", "https://portrattarkiv.se/latest")
                .body(body)
                .send()
                .await?
                .json()
                .await?;

            total = json["hits"]["total"].as_u64().unwrap_or(0);
            let hits = match json["hits"]["hits"].as_array() {
                Some(h) if !h.is_empty() => h,
                _ => break,
            };

            let mut cache = vec![];
            for hit in hits {
                if let Some(ee) = Self::parse_hit(self.catalog_id(), hit) {
                    cache.push(ee);
                }
            }
            self.process_cache(&mut cache).await?;
            from += BATCH_SIZE;
        }
        Ok(())
    }
}

impl BespokeScraper4837 {
    /// Convert one Elasticsearch hit into an `ExtendedEntry`, or return `None` if it's unusable.
    pub(crate) fn parse_hit(catalog_id: usize, hit: &serde_json::Value) -> Option<ExtendedEntry> {
        let id = hit["_id"].as_str()?;
        let src = &hit["_source"];
        let first = src["FirstName"].as_str().unwrap_or_default();
        let last = src["LastName"].as_str().unwrap_or_default();
        let name = format!("{} {}", first, last).trim().to_string();
        if name.is_empty() {
            return None;
        }

        let born = Self::extract_date(src, "BirthDate", "BirthYear");
        let died = Self::extract_date(src, "DeathDate", "DeathYear");

        let desc = src["Facts"]["PortraitBioText"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let is_person = hit["_type"].as_str().map(|t| t == "Person").unwrap_or(false);

        Some(ExtendedEntry {
            entry: Entry {
                catalog: catalog_id,
                ext_id: id.to_string(),
                ext_url: format!("https://portrattarkiv.se/details/{}", id),
                ext_name: name,
                ext_desc: desc,
                type_name: is_person.then(|| "Q5".to_string()),
                random: rand::rng().random(),
                ..Default::default()
            },
            born,
            died,
            ..Default::default()
        })
    }

    /// Prefer the full date string (`BirthDate` / `DeathDate`); fall back to year-only.
    fn extract_date(src: &serde_json::Value, date_key: &str, year_key: &str) -> Option<PersonDate> {
        if let Some(s) = src[date_key].as_str().filter(|s| !s.is_empty()) {
            return PersonDate::from_db_string(s);
        }
        if let Some(y) = src[year_key].as_u64() {
            let year_str = y.to_string();
            return PersonDate::from_db_string(&year_str);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    fn make_scraper() -> BespokeScraper4837 {
        BespokeScraper4837 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_4837_catalog_id() {
        assert_eq!(make_scraper().catalog_id(), 4837);
    }

    #[test]
    fn test_4837_parse_hit_full() {
        let hit = serde_json::json!({
            "_id": "abc123",
            "_type": "Person",
            "_source": {
                "FirstName": "Karl",
                "LastName": "Månsson",
                "BirthDate": "1898-11-01",
                "BirthYear": 1898,
                "DeathDate": "1965-03-12",
                "DeathYear": 1965
            }
        });
        let ee = BespokeScraper4837::parse_hit(4837, &hit).unwrap();
        assert_eq!(ee.entry.ext_id, "abc123");
        assert_eq!(ee.entry.ext_name, "Karl Månsson");
        assert_eq!(ee.entry.ext_url, "https://portrattarkiv.se/details/abc123");
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert!(ee.born.is_some());
        assert!(ee.died.is_some());
    }

    #[test]
    fn test_4837_parse_hit_year_only_fallback() {
        let hit = serde_json::json!({
            "_id": "def456",
            "_type": "Person",
            "_source": {
                "FirstName": "Anna",
                "LastName": "Svensson",
                "BirthYear": 1900
            }
        });
        let ee = BespokeScraper4837::parse_hit(4837, &hit).unwrap();
        assert_eq!(ee.entry.ext_name, "Anna Svensson");
        assert!(ee.born.is_some());
        assert!(ee.died.is_none());
    }

    #[test]
    fn test_4837_parse_hit_non_person_type() {
        let hit = serde_json::json!({
            "_id": "ghi789",
            "_type": "Organisation",
            "_source": {
                "FirstName": "Some",
                "LastName": "Org"
            }
        });
        let ee = BespokeScraper4837::parse_hit(4837, &hit).unwrap();
        assert_eq!(ee.entry.type_name, None);
    }

    #[test]
    fn test_4837_parse_hit_missing_name_returns_none() {
        let hit = serde_json::json!({
            "_id": "jkl000",
            "_type": "Person",
            "_source": {}
        });
        let result = BespokeScraper4837::parse_hit(4837, &hit);
        assert!(result.is_none());
    }

    #[test]
    fn test_4837_parse_hit_missing_id_returns_none() {
        let hit = serde_json::json!({
            "_type": "Person",
            "_source": { "FirstName": "No", "LastName": "Id" }
        });
        let result = BespokeScraper4837::parse_hit(4837, &hit);
        assert!(result.is_none());
    }

    #[test]
    fn test_4837_parse_hit_bio_text_desc() {
        let hit = serde_json::json!({
            "_id": "bio001",
            "_type": "Person",
            "_source": {
                "FirstName": "Test",
                "LastName": "Person",
                "Facts": {
                    "PortraitBioText": ["A painter from Stockholm."]
                }
            }
        });
        let ee = BespokeScraper4837::parse_hit(4837, &hit).unwrap();
        assert_eq!(ee.entry.ext_desc, "A painter from Stockholm.");
    }
}
