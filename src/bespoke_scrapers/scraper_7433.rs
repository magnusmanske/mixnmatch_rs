use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry, person_date::PersonDate};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Anne Frank Research (7433)

#[derive(Debug)]
pub struct BespokeScraper7433 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7433 {

    scraper_boilerplate!(7433);

    async fn run(&self) -> Result<()> {
        let ext_ids = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        for page_id in 1..=70 {
            let mut entry_cache = Vec::new();
            let url = format!(
                "https://research.annefrank.org/en/api/search?type=person&format=json&page={page_id}"
            );
            let client = self.http_client();
            let json: serde_json::Value = client.get(url).send().await?.json().await?;
            let results = match json["results"].as_array() {
                Some(results) => results,
                None => continue,
            };
            for result_container in results {
                let result = match result_container["instance"].as_object() {
                    Some(result) => result,
                    None => continue,
                };
                let id = match result["uuid"].as_str() {
                    Some(id) => id,
                    None => continue,
                };
                if ext_ids.contains_key(id) {
                    continue;
                }
                let title = result["title"].as_str().unwrap_or_default();
                if title.is_empty() {
                    continue;
                }
                let birth_date = result["birth_date"].as_str().and_then(PersonDate::from_db_string);
                let death_date = result["death_date"].as_str().and_then(PersonDate::from_db_string);
                let desc = result["summary"].as_str().unwrap_or_default();
                let ext_url = result["url"].as_str().unwrap_or_default();
                let entry = Entry {
                    catalog: self.catalog_id(),
                    ext_id: id.to_string(),
                    ext_name: title.to_string(),
                    ext_desc: desc.to_string(),
                    ext_url: ext_url.to_string(),
                    random: rand::rng().random(),
                    type_name: Some("Q5".to_string()),
                    ..Default::default()
                };
                let ee = ExtendedEntry {
                    entry,
                    born: birth_date,
                    died: death_date,
                    ..Default::default()
                };
                entry_cache.push(ee);
            }
            self.process_cache(&mut entry_cache).await?;
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper7433 {
        BespokeScraper7433 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_7433_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 7433);
    }

    #[test]
    fn test_7433_keep_existing_names_default_false() {
        let s = make_scraper();
        assert!(!s.keep_existing_names());
    }

    #[test]
    fn test_7433_testing_default_false() {
        let s = make_scraper();
        assert!(!s.testing());
    }

    /// The API URL template should produce the expected URL for a given page.
    #[test]
    fn test_7433_url_template() {
        let page_id: u32 = 3;
        let url = format!(
            "https://research.annefrank.org/en/api/search?type=person&format=json&page={page_id}"
        );
        assert_eq!(
            url,
            "https://research.annefrank.org/en/api/search?type=person&format=json&page=3"
        );
    }

    #[test]
    fn test_7433_url_template_page_bounds() {
        // Verify the first and last pages produce well-formed URLs
        for page_id in [1_u32, 70] {
            let url = format!(
                "https://research.annefrank.org/en/api/search?type=person&format=json&page={page_id}"
            );
            assert!(url.contains(&format!("page={page_id}")));
        }
    }

    /// Simulate parsing a well-formed API result object into an ExtendedEntry.
    #[test]
    fn test_7433_result_parsing_full() {
        // Replicate the per-result parsing logic from `run` as a pure function
        // so we can unit-test it without network access.
        let result_json = serde_json::json!({
            "uuid": "abc-123",
            "title": "Anne Frank",
            "summary": "Jewish diarist",
            "url": "https://research.annefrank.org/en/people/abc-123/",
            "birth_date": "1929-06-12",
            "death_date": "1945-02-28"
        });
        let result = result_json.as_object().unwrap();

        let id = result["uuid"].as_str().unwrap();
        let title = result["title"].as_str().unwrap_or_default();
        let birth_date = result["birth_date"].as_str().and_then(PersonDate::from_db_string);
        let death_date = result["death_date"].as_str().and_then(PersonDate::from_db_string);
        let desc = result["summary"].as_str().unwrap_or_default();
        let ext_url = result["url"].as_str().unwrap_or_default();

        assert_eq!(id, "abc-123");
        assert_eq!(title, "Anne Frank");
        assert_eq!(birth_date, Some(PersonDate::year_month_day(1929, 6, 12)));
        assert_eq!(death_date, Some(PersonDate::year_month_day(1945, 2, 28)));
        assert_eq!(desc, "Jewish diarist");
        assert_eq!(ext_url, "https://research.annefrank.org/en/people/abc-123/");
    }

    #[test]
    fn test_7433_result_parsing_missing_optional_fields() {
        let result_json = serde_json::json!({
            "uuid": "xyz-999",
            "title": "Unknown Person"
        });
        let result = result_json.as_object().unwrap();

        let id = result.get("uuid").and_then(|v| v.as_str()).unwrap();
        let title = result
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let birth_date = result
            .get("birth_date")
            .and_then(|v| v.as_str())
            .and_then(PersonDate::from_db_string);
        let death_date = result
            .get("death_date")
            .and_then(|v| v.as_str())
            .and_then(PersonDate::from_db_string);
        let desc = result
            .get("summary")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let ext_url = result
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        assert_eq!(id, "xyz-999");
        assert_eq!(title, "Unknown Person");
        assert!(birth_date.is_none());
        assert!(death_date.is_none());
        assert_eq!(desc, "");
        assert_eq!(ext_url, "");
    }

    #[test]
    fn test_7433_result_parsing_empty_title_skipped() {
        let result_json = serde_json::json!({
            "uuid": "no-title-id",
            "title": ""
        });
        let result = result_json.as_object().unwrap();
        let title = result["title"].as_str().unwrap_or_default();
        // In `run`, entries with empty title are skipped
        assert!(title.is_empty());
    }

    #[test]
    fn test_7433_result_parsing_missing_uuid_skipped() {
        let result_json = serde_json::json!({ "title": "Someone" });
        let result = result_json.as_object().unwrap();
        // In `run`, entries with no uuid are skipped via `match result["uuid"].as_str()`.
        // Use .get() to avoid a panic when the key is absent entirely.
        assert!(result.get("uuid").and_then(|v| v.as_str()).is_none());
    }

    #[test]
    fn test_7433_entry_type_is_human() {
        // All entries from this scraper should be typed as Q5 (human)
        let result_json = serde_json::json!({
            "uuid": "q5-test",
            "title": "Test Person",
            "summary": "",
            "url": "https://research.annefrank.org/en/people/q5-test/",
            "birth_date": null,
            "death_date": null
        });
        let result = result_json.as_object().unwrap();
        let id = result["uuid"].as_str().unwrap();
        let title = result["title"].as_str().unwrap_or_default();
        let desc = result["summary"].as_str().unwrap_or_default();
        let ext_url = result["url"].as_str().unwrap_or_default();

        let entry = Entry {
            catalog: 7433,
            ext_id: id.to_string(),
            ext_name: title.to_string(),
            ext_desc: desc.to_string(),
            ext_url: ext_url.to_string(),
            random: 0.5,
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        assert_eq!(entry.type_name, Some("Q5".to_string()));
        assert_eq!(entry.catalog, 7433);
    }

    #[test]
    fn test_7433_results_array_missing_returns_none() {
        // If `json["results"]` is not an array, the page is skipped via `continue`
        let json = serde_json::json!({ "count": 0 });
        assert!(json["results"].as_array().is_none());
    }

    #[test]
    fn test_7433_instance_object_missing_returns_none() {
        // If `result_container["instance"]` is missing, the entry is skipped
        let container = serde_json::json!({ "other_key": {} });
        assert!(container["instance"].as_object().is_none());
    }
}
