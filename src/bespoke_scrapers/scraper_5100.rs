use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// MTMT - Hungarian Scientific Authors (5100)

#[derive(Debug)]
pub struct BespokeScraper5100 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5100 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        5100
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for first in b'A'..=b'Z' {
            for second in b'a'..=b'z' {
                let prefix = format!("{}{}", first as char, second as char);
                let url = format!(
                    "https://m2.mtmt.hu/api/author?size=1000&depth=0&cond=label;prefix;{}&sort=familyName,asc&sort=givenName,asc&labelLang=eng&format=json",
                    prefix
                );
                let resp = client.get(&url).send().await?;
                let json: serde_json::Value = match resp.json().await {
                    Ok(j) => j,
                    Err(_) => continue,
                };
                let content = match json["content"].as_array() {
                    Some(c) => c,
                    None => continue,
                };
                for item in content {
                    if let Some(ee) = Self::parse_item(self.catalog_id(), item) {
                        entry_cache.push(ee);
                    }
                }
                if entry_cache.len() >= 100 {
                    self.process_cache(&mut entry_cache).await?;
                    entry_cache.clear();
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper5100 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let mtid = item["mtid"].as_i64().or_else(|| {
            item["mtid"].as_str().and_then(|s| s.parse::<i64>().ok())
        })?;
        let mtid_str = mtid.to_string();

        let given_name = item["givenName"].as_str().unwrap_or_default();
        let family_name = item["familyName"].as_str().unwrap_or_default();
        let name = Self::build_name(given_name, family_name);
        if name.is_empty() {
            return None;
        }

        let label = item["label"].as_str().unwrap_or_default();
        let ext_url = format!(
            "https://m2.mtmt.hu/gui2/?type=authors&mode=browse&sel={}",
            mtid_str
        );

        let entry = Entry {
            catalog: catalog_id,
            ext_id: mtid_str,
            ext_name: name,
            ext_desc: label.to_string(),
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

    /// Build a name from given name and family name.
    /// If family_name is ".", use only given_name.
    /// If given_name is ".", use only family_name.
    /// Otherwise, format as "givenName familyName".
    pub(crate) fn build_name(given_name: &str, family_name: &str) -> String {
        if family_name == "." {
            return given_name.to_string();
        }
        if given_name == "." {
            return family_name.to_string();
        }
        format!("{} {}", given_name, family_name).trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper5100 {
        BespokeScraper5100 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_5100_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 5100);
    }

    #[test]
    fn test_5100_build_name_normal() {
        assert_eq!(
            BespokeScraper5100::build_name("János", "Nagy"),
            "János Nagy"
        );
    }

    #[test]
    fn test_5100_build_name_dot_family() {
        // Family name is "." => use only given name
        assert_eq!(BespokeScraper5100::build_name("Cher", "."), "Cher");
    }

    #[test]
    fn test_5100_build_name_dot_given() {
        // Given name is "." => use only family name
        assert_eq!(BespokeScraper5100::build_name(".", "Institute"), "Institute");
    }

    #[test]
    fn test_5100_build_name_both_empty() {
        assert_eq!(BespokeScraper5100::build_name("", ""), "");
    }

    #[test]
    fn test_5100_build_name_only_given() {
        assert_eq!(BespokeScraper5100::build_name("Solo", ""), "Solo");
    }

    #[test]
    fn test_5100_build_name_only_family() {
        assert_eq!(BespokeScraper5100::build_name("", "OnlyFamily"), "OnlyFamily");
    }

    #[test]
    fn test_5100_parse_item_full() {
        let item = serde_json::json!({
            "mtid": 10012345,
            "givenName": "István",
            "familyName": "Szabó",
            "label": "Szabó, István (physicist)"
        });
        let ee = BespokeScraper5100::parse_item(5100, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "10012345");
        assert_eq!(ee.entry.ext_name, "István Szabó");
        assert_eq!(ee.entry.ext_desc, "Szabó, István (physicist)");
        assert_eq!(
            ee.entry.ext_url,
            "https://m2.mtmt.hu/gui2/?type=authors&mode=browse&sel=10012345"
        );
        assert_eq!(ee.entry.catalog, 5100);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_5100_parse_item_dot_family_name() {
        let item = serde_json::json!({
            "mtid": 555,
            "givenName": "Madonna",
            "familyName": ".",
            "label": "Madonna"
        });
        let ee = BespokeScraper5100::parse_item(5100, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Madonna");
    }

    #[test]
    fn test_5100_parse_item_dot_given_name() {
        let item = serde_json::json!({
            "mtid": 777,
            "givenName": ".",
            "familyName": "Research Group",
            "label": "Research Group"
        });
        let ee = BespokeScraper5100::parse_item(5100, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Research Group");
    }

    #[test]
    fn test_5100_parse_item_missing_mtid() {
        let item = serde_json::json!({
            "givenName": "Test",
            "familyName": "User",
            "label": "Test User"
        });
        assert!(BespokeScraper5100::parse_item(5100, &item).is_none());
    }

    #[test]
    fn test_5100_parse_item_empty_names() {
        let item = serde_json::json!({
            "mtid": 999,
            "label": "something"
        });
        assert!(BespokeScraper5100::parse_item(5100, &item).is_none());
    }

    #[test]
    fn test_5100_parse_item_string_mtid() {
        let item = serde_json::json!({
            "mtid": "12345",
            "givenName": "Test",
            "familyName": "Author",
            "label": "Author, Test"
        });
        let ee = BespokeScraper5100::parse_item(5100, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "12345");
    }

    #[test]
    fn test_5100_parse_item_no_label() {
        let item = serde_json::json!({
            "mtid": 42,
            "givenName": "A",
            "familyName": "B"
        });
        let ee = BespokeScraper5100::parse_item(5100, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_5100_content_array_parsing() {
        let json = serde_json::json!({
            "content": [
                { "mtid": 1, "givenName": "A", "familyName": "B", "label": "B, A" },
                { "mtid": 2, "givenName": "C", "familyName": "D", "label": "D, C" }
            ]
        });
        let content = json["content"].as_array().unwrap();
        let entries: Vec<ExtendedEntry> = content
            .iter()
            .filter_map(|item| BespokeScraper5100::parse_item(5100, item))
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "1");
        assert_eq!(entries[1].entry.ext_id, "2");
    }

    #[test]
    fn test_5100_url_prefix_generation() {
        // Verify the double-letter prefix iteration produces expected patterns
        let first_a = b'A';
        let second_a = b'a';
        let prefix_aa = format!("{}{}", first_a as char, second_a as char);
        assert_eq!(prefix_aa, "Aa");

        let first_z = b'Z';
        let second_z = b'z';
        let prefix_zz = format!("{}{}", first_z as char, second_z as char);
        assert_eq!(prefix_zz, "Zz");
    }
}
