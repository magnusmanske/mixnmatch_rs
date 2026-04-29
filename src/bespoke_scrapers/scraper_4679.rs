use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashSet;

use super::BespokeScraper;

// ______________________________________________________
// DC Comics characters (4679)
//
// 12-page paginated proxy-search endpoint. Every row is a DC fictional
// character, so we attach P31=Q1114461 (comics character) and
// P1080=Q1152150 (DC Universe) to every entry. The catalog's MnM
// type is Q15632617 (fictional human). The character id is the trailing
// path segment of the API's `search_api_url` field; description is the
// stripped `body:value` HTML.

const PAGE_COUNT: u32 = 12;

#[derive(Debug)]
pub struct BespokeScraper4679 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4679 {
    scraper_boilerplate!(4679);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for page in 1..=PAGE_COUNT {
            let url = format!(
                "https://www.dccomics.com/proxy/search?type=generic_character&page={page}&sortBy=title-ASC"
            );
            let response = match client.get(&url).send().await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(_) => continue,
            };
            let arr = match json["results"].as_array() {
                Some(arr) => arr,
                None => continue,
            };
            for c in arr {
                if let Some(ee) = Self::parse_item(self.catalog_id(), c) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4679 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        c: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let fields = c.get("fields")?;
        let search_api_url = fields.get("search_api_url")?.as_str()?;
        let id = Self::trailing_path_segment(search_api_url);
        if id.is_empty() {
            return None;
        }
        let ext_name = fields
            .get("dc_solr_sortable_title")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        if ext_name.is_empty() {
            return None;
        }
        let body_value = fields
            .get("body:value")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let ext_desc = Self::strip_tags(body_value);

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        // P31 = instance of; Q1114461 = comics character.
        aux.insert(AuxiliaryRow::new(31, "Q1114461".to_string()));
        // P1080 = from narrative universe; Q1152150 = DC Universe.
        aux.insert(AuxiliaryRow::new(1080, "Q1152150".to_string()));

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id,
            ext_name,
            ext_desc,
            ext_url: format!("https://www.dccomics.com{search_api_url}"),
            random: rand::rng().random(),
            // Q15632617 = fictional human
            type_name: Some("Q15632617".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            ..Default::default()
        })
    }

    /// Mirror PHP `preg_replace('|^.*/|', '', $url)`: drop everything up
    /// to and including the last `/`. Returns the input unchanged if
    /// there's no slash.
    pub(crate) fn trailing_path_segment(url: &str) -> String {
        match url.rfind('/') {
            Some(idx) => url[idx + 1..].to_string(),
            None => url.to_string(),
        }
    }

    /// Crude HTML-tag stripper to mirror PHP `strip_tags`. Removes
    /// anything between `<` and the next `>`. Doesn't handle malformed
    /// HTML perfectly, but matches the (likewise crude) PHP behaviour
    /// for the common cases in this catalog.
    pub(crate) fn strip_tags(s: &str) -> String {
        lazy_static! {
            static ref RE_TAGS: Regex = Regex::new(r"<[^>]*>").expect("regex");
        }
        RE_TAGS.replace_all(s, "").trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4679_trailing_path_segment() {
        assert_eq!(
            BespokeScraper4679::trailing_path_segment("/characters/superman"),
            "superman"
        );
        assert_eq!(
            BespokeScraper4679::trailing_path_segment("plain"),
            "plain"
        );
    }

    #[test]
    fn test_4679_strip_tags_basic() {
        assert_eq!(
            BespokeScraper4679::strip_tags("<p>Hello <b>world</b></p>"),
            "Hello world"
        );
    }

    #[test]
    fn test_4679_strip_tags_no_tags() {
        assert_eq!(BespokeScraper4679::strip_tags("plain text"), "plain text");
    }

    #[test]
    fn test_4679_parse_item_full() {
        let c = serde_json::json!({
            "fields": {
                "search_api_url": "/characters/superman",
                "dc_solr_sortable_title": "Superman",
                "body:value": ["<p>Last Son of <em>Krypton</em>.</p>"]
            }
        });
        let ee = BespokeScraper4679::parse_item(4679, &c).unwrap();
        assert_eq!(ee.entry.ext_id, "superman");
        assert_eq!(ee.entry.ext_name, "Superman");
        assert_eq!(ee.entry.ext_desc, "Last Son of Krypton.");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.dccomics.com/characters/superman"
        );
        assert_eq!(ee.entry.type_name, Some("Q15632617".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q1114461".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(1080, "Q1152150".to_string())));
        assert_eq!(ee.aux.len(), 2);
    }

    #[test]
    fn test_4679_parse_item_no_body_yields_empty_desc() {
        let c = serde_json::json!({
            "fields": {
                "search_api_url": "/characters/x",
                "dc_solr_sortable_title": "X"
            }
        });
        let ee = BespokeScraper4679::parse_item(4679, &c).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_4679_parse_item_missing_search_api_url_skipped() {
        let c = serde_json::json!({
            "fields": {"dc_solr_sortable_title": "X"}
        });
        assert!(BespokeScraper4679::parse_item(4679, &c).is_none());
    }

    #[test]
    fn test_4679_parse_item_missing_title_skipped() {
        let c = serde_json::json!({
            "fields": {"search_api_url": "/characters/x"}
        });
        assert!(BespokeScraper4679::parse_item(4679, &c).is_none());
    }
}
