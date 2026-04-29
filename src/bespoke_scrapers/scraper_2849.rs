use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
    person_date::PersonDate,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashSet;

use super::BespokeScraper;

// ______________________________________________________
// Nordic Women's Literature (2849)
//
// Single bulk JSON dump of writers. Every entry is by definition female
// and a writer, so we attach P21=Q6581072 (female) and P106=Q36180
// (writer) to every row at scrape time. The PHP version achieved this
// with two post-hoc INSERT IGNOREs against the auxiliary table; doing it
// per-entry here keeps the scraper self-contained and lets the standard
// `process_cache` path persist aux alongside the entry.

#[derive(Debug)]
pub struct BespokeScraper2849 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper2849 {
    scraper_boilerplate!(2849);

    async fn run(&self) -> Result<()> {
        let url = "https://nordicwomensliterature.net/wp-json/nwl/v1/writers/en";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let writers = match json.as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for writer in writers {
            if let Some(ee) = Self::parse_item(self.catalog_id(), writer) {
                entry_cache.push(ee);
                self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper2849 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        writer: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let profile_url = writer["profile_url"].as_str()?;
        let ext_id = Self::extract_id_from_url(profile_url)?;
        let raw_name = writer["name"].as_str().unwrap_or_default();
        if raw_name.is_empty() {
            return None;
        }
        let ext_name = Self::flip_lastname_first(raw_name);
        let ext_desc = writer["country"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let entry = Entry {
            catalog: catalog_id,
            ext_id,
            ext_name,
            ext_desc,
            ext_url: profile_url.to_string(),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        // P21 = sex or gender; Q6581072 = female. Catalog is female-only by definition.
        aux.insert(AuxiliaryRow::new(21, "Q6581072".to_string()));
        // P106 = occupation; Q36180 = writer. Catalog is writers-only by definition.
        aux.insert(AuxiliaryRow::new(106, "Q36180".to_string()));

        let born = writer["born"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(PersonDate::from_db_string);
        let died = writer["dead"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(PersonDate::from_db_string);

        Some(ExtendedEntry {
            entry,
            aux,
            born,
            died,
            ..Default::default()
        })
    }

    /// Extract the trailing path segment from a profile URL, e.g.
    /// `https://.../forfattare/karin-boye/` → `karin-boye`. Mirrors PHP
    /// preg_match `|/([^/]+)/$|`.
    pub(crate) fn extract_id_from_url(url: &str) -> Option<String> {
        lazy_static! {
            static ref RE_TRAILING_SLUG: Regex = Regex::new(r"/([^/]+)/$").expect("regex");
        }
        RE_TRAILING_SLUG
            .captures(url)?
            .get(1)
            .map(|m| m.as_str().to_string())
    }

    /// Convert "Lastname, Firstname" → "Firstname Lastname". Names without
    /// a comma are returned unchanged.
    pub(crate) fn flip_lastname_first(name: &str) -> String {
        lazy_static! {
            static ref RE_LASTNAME_FIRST: Regex =
                Regex::new(r"^(.+?), (.+)$").expect("regex");
        }
        match RE_LASTNAME_FIRST.captures(name) {
            Some(caps) => format!("{} {}", &caps[2], &caps[1]),
            None => name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2849_extract_id_from_url() {
        assert_eq!(
            BespokeScraper2849::extract_id_from_url(
                "https://nordicwomensliterature.net/writer/karin-boye/"
            ),
            Some("karin-boye".to_string())
        );
    }

    #[test]
    fn test_2849_extract_id_no_trailing_slash_fails() {
        assert_eq!(
            BespokeScraper2849::extract_id_from_url("https://example.com/writer/karin-boye"),
            None
        );
    }

    #[test]
    fn test_2849_flip_lastname_first() {
        assert_eq!(
            BespokeScraper2849::flip_lastname_first("Boye, Karin"),
            "Karin Boye"
        );
    }

    #[test]
    fn test_2849_flip_lastname_first_no_comma_unchanged() {
        assert_eq!(
            BespokeScraper2849::flip_lastname_first("Karin Boye"),
            "Karin Boye"
        );
    }

    #[test]
    fn test_2849_parse_item_full() {
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/karin-boye/",
            "name": "Boye, Karin",
            "country": "Sweden",
            "born": "1900-10-26",
            "dead": "1941-04-24"
        });
        let ee = BespokeScraper2849::parse_item(2849, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "karin-boye");
        assert_eq!(ee.entry.ext_name, "Karin Boye");
        assert_eq!(ee.entry.ext_desc, "Sweden");
        assert_eq!(
            ee.entry.ext_url,
            "https://nordicwomensliterature.net/writer/karin-boye/"
        );
        assert_eq!(ee.entry.catalog, 2849);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert_eq!(ee.born, Some(PersonDate::year_month_day(1900, 10, 26)));
        assert_eq!(ee.died, Some(PersonDate::year_month_day(1941, 4, 24)));
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581072".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q36180".to_string())));
        assert_eq!(ee.aux.len(), 2);
    }

    #[test]
    fn test_2849_parse_item_no_country_gives_empty_desc() {
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/no-country/",
            "name": "Doe, Jane",
            "born": "1850",
            "dead": "1900"
        });
        let ee = BespokeScraper2849::parse_item(2849, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_2849_parse_item_missing_dates_are_none() {
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/no-dates/",
            "name": "Doe, Jane",
            "country": "Norway"
        });
        let ee = BespokeScraper2849::parse_item(2849, &item).unwrap();
        assert!(ee.born.is_none());
        assert!(ee.died.is_none());
    }

    #[test]
    fn test_2849_parse_item_empty_dates_are_none() {
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/empty-dates/",
            "name": "Doe, Jane",
            "born": "",
            "dead": ""
        });
        let ee = BespokeScraper2849::parse_item(2849, &item).unwrap();
        assert!(ee.born.is_none());
        assert!(ee.died.is_none());
    }

    #[test]
    fn test_2849_parse_item_missing_profile_url_skipped() {
        let item = serde_json::json!({"name": "No URL"});
        assert!(BespokeScraper2849::parse_item(2849, &item).is_none());
    }

    #[test]
    fn test_2849_parse_item_missing_name_skipped() {
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/anonymous/"
        });
        assert!(BespokeScraper2849::parse_item(2849, &item).is_none());
    }

    #[test]
    fn test_2849_parse_item_aux_attached_to_dateless_entry() {
        // Even when person dates are missing, the female + writer aux must
        // still be attached — that's the whole point of the post-hoc SQL
        // INSERT IGNOREs in the original PHP.
        let item = serde_json::json!({
            "profile_url": "https://nordicwomensliterature.net/writer/dateless/",
            "name": "Doe, Jane"
        });
        let ee = BespokeScraper2849::parse_item(2849, &item).unwrap();
        assert_eq!(ee.aux.len(), 2);
    }
}
