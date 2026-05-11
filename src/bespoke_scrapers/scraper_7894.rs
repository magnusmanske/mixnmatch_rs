use crate::{app_state::AppContext, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use rand::RngExt;
use std::sync::Arc;

use super::BespokeScraper;

// ______________________________________________________
// PASE — Prosopography of Anglo-Saxon England (catalog 7894, Wikidata P2625)
//
// Site is a JS-rendered SPA backed by a public JSON API:
//   GET /pase/list/person/?page=N   → { pagination: {totalPages, …}, objects: [{id,name,floruit,details}] }
//   GET /pase/detail/person/?objectid=N  (per-person, ~1MB — not needed for the index sweep)
// The list endpoint already carries everything we need for an MnM entry
// (id, display name, short description), so a full sweep is ~200 page
// requests rather than ~20k detail requests.
//
// PASE has no precise birth/death dates — only "floruit" notation
// (e.g. "m x" = mid-10th century). The summary `details` field carries
// the floruit plus a one-line role, which we use as ext_desc.

const BASE: &str = "http://pase.ac.uk/pase";

#[derive(Debug)]
pub struct BespokeScraper7894 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7894 {
    scraper_boilerplate!(7894);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache: Vec<ExtendedEntry> = vec![];
        let mut page: u32 = 1;
        let mut total_pages: u32 = 1;
        while page <= total_pages {
            let url = format!("{BASE}/list/person/?page={page}");
            let json: serde_json::Value = client.get(&url).send().await?.json().await?;
            if page == 1 {
                total_pages = json["pagination"]["totalPages"]
                    .as_u64()
                    .ok_or_else(|| anyhow!("PASE list missing pagination.totalPages"))?
                    as u32;
            }
            let objects = json["objects"]
                .as_array()
                .ok_or_else(|| anyhow!("PASE list missing objects[]"))?;
            for item in objects {
                if let Some(ee) = Self::parse_entry(self.catalog_id(), item) {
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

impl BespokeScraper7894 {
    pub(crate) fn parse_entry(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = item.get("id").and_then(|v| v.as_u64())?;
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())?
            .to_string();
        let details = item
            .get("details")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("")
            .to_string();
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.to_string(),
            ext_name: name,
            ext_desc: details,
            ext_url: format!("{BASE}/?list=person&detail=person&detailid={id}"),
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

    fn sample_object() -> serde_json::Value {
        serde_json::json!({
            "id": 33165,
            "name": "Abarhilda 1",
            "floruit": "l viii - m viii",
            "details": "(l viii - m viii) Widow in Frisia"
        })
    }

    #[test]
    fn test_7894_parse_entry_basic_fields() {
        let ee = BespokeScraper7894::parse_entry(7894, &sample_object()).unwrap();
        assert_eq!(ee.entry.catalog, 7894);
        assert_eq!(ee.entry.ext_id, "33165");
        assert_eq!(ee.entry.ext_name, "Abarhilda 1");
        assert_eq!(ee.entry.ext_desc, "(l viii - m viii) Widow in Frisia");
        assert_eq!(ee.entry.type_name.as_deref(), Some("Q5"));
    }

    #[test]
    fn test_7894_parse_entry_ext_url_uses_detail_view() {
        let ee = BespokeScraper7894::parse_entry(7894, &sample_object()).unwrap();
        assert_eq!(
            ee.entry.ext_url,
            "https://pase.ac.uk/pase/?list=person&detail=person&detailid=33165"
        );
    }

    #[test]
    fn test_7894_parse_entry_handles_missing_floruit() {
        // Some PASE records have floruit=null but still have details. The scraper
        // doesn't read floruit (details already carries the visible date hint),
        // so this should round-trip cleanly.
        let item = serde_json::json!({
            "id": 168343,
            "name": "Abba 1",
            "floruit": null,
            "details": "Name inscribed in the Catacomb of Commodilla (via Ostiense, Rome)"
        });
        let ee = BespokeScraper7894::parse_entry(7894, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "168343");
        assert!(!ee.entry.ext_desc.contains("null"));
    }

    #[test]
    fn test_7894_parse_entry_missing_id_returns_none() {
        let item = serde_json::json!({"name": "X", "details": ""});
        assert!(BespokeScraper7894::parse_entry(7894, &item).is_none());
    }

    #[test]
    fn test_7894_parse_entry_missing_name_returns_none() {
        // PASE always supplies a name, but defensively skip if absent/blank.
        let item = serde_json::json!({"id": 1, "name": "  "});
        assert!(BespokeScraper7894::parse_entry(7894, &item).is_none());
    }

    #[test]
    fn test_7894_parse_entry_trims_name() {
        let item = serde_json::json!({"id": 7, "name": "  Edmund 1  ", "details": "x"});
        let ee = BespokeScraper7894::parse_entry(7894, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Edmund 1");
    }

    #[test]
    fn test_7894_parse_entry_missing_details_yields_empty_desc() {
        let item = serde_json::json!({"id": 9, "name": "Y"});
        let ee = BespokeScraper7894::parse_entry(7894, &item).unwrap();
        assert_eq!(ee.entry.ext_desc, "");
    }
}
