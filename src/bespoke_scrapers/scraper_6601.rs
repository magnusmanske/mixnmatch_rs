use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashMap;

use super::{
    BespokeScraper,
    scraper_6600::{artist_id_from_url, pinakothek_search_url},
};

// ______________________________________________________
// Pinakothek artworks (6601)
//
// Companion to scraper_6600. Walks the same 100-year-bucket search API
// (URL builder + artist-id parser shared via `super::scraper_6600`)
// and inserts artwork entries into catalog 6601. After each insert,
// looks up the artwork's artist in catalog 6600 and adds a P170
// (creator) MnM relation. Best-effort: missing artists are silently
// skipped — running scraper_6600 first will close the gap on the
// next 6601 sweep.
//
// Bypasses the default `process_cache` path because relations need
// the inserted entry id, only known after `insert_new` returns.

const ARTIST_CATALOG_ID: usize = 6600;
const FIRST_YEAR: u32 = 1300;
const STOP_YEAR: u32 = 2100;
const PAGE_SIZE: usize = 1000;

#[derive(Debug)]
pub struct BespokeScraper6601 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6601 {
    scraper_boilerplate!(6601);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();

        // Pre-fetch existing artworks (skip dupes) and the artist
        // ext_id → entry_id map (for the P170 link). Both are simple
        // hash lookups for the rest of the run.
        let existing_artworks: HashMap<String, usize> = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        let artist2entry: HashMap<String, usize> = self
            .app()
            .storage()
            .get_all_external_ids(ARTIST_CATALOG_ID)
            .await?;

        let mut year = FIRST_YEAR;
        while year <= STOP_YEAR {
            let year_to = year + 100;
            let mut page: usize = 1;
            let mut max = PAGE_SIZE * 2;
            while (page - 1) * PAGE_SIZE < max {
                let url = pinakothek_search_url(year, year_to, page);
                let response = match client.get(&url).send().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let json: serde_json::Value = match response.json().await {
                    Ok(j) => j,
                    Err(_) => break,
                };
                if let Some(total) = json["search"]["totalCount"].as_u64() {
                    max = total as usize;
                }
                let items = match json["items"].as_array() {
                    Some(arr) => arr,
                    None => break,
                };
                for item in items {
                    let parsed = match Self::parse_artwork(self.catalog_id(), item) {
                        Some(p) => p,
                        None => continue,
                    };
                    if existing_artworks.contains_key(&parsed.ee.entry.ext_id) {
                        continue;
                    }
                    let mut ee = parsed.ee;
                    ee.insert_new(self.app()).await?;
                    if let Some(artist_ext_id) = parsed.artist_ext_id {
                        if let Some(artist_entry_id) = artist2entry.get(&artist_ext_id) {
                            // P170 = creator
                            let _ = ee.entry.add_mnm_relation(170, *artist_entry_id).await;
                        }
                    }
                }
                page += 1;
            }
            year += 100;
        }
        Ok(())
    }
}

/// Output of [`BespokeScraper6601::parse_artwork`]: the entry to insert
/// plus the artist's ext_id (if extractable) for the post-insert P170
/// link lookup.
#[derive(Debug)]
pub(crate) struct ParsedArtwork {
    pub(crate) ee: ExtendedEntry,
    pub(crate) artist_ext_id: Option<String>,
}

impl BespokeScraper6601 {
    pub(crate) fn parse_artwork(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ParsedArtwork> {
        let url = item.get("url").and_then(|x| x.as_str()).unwrap_or("");
        let id = artwork_id_from_url(url)?;
        let mut name = item
            .get("title")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if name.is_empty() {
            // Mirrors PHP `if ($o_artwork->name=='') $o_artwork->name = '<no title>';`.
            name = "<no title>".to_string();
        }
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id,
            ext_name: name,
            ext_url: url.to_string(),
            random: rand::rng().random(),
            // Q838948 = work of art
            type_name: Some("Q838948".to_string()),
            ..Default::default()
        };
        let artist_ext_id = item
            .get("artistInfo")
            .and_then(|info| info.get("url"))
            .and_then(|u| u.as_str())
            .and_then(artist_id_from_url);
        Some(ParsedArtwork {
            ee: ExtendedEntry {
                entry,
                ..Default::default()
            },
            artist_ext_id,
        })
    }
}

/// Extract the artwork slug from a Pinakothek `…/artwork/SLUG/…` URL.
pub(crate) fn artwork_id_from_url(url: &str) -> Option<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"/artwork/(.+?)/").expect("regex");
    }
    RE.captures(url)?.get(1).map(|m| m.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_6601_artwork_id_from_url() {
        assert_eq!(
            artwork_id_from_url(
                "https://www.sammlung.pinakothek.de/de/artwork/2k1qxRwbXM/works"
            ),
            Some("2k1qxRwbXM".to_string())
        );
    }

    #[test]
    fn test_6601_artwork_id_no_match() {
        assert_eq!(artwork_id_from_url(""), None);
        assert_eq!(artwork_id_from_url("/artwork/no-trailing"), None);
    }

    #[test]
    fn test_6601_parse_artwork_full() {
        let item = serde_json::json!({
            "title": "Sunflowers",
            "url": "https://www.sammlung.pinakothek.de/de/artwork/abc/works",
            "artistInfo": {
                "fullName": "Vincent van Gogh",
                "url": "https://www.sammlung.pinakothek.de/de/artist/van-gogh-vincent/works"
            }
        });
        let p = BespokeScraper6601::parse_artwork(6601, &item).unwrap();
        assert_eq!(p.ee.entry.ext_id, "abc");
        assert_eq!(p.ee.entry.ext_name, "Sunflowers");
        assert_eq!(p.ee.entry.type_name, Some("Q838948".to_string()));
        assert_eq!(p.artist_ext_id, Some("van-gogh-vincent".to_string()));
    }

    #[test]
    fn test_6601_parse_artwork_default_title() {
        let item = serde_json::json!({
            "title": "",
            "url": "https://www.sammlung.pinakothek.de/de/artwork/abc/works"
        });
        let p = BespokeScraper6601::parse_artwork(6601, &item).unwrap();
        assert_eq!(p.ee.entry.ext_name, "<no title>");
    }

    #[test]
    fn test_6601_parse_artwork_skips_when_url_unparseable() {
        let item = serde_json::json!({
            "title": "X",
            "url": "https://example.com/no-artwork-segment"
        });
        assert!(BespokeScraper6601::parse_artwork(6601, &item).is_none());
    }

    #[test]
    fn test_6601_parse_artwork_no_artist_link() {
        let item = serde_json::json!({
            "title": "Untitled",
            "url": "https://www.sammlung.pinakothek.de/de/artwork/xyz/works"
        });
        let p = BespokeScraper6601::parse_artwork(6601, &item).unwrap();
        assert!(p.artist_ext_id.is_none());
    }

    #[test]
    fn test_6601_parse_artwork_artist_url_unparseable_no_link() {
        let item = serde_json::json!({
            "title": "X",
            "url": "https://www.sammlung.pinakothek.de/de/artwork/abc/works",
            "artistInfo": {"url": "https://example.com/x"}
        });
        let p = BespokeScraper6601::parse_artwork(6601, &item).unwrap();
        assert!(p.artist_ext_id.is_none());
    }
}
