use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Pinakothek artists (6600)
//
// One half of the Pinakothek import. Catalog 6600 is the artist
// catalog; 6601 is the artworks. The PHP json_scraper handled both in
// the same outer loop because the upstream returns artworks (each with
// a nested `artistInfo`). Here we keep them as two independent
// scrapers — running 6600 populates only artists, running 6601
// populates only artworks (and links them via P170 to whichever
// artists already exist in 6600).
//
// The fetch loop walks the API in 100-year buckets from 1200 to 2100,
// paging through each bucket until the cumulative cursor passes the
// per-bucket `totalCount`.

const FIRST_YEAR: u32 = 1300; // PHP `$year = 1200; while (… ) $year += 100;` → first iteration is 1300
const STOP_YEAR: u32 = 2100;
const PAGE_SIZE: usize = 1000;

#[derive(Debug)]
pub struct BespokeScraper6600 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6600 {
    scraper_boilerplate!(6600);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        let mut year = FIRST_YEAR;
        while year <= STOP_YEAR {
            let year_to = year + 100;
            let mut page: usize = 1;
            // Start with `max = batch_size*2` so the first iteration always runs;
            // we update it from the API's `totalCount` after each fetch.
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
                    if let Some(ee) = Self::parse_artist(self.catalog_id(), item) {
                        entry_cache.push(ee);
                        self.maybe_flush_cache(&mut entry_cache).await?;
                    }
                }
                page += 1;
            }
            year += 100;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper6600 {
    /// Pull just the artist `(id, name, url)` triple out of an artwork
    /// item. Returns `None` if the artist URL doesn't match
    /// `/artist/(.+?)/` — the PHP `if (preg_match…) {…}` branch is
    /// skipped silently in that case.
    pub(crate) fn parse_artist(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let info = item.get("artistInfo")?;
        let url = info.get("url").and_then(|x| x.as_str()).unwrap_or("");
        let id = artist_id_from_url(url)?;
        let mut name = info
            .get("fullName")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if name.is_empty() {
            // Mirrors PHP `if ($o_artist->name=='') $o_artist->name = '<unknown>';`.
            // Without this the entry would be skipped further upstream.
            name = "<unknown>".to_string();
        }
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id,
            ext_name: name,
            ext_url: url.to_string(),
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

/// Build the Pinakothek search URL for one year-bucket page. The
/// upstream filter blob is rooted at the empty defaults except for
/// `yearRange.min` / `yearRange.max`, which we substitute. Used by
/// both 6600 (artists) and 6601 (artworks) at runtime.
pub(crate) fn pinakothek_search_url(year: u32, year_to: u32, page: usize) -> String {
    format!(
        "https://www.sammlung.pinakothek.de/api/search?&page={page}&perPage={PAGE_SIZE}&filters={{%22yearRange%22:{{%22min%22:{year},%22max%22:{year_to}}},%22artist%22:%22%22,%22title%22:%22%22,%22inventoryId%22:%22%22,%22origin%22:%22%22,%22material%22:%22%22,%22locationCode%22:%22%22,%22department%22:%22%22,%22genre%22:%22%22,%22year%22:%22%22,%22onDisplay%22:false,%22onHidden%22:false,%22withPicture%22:false,%22publicDomain%22:false}}"
    )
}

/// Extract the artist slug from a Pinakothek `…/artist/SLUG/…` URL.
/// Returns `None` for URLs that don't match — those rows skip artist
/// creation in the PHP loop.
pub(crate) fn artist_id_from_url(url: &str) -> Option<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"/artist/(.+?)/").expect("regex");
    }
    RE.captures(url)?.get(1).map(|m| m.as_str().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_6600_artist_id_from_url() {
        assert_eq!(
            artist_id_from_url("https://www.sammlung.pinakothek.de/de/artist/picasso-pablo/works"),
            Some("picasso-pablo".to_string())
        );
    }

    #[test]
    fn test_6600_artist_id_from_url_no_match() {
        assert_eq!(artist_id_from_url("https://example.com/x"), None);
        assert_eq!(artist_id_from_url("/artist/no-trailing-slash"), None);
    }

    #[test]
    fn test_6600_parse_artist_full() {
        let item = serde_json::json!({
            "artistInfo": {
                "fullName": "Pablo Picasso",
                "url": "https://www.sammlung.pinakothek.de/de/artist/picasso-pablo/works"
            }
        });
        let ee = BespokeScraper6600::parse_artist(6600, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "picasso-pablo");
        assert_eq!(ee.entry.ext_name, "Pablo Picasso");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.sammlung.pinakothek.de/de/artist/picasso-pablo/works"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_6600_parse_artist_unknown_name_default() {
        let item = serde_json::json!({
            "artistInfo": {
                "fullName": "",
                "url": "https://www.sammlung.pinakothek.de/de/artist/anonymous/works"
            }
        });
        let ee = BespokeScraper6600::parse_artist(6600, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "<unknown>");
    }

    #[test]
    fn test_6600_parse_artist_no_match_skipped() {
        let item = serde_json::json!({
            "artistInfo": {
                "fullName": "Anonymous",
                "url": "https://www.sammlung.pinakothek.de/no-artist-segment"
            }
        });
        assert!(BespokeScraper6600::parse_artist(6600, &item).is_none());
    }

    #[test]
    fn test_6600_parse_artist_no_artist_info_skipped() {
        let item = serde_json::json!({"title": "Untitled"});
        assert!(BespokeScraper6600::parse_artist(6600, &item).is_none());
    }

    #[test]
    fn test_6600_url_includes_pagination() {
        let url = pinakothek_search_url(1900, 2000, 3);
        assert!(url.contains("page=3"));
        assert!(url.contains("perPage=1000"));
        assert!(url.contains("%22min%22:1900"));
        assert!(url.contains("%22max%22:2000"));
    }
}
