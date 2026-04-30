use crate::{
    app_state::AppState,
    coordinates::CoordinateLocation,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// Museweb: Birkbeck (BBK) database of UK museum properties (catalog 5335).
// The page embeds museum data as a JavaScript array: var museums=[[id,name,desc,lat,lon],...];

#[derive(Debug)]
pub struct BespokeScraper5335 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5335 {
    scraper_boilerplate!(5335);

    async fn run(&self) -> Result<()> {
        let url = "https://museweb.dcs.bbk.ac.uk/browseproperties";
        let html = self.http_client().get(url).send().await?.text().await?;
        let entries = Self::parse_page(self.catalog_id(), &html)?;
        let mut cache = vec![];
        for ee in entries {
            cache.push(ee);
            self.maybe_flush_cache(&mut cache).await?;
        }
        self.process_cache(&mut cache).await?;
        Ok(())
    }
}

impl BespokeScraper5335 {
    /// Extract the `var museums=[...]` JS array and convert it to `ExtendedEntry` records.
    pub(crate) fn parse_page(catalog_id: usize, html: &str) -> Result<Vec<ExtendedEntry>> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"(?s)var museums=(\[.+?\]);").unwrap();
        }
        let caps = RE
            .captures(html)
            .ok_or_else(|| anyhow!("museums JS variable not found in page"))?;
        let arr: Vec<serde_json::Value> = serde_json::from_str(&caps[1])?;

        let mut entries = Vec::with_capacity(arr.len());
        for item in &arr {
            let Some(row) = item.as_array() else { continue };
            if row.len() < 5 {
                continue;
            }
            let Some(ext_id) = row[0].as_str() else { continue };
            let ext_name = row[1].as_str().unwrap_or_default().trim().to_string();
            if ext_name.is_empty() {
                continue;
            }
            let Some(lat) = row[3].as_f64() else { continue };
            let Some(lon) = row[4].as_f64() else { continue };

            let mut ee = ExtendedEntry {
                entry: Entry {
                    catalog: catalog_id,
                    ext_id: ext_id.to_string(),
                    ext_url: format!("https://museweb.dcs.bbk.ac.uk/Museum/{}", ext_id),
                    ext_name,
                    ext_desc: row[2].as_str().unwrap_or_default().trim().to_string(),
                    type_name: Some("Q33506".to_string()),
                    random: rand::rng().random(),
                    ..Default::default()
                },
                ..Default::default()
            };
            ee.location = Some(CoordinateLocation::new(lat, lon));
            entries.push(ee);
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    fn make_scraper() -> BespokeScraper5335 {
        BespokeScraper5335 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_5335_catalog_id() {
        assert_eq!(make_scraper().catalog_id(), 5335);
    }

    #[test]
    fn test_5335_keep_existing_names_default() {
        assert!(!make_scraper().keep_existing_names());
    }

    #[test]
    fn test_5335_parse_valid_entry() {
        let html = r#"var museums=[["mm.test.001", " TEST MUSEUM", "Category:Test", 51.5074, -0.1278]];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.entry.ext_id, "mm.test.001");
        assert_eq!(e.entry.ext_name, "TEST MUSEUM");
        assert_eq!(e.entry.ext_desc, "Category:Test");
        assert_eq!(
            e.entry.ext_url,
            "https://museweb.dcs.bbk.ac.uk/Museum/mm.test.001"
        );
        assert_eq!(e.entry.type_name, Some("Q33506".to_string()));
        let loc = e.location.unwrap();
        assert!((loc.lat() - 51.5074).abs() < 0.0001);
        assert!((loc.lon() - (-0.1278)).abs() < 0.0001);
    }

    #[test]
    fn test_5335_parse_multiple_entries() {
        let html = r#"var museums=[
            ["mm.test.001", " MUSEUM A", "Cat A", 51.0, -1.0],
            ["mm.test.002", " MUSEUM B", "Cat B", 52.0, -2.0]
        ];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "mm.test.001");
        assert_eq!(entries[1].entry.ext_id, "mm.test.002");
    }

    #[test]
    fn test_5335_parse_missing_variable_errors() {
        let html = r#"<html>no museums variable here</html>"#;
        assert!(BespokeScraper5335::parse_page(5335, html).is_err());
    }

    #[test]
    fn test_5335_parse_skips_short_rows() {
        let html = r#"var museums=[["mm.x.001", "SHORT"]];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5335_parse_trims_name_whitespace() {
        let html =
            r#"var museums=[["mm.t.001", "  NAME WITH SPACES  ", "Desc", 51.0, 0.0]];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert_eq!(entries[0].entry.ext_name, "NAME WITH SPACES");
    }

    #[test]
    fn test_5335_entry_type_is_museum() {
        let html = r#"var museums=[["mm.x.1", "TEST", "Cat", 51.0, 0.0]];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert_eq!(entries[0].entry.type_name, Some("Q33506".to_string()));
    }

    #[test]
    fn test_5335_parse_skips_empty_name() {
        let html = r#"var museums=[["mm.x.1", "   ", "Cat", 51.0, 0.0]];"#;
        let entries = BespokeScraper5335::parse_page(5335, html).unwrap();
        assert!(entries.is_empty());
    }
}
