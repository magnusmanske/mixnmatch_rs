use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Deutsche Biographie (1619)

const BEACON_URLS: &[&str] = &[
    "https://www.historische-kommission-muenchen-editionen.de/beacon_adb.txt",
    "https://www.historische-kommission-muenchen-editionen.de/beacon_ndb.txt",
    "https://www.historische-kommission-muenchen-editionen.de/beacon_db_register.txt",
];

#[derive(Debug)]
pub struct BespokeScraper1619 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper1619 {

    scraper_boilerplate!(1619);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();

        // Step 1: Collect all GND IDs from beacon files
        let mut gnd_ids: Vec<String> = Vec::new();
        for url in BEACON_URLS {
            let text = client.get(*url).send().await?.text().await?;
            let ids = Self::parse_beacon_ids(&text);
            gnd_ids.extend(ids);
        }

        // Deduplicate
        gnd_ids.sort();
        gnd_ids.dedup();

        // Step 2: For each GND ID, fetch HTML and parse entry
        let mut entry_cache = Vec::new();
        for gnd_id in &gnd_ids {
            let page_url = format!("http://www.deutsche-biographie.de/pnd{}.html", gnd_id);
            let html = match client.get(&page_url).send().await {
                Ok(resp) => match resp.text().await {
                    Ok(text) => text.replace('\n', ""),
                    Err(_) => continue,
                },
                Err(_) => continue,
            };

            let name = match Self::parse_name(&html) {
                Some(n) => n,
                None => continue,
            };
            let desc = Self::parse_description(&html).unwrap_or_default();

            let entry = Entry {
                catalog: self.catalog_id(),
                ext_id: gnd_id.clone(),
                ext_url: page_url,
                ext_name: name,
                ext_desc: desc,
                random: rand::rng().random(),
                type_name: Some("Q5".to_string()),
                ..Default::default()
            };
            let mut ee = ExtendedEntry {
                entry,
                ..Default::default()
            };
            ee.aux
                .insert(AuxiliaryRow::new(227, gnd_id.clone()));
            entry_cache.push(ee);
            self.maybe_flush_cache(&mut entry_cache).await?;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper1619 {
    /// Parse GND IDs from a beacon text file. Each line that matches `^\d+X?$` is a GND ID.
    pub(crate) fn parse_beacon_ids(text: &str) -> Vec<String> {
        lazy_static! {
            static ref RE_GND: Regex = Regex::new(r"(?m)^(\d+X?)$").unwrap();
        }
        RE_GND
            .captures_iter(text)
            .filter_map(|caps| Some(caps.get(1)?.as_str().to_string()))
            .collect()
    }

    /// Extract name from `<h1...>NAME</h1>` and reformat "Lastname, Firstname" to "Firstname Lastname".
    pub(crate) fn parse_name(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_NAME: Regex = Regex::new(r"<h1[^>]*>([^<]+)</h1>").unwrap();
        }
        let raw = RE_NAME.captures(html)?.get(1)?.as_str().trim().to_string();
        if raw.is_empty() {
            return None;
        }
        Some(Self::reformat_name(&raw))
    }

    /// Reformat "Lastname, Firstname" to "Firstname Lastname".
    /// If there is no comma, return as-is.
    pub(crate) fn reformat_name(name: &str) -> String {
        if let Some((last, first)) = name.split_once(',') {
            let first = first.trim();
            let last = last.trim();
            if first.is_empty() {
                last.to_string()
            } else {
                format!("{} {}", first, last)
            }
        } else {
            name.to_string()
        }
    }

    /// Extract description from the "Lebensdaten" section.
    /// Finds content between "Lebensdaten</dt>" and "</dl>", strips HTML tags,
    /// and removes "Normdaten" and "Namensvarianten" trailing text.
    pub(crate) fn parse_description(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_DESC_BLOCK: Regex =
                Regex::new(r"(?i)Lebensdaten</dt>(.*?)</dl>").unwrap();
            static ref RE_HTML_TAGS: Regex = Regex::new(r"<[^>]*>").unwrap();
            static ref RE_NORMDATEN: Regex =
                Regex::new(r"(?i)Normdaten.*$").unwrap();
            static ref RE_NAMENSVARIANTEN: Regex =
                Regex::new(r"(?i)Namensvarianten.*$").unwrap();
        }
        let block = RE_DESC_BLOCK.captures(html)?.get(1)?.as_str();
        let text = RE_HTML_TAGS.replace_all(block, "");
        let text = RE_NORMDATEN.replace(&text, "");
        let text = RE_NAMENSVARIANTEN.replace(&text, "");
        let text = text.trim().to_string();
        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_1619_parse_beacon_ids_basic() {
        let text = "#FORMAT: BEACON\n#PREFIX: http://d-nb.info/gnd/\n118522426\n118602640\n10234567X\n";
        let ids = BespokeScraper1619::parse_beacon_ids(text);
        assert_eq!(ids, vec!["118522426", "118602640", "10234567X"]);
    }

    #[test]
    fn test_1619_parse_beacon_ids_skips_comments_and_blanks() {
        let text = "# comment line\n\n12345\n# another comment\n67890X\n  \n";
        let ids = BespokeScraper1619::parse_beacon_ids(text);
        assert_eq!(ids, vec!["12345", "67890X"]);
    }

    #[test]
    fn test_1619_parse_beacon_ids_rejects_non_gnd() {
        let text = "abc123\nhttp://example.com\n99999\nXXX\n";
        let ids = BespokeScraper1619::parse_beacon_ids(text);
        assert_eq!(ids, vec!["99999"]);
    }

    #[test]
    fn test_1619_parse_beacon_ids_empty() {
        let ids = BespokeScraper1619::parse_beacon_ids("");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_1619_reformat_name_with_comma() {
        assert_eq!(
            BespokeScraper1619::reformat_name("Bismarck, Otto von"),
            "Otto von Bismarck"
        );
    }

    #[test]
    fn test_1619_reformat_name_no_comma() {
        assert_eq!(
            BespokeScraper1619::reformat_name("Charlemagne"),
            "Charlemagne"
        );
    }

    #[test]
    fn test_1619_reformat_name_comma_no_firstname() {
        assert_eq!(
            BespokeScraper1619::reformat_name("Aristotle, "),
            "Aristotle"
        );
    }

    #[test]
    fn test_1619_reformat_name_extra_spaces() {
        assert_eq!(
            BespokeScraper1619::reformat_name("  Müller ,  Hans  "),
            "Hans Müller"
        );
    }

    #[test]
    fn test_1619_parse_name_basic() {
        let html = r#"<h1 class="name">Bismarck, Otto von</h1>"#;
        assert_eq!(
            BespokeScraper1619::parse_name(html),
            Some("Otto von Bismarck".to_string())
        );
    }

    #[test]
    fn test_1619_parse_name_no_comma() {
        let html = r#"<h1>Charlemagne</h1>"#;
        assert_eq!(
            BespokeScraper1619::parse_name(html),
            Some("Charlemagne".to_string())
        );
    }

    #[test]
    fn test_1619_parse_name_empty_h1() {
        let html = r#"<h1></h1>"#;
        assert_eq!(BespokeScraper1619::parse_name(html), None);
    }

    #[test]
    fn test_1619_parse_name_no_h1() {
        let html = r#"<h2>Not a heading</h2>"#;
        assert_eq!(BespokeScraper1619::parse_name(html), None);
    }

    #[test]
    fn test_1619_parse_description_basic() {
        let html = r#"<dt>Lebensdaten</dt><dd>1815 – 1898</dd></dl>"#;
        assert_eq!(
            BespokeScraper1619::parse_description(html),
            Some("1815 – 1898".to_string())
        );
    }

    #[test]
    fn test_1619_parse_description_strips_html() {
        let html = r#"<dt>Lebensdaten</dt><dd><b>1815</b> – <i>1898</i></dd></dl>"#;
        assert_eq!(
            BespokeScraper1619::parse_description(html),
            Some("1815 – 1898".to_string())
        );
    }

    #[test]
    fn test_1619_parse_description_strips_normdaten() {
        let html = r#"<dt>Lebensdaten</dt><dd>1815 – 1898 Normdaten (Person): GND 12345</dd></dl>"#;
        assert_eq!(
            BespokeScraper1619::parse_description(html),
            Some("1815 – 1898".to_string())
        );
    }

    #[test]
    fn test_1619_parse_description_strips_namensvarianten() {
        let html =
            r#"<dt>Lebensdaten</dt><dd>1815 – 1898 Namensvarianten: Fürst Bismarck</dd></dl>"#;
        assert_eq!(
            BespokeScraper1619::parse_description(html),
            Some("1815 – 1898".to_string())
        );
    }

    #[test]
    fn test_1619_parse_description_empty() {
        let html = r#"<dt>Lebensdaten</dt></dl>"#;
        assert_eq!(BespokeScraper1619::parse_description(html), None);
    }

    #[test]
    fn test_1619_parse_description_no_lebensdaten() {
        let html = r#"<dt>Beruf</dt><dd>Politiker</dd></dl>"#;
        assert_eq!(BespokeScraper1619::parse_description(html), None);
    }

    #[test]
    fn test_1619_ext_url_format() {
        let gnd = "118522426";
        let url = format!("http://www.deutsche-biographie.de/pnd{}.html", gnd);
        assert_eq!(url, "http://www.deutsche-biographie.de/pnd118522426.html");
    }

    #[test]
    fn test_1619_beacon_urls_count() {
        assert_eq!(BEACON_URLS.len(), 3);
    }
}
