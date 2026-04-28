use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// BIU Santé - French medical biographies (1379)

#[derive(Debug)]
pub struct BespokeScraper1379 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper1379 {

    scraper_boilerplate!(1379);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = Vec::new();
        let mut offset = 0;
        while offset <= 48000 {
            let url = format!(
                "https://www.biusante.parisdescartes.fr/histoire/biographies/?naissance_annee=100...1999&offset={}",
                offset
            );
            let html = match client.get(&url).send().await {
                Ok(resp) => match resp.text().await {
                    Ok(text) => text,
                    Err(_) => {
                        offset += 500;
                        continue;
                    }
                },
                Err(_) => {
                    offset += 500;
                    continue;
                }
            };
            let html = Self::collapse_whitespace(&html);
            let entries = Self::parse_entries(self.catalog_id(), &html);
            entry_cache.extend(entries);
            self.maybe_flush_cache(&mut entry_cache).await?;
            offset += 500;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper1379 {
    /// Collapse all runs of whitespace (including newlines) to a single space.
    pub(crate) fn collapse_whitespace(html: &str) -> String {
        lazy_static! {
            static ref RE_WS: Regex = Regex::new(r"\s+").unwrap();
        }
        RE_WS.replace_all(html, " ").to_string()
    }

    /// Clean &nbsp; (with or without trailing comma/semicolon) and replace with a space.
    pub(crate) fn clean_nbsp(s: &str) -> String {
        lazy_static! {
            static ref RE_NBSP: Regex = Regex::new(r"&nbsp;?,?").unwrap();
        }
        RE_NBSP.replace_all(s, " ").trim().to_string()
    }

    /// Reformat "Lastname, Firstname" to "Firstname Lastname".
    pub(crate) fn reformat_name(name: &str) -> String {
        let name = Self::clean_nbsp(name);
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

    /// Clean description: remove surrounding parentheses and clean &nbsp;
    pub(crate) fn clean_description(desc: &str) -> String {
        let desc = Self::clean_nbsp(desc);
        let desc = desc.trim();
        // Strip leading '(' and trailing ')'
        let desc = desc.strip_prefix('(').unwrap_or(desc);
        let desc = desc.strip_suffix(')').unwrap_or(desc);
        desc.trim().to_string()
    }

    /// Parse all entries from a page of results.
    /// Matches: `<a href="./\?refbiogr=(\d+)"> <b>(NAME)</b> </a>\s*(DESC)\s*</td>`
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ENTRY: Regex = Regex::new(
                r#"<a href="\./\?refbiogr=(\d+)">\s*<b>([^<]+)</b>\s*</a>\s*([^<]*?)\s*</td>"#
            )
            .unwrap();
        }
        RE_ENTRY
            .captures_iter(html)
            .filter_map(|caps| {
                let id = caps.get(1)?.as_str().to_string();
                let raw_name = caps.get(2)?.as_str();
                let raw_desc = caps.get(3)?.as_str();
                let name = Self::reformat_name(raw_name);
                if name.is_empty() || id.is_empty() {
                    return None;
                }
                let desc = Self::clean_description(raw_desc);
                let ext_url = format!(
                    "http://www.biusante.parisdescartes.fr/histoire/biographies/index.php?cle={}",
                    id
                );
                let entry = Entry {
                    catalog: catalog_id,
                    ext_id: id,
                    ext_name: name,
                    ext_desc: desc,
                    ext_url,
                    random: rand::rng().random(),
                    type_name: Some("Q5".to_string()),
                    ..Default::default()
                };
                Some(ExtendedEntry {
                    entry,
                    ..Default::default()
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_1379_collapse_whitespace() {
        assert_eq!(
            BespokeScraper1379::collapse_whitespace("hello  \n  world"),
            "hello world"
        );
        assert_eq!(
            BespokeScraper1379::collapse_whitespace("  a\t\tb  "),
            " a b "
        );
        assert_eq!(BespokeScraper1379::collapse_whitespace("nospace"), "nospace");
    }

    #[test]
    fn test_1379_clean_nbsp() {
        assert_eq!(BespokeScraper1379::clean_nbsp("hello&nbsp;world"), "hello world");
        assert_eq!(BespokeScraper1379::clean_nbsp("hello&nbsp,world"), "hello world");
        assert_eq!(BespokeScraper1379::clean_nbsp("no entities"), "no entities");
        assert_eq!(BespokeScraper1379::clean_nbsp("&nbsp;"), "");
    }

    #[test]
    fn test_1379_reformat_name_with_comma() {
        assert_eq!(
            BespokeScraper1379::reformat_name("Dupont, Jean"),
            "Jean Dupont"
        );
    }

    #[test]
    fn test_1379_reformat_name_no_comma() {
        assert_eq!(
            BespokeScraper1379::reformat_name("Avicenne"),
            "Avicenne"
        );
    }

    #[test]
    fn test_1379_reformat_name_nbsp() {
        assert_eq!(
            BespokeScraper1379::reformat_name("Dupont,&nbsp;Jean"),
            "Jean Dupont"
        );
    }

    #[test]
    fn test_1379_reformat_name_comma_no_first() {
        assert_eq!(
            BespokeScraper1379::reformat_name("Lastname, "),
            "Lastname"
        );
    }

    #[test]
    fn test_1379_clean_description_with_parens() {
        assert_eq!(
            BespokeScraper1379::clean_description("(1800-1870)"),
            "1800-1870"
        );
    }

    #[test]
    fn test_1379_clean_description_no_parens() {
        assert_eq!(
            BespokeScraper1379::clean_description("medecin"),
            "medecin"
        );
    }

    #[test]
    fn test_1379_clean_description_nbsp() {
        assert_eq!(
            BespokeScraper1379::clean_description("(1800&nbsp;-&nbsp;1870)"),
            "1800 - 1870"
        );
    }

    #[test]
    fn test_1379_parse_entries_basic() {
        // The regex expects the literal "./?refbiogr=..." from the HTML after whitespace collapse
        let html2 = r#"<a href="./?refbiogr=12345"> <b>Dupont, Jean</b> </a> (1800-1870) </td>"#;
        let entries = BespokeScraper1379::parse_entries(1379, html2);
        assert_eq!(entries.len(), 1);
        let e = &entries[0].entry;
        assert_eq!(e.ext_id, "12345");
        assert_eq!(e.ext_name, "Jean Dupont");
        assert_eq!(e.ext_desc, "1800-1870");
        assert_eq!(
            e.ext_url,
            "http://www.biusante.parisdescartes.fr/histoire/biographies/index.php?cle=12345"
        );
        assert_eq!(e.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_1379_parse_entries_multiple() {
        let html = concat!(
            r#"<a href="./?refbiogr=100"> <b>Martin, Pierre</b> </a> (1750-1820) </td>"#,
            r#"<a href="./?refbiogr=200"> <b>Curie, Marie</b> </a> (1867-1934) </td>"#,
        );
        let entries = BespokeScraper1379::parse_entries(1379, html);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "100");
        assert_eq!(entries[0].entry.ext_name, "Pierre Martin");
        assert_eq!(entries[1].entry.ext_id, "200");
        assert_eq!(entries[1].entry.ext_name, "Marie Curie");
    }

    #[test]
    fn test_1379_parse_entries_empty_html() {
        let entries = BespokeScraper1379::parse_entries(1379, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_1379_parse_entries_no_matches() {
        let html = "<html><body>No biographies here</body></html>";
        let entries = BespokeScraper1379::parse_entries(1379, html);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_1379_parse_entries_no_desc() {
        let html = r#"<a href="./?refbiogr=300"> <b>Solo, Han</b> </a> </td>"#;
        let entries = BespokeScraper1379::parse_entries(1379, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_desc, "");
    }

    #[test]
    fn test_1379_ext_url_format() {
        let id = "42";
        let url = format!(
            "http://www.biusante.parisdescartes.fr/histoire/biographies/index.php?cle={}",
            id
        );
        assert_eq!(
            url,
            "http://www.biusante.parisdescartes.fr/histoire/biographies/index.php?cle=42"
        );
    }

    #[test]
    fn test_1379_pagination_range() {
        // Verify the pagination produces the expected number of pages
        let offsets: Vec<usize> = (0..=48000).step_by(500).collect();
        assert_eq!(offsets.len(), 97); // 0, 500, 1000, ..., 48000
        assert_eq!(*offsets.first().unwrap(), 0);
        assert_eq!(*offsets.last().unwrap(), 48000);
    }
}
