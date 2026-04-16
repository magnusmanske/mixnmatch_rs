use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Wiki Ormianie - Armenian community wiki (5311)

const START_URL: &str =
    "https://wiki.ormianie.pl/index.php?title=Kategoria:Biografie_według_miejsc";

#[derive(Debug)]
pub struct BespokeScraper5311 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5311 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        5311
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = Vec::new();

        // Step 1: Fetch the top-level category page
        let top_html = client.get(START_URL).send().await?.text().await?;
        let top_html = Self::collapse_whitespace(&top_html);
        let subcategory_urls = Self::parse_subcategory_links(&top_html);

        // Step 2: For each subcategory, fetch its page and follow pagination
        for relative_url in &subcategory_urls {
            let mut page_url = Some(format!("https://wiki.ormianie.pl{}", relative_url));

            while let Some(current_url) = page_url.take() {
                let html = match client.get(&current_url).send().await {
                    Ok(resp) => match resp.text().await {
                        Ok(text) => text,
                        Err(_) => break,
                    },
                    Err(_) => break,
                };
                let html = Self::collapse_whitespace(&html);
                let entries = Self::parse_entries(self.catalog_id(), &html);
                entry_cache.extend(entries);
                if entry_cache.len() >= 100 {
                    self.process_cache(&mut entry_cache).await?;
                    entry_cache.clear();
                }
                page_url = Self::find_next_page_url(&html);
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper5311 {
    /// Collapse all runs of whitespace (including newlines) to a single space.
    pub(crate) fn collapse_whitespace(html: &str) -> String {
        lazy_static! {
            static ref RE_WS: Regex = Regex::new(r"\s+").unwrap();
        }
        RE_WS.replace_all(html, " ").to_string()
    }

    /// Parse subcategory links from the top-level category page.
    /// Matches `<li><a href="(/index.php\?title=Kategoria:Osoby_[^"]*)"`.
    pub(crate) fn parse_subcategory_links(html: &str) -> Vec<String> {
        lazy_static! {
            static ref RE_SUBCAT: Regex = Regex::new(
                r#"<li><a href="(/index\.php\?title=Kategoria:Osoby_[^"]*)""#
            )
            .unwrap();
        }
        RE_SUBCAT
            .captures_iter(html)
            .filter_map(|caps| Some(caps.get(1)?.as_str().to_string()))
            .collect()
    }

    /// Find the "następne 200" (next 200) pagination link.
    /// Returns the absolute URL if found.
    pub(crate) fn find_next_page_url(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_NEXT: Regex = Regex::new(
                r#"<a href="(/index\.php\?[^"]*)"[^>]*>następne 200</a>"#
            )
            .unwrap();
        }
        let relative = RE_NEXT.captures(html)?.get(1)?.as_str();
        let url = relative.replace("&amp;", "&");
        Some(format!("https://wiki.ormianie.pl{}", url))
    }

    /// Parse person entries from a subcategory page.
    /// Finds the block between "Strony w kategorii" heading and `<div id="catlinks"`,
    /// then matches all person links.
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        let block = match Self::extract_pages_block(html) {
            Some(b) => b,
            None => return vec![],
        };
        Self::parse_entries_from_block(catalog_id, &block)
    }

    /// Extract the block between "Strony w kategorii" heading and catlinks div.
    pub(crate) fn extract_pages_block(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_BLOCK: Regex =
                Regex::new(r#"(?s)<h2>Strony w kategorii[^<]*</h2>(.*?)<div id="catlinks""#)
                    .unwrap();
        }
        Some(RE_BLOCK.captures(html)?.get(1)?.as_str().to_string())
    }

    /// Parse individual entries from the pages block.
    pub(crate) fn parse_entries_from_block(
        catalog_id: usize,
        block: &str,
    ) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ENTRY: Regex = Regex::new(
                r#"<li><a href="/index\.php\?title=([^"]+)" title="([^"]+)">"#
            )
            .unwrap();
        }
        RE_ENTRY
            .captures_iter(block)
            .filter_map(|caps| {
                let id = caps.get(1)?.as_str().to_string();
                let name = caps.get(2)?.as_str().trim().to_string();
                if name.is_empty() || id.is_empty() {
                    return None;
                }
                // Skip category pages that might appear in the block
                if id.starts_with("Kategoria:") {
                    return None;
                }
                let ext_url =
                    format!("https://wiki.ormianie.pl/index.php?title={}", id);
                let entry = Entry {
                    catalog: catalog_id,
                    ext_id: id,
                    ext_name: name,
                    ext_desc: String::new(),
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
    fn test_5311_collapse_whitespace() {
        assert_eq!(
            BespokeScraper5311::collapse_whitespace("hello  \n  world"),
            "hello world"
        );
    }

    #[test]
    fn test_5311_parse_subcategory_links_basic() {
        let html = concat!(
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Krakowa" title="Osoby z Krakowa">Osoby z Krakowa</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Warszawy" title="Osoby z Warszawy">Osoby z Warszawy</a></li>"#,
        );
        let links = BespokeScraper5311::parse_subcategory_links(html);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0], "/index.php?title=Kategoria:Osoby_z_Krakowa");
        assert_eq!(links[1], "/index.php?title=Kategoria:Osoby_z_Warszawy");
    }

    #[test]
    fn test_5311_parse_subcategory_links_ignores_non_osoby() {
        let html = concat!(
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Krakowa" title="test">test</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Inne" title="other">other</a></li>"#,
        );
        let links = BespokeScraper5311::parse_subcategory_links(html);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0], "/index.php?title=Kategoria:Osoby_z_Krakowa");
    }

    #[test]
    fn test_5311_parse_subcategory_links_empty() {
        let links = BespokeScraper5311::parse_subcategory_links("<html></html>");
        assert!(links.is_empty());
    }

    #[test]
    fn test_5311_find_next_page_url_present() {
        let html = r#"<a href="/index.php?title=Kategoria:Osoby_z_Krakowa&amp;pagefrom=M" class="mw-nextlink">następne 200</a>"#;
        let url = BespokeScraper5311::find_next_page_url(html);
        assert_eq!(
            url,
            Some("https://wiki.ormianie.pl/index.php?title=Kategoria:Osoby_z_Krakowa&pagefrom=M".to_string())
        );
    }

    #[test]
    fn test_5311_find_next_page_url_absent() {
        let html = "<html>no next link</html>";
        assert_eq!(BespokeScraper5311::find_next_page_url(html), None);
    }

    #[test]
    fn test_5311_find_next_page_url_amp_replaced() {
        let html = r#"<a href="/index.php?title=Kat&amp;from=A&amp;to=Z" class="next">następne 200</a>"#;
        let url = BespokeScraper5311::find_next_page_url(html).unwrap();
        assert!(!url.contains("&amp;"));
        assert!(url.contains("&from=A&to=Z"));
    }

    #[test]
    fn test_5311_extract_pages_block() {
        let html = concat!(
            r#"<h2>Strony w kategorii „Osoby z Krakowa"</h2>"#,
            r#"<ul><li>person list</li></ul>"#,
            r#"<div id="catlinks""#,
        );
        let block = BespokeScraper5311::extract_pages_block(html).unwrap();
        assert!(block.contains("person list"));
    }

    #[test]
    fn test_5311_extract_pages_block_missing() {
        let html = "<h2>Other heading</h2><div id=\"catlinks\"";
        assert_eq!(BespokeScraper5311::extract_pages_block(html), None);
    }

    #[test]
    fn test_5311_parse_entries_from_block_basic() {
        let block = concat!(
            r#"<li><a href="/index.php?title=Jan_Kowalski" title="Jan Kowalski">Jan Kowalski</a></li>"#,
            r#"<li><a href="/index.php?title=Anna_Nowak" title="Anna Nowak">Anna Nowak</a></li>"#,
        );
        let entries = BespokeScraper5311::parse_entries_from_block(5311, block);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "Jan_Kowalski");
        assert_eq!(entries[0].entry.ext_name, "Jan Kowalski");
        assert_eq!(
            entries[0].entry.ext_url,
            "https://wiki.ormianie.pl/index.php?title=Jan_Kowalski"
        );
        assert_eq!(entries[0].entry.type_name, Some("Q5".to_string()));
        assert_eq!(entries[0].entry.ext_desc, "");
        assert_eq!(entries[1].entry.ext_id, "Anna_Nowak");
        assert_eq!(entries[1].entry.ext_name, "Anna Nowak");
    }

    #[test]
    fn test_5311_parse_entries_from_block_skips_categories() {
        let block = concat!(
            r#"<li><a href="/index.php?title=Jan_Kowalski" title="Jan Kowalski">Jan Kowalski</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Inne" title="Kategoria:Inne">Kategoria:Inne</a></li>"#,
        );
        let entries = BespokeScraper5311::parse_entries_from_block(5311, block);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "Jan_Kowalski");
    }

    #[test]
    fn test_5311_parse_entries_from_block_empty() {
        let entries = BespokeScraper5311::parse_entries_from_block(5311, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5311_parse_entries_full_page() {
        let html = concat!(
            r#"<h1>Category page</h1>"#,
            r#"<h2>Strony w kategorii „Osoby z Lwowa"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Piotr_Barącz" title="Piotr Barącz">Piotr Barącz</a></li>"#,
            r#"<li><a href="/index.php?title=Grzegorz_Piramowicz" title="Grzegorz Piramowicz">Grzegorz Piramowicz</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks""#,
            r#" class="catlinks">other</div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_name, "Piotr Barącz");
        assert_eq!(entries[1].entry.ext_name, "Grzegorz Piramowicz");
    }

    #[test]
    fn test_5311_parse_entries_no_block() {
        let html = "<html><body>no people block</body></html>";
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5311_ext_url_format() {
        let id = "Test_Person";
        let url = format!("https://wiki.ormianie.pl/index.php?title={}", id);
        assert_eq!(
            url,
            "https://wiki.ormianie.pl/index.php?title=Test_Person"
        );
    }

    #[test]
    fn test_5311_start_url() {
        assert_eq!(
            START_URL,
            "https://wiki.ormianie.pl/index.php?title=Kategoria:Biografie_według_miejsc"
        );
    }

    #[test]
    fn test_5311_catalog_id() {
        assert_eq!(
            BespokeScraper5311 {
                app: crate::app_state::get_test_app()
            }
            .catalog_id(),
            5311
        );
    }
}
