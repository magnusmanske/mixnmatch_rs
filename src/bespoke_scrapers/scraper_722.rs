use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Dreadnought Project - Naval Personnel (722)

const START_URL: &str = "http://www.dreadnoughtproject.org/tfs/index.php?title=Category:People";

#[derive(Debug)]
pub struct BespokeScraper722 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper722 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        722
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = Vec::new();
        let mut url = Some(START_URL.to_string());

        while let Some(current_url) = url.take() {
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
            url = Self::find_next_page_url(&html);
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper722 {
    /// Collapse all runs of whitespace (including newlines) to a single space.
    pub(crate) fn collapse_whitespace(html: &str) -> String {
        lazy_static! {
            static ref RE_WS: Regex = Regex::new(r"\s+").unwrap();
        }
        RE_WS.replace_all(html, " ").to_string()
    }

    /// Find the "next 200" link for pagination.
    /// Matches `<a href="(URL)" title="Category:People">next 200</a>` and replaces `&amp;` with `&`.
    pub(crate) fn find_next_page_url(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_NEXT: Regex = Regex::new(
                r#"<a href="([^"]+)" title="Category:People">next 200</a>"#
            )
            .unwrap();
        }
        let raw_url = RE_NEXT.captures(html)?.get(1)?.as_str();
        let url = raw_url.replace("&amp;", "&");
        // If the URL is relative, make it absolute
        if url.starts_with('/') {
            Some(format!("http://www.dreadnoughtproject.org{}", url))
        } else if url.starts_with("http") {
            Some(url)
        } else {
            Some(format!(
                "http://www.dreadnoughtproject.org/tfs/index.php?{}",
                url
            ))
        }
    }

    /// Parse entries from the "Pages in category" block.
    /// Finds the block between `<h2>Pages in category "People"</h2>` and `<div id="catlinks"`,
    /// then matches all `<li><a href="/tfs/index.php/([^"]+)" title="([^"]+)">`.
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        let block = match Self::extract_people_block(html) {
            Some(b) => b,
            None => return vec![],
        };
        Self::parse_entries_from_block(catalog_id, &block)
    }

    /// Extract the block between the "Pages in category" heading and catlinks div.
    pub(crate) fn extract_people_block(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_BLOCK: Regex = Regex::new(
                r#"(?s)<h2>Pages in category "People"</h2>(.*?)<div id="catlinks""#
            )
            .unwrap();
        }
        Some(RE_BLOCK.captures(html)?.get(1)?.as_str().to_string())
    }

    /// Parse individual entries from the people block.
    pub(crate) fn parse_entries_from_block(
        catalog_id: usize,
        block: &str,
    ) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ENTRY: Regex = Regex::new(
                r#"<li><a href="/tfs/index\.php/([^"]+)" title="([^"]+)">"#
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
                let ext_url = format!(
                    "http://www.dreadnoughtproject.org/tfs/index.php?title={}",
                    id
                );
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
    fn test_722_collapse_whitespace() {
        assert_eq!(
            BespokeScraper722::collapse_whitespace("hello  \n  world"),
            "hello world"
        );
        assert_eq!(
            BespokeScraper722::collapse_whitespace("a\t\tb"),
            "a b"
        );
    }

    #[test]
    fn test_722_find_next_page_url_present() {
        let html = r#"<a href="/tfs/index.php?title=Category:People&amp;pagefrom=D" title="Category:People">next 200</a>"#;
        let url = BespokeScraper722::find_next_page_url(html);
        assert_eq!(
            url,
            Some(
                "http://www.dreadnoughtproject.org/tfs/index.php?title=Category:People&pagefrom=D"
                    .to_string()
            )
        );
    }

    #[test]
    fn test_722_find_next_page_url_absent() {
        let html = r#"<a href="/other" title="Other">next 200</a>"#;
        assert_eq!(BespokeScraper722::find_next_page_url(html), None);
    }

    #[test]
    fn test_722_find_next_page_url_no_link() {
        let html = "<html>no next link</html>";
        assert_eq!(BespokeScraper722::find_next_page_url(html), None);
    }

    #[test]
    fn test_722_find_next_page_url_amp_replaced() {
        let html = r#"<a href="/tfs/index.php?title=Category:People&amp;pagefrom=X&amp;sort=name" title="Category:People">next 200</a>"#;
        let url = BespokeScraper722::find_next_page_url(html).unwrap();
        assert!(!url.contains("&amp;"));
        assert!(url.contains("&pagefrom=X&sort=name"));
    }

    #[test]
    fn test_722_extract_people_block() {
        let html = concat!(
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul><li>content here</li></ul>"#,
            r#"<div id="catlinks""#,
        );
        let block = BespokeScraper722::extract_people_block(html).unwrap();
        assert!(block.contains("content here"));
    }

    #[test]
    fn test_722_extract_people_block_missing() {
        let html = "<h2>Other heading</h2><div id=\"catlinks\"";
        assert_eq!(BespokeScraper722::extract_people_block(html), None);
    }

    #[test]
    fn test_722_parse_entries_from_block_basic() {
        let block = concat!(
            r#"<li><a href="/tfs/index.php/John_Smith" title="John Smith">John Smith</a></li>"#,
            r#"<li><a href="/tfs/index.php/Jane_Doe" title="Jane Doe">Jane Doe</a></li>"#,
        );
        let entries = BespokeScraper722::parse_entries_from_block(722, block);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "John_Smith");
        assert_eq!(entries[0].entry.ext_name, "John Smith");
        assert_eq!(
            entries[0].entry.ext_url,
            "http://www.dreadnoughtproject.org/tfs/index.php?title=John_Smith"
        );
        assert_eq!(entries[0].entry.type_name, Some("Q5".to_string()));
        assert_eq!(entries[1].entry.ext_id, "Jane_Doe");
        assert_eq!(entries[1].entry.ext_name, "Jane Doe");
    }

    #[test]
    fn test_722_parse_entries_from_block_empty() {
        let entries = BespokeScraper722::parse_entries_from_block(722, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_722_parse_entries_full_page() {
        let html = concat!(
            r#"<h1>Category:People</h1>"#,
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/tfs/index.php/Admiral_Nelson" title="Admiral Nelson">Admiral Nelson</a></li>"#,
            r#"<li><a href="/tfs/index.php/Captain_Cook" title="Captain Cook">Captain Cook</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks""#,
            r#" class="catlinks">other stuff</div>"#,
        );
        let entries = BespokeScraper722::parse_entries(722, html);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_name, "Admiral Nelson");
        assert_eq!(entries[1].entry.ext_name, "Captain Cook");
    }

    #[test]
    fn test_722_parse_entries_no_people_block() {
        let html = "<html><body>No people here</body></html>";
        let entries = BespokeScraper722::parse_entries(722, html);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_722_parse_entries_desc_is_empty() {
        let block = r#"<li><a href="/tfs/index.php/Test_Person" title="Test Person">Test Person</a></li>"#;
        let entries = BespokeScraper722::parse_entries_from_block(722, block);
        assert_eq!(entries[0].entry.ext_desc, "");
    }

    #[test]
    fn test_722_ext_url_format() {
        let id = "John_Jellicoe%2C_1st_Earl_Jellicoe";
        let url = format!(
            "http://www.dreadnoughtproject.org/tfs/index.php?title={}",
            id
        );
        assert!(url.starts_with("http://www.dreadnoughtproject.org/tfs/index.php?title="));
        assert!(url.ends_with(id));
    }

    #[test]
    fn test_722_start_url() {
        assert_eq!(
            START_URL,
            "http://www.dreadnoughtproject.org/tfs/index.php?title=Category:People"
        );
    }

    #[test]
    fn test_722_catalog_id() {
        assert_eq!(
            BespokeScraper722 {
                app: crate::app_state::get_test_app()
            }
            .catalog_id(),
            722
        );
    }
}
