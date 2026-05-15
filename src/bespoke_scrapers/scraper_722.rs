use std::sync::Arc;
use crate::{app_state::AppContext, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::LazyLock;
use rand::RngExt;
use scraper::{ElementRef, Html, Selector};

use super::BespokeScraper;

// ______________________________________________________
// Dreadnought Project - Naval Personnel (722)

const START_URL: &str = "http://www.dreadnoughtproject.org/tfs/index.php?title=Category:People";

const ENTRY_HREF_PREFIX: &str = "/tfs/index.php/";
const PEOPLE_HEADING: &str = r#"Pages in category "People""#;
const NEXT_PAGE_LABEL: &str = "next 200";
const CATEGORY_TITLE: &str = "Category:People";

#[derive(Debug)]
pub struct BespokeScraper722 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper722 {

    scraper_boilerplate!(722);

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
            let entries = Self::parse_entries(self.catalog_id(), &html);
            entry_cache.extend(entries);
            self.maybe_flush_cache(&mut entry_cache).await?;
            url = Self::find_next_page_url(&html);
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

fn a_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());
    &S
}
fn li_a_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("li > a[href]").unwrap());
    &S
}
fn h2_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("h2").unwrap());
    &S
}

impl BespokeScraper722 {
    /// Find the "next 200" link for pagination. The DOM decodes `&amp;`
    /// in attribute values automatically, so no manual replace is needed.
    /// Returns an absolute URL (relative `/...` paths are prefixed with
    /// the host; bare query-string paths are joined to the start URL).
    pub(crate) fn find_next_page_url(html: &str) -> Option<String> {
        let doc = Html::parse_fragment(html);
        for a in doc.select(a_selector()) {
            if a.text().collect::<String>().trim() != NEXT_PAGE_LABEL {
                continue;
            }
            if a.value().attr("title") != Some(CATEGORY_TITLE) {
                continue;
            }
            let href = a.value().attr("href")?;
            return Some(absolutize(href));
        }
        None
    }

    /// Parse entries from the "Pages in category" block. The block
    /// boundary (heading → `<div id="catlinks">`) is preserved by
    /// walking the heading's following siblings.
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        let doc = Html::parse_fragment(html);
        let Some(heading) = doc.select(h2_selector()).find(|h2| {
            h2.text().collect::<String>().trim() == PEOPLE_HEADING
        }) else {
            return vec![];
        };

        let mut entries = Vec::new();
        for sib in heading.next_siblings() {
            let Some(elem) = ElementRef::wrap(sib) else {
                continue;
            };
            if is_catlinks_div(elem) {
                break;
            }
            for a in elem.select(li_a_selector()) {
                if let Some(ee) = entry_from_anchor(catalog_id, a) {
                    entries.push(ee);
                }
            }
        }
        entries
    }
}

fn is_catlinks_div(elem: ElementRef<'_>) -> bool {
    elem.value().name() == "div" && elem.value().attr("id") == Some("catlinks")
}

fn absolutize(href: &str) -> String {
    if href.starts_with('/') {
        format!("http://www.dreadnoughtproject.org{href}")
    } else if href.starts_with("http") {
        href.to_string()
    } else {
        format!("http://www.dreadnoughtproject.org/tfs/index.php?{href}")
    }
}

fn entry_from_anchor(catalog_id: usize, a: ElementRef<'_>) -> Option<ExtendedEntry> {
    let href = a.value().attr("href")?;
    let id = href.strip_prefix(ENTRY_HREF_PREFIX)?;
    if id.is_empty() {
        return None;
    }
    let name = a.value().attr("title").unwrap_or("").trim().to_string();
    if name.is_empty() {
        return None;
    }
    let ext_url = format!("http://www.dreadnoughtproject.org/tfs/index.php?title={id}");
    let entry = Entry {
        catalog: catalog_id,
        ext_id: id.to_string(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_722_find_next_page_url_wrong_title() {
        // Anchor text matches but title does not — must be skipped.
        let html = r#"<a href="/other" title="Other">next 200</a>"#;
        assert_eq!(BespokeScraper722::find_next_page_url(html), None);
    }

    #[test]
    fn test_722_find_next_page_url_no_link() {
        let html = "<html>no next link</html>";
        assert_eq!(BespokeScraper722::find_next_page_url(html), None);
    }

    #[test]
    fn test_722_find_next_page_url_amp_decoded() {
        let html = r#"<a href="/tfs/index.php?title=Category:People&amp;pagefrom=X&amp;sort=name" title="Category:People">next 200</a>"#;
        let url = BespokeScraper722::find_next_page_url(html).unwrap();
        assert!(!url.contains("&amp;"));
        assert!(url.contains("&pagefrom=X&sort=name"));
    }

    #[test]
    fn test_722_parse_entries_basic() {
        let html = concat!(
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/tfs/index.php/John_Smith" title="John Smith">John Smith</a></li>"#,
            r#"<li><a href="/tfs/index.php/Jane_Doe" title="Jane Doe">Jane Doe</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks"></div>"#,
        );
        let entries = BespokeScraper722::parse_entries(722, html);
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
    fn test_722_parse_entries_no_heading() {
        let entries = BespokeScraper722::parse_entries(722, "<html><body>nothing</body></html>");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_722_parse_entries_empty() {
        let entries = BespokeScraper722::parse_entries(722, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_722_parse_entries_stops_at_catlinks() {
        // <li><a> elements inside #catlinks (post-content footer) must be ignored.
        let html = concat!(
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/tfs/index.php/Real_Person" title="Real Person">Real Person</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks">"#,
            r#"<ul><li><a href="/tfs/index.php/Footer_Link" title="Footer Link">Footer Link</a></li></ul>"#,
            r#"</div>"#,
        );
        let entries = BespokeScraper722::parse_entries(722, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "Real_Person");
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
            r#"<div id="catlinks" class="catlinks">other stuff</div>"#,
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
        let html = concat!(
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/tfs/index.php/Test_Person" title="Test Person">Test Person</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks"></div>"#,
        );
        let entries = BespokeScraper722::parse_entries(722, html);
        assert_eq!(entries[0].entry.ext_desc, "");
    }

    #[test]
    fn test_722_parse_entries_tolerates_extra_attributes() {
        let html = concat!(
            r#"<h2>Pages in category "People"</h2>"#,
            r#"<ul>"#,
            r#"<li class="page"><a class="link" href="/tfs/index.php/X" title="X">X</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks"></div>"#,
        );
        let entries = BespokeScraper722::parse_entries(722, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "X");
    }

    #[test]
    fn test_722_absolutize_relative_path() {
        assert_eq!(
            absolutize("/tfs/foo"),
            "http://www.dreadnoughtproject.org/tfs/foo"
        );
    }

    #[test]
    fn test_722_absolutize_absolute_url() {
        assert_eq!(
            absolutize("http://example.com/foo"),
            "http://example.com/foo"
        );
    }

    #[test]
    fn test_722_absolutize_query_only() {
        assert_eq!(
            absolutize("title=Other"),
            "http://www.dreadnoughtproject.org/tfs/index.php?title=Other"
        );
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
                app: std::sync::Arc::new(crate::app_state::get_test_app())
            }
            .catalog_id(),
            722
        );
    }
}
