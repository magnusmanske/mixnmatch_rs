use std::sync::Arc;
use crate::{app_state::AppContext, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::LazyLock;
use rand::RngExt;
use scraper::{ElementRef, Html, Selector};

use super::BespokeScraper;

// ______________________________________________________
// Wiki Ormianie - Armenian community wiki (5311)

const START_URL: &str =
    "https://wiki.ormianie.pl/index.php?title=Kategoria:Biografie_według_miejsc";

const SUBCATEGORY_PREFIX: &str = "/index.php?title=Kategoria:Osoby_";
const ENTRY_PREFIX: &str = "/index.php?title=";
const NEXT_PAGE_LABEL: &str = "następne 200";
const PAGES_HEADING_PREFIX: &str = "Strony w kategorii";

#[derive(Debug)]
pub struct BespokeScraper5311 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5311 {

    scraper_boilerplate!(5311);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = Vec::new();

        // Step 1: Fetch the top-level category page
        let top_html = client.get(START_URL).send().await?.text().await?;
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
                let entries = Self::parse_entries(self.catalog_id(), &html);
                entry_cache.extend(entries);
                self.maybe_flush_cache(&mut entry_cache).await?;
                page_url = Self::find_next_page_url(&html);
            }
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

impl BespokeScraper5311 {
    /// Parse subcategory links (`/index.php?title=Kategoria:Osoby_*`) from
    /// the top-level category page. Returns the *relative* hrefs as found
    /// on the page; the caller prepends the host.
    pub(crate) fn parse_subcategory_links(html: &str) -> Vec<String> {
        let doc = Html::parse_fragment(html);
        doc.select(li_a_selector())
            .filter_map(|a| {
                let href = a.value().attr("href")?;
                href.starts_with(SUBCATEGORY_PREFIX)
                    .then(|| href.to_string())
            })
            .collect()
    }

    /// Find the "następne 200" (next 200) pagination link. The DOM
    /// returns the `href` value with HTML entities already decoded
    /// (e.g. `&amp;` → `&`), so the manual replace from the old regex
    /// path is no longer needed.
    pub(crate) fn find_next_page_url(html: &str) -> Option<String> {
        let doc = Html::parse_fragment(html);
        for a in doc.select(a_selector()) {
            if a.text().collect::<String>().trim() != NEXT_PAGE_LABEL {
                continue;
            }
            let href = a.value().attr("href")?;
            if href.starts_with("/index.php") {
                return Some(format!("https://wiki.ormianie.pl{href}"));
            }
        }
        None
    }

    /// Parse person entries from a subcategory page. The block boundary
    /// (`<h2>Strony w kategorii …</h2>` → `<div id="catlinks">`) is
    /// preserved by walking the heading's following siblings until the
    /// catlinks div is reached; non-person `<li><a>` links elsewhere on
    /// the page (sidebar, etc.) are ignored.
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        let doc = Html::parse_fragment(html);
        let Some(heading) = doc.select(h2_selector()).find(|h2| {
            h2.text()
                .collect::<String>()
                .trim_start()
                .starts_with(PAGES_HEADING_PREFIX)
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

fn entry_from_anchor(catalog_id: usize, a: ElementRef<'_>) -> Option<ExtendedEntry> {
    let href = a.value().attr("href")?;
    let id = href.strip_prefix(ENTRY_PREFIX)?;
    if id.is_empty() || id.starts_with("Kategoria:") {
        return None;
    }
    // The MediaWiki category-listing markup uses `title` for the
    // human-readable page name; the link text matches it but is
    // slightly less robust against trailing whitespace.
    let name = a.value().attr("title").unwrap_or("").trim().to_string();
    if name.is_empty() {
        return None;
    }
    let ext_url = format!("https://wiki.ormianie.pl/index.php?title={id}");
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
    fn test_5311_parse_subcategory_links_basic() {
        let html = concat!(
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Krakowa" title="Osoby z Krakowa">Osoby z Krakowa</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Warszawy" title="Osoby z Warszawy">Osoby z Warszawy</a></li>"#,
            r#"</ul>"#,
        );
        let links = BespokeScraper5311::parse_subcategory_links(html);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0], "/index.php?title=Kategoria:Osoby_z_Krakowa");
        assert_eq!(links[1], "/index.php?title=Kategoria:Osoby_z_Warszawy");
    }

    #[test]
    fn test_5311_parse_subcategory_links_ignores_non_osoby() {
        let html = concat!(
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Kategoria:Osoby_z_Krakowa" title="test">test</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Inne" title="other">other</a></li>"#,
            r#"</ul>"#,
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
    fn test_5311_find_next_page_url_amp_decoded() {
        // scraper decodes `&amp;` in attribute values automatically
        let html = r#"<a href="/index.php?title=Kat&amp;from=A&amp;to=Z" class="next">następne 200</a>"#;
        let url = BespokeScraper5311::find_next_page_url(html).unwrap();
        assert!(!url.contains("&amp;"));
        assert!(url.contains("&from=A&to=Z"));
    }

    #[test]
    fn test_5311_find_next_page_url_ignores_other_links() {
        // Anchor text must be exactly the pagination label.
        let html = r#"<a href="/index.php?title=Other">previous 200</a>
                      <a href="/index.php?title=Real&amp;p=2">następne 200</a>"#;
        let url = BespokeScraper5311::find_next_page_url(html).unwrap();
        assert!(url.contains("title=Real"));
    }

    #[test]
    fn test_5311_parse_entries_basic() {
        let html = concat!(
            r#"<h2>Strony w kategorii „Osoby z Lwowa"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Jan_Kowalski" title="Jan Kowalski">Jan Kowalski</a></li>"#,
            r#"<li><a href="/index.php?title=Anna_Nowak" title="Anna Nowak">Anna Nowak</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks">other</div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
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
    fn test_5311_parse_entries_skips_categories() {
        let html = concat!(
            r#"<h2>Strony w kategorii „X"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Jan_Kowalski" title="Jan Kowalski">Jan Kowalski</a></li>"#,
            r#"<li><a href="/index.php?title=Kategoria:Inne" title="Kategoria:Inne">Kategoria:Inne</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks"></div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "Jan_Kowalski");
    }

    #[test]
    fn test_5311_parse_entries_stops_at_catlinks() {
        // Links inside #catlinks (the post-content footer) must be ignored,
        // matching the original "block boundary" behaviour.
        let html = concat!(
            r#"<h2>Strony w kategorii „X"</h2>"#,
            r#"<ul>"#,
            r#"<li><a href="/index.php?title=Real_Person" title="Real Person">Real Person</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks">"#,
            r#"<ul><li><a href="/index.php?title=Footer_Link" title="Footer Link">Footer Link</a></li></ul>"#,
            r#"</div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "Real_Person");
    }

    #[test]
    fn test_5311_parse_entries_no_heading() {
        let entries = BespokeScraper5311::parse_entries(5311, "<html><body>nothing</body></html>");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5311_parse_entries_empty() {
        let entries = BespokeScraper5311::parse_entries(5311, "");
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
            r#"<div id="catlinks" class="catlinks">other</div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_name, "Piotr Barącz");
        assert_eq!(entries[1].entry.ext_name, "Grzegorz Piramowicz");
    }

    #[test]
    fn test_5311_parse_entries_tolerates_extra_attributes() {
        // A regression test for the previous regex's brittleness: extra
        // attributes between `<li>` and `<a>` or on `<a>` should not
        // break extraction.
        let html = concat!(
            r#"<h2>Strony w kategorii „X"</h2>"#,
            r#"<ul>"#,
            r#"<li class="x"><a class="y" href="/index.php?title=Test" title="Test">Test</a></li>"#,
            r#"</ul>"#,
            r#"<div id="catlinks"></div>"#,
        );
        let entries = BespokeScraper5311::parse_entries(5311, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "Test");
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
                app: std::sync::Arc::new(crate::app_state::get_test_app())
            }
            .catalog_id(),
            5311
        );
    }
}
