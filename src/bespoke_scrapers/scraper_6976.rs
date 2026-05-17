use std::sync::Arc;
use crate::{app_state::AppContext, entry::{Entry, EntryWriter}};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::LazyLock;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};

use super::BespokeScraper;

// ______________________________________________________
// Hessian Biography person (6976)

#[derive(Debug)]
pub struct BespokeScraper6976 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6976 {

    scraper_boilerplate!(6976);

    async fn run(&self) -> Result<()> {
        // TODO add new?

        // Run all existing entries for metadata
        let ext_id2entry_id = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        let futures = ext_id2entry_id
            .into_values()
            .map(|entry_id| self.add_missing_aux_6976(entry_id))
            .collect::<Vec<_>>();

        // Run 5 in parallel
        let stream = futures::stream::iter(futures).buffer_unordered(5);
        let _ = stream.collect::<Vec<_>>().await;
        Ok(())
    }
}

/// Maps the German relation header (text inside `<h3>` within a `<dd>`)
/// to the Wikidata-style property number this scraper writes as auxiliary.
const H3_TEXT_TO_PROP: &[(&str, usize)] = &[
    ("Vater:", 22),
    ("Mutter:", 25),
    ("Partner:", 26),
    ("Verwandte:", 1038),
];

impl BespokeScraper6976 {
    /// Scraper-specific `add_missing_aux` that walks the Hessian biography HTML
    /// and attaches GND, family-relation auxiliary values, and MnM relations.
    pub(crate) async fn add_missing_aux_6976(&self, entry_id: usize) -> Result<()> {
        let mut entry = Entry::from_id(entry_id, self.app()).await?;
        let existing_aux = EntryWriter::new(self.app(), &mut entry).get_aux().await?;
        let url = entry.ext_url.clone();
        let text = self.load_single_line_text_from_url(&url).await?;

        // Extract everything from the DOM up-front into owned data — `Html`
        // and `ElementRef` are !Send (html5ever's tendril is not Sync), so
        // we cannot hold them across the `.await`s below.
        let (gnd_opt, relations) = extract_page_data(&text);

        if !existing_aux.iter().any(|auxiliary| auxiliary.prop_numeric() == 227)
            && let Some(gnd) = gnd_opt {
            EntryWriter::new(self.app(), &mut entry).set_auxiliary(227, Some(gnd)).await?;
        }

        for (prop_numeric, subject_ids) in relations {
            let _ = self
                .attach_subjects_as_aux(prop_numeric, &subject_ids, &mut entry)
                .await;
        }
        Ok(())
    }

    pub(crate) async fn attach_subjects_as_aux(
        &self,
        prop_numeric: usize,
        subject_ids: &[String],
        entry: &mut Entry,
    ) -> Result<()> {
        for subject_id in subject_ids {
            if let Some(gnd) = self.get_subject_gnd(subject_id).await {
                let query = format!("haswbstatement:P227={gnd}");
                let items_with_gnd = self
                    .app()
                    .wikidata()
                    .search_api(&query)
                    .await
                    .unwrap_or_default();
                if items_with_gnd.len() == 1 {
                    let item = items_with_gnd[0].clone();
                    let _ = EntryWriter::new(self.app(), entry).set_auxiliary(prop_numeric, Some(item)).await;
                } else if let Ok(target_entry) =
                    Entry::from_ext_id(self.catalog_id(), &gnd, self.app()).await
                {
                    if let Ok(target_entry_id) = target_entry.get_valid_id() {
                        let _ = EntryWriter::new(self.app(), entry).add_mnm_relation(prop_numeric, target_entry_id).await;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn get_subject_gnd(&self, subject_id: &str) -> Option<String> {
        let url = format!("https://www.lagis-hessen.de/de/subjects/idrec/sn/bio/id/{subject_id}");
        let text = self.load_single_line_text_from_url(&url).await.ok()?;
        Self::get_main_gnd_from_text(&text)
    }

    pub(crate) fn get_main_gnd_from_text(text: &str) -> Option<String> {
        extract_page_data(text).0
    }
}

/// Parse the HTML once and pull out everything `add_missing_aux_6976`
/// needs as owned data: the primary GND (if any), and a list of
/// `(prop_numeric, subject_ids)` relation pairs ready to be written.
///
/// Combining both extractions here keeps the DOM types scoped to a
/// single synchronous function so the caller's future stays `Send`.
fn extract_page_data(text: &str) -> (Option<String>, Vec<(usize, Vec<String>)>) {
    let doc = Html::parse_fragment(text);
    let gnd = find_main_gnd(&doc);
    let mut relations: Vec<(usize, Vec<String>)> = Vec::new();
    for dd in doc.select(dd_selector()) {
        let subject_ids = collect_subject_ids(dd);
        if subject_ids.is_empty() {
            continue;
        }
        let h3_texts: Vec<String> = dd
            .select(h3_selector())
            .map(|h3| h3.text().collect::<String>().trim().to_string())
            .collect();
        for (key, prop_numeric) in H3_TEXT_TO_PROP {
            if h3_texts.iter().any(|t| t == *key) {
                relations.push((*prop_numeric, subject_ids.clone()));
            }
        }
    }
    (gnd, relations)
}

/// Find the GND number that appears in the `<p>` immediately following
/// `<h2>GND-Nummer</h2>`. Whitespace-only text nodes between the heading
/// and the `<p>` are skipped transparently by walking sibling elements.
fn find_main_gnd(doc: &Html) -> Option<String> {
    for h2 in doc.select(h2_selector()) {
        if h2.text().collect::<String>().trim() != "GND-Nummer" {
            continue;
        }
        let p = h2
            .next_siblings()
            .filter_map(ElementRef::wrap)
            .find(|e| e.value().name() == "p")?;
        let gnd = p.text().collect::<String>().trim().to_string();
        if !gnd.is_empty() {
            return Some(gnd);
        }
    }
    None
}

/// `/de/subjects/idrec/sn/bio/id/{digits}` — applied to a single `href`
/// attribute value, so the regex surface is bounded to the URL path
/// rather than the whole HTML document.
static RE_SUBJECT_ID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^/[a-z]+/subjects/idrec/sn/bio/id/(\d+)").unwrap()
});

fn dd_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("dd").unwrap());
    &S
}
fn h2_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("h2").unwrap());
    &S
}
fn h3_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("h3").unwrap());
    &S
}
fn a_selector() -> &'static Selector {
    static S: LazyLock<Selector> = LazyLock::new(|| Selector::parse("a[href]").unwrap());
    &S
}

fn collect_subject_ids(dd: ElementRef<'_>) -> Vec<String> {
    dd.select(a_selector())
        .filter_map(|a| {
            let href = a.value().attr("href")?;
            RE_SUBJECT_ID
                .captures(href)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper6976 {
        BespokeScraper6976 {
            app: std::sync::Arc::new(crate::app_state::get_test_app()),
        }
    }

    #[test]
    fn test_6976_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 6976);
    }

    #[test]
    fn test_6976_keep_existing_names_default_false() {
        let s = make_scraper();
        assert!(!s.keep_existing_names());
    }

    #[test]
    fn test_6976_testing_default_false() {
        let s = make_scraper();
        assert!(!s.testing());
    }

    // ---- get_main_gnd_from_text ----

    #[test]
    fn test_6976_get_main_gnd_from_text_found() {
        let html = r#"<h2>GND-Nummer</h2><p>118522426</p>"#;
        assert_eq!(
            BespokeScraper6976::get_main_gnd_from_text(html),
            Some("118522426".to_string())
        );
    }

    #[test]
    fn test_6976_get_main_gnd_from_text_with_whitespace() {
        // Whitespace text nodes between the <h2> and <p> are skipped by the
        // sibling-element walk.
        let html = "<h2>GND-Nummer</h2>   <p>10234567X</p>";
        assert_eq!(
            BespokeScraper6976::get_main_gnd_from_text(html),
            Some("10234567X".to_string())
        );
    }

    #[test]
    fn test_6976_get_main_gnd_from_text_not_found() {
        let html = r#"<h2>Some Other Header</h2><p>not-a-gnd</p>"#;
        assert!(BespokeScraper6976::get_main_gnd_from_text(html).is_none());
    }

    #[test]
    fn test_6976_get_main_gnd_from_text_empty() {
        assert!(BespokeScraper6976::get_main_gnd_from_text("").is_none());
    }

    #[test]
    fn test_6976_get_main_gnd_from_text_stops_at_first_p() {
        let html = r#"<h2>GND-Nummer</h2><p>118522426</p><p>other content</p>"#;
        assert_eq!(
            BespokeScraper6976::get_main_gnd_from_text(html),
            Some("118522426".to_string())
        );
    }

    #[test]
    fn test_6976_get_main_gnd_from_text_gnd_with_x_suffix() {
        let html = r#"<h2>GND-Nummer</h2><p>10234567X</p>"#;
        assert_eq!(
            BespokeScraper6976::get_main_gnd_from_text(html),
            Some("10234567X".to_string())
        );
    }

    #[test]
    fn test_6976_get_main_gnd_tolerates_attributes_on_p() {
        // Robustness gain over the previous regex, which required the
        // literal `<p>` open tag and would miss a styled paragraph.
        let html = r#"<h2>GND-Nummer</h2><p class="meta">118522426</p>"#;
        assert_eq!(
            BespokeScraper6976::get_main_gnd_from_text(html),
            Some("118522426".to_string())
        );
    }

    // ---- collect_subject_ids ----

    #[test]
    fn test_6976_collect_subject_ids_single() {
        let html = r#"<dd><a href="/de/subjects/idrec/sn/bio/id/42">x</a></dd>"#;
        let doc = Html::parse_fragment(html);
        let dd = doc.select(dd_selector()).next().unwrap();
        assert_eq!(collect_subject_ids(dd), vec!["42".to_string()]);
    }

    #[test]
    fn test_6976_collect_subject_ids_multiple() {
        let html = r#"
            <dd>
                <a href="/de/subjects/idrec/sn/bio/id/10">a</a>
                <a href="/en/subjects/idrec/sn/bio/id/20">b</a>
            </dd>"#;
        let doc = Html::parse_fragment(html);
        let dd = doc.select(dd_selector()).next().unwrap();
        assert_eq!(
            collect_subject_ids(dd),
            vec!["10".to_string(), "20".to_string()]
        );
    }

    #[test]
    fn test_6976_collect_subject_ids_skips_non_bio_paths() {
        let html = r#"<dd><a href="/de/subjects/idrec/sn/art/id/99">x</a></dd>"#;
        let doc = Html::parse_fragment(html);
        let dd = doc.select(dd_selector()).next().unwrap();
        assert!(collect_subject_ids(dd).is_empty());
    }

    // ---- H3_TEXT_TO_PROP ----

    #[test]
    fn test_6976_h3_text_to_prop_contains_expected_relations() {
        let table: std::collections::HashMap<&str, usize> =
            H3_TEXT_TO_PROP.iter().copied().collect();
        assert_eq!(table.get("Vater:"), Some(&22));
        assert_eq!(table.get("Mutter:"), Some(&25));
        assert_eq!(table.get("Partner:"), Some(&26));
        assert_eq!(table.get("Verwandte:"), Some(&1038));
    }
}
