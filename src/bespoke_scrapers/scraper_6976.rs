use crate::{app_state::AppState, entry::Entry};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Hessian Biography person (6976)

#[derive(Debug)]
pub struct BespokeScraper6976 {
    pub(super) app: AppState,
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

impl BespokeScraper6976 {
    /// Scraper-specific `add_missing_aux` that walks the Hessian biography HTML
    /// and attaches GND, family-relation aux values, and MnM relations.
    pub(crate) async fn add_missing_aux_6976(&self, entry_id: usize) -> Result<()> {
        const KEYS2PROP: &[(&str, usize)] = &[
            ("<h3>Vater:</h3>", 22),
            ("<h3>Mutter:</h3>", 25),
            ("<h3>Partner:</h3>", 26),
            ("<h3>Verwandte:</h3>", 1038),
        ];
        lazy_static! {
            static ref RE_DD: Regex = Regex::new(r#"<dd>(.+?)</dd>"#).unwrap();
            static ref RE_SUBJECT: Regex =
                Regex::new(r#"<a href="/[a-z]+/subjects/idrec/sn/bio/id/(\d+)""#).unwrap();
        }
        let entry = Entry::from_id(entry_id, &self.app).await?;
        let existing_aux = entry.get_aux().await?;
        let url = &entry.ext_url;
        let text = self.load_single_line_text_from_url(url).await?;

        if !existing_aux.iter().any(|aux| aux.prop_numeric() == 227) {
            if let Some(gnd) = Self::get_main_gnd_from_text(&text) {
                entry.set_auxiliary(227, Some(gnd)).await?;
            }
        }

        for cap_dd_group in RE_DD.captures_iter(&text) {
            let cap_dd = cap_dd_group.get(1).unwrap().as_str();
            let subject_ids = RE_SUBJECT
                .captures_iter(cap_dd)
                .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
                .collect::<Vec<String>>();
            if subject_ids.is_empty() {
                continue;
            }
            for (key, prop_numeric) in KEYS2PROP {
                if cap_dd.contains(key) {
                    let _ = self
                        .attach_subjects_as_aux(*prop_numeric, &subject_ids, &entry)
                        .await;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn attach_subjects_as_aux(
        &self,
        prop_numeric: usize,
        subject_ids: &[String],
        entry: &Entry,
    ) -> Result<()> {
        for subject_id in subject_ids {
            if let Some(gnd) = self.get_subject_gnd(subject_id).await {
                let query = format!("haswbstatement:P227={gnd}");
                let items_with_gnd = self
                    .app
                    .wikidata()
                    .search_api(&query)
                    .await
                    .unwrap_or_default();
                if items_with_gnd.len() == 1 {
                    let item = items_with_gnd[0].clone();
                    let _ = entry.set_auxiliary(prop_numeric, Some(item)).await;
                } else if let Ok(target_entry) =
                    Entry::from_ext_id(self.catalog_id(), &gnd, &self.app).await
                {
                    if let Ok(target_entry_id) = target_entry.get_valid_id() {
                        let _ = entry.add_mnm_relation(prop_numeric, target_entry_id).await;
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
        lazy_static! {
            static ref RE_GND: Regex = Regex::new(r#"<h2>GND-Nummer</h2>\s*<p>(.+?)</p>"#).unwrap();
        }
        let captures = RE_GND.captures(text)?;
        Some(captures[1].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper6976 {
        BespokeScraper6976 {
            app: crate::app_state::get_test_app(),
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
        // The regex uses \s* between the two tags, so whitespace is allowed
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
    fn test_6976_get_main_gnd_from_text_stops_at_closing_p() {
        // Should not greedily consume beyond the first </p>
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

    // ---- RE_DD / RE_SUBJECT regex logic ----

    #[test]
    fn test_6976_re_dd_captures_dd_content() {
        let re_dd = Regex::new(r#"<dd>(.+?)</dd>"#).unwrap();
        let html = r#"<dd>some content here</dd>"#;
        let caps = re_dd.captures(html).unwrap();
        assert_eq!(caps.get(1).unwrap().as_str(), "some content here");
    }

    #[test]
    fn test_6976_re_dd_captures_multiple() {
        let re_dd = Regex::new(r#"<dd>(.+?)</dd>"#).unwrap();
        let html = r#"<dd>first</dd><dd>second</dd>"#;
        let results: Vec<&str> = re_dd
            .captures_iter(html)
            .filter_map(|c| c.get(1).map(|m| m.as_str()))
            .collect();
        assert_eq!(results, vec!["first", "second"]);
    }

    #[test]
    fn test_6976_re_subject_captures_id() {
        let re_subject = Regex::new(r#"<a href="/[a-z]+/subjects/idrec/sn/bio/id/(\d+)""#).unwrap();
        let html = r#"<a href="/de/subjects/idrec/sn/bio/id/42""#;
        let caps = re_subject.captures(html).unwrap();
        assert_eq!(caps.get(1).unwrap().as_str(), "42");
    }

    #[test]
    fn test_6976_re_subject_captures_multiple_ids() {
        let re_subject = Regex::new(r#"<a href="/[a-z]+/subjects/idrec/sn/bio/id/(\d+)""#).unwrap();
        let html =
            r#"<a href="/de/subjects/idrec/sn/bio/id/10"<a href="/en/subjects/idrec/sn/bio/id/20""#;
        let ids: Vec<&str> = re_subject
            .captures_iter(html)
            .filter_map(|c| c.get(1).map(|m| m.as_str()))
            .collect();
        assert_eq!(ids, vec!["10", "20"]);
    }

    #[test]
    fn test_6976_re_subject_does_not_match_non_bio() {
        let re_subject = Regex::new(r#"<a href="/[a-z]+/subjects/idrec/sn/bio/id/(\d+)""#).unwrap();
        // Different path segment
        let html = r#"<a href="/de/subjects/idrec/sn/art/id/99""#;
        assert!(re_subject.captures(html).is_none());
    }

    // ---- KEYS2PROP lookup logic (unit-tested as plain data) ----

    #[test]
    fn test_6976_keys2prop_contains_expected_relations() {
        const KEYS2PROP: &[(&str, usize)] = &[
            ("<h3>Vater:</h3>", 22),
            ("<h3>Mutter:</h3>", 25),
            ("<h3>Partner:</h3>", 26),
            ("<h3>Verwandte:</h3>", 1038),
        ];
        assert!(
            KEYS2PROP
                .iter()
                .any(|(k, p)| *k == "<h3>Vater:</h3>" && *p == 22)
        );
        assert!(
            KEYS2PROP
                .iter()
                .any(|(k, p)| *k == "<h3>Mutter:</h3>" && *p == 25)
        );
        assert!(
            KEYS2PROP
                .iter()
                .any(|(k, p)| *k == "<h3>Partner:</h3>" && *p == 26)
        );
        assert!(
            KEYS2PROP
                .iter()
                .any(|(k, p)| *k == "<h3>Verwandte:</h3>" && *p == 1038)
        );
    }
}
