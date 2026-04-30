use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// GEMET – General Multilingual Environmental Thesaurus (catalog 5959).
// Downloads a line-oriented RDF/XML export and adds any concept not yet in the catalog.

#[derive(Debug)]
pub struct BespokeScraper5959 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5959 {
    scraper_boilerplate!(5959);

    async fn run(&self) -> Result<()> {
        let url = "https://www.eionet.europa.eu/gemet/exports/latest/en/gemet-definitions.rdf";
        let text = self
            .http_client()
            .get(url)
            .send()
            .await?
            .text()
            .await?;

        let existing = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;

        let entries = Self::parse_rdf(self.catalog_id(), &text, &existing);
        let mut cache = vec![];
        for ee in entries {
            cache.push(ee);
            self.maybe_flush_cache(&mut cache).await?;
        }
        self.process_cache(&mut cache).await?;
        Ok(())
    }
}

impl BespokeScraper5959 {
    /// Parse the GEMET RDF export line by line and return entries absent from `existing`.
    ///
    /// Each concept block looks like:
    /// ```xml
    /// <rdf:Description rdf:about="concept/100">
    ///   <skos:prefLabel>administrative body</skos:prefLabel>
    ///   <skos:definition>Any governmental agency …</skos:definition>
    /// </rdf:Description>
    /// ```
    pub(crate) fn parse_rdf(
        catalog_id: usize,
        text: &str,
        existing: &std::collections::HashMap<String, usize>,
    ) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ABOUT: Regex =
                Regex::new(r#"rdf:about="concept/(\d+)""#).unwrap();
            static ref RE_LABEL: Regex =
                Regex::new(r#"<skos:prefLabel>(.+?)</skos:prefLabel>"#).unwrap();
            static ref RE_DEF: Regex =
                Regex::new(r#"<skos:definition>(.+?)</skos:definition>"#).unwrap();
        }

        let mut entries = vec![];
        let mut id = String::new();
        let mut label = String::new();
        let mut desc = String::new();

        for line in text.lines() {
            if let Some(caps) = RE_ABOUT.captures(line) {
                id = caps[1].to_string();
                label.clear();
                desc.clear();
            } else if let Some(caps) = RE_LABEL.captures(line) {
                label = caps[1].to_string();
            } else if let Some(caps) = RE_DEF.captures(line) {
                desc = caps[1].to_string();
            } else if line.contains("</rdf:Description>") {
                if !id.is_empty() && !label.is_empty() && !existing.contains_key(&id) {
                    entries.push(ExtendedEntry {
                        entry: Entry {
                            catalog: catalog_id,
                            ext_id: id.clone(),
                            ext_url: format!(
                                "https://www.eionet.europa.eu/gemet/en/concept/{}",
                                id
                            ),
                            ext_name: label.clone(),
                            ext_desc: desc.clone(),
                            random: rand::rng().random(),
                            ..Default::default()
                        },
                        ..Default::default()
                    });
                }
                id.clear();
                label.clear();
                desc.clear();
            }
        }
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;
    use std::collections::HashMap;

    fn make_scraper() -> BespokeScraper5959 {
        BespokeScraper5959 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_5959_catalog_id() {
        assert_eq!(make_scraper().catalog_id(), 5959);
    }

    #[test]
    fn test_5959_parse_single_concept() {
        let rdf = r#"
    <rdf:Description rdf:about="concept/100">
    <skos:prefLabel>administrative body</skos:prefLabel>
    <skos:definition>Any governmental agency or organization.</skos:definition>
    </rdf:Description>
"#;
        let entries = BespokeScraper5959::parse_rdf(5959, rdf, &HashMap::new());
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.entry.ext_id, "100");
        assert_eq!(e.entry.ext_name, "administrative body");
        assert_eq!(e.entry.ext_desc, "Any governmental agency or organization.");
        assert_eq!(
            e.entry.ext_url,
            "https://www.eionet.europa.eu/gemet/en/concept/100"
        );
    }

    #[test]
    fn test_5959_parse_concept_without_definition() {
        let rdf = r#"
    <rdf:Description rdf:about="concept/200">
    <skos:prefLabel>animal life</skos:prefLabel>
    </rdf:Description>
"#;
        let entries = BespokeScraper5959::parse_rdf(5959, rdf, &HashMap::new());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_name, "animal life");
        assert_eq!(entries[0].entry.ext_desc, "");
    }

    #[test]
    fn test_5959_skips_existing_ids() {
        let rdf = r#"
    <rdf:Description rdf:about="concept/100">
    <skos:prefLabel>already known</skos:prefLabel>
    </rdf:Description>
"#;
        let mut existing = HashMap::new();
        existing.insert("100".to_string(), 42usize);
        let entries = BespokeScraper5959::parse_rdf(5959, rdf, &existing);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5959_skips_entry_without_label() {
        let rdf = r#"
    <rdf:Description rdf:about="concept/300">
    <skos:definition>Definition without label.</skos:definition>
    </rdf:Description>
"#;
        let entries = BespokeScraper5959::parse_rdf(5959, rdf, &HashMap::new());
        assert!(entries.is_empty());
    }

    #[test]
    fn test_5959_parse_multiple_concepts() {
        let rdf = r#"
    <rdf:Description rdf:about="concept/1">
    <skos:prefLabel>first</skos:prefLabel>
    </rdf:Description>
    <rdf:Description rdf:about="concept/2">
    <skos:prefLabel>second</skos:prefLabel>
    </rdf:Description>
"#;
        let entries = BespokeScraper5959::parse_rdf(5959, rdf, &HashMap::new());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "1");
        assert_eq!(entries[1].entry.ext_id, "2");
    }
}
