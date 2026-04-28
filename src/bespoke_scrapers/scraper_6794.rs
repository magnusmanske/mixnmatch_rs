use crate::app_state::AppState;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use lazy_static::lazy_static;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Zentrales Personenregister aus den Beständen des Herder-Instituts (6794)

#[derive(Debug)]
pub struct BespokeScraper6794 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6794 {

    scraper_boilerplate!(6794);

    async fn run(&self) -> Result<()> {
        lazy_static! {
            static ref PROP_RE: Vec<(usize, Regex)> = {
                vec![(
                    227,
                    Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap(),
                )]
            };
        }

        // Run all existing entries for metadata
        let ext_id2entry_id = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        let futures = ext_id2entry_id
            .into_values()
            .map(|entry_id| self.add_missing_aux(entry_id, &PROP_RE))
            .collect::<Vec<_>>();

        // Run 5 in parallel
        let stream = futures::stream::iter(futures).buffer_unordered(5);
        let _ = stream.collect::<Vec<_>>().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    fn make_scraper() -> BespokeScraper6794 {
        BespokeScraper6794 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_6794_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 6794);
    }

    #[test]
    fn test_6794_keep_existing_names_default_false() {
        let s = make_scraper();
        // Default implementation returns false
        assert!(!s.keep_existing_names());
    }

    #[test]
    fn test_6794_testing_default_false() {
        let s = make_scraper();
        assert!(!s.testing());
    }

    /// Verify the GND regex used inside `run` matches the expected HTML pattern.
    #[test]
    fn test_6794_gnd_regex_matches_expected_html() {
        let re = Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap();
        let html = r#"<a href="http://d-nb.info/gnd/118522426""#;
        let caps = re.captures(html).unwrap();
        assert_eq!(&caps[1], "118522426");
    }

    #[test]
    fn test_6794_gnd_regex_does_not_match_other_urls() {
        let re = Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap();
        assert!(
            re.captures(r#"<a href="https://viaf.org/viaf/12345""#)
                .is_none()
        );
        assert!(
            re.captures(r#"<a href="https://d-nb.info/gnd/118522426""#)
                .is_none()
        );
    }

    #[test]
    fn test_6794_gnd_regex_captures_complex_id() {
        let re = Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap();
        // GND IDs can have trailing X characters
        let html = r#"<a href="http://d-nb.info/gnd/10234567X""#;
        let caps = re.captures(html).unwrap();
        assert_eq!(&caps[1], "10234567X");
    }

    #[test]
    fn test_6794_gnd_regex_stops_at_closing_quote() {
        let re = Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap();
        // Should not consume beyond the closing quote
        let html = r#"<a href="http://d-nb.info/gnd/118522426" class="external">"#;
        let caps = re.captures(html).unwrap();
        assert_eq!(&caps[1], "118522426");
    }
}
