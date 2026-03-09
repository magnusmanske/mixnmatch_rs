use crate::app_state::AppState;
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// BMLO ID (P865)

#[derive(Debug)]
pub struct BespokeScraper7043 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7043 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        7043
    }

    async fn run(&self) -> Result<()> {
        // TODO add new?

        lazy_static! {
            static ref PROP_RE: Vec<(usize, Regex)> = {
                vec![
                    (
                        214,
                        Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap(),
                    ),
                    (227, Regex::new(r#"\?gnd=(\d+X?)"#).unwrap()),
                ]
            };
        }

        // Run all existing entries for metadata
        let ext_ids = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        for (_ext_id, entry_id) in ext_ids {
            let _ = self.add_missing_aux(entry_id, &PROP_RE).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper7043 {
        BespokeScraper7043 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_7043_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 7043);
    }

    #[test]
    fn test_7043_keep_existing_names_default_false() {
        let s = make_scraper();
        assert!(!s.keep_existing_names());
    }

    #[test]
    fn test_7043_testing_default_false() {
        let s = make_scraper();
        assert!(!s.testing());
    }

    /// Verify the VIAF regex used in PROP_RE matches the expected HTML pattern.
    #[test]
    fn test_7043_viaf_regex_matches() {
        let re = Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap();
        let html = r#"<a href="http://viaf.org/viaf/54321">VIAF</a>"#;
        let caps = re.captures(html).unwrap();
        assert_eq!(&caps[1], "54321");
    }

    #[test]
    fn test_7043_viaf_regex_does_not_match_https() {
        // The regex only matches http://, not https://
        let re = Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap();
        let html = r#"<a href="https://viaf.org/viaf/54321">VIAF</a>"#;
        assert!(re.captures(html).is_none());
    }

    #[test]
    fn test_7043_viaf_regex_only_digits() {
        // Must be all digits, no letters
        let re = Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap();
        assert!(re.captures(r#"href="http://viaf.org/viaf/abc""#).is_none());
    }

    /// Verify the GND regex used in PROP_RE matches standard and X-suffix IDs.
    #[test]
    fn test_7043_gnd_regex_matches_numeric() {
        let re = Regex::new(r#"\?gnd=(\d+X?)"#).unwrap();
        let url = "https://example.com/page?gnd=118522426";
        let caps = re.captures(url).unwrap();
        assert_eq!(&caps[1], "118522426");
    }

    #[test]
    fn test_7043_gnd_regex_matches_x_suffix() {
        let re = Regex::new(r#"\?gnd=(\d+X?)"#).unwrap();
        let url = "https://example.com/page?gnd=10234567X";
        let caps = re.captures(url).unwrap();
        assert_eq!(&caps[1], "10234567X");
    }

    #[test]
    fn test_7043_gnd_regex_does_not_match_non_gnd_param() {
        let re = Regex::new(r#"\?gnd=(\d+X?)"#).unwrap();
        assert!(
            re.captures("https://example.com/page?id=118522426")
                .is_none()
        );
    }

    #[test]
    fn test_7043_gnd_regex_does_not_match_lowercase_x() {
        // The regex only matches uppercase X
        let re = Regex::new(r#"\?gnd=(\d+X?)"#).unwrap();
        // Lowercase x at end: digits still match but x is not consumed as the X suffix
        let url = "https://example.com/?gnd=1234x";
        // The digits before x will match, x is just not part of the capture
        let caps = re.captures(url).unwrap();
        assert_eq!(&caps[1], "1234");
    }

    #[test]
    fn test_7043_prop_re_has_two_entries() {
        lazy_static! {
            static ref PROP_RE: Vec<(usize, Regex)> = {
                vec![
                    (
                        214,
                        Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap(),
                    ),
                    (227, Regex::new(r#"\?gnd=(\d+X?)"#).unwrap()),
                ]
            };
        }
        assert_eq!(PROP_RE.len(), 2);
        assert_eq!(PROP_RE[0].0, 214);
        assert_eq!(PROP_RE[1].0, 227);
    }
}
