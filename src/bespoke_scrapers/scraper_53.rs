use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Genealogics

#[derive(Debug)]
pub struct BespokeScraper53 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper53 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        53
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let base_url =
            "https://www.genealogics.org/placesearch.php?tree=LEO&psearch=England&order=name&offset=";
        let client = self.http_client();

        // Warmup request (as in the original PHP)
        let _ = client.get(base_url).send().await;

        let mut entry_cache = vec![];
        let mut offset: u64 = 0;
        while offset < 131300 {
            let url = format!("{}{}", base_url, offset);
            let html = match client.get(&url).send().await {
                Ok(resp) => match resp.text().await {
                    Ok(text) if !text.is_empty() => text,
                    _ => {
                        offset += 100;
                        continue;
                    }
                },
                Err(_) => {
                    offset += 100;
                    continue;
                }
            };
            let entries = Self::parse_page(self.catalog_id(), &html);
            entry_cache.extend(entries);
            if entry_cache.len() >= 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
            offset += 100;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper53 {
    /// Parse a single page of Genealogics search results into entries.
    pub(crate) fn parse_page(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_WHITESPACE: Regex = Regex::new(r"\s+").unwrap();
            static ref RE_BLOCK: Regex = Regex::new(
                r#"<div class="titlebox"> <span class="subhead"><strong>Lived In</strong>(.*)"#
            )
            .unwrap();
            static ref RE_PERSON: Regex = Regex::new(
                r#"<a href="getperson\.php\?personID=(I\d+).*?">([^ <]+ [^<]*?)</a>"#
            )
            .unwrap();
        }

        // Collapse whitespace to single spaces
        let html = RE_WHITESPACE.replace_all(html, " ").to_string();

        // Find the "Lived In" block
        let block = match RE_BLOCK.captures(&html) {
            Some(caps) => caps.get(1).map(|m| m.as_str().to_string()),
            None => return vec![],
        };
        let block = match block {
            Some(b) => b,
            None => return vec![],
        };

        RE_PERSON
            .captures_iter(&block)
            .filter_map(|caps| {
                let id = caps.get(1)?.as_str().to_string();
                let raw_name = caps.get(2)?.as_str().to_string();
                let name = Self::clean_name(&raw_name);
                if name.is_empty() || id.is_empty() {
                    return None;
                }
                let ext_url = format!(
                    "https://www.genealogics.org/getperson.php?personID={}&tree=LEO",
                    id
                );
                let entry = Entry {
                    catalog: catalog_id,
                    ext_id: id,
                    ext_url,
                    ext_name: name,
                    ext_desc: "new entry".to_string(),
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

    /// Clean a person's name by stripping nobility titles, pipe-suffixes,
    /// commas, trailing numbers, duplicate surname endings, and "King of..."
    /// suffixes.
    pub(crate) fn clean_name(name: &str) -> String {
        lazy_static! {
            static ref RE_TITLES: Regex = Regex::new(
                r"\b(?:\d\.)?(?:Baron|Baronnes?|Baroness|Bt\.|Chevalier|Comte|Comtesse|Count|Countess|Dame|Duchess|Duke|Earl|Emir|Emperor|Empress|Graf|Gr[aä]fin|Grand Duke|Grand Duchess|Grossgraf|Herzog|Herzogin|Infanta|Infante|King|Knight|Kronprinz|Kronprinzessin|Lady|Lord|Marchioness|Margrave|Margravine|Marquess|Marquis|Marquise|Markgraf|Markgr[aä]fin|Prince|Princess|Prinz|Prinzessin|Queen|Ritter|Sir|Sultan|Tsarevna|Tsaritsa|Tsar|Vicomte|Vicomtesse|Viscount|Viscountess|[Vv]on|zu|de|di|del|della|dos|das|van|ten|ter|het) "
            )
            .unwrap();
            static ref RE_PIPE: Regex = Regex::new(r"\|\S+").unwrap();
            static ref RE_TRAILING_NUMBERS: Regex = Regex::new(r" [0-9-]+").unwrap();
            // Note: Rust regex doesn't support backreferences. We handle
            // duplicate-surname stripping in code instead.
            static ref RE_COMMA: Regex = Regex::new(r",").unwrap();
            static ref RE_KING_OF: Regex = Regex::new(r"King of.*").unwrap();
        }

        let mut s = name.to_string();
        s = RE_TITLES.replace_all(&s, "").to_string();
        s = RE_PIPE.replace_all(&s, "").to_string();
        s = s.replace(',', "");
        s = RE_TRAILING_NUMBERS.replace_all(&s, "").to_string();
        s = Self::strip_duplicate_surname(&s);
        s = RE_KING_OF.replace(&s, "").to_string();
        s = s.trim().to_string();
        s
    }

    /// Strips a trailing duplicate suffix, e.g. "John Smith Smith" -> "John Smith".
    /// Mirrors PHP: preg_replace('/( .*?)\1$/', '$1', $name)
    fn strip_duplicate_surname(s: &str) -> String {
        // Find the first space; the suffix after it is a candidate
        if let Some(first_space) = s.find(' ') {
            let suffix = &s[first_space..]; // " Smith Smith"
            let half = suffix.len() / 2;
            if half > 0 && suffix.len().is_multiple_of(2) && suffix[..half] == suffix[half..] {
                return format!("{}{}", &s[..first_space], &suffix[..half]);
            }
        }
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_53_clean_name_basic() {
        assert_eq!(BespokeScraper53::clean_name("John Smith"), "John Smith");
    }

    #[test]
    fn test_53_clean_name_strips_baron() {
        assert_eq!(
            BespokeScraper53::clean_name("Baron John Smith"),
            "John Smith"
        );
    }

    #[test]
    fn test_53_clean_name_strips_sir() {
        assert_eq!(
            BespokeScraper53::clean_name("Sir Walter Raleigh"),
            "Walter Raleigh"
        );
    }

    #[test]
    fn test_53_clean_name_strips_numbered_title() {
        assert_eq!(
            BespokeScraper53::clean_name("3.Duke John Smith"),
            "John Smith"
        );
    }

    #[test]
    fn test_53_clean_name_strips_pipe_suffix() {
        assert_eq!(
            BespokeScraper53::clean_name("John Smith|extra"),
            "John Smith"
        );
    }

    #[test]
    fn test_53_clean_name_strips_commas() {
        assert_eq!(
            BespokeScraper53::clean_name("Smith, John"),
            "Smith John"
        );
    }

    #[test]
    fn test_53_clean_name_strips_trailing_numbers() {
        assert_eq!(
            BespokeScraper53::clean_name("John Smith 1450-1510"),
            "John Smith"
        );
    }

    #[test]
    fn test_53_clean_name_strips_duplicate_surname() {
        // "John Smith Smith" -> "John Smith"
        assert_eq!(
            BespokeScraper53::clean_name("John Smith Smith"),
            "John Smith"
        );
    }

    #[test]
    fn test_53_clean_name_strips_king_of() {
        // "King " is first stripped by the title regex, leaving "Henry of England"
        // Then "King of.*" no longer matches. This mirrors the PHP behavior.
        assert_eq!(
            BespokeScraper53::clean_name("Henry King of England"),
            "Henry of England"
        );
    }

    #[test]
    fn test_53_clean_name_combined() {
        // Multiple cleanups at once
        let result = BespokeScraper53::clean_name("Sir Earl John Smith|suffix 1234");
        assert!(!result.contains("Sir"));
        assert!(!result.contains("Earl"));
        assert!(!result.contains("|suffix"));
        assert!(!result.contains("1234"));
    }

    #[test]
    fn test_53_clean_name_empty() {
        assert_eq!(BespokeScraper53::clean_name(""), "");
    }

    #[test]
    fn test_53_clean_name_princess() {
        assert_eq!(
            BespokeScraper53::clean_name("Princess Maria Theresa"),
            "Maria Theresa"
        );
    }

    #[test]
    fn test_53_parse_page_with_entries() {
        let html = r#"
        <div class="titlebox"> <span class="subhead"><strong>Lived In</strong>
        <a href="getperson.php?personID=I12345&tree=LEO">John Smith</a>
        <a href="getperson.php?personID=I67890&tree=LEO">Mary Johnson</a>
        </span></div>
        "#;
        let entries = BespokeScraper53::parse_page(53, html);
        assert_eq!(entries.len(), 2);

        let e0 = &entries[0].entry;
        assert_eq!(e0.ext_id, "I12345");
        assert_eq!(e0.ext_name, "John Smith");
        assert_eq!(e0.ext_desc, "new entry");
        assert_eq!(
            e0.ext_url,
            "https://www.genealogics.org/getperson.php?personID=I12345&tree=LEO"
        );
        assert_eq!(e0.catalog, 53);
        assert_eq!(e0.type_name, Some("Q5".to_string()));

        let e1 = &entries[1].entry;
        assert_eq!(e1.ext_id, "I67890");
        assert_eq!(e1.ext_name, "Mary Johnson");
    }

    #[test]
    fn test_53_parse_page_no_block() {
        let html = "<html><body>No relevant content here</body></html>";
        let entries = BespokeScraper53::parse_page(53, html);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_53_parse_page_empty() {
        let entries = BespokeScraper53::parse_page(53, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_53_parse_page_cleans_names() {
        let html = r#"
        <div class="titlebox"> <span class="subhead"><strong>Lived In</strong>
        <a href="getperson.php?personID=I11111&tree=LEO">Sir Baron William Cecil</a>
        </span></div>
        "#;
        let entries = BespokeScraper53::parse_page(53, html);
        assert_eq!(entries.len(), 1);
        let name = &entries[0].entry.ext_name;
        assert!(!name.contains("Sir"));
        assert!(!name.contains("Baron"));
        assert!(name.contains("William"));
        assert!(name.contains("Cecil"));
    }

    #[test]
    fn test_53_parse_page_whitespace_collapse() {
        // Ensure multi-line HTML with varied whitespace is handled
        let html = "<div class=\"titlebox\">\n  <span class=\"subhead\"><strong>Lived In</strong>\n\
            <a href=\"getperson.php?personID=I99999&tree=LEO\">Jane   Doe</a>\n\
            </span></div>";
        let entries = BespokeScraper53::parse_page(53, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "I99999");
    }

    #[test]
    fn test_53_parse_page_no_person_links() {
        let html = r#"
        <div class="titlebox"> <span class="subhead"><strong>Lived In</strong>
        No links here at all.
        </span></div>
        "#;
        let entries = BespokeScraper53::parse_page(53, html);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_53_ext_url_format() {
        let html = r#"
        <div class="titlebox"> <span class="subhead"><strong>Lived In</strong>
        <a href="getperson.php?personID=I55555&tree=LEO">Test Person</a>
        </span></div>
        "#;
        let entries = BespokeScraper53::parse_page(53, html);
        assert_eq!(entries.len(), 1);
        assert!(entries[0]
            .entry
            .ext_url
            .starts_with("https://www.genealogics.org/getperson.php?personID="));
        assert!(entries[0].entry.ext_url.contains("&tree=LEO"));
    }
}
