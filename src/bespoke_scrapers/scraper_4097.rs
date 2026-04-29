use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashSet;

use super::BespokeScraper;

// ______________________________________________________
// Biblio HIU Cas — Czech historical bibliography (4097)
//
// Per-letter A-Z fetch from `biblio.hiu.cas.cz/api/search`. Each row's
// name comes in `Lastname, Firstname` form, possibly with trailing
// dates `Lastname, Firstname, 1900-1980`. The PHP uses two regex passes:
// first to split a trailing 3+-digit suffix off into the description,
// then to flip "Lastname, Firstname" → "Firstname Lastname". The `id`
// field becomes a P6656 (NK ČR / Czech national authority) aux value.

#[derive(Debug)]
pub struct BespokeScraper4097 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4097 {
    scraper_boilerplate!(4097);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in 'A'..='Z' {
            let url = Self::build_url(letter);
            let json: serde_json::Value = client.get(&url).send().await?.json().await?;
            let content = match json["result"]["content"].as_array() {
                Some(c) => c,
                None => continue,
            };
            for c in content {
                if let Some(ee) = Self::parse_item(self.catalog_id(), c) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4097 {
    /// The query string is enormous and parameterised only by `prefix`.
    /// Building it in code keeps the URL composition close to the test
    /// surface; tests pin the prefix substitution.
    pub(crate) fn build_url(letter: char) -> String {
        let fonds = [
            1, 7, 8, 5, 9, 10, 6, 12, 13, 14, 15, 16, 19, 3, 4, 31, 45, 52, 56, 55, 58,
        ];
        let related: String = fonds
            .iter()
            .map(|f| format!("&recordRelatedRecordFond={f}"))
            .collect();
        format!(
            "https://biblio.hiu.cas.cz/api/search\
?exports=portaroSearchItemParagraph,portaroSearchItemMoreParagraph,portaroSearchItemAuthorityParagraph\
&fond=31\
&kind=document&kind=authority\
&pageNumber=1&pageSize=20000\
&prefix={letter}{related}\
&sorting=PNAZEV\
&type=authority-index\
&format=json"
        )
    }

    pub(crate) fn parse_item(
        catalog_id: usize,
        c: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let record_uuid = c.get("recordUuid")?.as_str()?;
        if record_uuid.is_empty() {
            return None;
        }
        let raw_name = c.get("name")?.as_str()?.to_string();
        let (mut ext_name, mut ext_desc) = Self::split_name_and_dates(&raw_name);
        ext_name = Self::flip_lastname_first(&ext_name);
        ext_desc = ext_desc.trim().to_string();

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        if let Some(id) = c.get("id").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
            // P6656 = NK ČR / Czech national authority identifier
            aux.insert(AuxiliaryRow::new(6656, id.to_string()));
        }

        let entry = Entry {
            catalog: catalog_id,
            ext_id: record_uuid.to_string(),
            ext_name,
            ext_desc,
            ext_url: format!("https://biblio.hiu.cas.cz/records/{record_uuid}"),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            ..Default::default()
        })
    }

    /// PHP applies `|(.+?), (\d{3}.+)$|` first, splitting any trailing
    /// 3+-digit token (typically a date range) off into the description.
    /// Returns `(name, desc)` after that first pass; the comma-flip is
    /// applied separately.
    pub(crate) fn split_name_and_dates(raw: &str) -> (String, String) {
        lazy_static! {
            static ref RE_TRAILING_DATES: Regex =
                Regex::new(r"^(.+?), (\d{3}.+)$").expect("regex");
        }
        match RE_TRAILING_DATES.captures(raw) {
            Some(caps) => (
                caps[1].trim().to_string(),
                caps[2].trim().to_string(),
            ),
            None => (raw.to_string(), String::new()),
        }
    }

    /// PHP `|(.+), (.+)$|` is a *greedy* split — it splits at the LAST
    /// comma, not the first. Mirror that with a manual rsplit so names
    /// like "García Lorca, Federico" come out correctly as "Federico
    /// García Lorca" rather than "Lorca, Federico García".
    pub(crate) fn flip_lastname_first(name: &str) -> String {
        let trimmed = name.trim();
        if let Some(idx) = trimmed.rfind(", ") {
            let last = trimmed[..idx].trim();
            let first = trimmed[idx + 2..].trim();
            if !last.is_empty() && !first.is_empty() {
                return format!("{first} {last}");
            }
        }
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4097_build_url_substitutes_prefix() {
        let url = BespokeScraper4097::build_url('K');
        assert!(url.contains("prefix=K"));
        assert!(url.starts_with("https://biblio.hiu.cas.cz/api/search?"));
    }

    #[test]
    fn test_4097_split_name_and_dates_with_dates() {
        // PHP regex `|(.+?), (\d{3}.+)$|` is non-greedy on group 1, but
        // group 2 requires `\d{3}…` so the engine has to extend group 1
        // past "Komenský, Jan" before group 2 can latch onto "1592-1670".
        // The first pass therefore strips the dates only — the comma
        // flip happens in a second pass via `flip_lastname_first`.
        let (name, desc) = BespokeScraper4097::split_name_and_dates("Komenský, Jan, 1592-1670");
        assert_eq!(name, "Komenský, Jan");
        assert_eq!(desc, "1592-1670");
    }

    #[test]
    fn test_4097_split_name_and_dates_without_dates() {
        let (name, desc) = BespokeScraper4097::split_name_and_dates("Komenský, Jan");
        assert_eq!(name, "Komenský, Jan");
        assert_eq!(desc, "");
    }

    #[test]
    fn test_4097_split_name_only_year_no_split() {
        // "Foo, 12" — the `\d{3}` requires at least 3 digits.
        let (name, desc) = BespokeScraper4097::split_name_and_dates("Foo, 12");
        assert_eq!(name, "Foo, 12");
        assert_eq!(desc, "");
    }

    #[test]
    fn test_4097_flip_lastname_first_simple() {
        assert_eq!(
            BespokeScraper4097::flip_lastname_first("Komenský, Jan"),
            "Jan Komenský"
        );
    }

    #[test]
    fn test_4097_flip_lastname_first_no_comma_unchanged() {
        assert_eq!(
            BespokeScraper4097::flip_lastname_first("Anonymous"),
            "Anonymous"
        );
    }

    #[test]
    fn test_4097_flip_lastname_first_uses_last_comma() {
        // "García Lorca, Federico" must flip on the rightmost ", " so
        // the surname compound stays intact.
        assert_eq!(
            BespokeScraper4097::flip_lastname_first("García Lorca, Federico"),
            "Federico García Lorca"
        );
    }

    #[test]
    fn test_4097_parse_item_full() {
        let c = serde_json::json!({
            "recordUuid": "uuid-123",
            "name": "Komenský, Jan, 1592-1670",
            "id": "jx20030822018"
        });
        let ee = BespokeScraper4097::parse_item(4097, &c).unwrap();
        assert_eq!(ee.entry.ext_id, "uuid-123");
        assert_eq!(ee.entry.ext_name, "Jan Komenský");
        assert_eq!(ee.entry.ext_desc, "1592-1670");
        assert_eq!(
            ee.entry.ext_url,
            "https://biblio.hiu.cas.cz/records/uuid-123"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(6656, "jx20030822018".to_string())));
    }

    #[test]
    fn test_4097_parse_item_no_id_no_aux() {
        let c = serde_json::json!({
            "recordUuid": "uuid-x",
            "name": "Anonymous"
        });
        let ee = BespokeScraper4097::parse_item(4097, &c).unwrap();
        assert!(ee.aux.is_empty());
    }

    #[test]
    fn test_4097_parse_item_empty_id_no_aux() {
        let c = serde_json::json!({
            "recordUuid": "uuid-x",
            "name": "Anonymous",
            "id": ""
        });
        let ee = BespokeScraper4097::parse_item(4097, &c).unwrap();
        assert!(ee.aux.is_empty());
    }

    #[test]
    fn test_4097_parse_item_missing_uuid_skipped() {
        let c = serde_json::json!({"name": "Foo"});
        assert!(BespokeScraper4097::parse_item(4097, &c).is_none());
    }

    #[test]
    fn test_4097_parse_item_missing_name_skipped() {
        let c = serde_json::json!({"recordUuid": "x"});
        assert!(BespokeScraper4097::parse_item(4097, &c).is_none());
    }
}
