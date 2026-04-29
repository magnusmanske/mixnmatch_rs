use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashSet;
use wikimisc::wikibase::LocaleString;

use super::BespokeScraper;

// ______________________________________________________
// Geschichtsquellen — autor (3386)
//
// JSON dump from a DataTables-style endpoint where each row is an object
// with stringified-integer keys ("0", "1", "2", "3"). Field "0._" is the
// author's name embedded in an `<a href="/autor/ID">Last, First</a>`
// anchor; "1._" is an alternate label that, when present, takes over as
// the entry name with the original flipped HTML-link name moved to
// `ext_desc` AND added as an alias (this matches PHP's `setAlias` call,
// which defaults to language=""). Field "2" is an optional GND ID,
// stored as P227 aux.

#[derive(Debug)]
pub struct BespokeScraper3386 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper3386 {
    scraper_boilerplate!(3386);

    async fn run(&self) -> Result<()> {
        let url = "https://www.geschichtsquellen.de/autor.json?item_id=0";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let rows = match json["data"].as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for row in rows {
            if let Some(ee) = Self::parse_item(self.catalog_id(), row) {
                entry_cache.push(ee);
                self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper3386 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        row: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let zero_html = row.get("0")?.get("_")?.as_str()?;
        let (id, html_name) = Self::extract_id_and_name(zero_html)?;
        let html_name_flipped = Self::flip_lastname_first(&html_name);

        let alt_name = row
            .get("1")
            .and_then(|v| v.get("_"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        // When the alt name exists, swap roles: the alt becomes ext_name,
        // the flipped HTML name becomes ext_desc *and* is added as an
        // alias (mirroring PHP `setAlias` with default empty language).
        let (ext_name, ext_desc, aliases) = match alt_name {
            Some(alt) => (
                alt.to_string(),
                html_name_flipped.clone(),
                vec![LocaleString::new("", html_name_flipped)],
            ),
            None => (html_name_flipped, String::new(), vec![]),
        };

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        if let Some(gnd) = row.get("2").and_then(Self::stringify_field) {
            if !gnd.is_empty() {
                // P227 = GND ID
                aux.insert(AuxiliaryRow::new(227, gnd));
            }
        }

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!("https://www.geschichtsquellen.de/autor/{id}"),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };

        Some(ExtendedEntry {
            entry,
            aux,
            aliases,
            ..Default::default()
        })
    }

    /// Pull `(id, name)` out of `<a href="/autor/ID">NAME</a>`. Returns
    /// `None` for rows whose anchor doesn't match — those rows are
    /// skipped by the PHP `else continue` path.
    pub(crate) fn extract_id_and_name(html: &str) -> Option<(String, String)> {
        lazy_static! {
            static ref RE_ANCHOR: Regex =
                Regex::new(r#"<a href="/autor/(\d+)">(.+?)</a>"#).expect("regex");
        }
        let caps = RE_ANCHOR.captures(html)?;
        Some((
            caps.get(1)?.as_str().to_string(),
            caps.get(2)?.as_str().to_string(),
        ))
    }

    /// "Lastname, Firstname" → "Firstname Lastname". Names without a
    /// comma are returned unchanged.
    pub(crate) fn flip_lastname_first(name: &str) -> String {
        lazy_static! {
            static ref RE_LASTNAME_FIRST: Regex =
                Regex::new(r"^(.+), (.+)$").expect("regex");
        }
        match RE_LASTNAME_FIRST.captures(name) {
            Some(caps) => format!("{} {}", &caps[2], &caps[1]),
            None => name.to_string(),
        }
    }

    /// PHP's `"{$x->two}"` happily stringifies both numeric and string
    /// JSON values; mirror that. Returns `None` for `null`/missing.
    fn stringify_field(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.to_string()),
            other => Some(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_3386_extract_id_and_name() {
        assert_eq!(
            BespokeScraper3386::extract_id_and_name(
                r#"<a href="/autor/12345">Bach, Johann</a>"#
            ),
            Some(("12345".to_string(), "Bach, Johann".to_string()))
        );
    }

    #[test]
    fn test_3386_extract_id_and_name_no_match() {
        assert_eq!(BespokeScraper3386::extract_id_and_name("plain text"), None);
    }

    #[test]
    fn test_3386_flip_lastname_first() {
        assert_eq!(
            BespokeScraper3386::flip_lastname_first("Bach, Johann"),
            "Johann Bach"
        );
    }

    #[test]
    fn test_3386_flip_lastname_first_no_comma_unchanged() {
        assert_eq!(
            BespokeScraper3386::flip_lastname_first("Hildegard"),
            "Hildegard"
        );
    }

    #[test]
    fn test_3386_parse_item_minimal_no_alt_no_gnd() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/100">Author, Anonymous</a>"#}
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        assert_eq!(ee.entry.ext_id, "100");
        assert_eq!(ee.entry.ext_name, "Anonymous Author");
        assert_eq!(ee.entry.ext_desc, "");
        assert_eq!(ee.entry.ext_url, "https://www.geschichtsquellen.de/autor/100");
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert!(ee.aux.is_empty());
        assert!(ee.aliases.is_empty());
    }

    #[test]
    fn test_3386_parse_item_with_alt_name_swaps_and_aliases() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/200">Aquinas, Thomas</a>"#},
            "1": {"_": "Thomas of Aquino"}
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        // alt name takes over ext_name; flipped HTML name moves to desc + alias
        assert_eq!(ee.entry.ext_name, "Thomas of Aquino");
        assert_eq!(ee.entry.ext_desc, "Thomas Aquinas");
        assert_eq!(ee.aliases.len(), 1);
        assert_eq!(
            ee.aliases[0],
            LocaleString::new("", "Thomas Aquinas".to_string())
        );
    }

    #[test]
    fn test_3386_parse_item_with_gnd() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/300">Bach, Johann</a>"#},
            "2": "118505165"
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(227, "118505165".to_string())));
    }

    #[test]
    fn test_3386_parse_item_with_numeric_gnd() {
        // PHP coerces `"{$x->two}"`, so a numeric JSON value still becomes a string.
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/400">Test, Author</a>"#},
            "2": 118505165
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(227, "118505165".to_string())));
    }

    #[test]
    fn test_3386_parse_item_empty_gnd_skipped() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/500">Test, Author</a>"#},
            "2": ""
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        assert!(ee.aux.is_empty());
    }

    #[test]
    fn test_3386_parse_item_empty_alt_treated_as_missing() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/autor/600">Bach, Johann</a>"#},
            "1": {"_": ""}
        });
        let ee = BespokeScraper3386::parse_item(3386, &row).unwrap();
        // Empty alt → no swap, ext_name stays as flipped HTML name.
        assert_eq!(ee.entry.ext_name, "Johann Bach");
        assert_eq!(ee.entry.ext_desc, "");
        assert!(ee.aliases.is_empty());
    }

    #[test]
    fn test_3386_parse_item_bad_anchor_skipped() {
        let row = serde_json::json!({"0": {"_": "not a link"}});
        assert!(BespokeScraper3386::parse_item(3386, &row).is_none());
    }

    #[test]
    fn test_3386_parse_item_missing_zero_skipped() {
        let row = serde_json::json!({"1": {"_": "alt"}});
        assert!(BespokeScraper3386::parse_item(3386, &row).is_none());
    }

    #[test]
    fn test_3386_parse_item_missing_zero_underscore_skipped() {
        let row = serde_json::json!({"0": "no underscore field"});
        assert!(BespokeScraper3386::parse_item(3386, &row).is_none());
    }
}
