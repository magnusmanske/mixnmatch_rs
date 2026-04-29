use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// booksearch.party — books (6717)
//
// Single bulk JSON fetch keyed by ISBN-10. Mirrors the PHP
// `formatIsbn10` helper — accepts only 10-character ISBNs (last
// character may be `X`) and emits them in the canonical
// `D-DDDD-DDDD-D` form. Bad ISBNs are silently skipped. The PHP did
// not set `ext_url` (the `'url' =>` key is commented out), so we
// leave it empty here too. Type is Q106833 (book).

#[derive(Debug)]
pub struct BespokeScraper6717 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6717 {
    scraper_boilerplate!(6717);

    async fn run(&self) -> Result<()> {
        let url = "https://booksearch.party/books.json";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let arr = match json.as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for r in arr {
            if let Some(ee) = Self::parse_item(self.catalog_id(), r) {
                entry_cache.push(ee);
                self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper6717 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        r: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let isbn_raw = Self::stringify(r.get("ISBN")?)?;
        let isbn = Self::format_isbn10(&isbn_raw)?;
        let title = r
            .get("title")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if title.is_empty() {
            return None;
        }
        let authors = r.get("authors").and_then(|x| x.as_str()).unwrap_or("");
        let year = match r.get("year") {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            _ => String::new(),
        };
        let publisher = r.get("publisher").and_then(|x| x.as_str()).unwrap_or("");
        let ext_desc = format!("{authors}; {year}; {publisher}");

        let entry = Entry {
            catalog: catalog_id,
            ext_id: isbn,
            ext_name: title,
            ext_desc,
            // PHP intentionally leaves the url empty (the `'url' =>`
            // line in the source is commented out), so do the same.
            ext_url: String::new(),
            random: rand::rng().random(),
            // Q106833 = (printed) book
            type_name: Some("Q106833".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
    }

    /// PHP `formatIsbn10`: accepts only `\d{9}[0-9X]` and emits
    /// `D-DDDD-DDDD-D`. Returns `None` for anything that doesn't
    /// match the 10-character pattern (mirroring the PHP `false`
    /// sentinel used as a skip signal).
    pub(crate) fn format_isbn10(isbn: &str) -> Option<String> {
        lazy_static! {
            static ref RE_ISBN10: Regex = Regex::new(r"^\d{9}[0-9X]$").expect("regex");
        }
        if !RE_ISBN10.is_match(isbn) {
            return None;
        }
        Some(format!(
            "{}-{}-{}-{}",
            &isbn[..1],
            &isbn[1..5],
            &isbn[5..9],
            &isbn[9..10]
        ))
    }

    fn stringify(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_6717_format_isbn10_numeric_check_digit() {
        assert_eq!(
            BespokeScraper6717::format_isbn10("0306406152"),
            Some("0-3064-0615-2".to_string())
        );
    }

    #[test]
    fn test_6717_format_isbn10_x_check_digit() {
        assert_eq!(
            BespokeScraper6717::format_isbn10("123456789X"),
            Some("1-2345-6789-X".to_string())
        );
    }

    #[test]
    fn test_6717_format_isbn10_too_short_rejected() {
        assert!(BespokeScraper6717::format_isbn10("12345").is_none());
    }

    #[test]
    fn test_6717_format_isbn10_too_long_rejected() {
        assert!(BespokeScraper6717::format_isbn10("12345678901").is_none());
    }

    #[test]
    fn test_6717_format_isbn10_letters_rejected() {
        assert!(BespokeScraper6717::format_isbn10("ABCDEFGHIJ").is_none());
    }

    #[test]
    fn test_6717_format_isbn10_x_only_in_check_position() {
        // X is allowed only as the last character — middle X means reject.
        assert!(BespokeScraper6717::format_isbn10("12345X7890").is_none());
    }

    #[test]
    fn test_6717_format_isbn10_x_lowercase_rejected() {
        // PHP regex `[0-9X]` is case-sensitive — lowercase x is NOT accepted.
        assert!(BespokeScraper6717::format_isbn10("123456789x").is_none());
    }

    #[test]
    fn test_6717_parse_item_full() {
        let r = serde_json::json!({
            "ISBN": "0306406152",
            "title": "Combinatorial Optimization",
            "authors": "Papadimitriou, Steiglitz",
            "year": 1982,
            "publisher": "Prentice-Hall"
        });
        let ee = BespokeScraper6717::parse_item(6717, &r).unwrap();
        assert_eq!(ee.entry.ext_id, "0-3064-0615-2");
        assert_eq!(ee.entry.ext_name, "Combinatorial Optimization");
        assert_eq!(
            ee.entry.ext_desc,
            "Papadimitriou, Steiglitz; 1982; Prentice-Hall"
        );
        // PHP doesn't set a URL for this catalog — verify we don't either.
        assert_eq!(ee.entry.ext_url, "");
        assert_eq!(ee.entry.type_name, Some("Q106833".to_string()));
    }

    #[test]
    fn test_6717_parse_item_string_year() {
        let r = serde_json::json!({
            "ISBN": "0306406152",
            "title": "X",
            "year": "2000",
            "authors": "A",
            "publisher": "P"
        });
        let ee = BespokeScraper6717::parse_item(6717, &r).unwrap();
        assert!(ee.entry.ext_desc.contains("; 2000;"));
    }

    #[test]
    fn test_6717_parse_item_skips_bad_isbn() {
        let r = serde_json::json!({"ISBN": "junk", "title": "X"});
        assert!(BespokeScraper6717::parse_item(6717, &r).is_none());
    }

    #[test]
    fn test_6717_parse_item_skips_empty_title() {
        let r = serde_json::json!({"ISBN": "0306406152", "title": ""});
        assert!(BespokeScraper6717::parse_item(6717, &r).is_none());
    }

    #[test]
    fn test_6717_parse_item_missing_optional_fields() {
        let r = serde_json::json!({"ISBN": "0306406152", "title": "X"});
        let ee = BespokeScraper6717::parse_item(6717, &r).unwrap();
        // All three optional fields default to empty: "; ; " desc
        assert_eq!(ee.entry.ext_desc, "; ; ");
    }
}
