use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;
use urlencoding::encode;

use super::BespokeScraper;

// ______________________________________________________
// biblionet.gr — Greek persons (5522)
//
// Per-letter scrape spanning the Greek alphabet, the digit "0", and
// A–Z. Each letter triggers a fixed `admin-ajax.php` POST that returns
// up to 5000 persons. Response shape is `[ [ {...}, {...}, … ] ]` —
// the items live inside the first element of the outer array.

#[derive(Debug)]
pub struct BespokeScraper5522 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5522 {
    scraper_boilerplate!(5522);

    async fn run(&self) -> Result<()> {
        let url = "https://biblionet.gr/wp-admin/admin-ajax.php";
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in Self::LETTERS {
            let body = format!(
                "action=return_persons&letter={}&word=&kind=1&page=1&persons=5000&order=aa",
                encode(letter)
            );
            let response = match client
                .post(url)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };
            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(_) => continue,
            };
            let inner = match json.get(0).and_then(|v| v.as_array()) {
                Some(arr) => arr,
                None => continue,
            };
            for p in inner {
                if let Some(ee) = Self::parse_item(self.catalog_id(), p) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper5522 {
    /// Greek alphabet (Α-Ω), then digit "0" (a deliberate placeholder
    /// in the PHP — surnames starting with a numeric prefix), then
    /// Latin A-Z. Mirrors the order in scripts/json_scraper.php; the
    /// upstream returns the same shape regardless of script.
    pub(crate) const LETTERS: &'static [&'static str] = &[
        "Α", "Β", "Γ", "Δ", "Ε", "Ζ", "Η", "Θ", "Ι", "Κ", "Λ", "Μ", "Ν", "Ξ", "0", "Π", "Ρ",
        "Σ", "Τ", "Υ", "Φ", "Χ", "Ψ", "Ω", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
        "K", "L", "M", "N", "0", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
    ];

    pub(crate) fn parse_item(
        catalog_id: usize,
        p: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = Self::stringify(p.get("PersonsID")?)?;
        if id.is_empty() {
            return None;
        }
        let ext_name = p
            .get("Persons1")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if ext_name.is_empty() {
            return None;
        }
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            // PHP keeps the URL path percent-encoded (Greek "πρόσωπο");
            // mirror that literally.
            ext_url: format!(
                "https://biblionet.gr/%CF%80%CF%81%CE%BF%CF%83%CF%89%CF%80%CE%BF/?personid={id}"
            ),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
    }

    fn stringify(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_5522_letters_contains_greek_and_latin() {
        assert!(BespokeScraper5522::LETTERS.contains(&"Α"));
        assert!(BespokeScraper5522::LETTERS.contains(&"Ω"));
        assert!(BespokeScraper5522::LETTERS.contains(&"A"));
        assert!(BespokeScraper5522::LETTERS.contains(&"Z"));
        assert!(BespokeScraper5522::LETTERS.contains(&"0"));
    }

    #[test]
    fn test_5522_parse_item_full() {
        let p = serde_json::json!({"PersonsID": 100, "Persons1": "Παπαδόπουλος Νίκος"});
        let ee = BespokeScraper5522::parse_item(5522, &p).unwrap();
        assert_eq!(ee.entry.ext_id, "100");
        assert_eq!(ee.entry.ext_name, "Παπαδόπουλος Νίκος");
        assert_eq!(
            ee.entry.ext_url,
            "https://biblionet.gr/%CF%80%CF%81%CE%BF%CF%83%CF%89%CF%80%CE%BF/?personid=100"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_5522_parse_item_string_id() {
        let p = serde_json::json!({"PersonsID": "abc", "Persons1": "Foo"});
        let ee = BespokeScraper5522::parse_item(5522, &p).unwrap();
        assert_eq!(ee.entry.ext_id, "abc");
    }

    #[test]
    fn test_5522_parse_item_trims_whitespace_in_name() {
        let p = serde_json::json!({"PersonsID": 1, "Persons1": "  Foo  "});
        let ee = BespokeScraper5522::parse_item(5522, &p).unwrap();
        assert_eq!(ee.entry.ext_name, "Foo");
    }

    #[test]
    fn test_5522_parse_item_missing_id_skipped() {
        let p = serde_json::json!({"Persons1": "Foo"});
        assert!(BespokeScraper5522::parse_item(5522, &p).is_none());
    }

    #[test]
    fn test_5522_parse_item_empty_name_skipped() {
        let p = serde_json::json!({"PersonsID": 1, "Persons1": "   "});
        assert!(BespokeScraper5522::parse_item(5522, &p).is_none());
    }
}
