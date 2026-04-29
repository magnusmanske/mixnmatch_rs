use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Meyers dictionary lemmata (4361)
//
// Per-letter A-Z fetch from woerterbuchnetz.de's dictionary API. Each
// entry is a Meyers Konversations-Lexikon lemma identified by `lemid`.
// Lemma strings arrive as HTML-entity-encoded text (`&auml;`, `&ouml;`,
// …); decoding via the `html_escape` crate matches PHP's
// `html_entity_decode($v->lemma, ENT_COMPAT, 'UTF-8')`. No type set in
// the source script.

#[derive(Debug)]
pub struct BespokeScraper4361 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4361 {
    scraper_boilerplate!(4361);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in 'A'..='Z' {
            let url = format!(
                "https://api.woerterbuchnetz.de/dictionaries/Meyers/lemmata/lemid/{letter}00000/100000/json"
            );
            // Defensive: the PHP `if (!isset($json) or $json == null) continue;`
            // — the API sometimes returns an empty body or non-JSON when
            // there are no lemmata starting with this letter.
            let response = match client.get(&url).send().await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(_) => continue,
            };
            let arr = match json.as_array() {
                Some(arr) => arr,
                None => continue,
            };
            for v in arr {
                if let Some(ee) = Self::parse_item(self.catalog_id(), v) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4361 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        v: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = v.get("lemid")?.as_str()?.to_string();
        if id.is_empty() {
            return None;
        }
        let raw_lemma = v.get("lemma")?.as_str()?;
        let name = html_escape::decode_html_entities(raw_lemma).into_owned();
        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name: name,
            ext_url: format!("https://www.woerterbuchnetz.de/Meyers?lemid={id}"),
            random: rand::rng().random(),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4361_parse_item_basic() {
        let v = serde_json::json!({"lemid": "A00001", "lemma": "Aachen"});
        let ee = BespokeScraper4361::parse_item(4361, &v).unwrap();
        assert_eq!(ee.entry.ext_id, "A00001");
        assert_eq!(ee.entry.ext_name, "Aachen");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.woerterbuchnetz.de/Meyers?lemid=A00001"
        );
        // No type_name set in the source script.
        assert_eq!(ee.entry.type_name, None);
    }

    #[test]
    fn test_4361_parse_item_decodes_html_entities() {
        let v = serde_json::json!({"lemid": "A12345", "lemma": "M&auml;rchen"});
        let ee = BespokeScraper4361::parse_item(4361, &v).unwrap();
        assert_eq!(ee.entry.ext_name, "Märchen");
    }

    #[test]
    fn test_4361_parse_item_decodes_numeric_entities() {
        let v = serde_json::json!({"lemid": "B1", "lemma": "&#196;ra"});
        let ee = BespokeScraper4361::parse_item(4361, &v).unwrap();
        assert_eq!(ee.entry.ext_name, "Ära");
    }

    #[test]
    fn test_4361_parse_item_missing_lemid_skipped() {
        let v = serde_json::json!({"lemma": "Foo"});
        assert!(BespokeScraper4361::parse_item(4361, &v).is_none());
    }

    #[test]
    fn test_4361_parse_item_empty_lemid_skipped() {
        let v = serde_json::json!({"lemid": "", "lemma": "Foo"});
        assert!(BespokeScraper4361::parse_item(4361, &v).is_none());
    }

    #[test]
    fn test_4361_parse_item_missing_lemma_skipped() {
        let v = serde_json::json!({"lemid": "A1"});
        assert!(BespokeScraper4361::parse_item(4361, &v).is_none());
    }
}
