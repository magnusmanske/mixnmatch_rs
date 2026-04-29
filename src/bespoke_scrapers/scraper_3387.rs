use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashMap;

use super::BespokeScraper;

// ______________________________________________________
// Geschichtsquellen — werk (3387)
//
// Companion to scraper_3386 (autor). Each werk row may carry an
// `<a href="/autor/ID">…</a>` link in field "3._" pointing at an
// already-imported author. After inserting the werk we add a P50
// (author) MnM relation to the author's entry id (looked up in catalog
// 3386 via `get_all_external_ids`). This requires bypassing the default
// `process_cache` path because relations have to be added against a
// known entry id, which is only available after `insert_new` returns.

const AUTHOR_CATALOG_ID: usize = 3386;

#[derive(Debug)]
pub struct BespokeScraper3387 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper3387 {
    scraper_boilerplate!(3387);

    async fn run(&self) -> Result<()> {
        let url = "https://www.geschichtsquellen.de/werk.json?item_id=0";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let rows = match json["data"].as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };

        // Pre-fetch the author ext_id → entry_id map once, so per-row
        // linking is a HashMap probe rather than an N+1 round-trip.
        let author2entry: HashMap<String, usize> = self
            .app()
            .storage()
            .get_all_external_ids(AUTHOR_CATALOG_ID)
            .await?;
        let existing_ext_ids: HashMap<String, usize> = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;

        for row in rows {
            let parsed = match Self::parse_item(self.catalog_id(), row) {
                Some(p) => p,
                None => continue,
            };
            // PHP `if (isset($ext_ids[$o->id])) continue;`. We deliberately
            // skip already-imported entries entirely (no update path) to
            // mirror that behaviour.
            if existing_ext_ids.contains_key(&parsed.ee.entry.ext_id) {
                continue;
            }

            let mut ee = parsed.ee;
            ee.insert_new(self.app()).await?;

            if let Some(author_ext_id) = parsed.author_ext_id {
                if let Some(author_entry_id) = author2entry.get(&author_ext_id) {
                    // P50 = author. Best-effort: a missing author entry just
                    // means the autor catalog wasn't (yet) imported; not an error.
                    let _ = ee.entry.add_mnm_relation(50, *author_entry_id).await;
                }
            }
        }
        Ok(())
    }
}

/// Output of [`BespokeScraper3387::parse_item`]. Holds the entry to
/// insert plus the optional author ext_id that needs to be looked up in
/// the autor catalog after insertion.
#[derive(Debug)]
pub(crate) struct ParsedWerk {
    pub(crate) ee: ExtendedEntry,
    pub(crate) author_ext_id: Option<String>,
}

impl BespokeScraper3387 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        row: &serde_json::Value,
    ) -> Option<ParsedWerk> {
        let zero_html = row.get("0")?.get("_")?.as_str()?;
        let (ext_id, html_name) = Self::extract_id_and_name(zero_html)?;

        let alt_name = row
            .get("1")
            .and_then(|v| v.get("_"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());
        let extra_desc = row
            .get("2")
            .and_then(|v| v.get("_"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        let (ext_name, mut ext_desc) = match alt_name {
            Some(alt) => (alt.to_string(), html_name.clone()),
            None => (html_name, String::new()),
        };
        if let Some(extra) = extra_desc {
            // Mirror PHP's `' | '` separator. When there's no prior desc
            // (no alt name), the result starts with " | …", which matches
            // PHP behaviour exactly (`$o->desc .= …` on an unset key
            // produces a leading null, but PHP coerces to empty).
            if ext_desc.is_empty() {
                ext_desc = extra.to_string();
            } else {
                ext_desc = format!("{ext_desc} | {extra}");
            }
        }

        let author_ext_id = row
            .get("3")
            .and_then(|v| v.get("_"))
            .and_then(|v| v.as_str())
            .and_then(Self::extract_author_id);

        let entry = Entry {
            catalog: catalog_id,
            ext_id: ext_id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!("https://www.geschichtsquellen.de/werk/{ext_id}"),
            random: rand::rng().random(),
            // Q47461344 = "literary work"
            type_name: Some("Q47461344".to_string()),
            ..Default::default()
        };

        Some(ParsedWerk {
            ee: ExtendedEntry {
                entry,
                ..Default::default()
            },
            author_ext_id,
        })
    }

    /// Pull `(id, name)` out of `<a href="/werk/ID">NAME</a>`.
    pub(crate) fn extract_id_and_name(html: &str) -> Option<(String, String)> {
        lazy_static! {
            static ref RE_ANCHOR: Regex =
                Regex::new(r#"<a href="/werk/(\d+)">(.+?)</a>"#).expect("regex");
        }
        let caps = RE_ANCHOR.captures(html)?;
        Some((
            caps.get(1)?.as_str().to_string(),
            caps.get(2)?.as_str().to_string(),
        ))
    }

    /// Extract the autor id from `<a href="/autor/ID">…</a>`.
    pub(crate) fn extract_author_id(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_AUTHOR: Regex =
                Regex::new(r#"<a href="/autor/(\d+)">.+?</a>"#).expect("regex");
        }
        RE_AUTHOR
            .captures(html)?
            .get(1)
            .map(|m| m.as_str().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_3387_extract_id_and_name() {
        assert_eq!(
            BespokeScraper3387::extract_id_and_name(
                r#"<a href="/werk/9001">Liber Annales</a>"#
            ),
            Some(("9001".to_string(), "Liber Annales".to_string()))
        );
    }

    #[test]
    fn test_3387_extract_id_and_name_no_match() {
        assert_eq!(BespokeScraper3387::extract_id_and_name("garbage"), None);
    }

    #[test]
    fn test_3387_extract_author_id() {
        assert_eq!(
            BespokeScraper3387::extract_author_id(
                r#"<a href="/autor/42">Bach, Johann</a>"#
            ),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_3387_extract_author_id_none() {
        assert_eq!(BespokeScraper3387::extract_author_id(""), None);
        assert_eq!(BespokeScraper3387::extract_author_id("plain text"), None);
    }

    #[test]
    fn test_3387_parse_item_minimal() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/100">Codex Anonymus</a>"#}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.ee.entry.ext_id, "100");
        assert_eq!(p.ee.entry.ext_name, "Codex Anonymus");
        assert_eq!(p.ee.entry.ext_desc, "");
        assert_eq!(
            p.ee.entry.ext_url,
            "https://www.geschichtsquellen.de/werk/100"
        );
        assert_eq!(p.ee.entry.type_name, Some("Q47461344".to_string()));
        assert!(p.author_ext_id.is_none());
    }

    #[test]
    fn test_3387_parse_item_alt_swaps_name() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/200">Annales</a>"#},
            "1": {"_": "Annales Regum"}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.ee.entry.ext_name, "Annales Regum");
        assert_eq!(p.ee.entry.ext_desc, "Annales");
    }

    #[test]
    fn test_3387_parse_item_alt_plus_extra_desc() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/300">Chronica</a>"#},
            "1": {"_": "Chronica Universalis"},
            "2": {"_": "12th century"}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.ee.entry.ext_name, "Chronica Universalis");
        assert_eq!(p.ee.entry.ext_desc, "Chronica | 12th century");
    }

    #[test]
    fn test_3387_parse_item_extra_desc_only() {
        // No alt name but extra desc present — PHP would yield desc =
        // "" then append " | extra" (an unset string concat in PHP).
        // We use just "extra" in that case (cleaner; same data either way).
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/400">Vita</a>"#},
            "2": {"_": "anonymous"}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.ee.entry.ext_name, "Vita");
        assert_eq!(p.ee.entry.ext_desc, "anonymous");
    }

    #[test]
    fn test_3387_parse_item_with_author_link() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/500">Liber Vitae</a>"#},
            "3": {"_": r#"<a href="/autor/77">Bede</a>"#}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.author_ext_id, Some("77".to_string()));
    }

    #[test]
    fn test_3387_parse_item_author_link_unparseable_no_link() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/600">Anonymous</a>"#},
            "3": {"_": "no link here"}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert!(p.author_ext_id.is_none());
    }

    #[test]
    fn test_3387_parse_item_empty_alt_treated_as_missing() {
        let row = serde_json::json!({
            "0": {"_": r#"<a href="/werk/700">Codex</a>"#},
            "1": {"_": ""},
            "2": {"_": "extra"}
        });
        let p = BespokeScraper3387::parse_item(3387, &row).unwrap();
        assert_eq!(p.ee.entry.ext_name, "Codex");
        assert_eq!(p.ee.entry.ext_desc, "extra");
    }

    #[test]
    fn test_3387_parse_item_bad_anchor_skipped() {
        let row = serde_json::json!({"0": {"_": "no link"}});
        assert!(BespokeScraper3387::parse_item(3387, &row).is_none());
    }
}
