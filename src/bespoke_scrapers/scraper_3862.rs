use std::sync::Arc;
use crate::{app_state::AppContext, entry::Entry, meta_entry::MetaEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// LOD Cloud datasets (3862)
//
// Top-level JSON object keyed by dataset id; each value carries `title`
// and a `description` map keyed by language. Description prefers
// English, then German, then empty — matches PHP `?? ` chain. No type
// is set in the source script, so type_name stays None.

#[derive(Debug)]
pub struct BespokeScraper3862 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper3862 {
    scraper_boilerplate!(3862);

    async fn run(&self) -> Result<()> {
        let url = "https://lod-cloud.net/lod-data.json";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let obj = match json.as_object() {
            Some(obj) => obj,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for (id, v) in obj {
            let Some(ee) = Self::parse_item(self.catalog_id(), id, v) else {
                continue;
            };
            entry_cache.push(ee);
            self.maybe_flush_cache(&mut entry_cache).await?;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper3862 {
    /// Build one `MetaEntry` from an upstream `(id, value)` pair.
    /// Returns `None` when the id is empty after trimming.
    ///
    /// Upstream is known to ship keys with surrounding whitespace —
    /// `"Names "`, `"OLU "`, `" data.odw.tw"`, etc. (~12 such keys at
    /// last check). Carrying that whitespace into `ext_id` produced
    /// invalid URLs (`/dataset/Names%20`) and, more critically, tripped
    /// MySQL's `PAD SPACE` collation on the `(catalog, ext_id)` unique
    /// index: `"Names "` collides with `"Names"`, the `INSERT IGNORE`
    /// is silently ignored, `last_insert_id` becomes 0, mysql_common
    /// maps that to `None`, and `entry_insert_as_new` errors with
    /// `EntryError::EntryInsertFailed` — the exact failure operators
    /// were seeing.
    ///
    /// Trim once at ingestion and treat ids that vanish entirely as
    /// data-quality noise: nothing useful to scrape, skip silently.
    pub(crate) fn parse_item(
        catalog_id: usize,
        id: &str,
        v: &serde_json::Value,
    ) -> Option<MetaEntry> {
        let id = id.trim();
        if id.is_empty() {
            return None;
        }
        let ext_name = v
            .get("title")
            .and_then(|t| t.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let ext_desc = v
            .get("description")
            .and_then(|d| {
                d.get("en")
                    .and_then(|s| s.as_str())
                    .or_else(|| d.get("de").and_then(|s| s.as_str()))
            })
            .unwrap_or("")
            .to_string();

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.to_string(),
            ext_name,
            ext_desc,
            ext_url: format!("https://lod-cloud.net/dataset/{id}"),
            random: rand::rng().random(),
            ..Default::default()
        };

        Some(MetaEntry {
            entry,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_3862_parse_item_full() {
        let v = serde_json::json!({
            "title": "DBpedia",
            "description": {"en": "An English desc", "de": "Eine deutsche Beschreibung"}
        });
        let ee = BespokeScraper3862::parse_item(3862, "dbpedia", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_id, "dbpedia");
        assert_eq!(ee.entry.ext_name, "DBpedia");
        assert_eq!(ee.entry.ext_desc, "An English desc");
        assert_eq!(ee.entry.ext_url, "https://lod-cloud.net/dataset/dbpedia");
        // No type_name in PHP — stays None.
        assert_eq!(ee.entry.type_name, None);
    }

    #[test]
    fn test_3862_parse_item_falls_back_to_german() {
        let v = serde_json::json!({
            "title": "Foo",
            "description": {"de": "Eine deutsche Beschreibung"}
        });
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_desc, "Eine deutsche Beschreibung");
    }

    #[test]
    fn test_3862_parse_item_empty_when_no_en_or_de() {
        let v = serde_json::json!({
            "title": "Foo",
            "description": {"fr": "Une description française"}
        });
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_3862_parse_item_no_description_field() {
        let v = serde_json::json!({"title": "Foo"});
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_3862_parse_item_falls_back_to_id_when_no_title() {
        let v = serde_json::json!({"description": {"en": "x"}});
        let ee = BespokeScraper3862::parse_item(3862, "the-id", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_name, "the-id");
    }

    #[test]
    fn test_3862_parse_item_empty_title_falls_back_to_id() {
        // PHP `$v->title ?? $id` only falls back on null. Empty-string
        // titles in PHP would stay empty. Treat them the same way as
        // missing here — there's no value in a blank ext_name and the
        // default fallback gives a usable label.
        let v = serde_json::json!({"title": "", "description": {"en": "x"}});
        let ee = BespokeScraper3862::parse_item(3862, "the-id", &v).expect("non-empty id");
        assert_eq!(ee.entry.ext_name, "the-id");
    }

    // -----------------------------------------------------------------
    // Whitespace handling: upstream ships ids like "Names ", "OLU ",
    // " data.odw.tw" etc. (verified live, ~12 such keys). Untrimmed,
    // these tripped the (catalog, ext_id) unique index under MySQL's
    // PAD SPACE collation, causing EntryError::EntryInsertFailed.
    // -----------------------------------------------------------------

    #[test]
    fn test_3862_parse_item_trims_trailing_space_id() {
        let v = serde_json::json!({"title": "Names dataset"});
        let ee = BespokeScraper3862::parse_item(3862, "Names ", &v).expect("non-empty after trim");
        assert_eq!(ee.entry.ext_id, "Names");
        assert_eq!(ee.entry.ext_url, "https://lod-cloud.net/dataset/Names");
    }

    #[test]
    fn test_3862_parse_item_trims_leading_space_id() {
        let v = serde_json::json!({"title": "Some Data"});
        let ee = BespokeScraper3862::parse_item(3862, " data.odw.tw", &v)
            .expect("non-empty after trim");
        assert_eq!(ee.entry.ext_id, "data.odw.tw");
        assert_eq!(ee.entry.ext_url, "https://lod-cloud.net/dataset/data.odw.tw");
    }

    #[test]
    fn test_3862_parse_item_skips_whitespace_only_id() {
        let v = serde_json::json!({"title": "should not appear"});
        assert!(BespokeScraper3862::parse_item(3862, "   ", &v).is_none());
    }

    #[test]
    fn test_3862_parse_item_skips_empty_id() {
        let v = serde_json::json!({"title": "should not appear"});
        assert!(BespokeScraper3862::parse_item(3862, "", &v).is_none());
    }

    #[test]
    fn test_3862_parse_item_id_fallback_uses_trimmed_id() {
        // When title is absent, ext_name falls back to the *trimmed* id —
        // not the raw whitespace-padded one. Otherwise the displayed name
        // would carry the same trailing-space ugliness.
        let v = serde_json::json!({});
        let ee = BespokeScraper3862::parse_item(3862, "OLU ", &v).expect("non-empty after trim");
        assert_eq!(ee.entry.ext_id, "OLU");
        assert_eq!(ee.entry.ext_name, "OLU");
    }
}
