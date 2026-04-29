use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
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
    pub(super) app: AppState,
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
            entry_cache.push(Self::parse_item(self.catalog_id(), id, v));
            self.maybe_flush_cache(&mut entry_cache).await?;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper3862 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        id: &str,
        v: &serde_json::Value,
    ) -> ExtendedEntry {
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

        ExtendedEntry {
            entry,
            ..Default::default()
        }
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
        let ee = BespokeScraper3862::parse_item(3862, "dbpedia", &v);
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
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v);
        assert_eq!(ee.entry.ext_desc, "Eine deutsche Beschreibung");
    }

    #[test]
    fn test_3862_parse_item_empty_when_no_en_or_de() {
        let v = serde_json::json!({
            "title": "Foo",
            "description": {"fr": "Une description française"}
        });
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v);
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_3862_parse_item_no_description_field() {
        let v = serde_json::json!({"title": "Foo"});
        let ee = BespokeScraper3862::parse_item(3862, "foo", &v);
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_3862_parse_item_falls_back_to_id_when_no_title() {
        let v = serde_json::json!({"description": {"en": "x"}});
        let ee = BespokeScraper3862::parse_item(3862, "the-id", &v);
        assert_eq!(ee.entry.ext_name, "the-id");
    }

    #[test]
    fn test_3862_parse_item_empty_title_falls_back_to_id() {
        // PHP `$v->title ?? $id` only falls back on null. Empty-string
        // titles in PHP would stay empty. Treat them the same way as
        // missing here — there's no value in a blank ext_name and the
        // default fallback gives a usable label.
        let v = serde_json::json!({"title": "", "description": {"en": "x"}});
        let ee = BespokeScraper3862::parse_item(3862, "the-id", &v);
        assert_eq!(ee.entry.ext_name, "the-id");
    }
}
