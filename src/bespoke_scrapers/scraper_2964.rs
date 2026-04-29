use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    coordinates::CoordinateLocation,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use wikimisc::timestamp::TimeStamp;

use super::BespokeScraper;

// ______________________________________________________
// Pantheon (2964)
//
// Single Mix'n'match catalog backed by five distinct API endpoints —
// person, place, country, occupation, era — each with its own item
// type. Some rows already have a `wp_id` pointing at a Wikidata QID,
// which we materialise as a pre-match (entry.q + USER_AUX_MATCH) so the
// next automatch pass doesn't re-search what's already known. Rows that
// expose `lat`/`lon` get a P625 coordinate location.

#[derive(Debug)]
pub struct BespokeScraper2964 {
    pub(super) app: AppState,
}

/// `(api_key, default_type)` pairs; the same shape used by the PHP
/// `$data` array. Listed as a const so tests and the production loop
/// share a single source of truth.
const PANTHEON_ENDPOINTS: &[(&str, &str)] = &[
    ("person", "Q5"),
    ("place", "Q2221906"),
    ("country", "Q6256"),
    ("occupation", "Q28640"),
    ("era", "Q11514315"),
];

/// Description-line keys, in order. Mirrors the PHP foreach list.
const DESC_FIELDS: &[&str] = &[
    "id",
    "description",
    "start_year",
    "end_year",
    "industry",
    "domain",
    "group",
    "occupation",
    "birthdate",
    "birthyear",
    "bplace_name",
    "deathdate",
    "deathyear",
    "dplace_name",
];

#[async_trait]
impl BespokeScraper for BespokeScraper2964 {
    scraper_boilerplate!(2964);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for (key, default_type) in PANTHEON_ENDPOINTS {
            let url = format!("https://api.pantheon.world/{key}");
            let json: serde_json::Value = client.get(&url).send().await?.json().await?;
            let arr = match json.as_array() {
                Some(arr) => arr,
                None => continue,
            };
            for item in arr {
                if let Some(ee) =
                    Self::parse_item(self.catalog_id(), key, default_type, item)
                {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper2964 {
    /// Convert a single API row into an `ExtendedEntry`. `key` is the
    /// endpoint name (`"person"`, `"place"`, …); the slug field name
    /// the API returns is either plain `"slug"` or `"{key}_slug"`,
    /// and the human label is either `"name"` or the value at `key`.
    pub(crate) fn parse_item(
        catalog_id: usize,
        key: &str,
        default_type: &str,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let slug = Self::resolve_slug(key, item)?;
        let name = Self::resolve_name(key, item)?;
        let desc = Self::build_desc(item);
        let ext_url = format!("https://pantheon.world/profile/{key}/{slug}");

        let pre_match_q = item
            .get("wp_id")
            .and_then(|v| v.as_str())
            .and_then(Self::parse_wp_id);
        let user_and_ts = pre_match_q.map(|_| (USER_AUX_MATCH, TimeStamp::now()));

        let entry = Entry {
            catalog: catalog_id,
            ext_id: slug,
            ext_name: name,
            ext_desc: desc,
            ext_url,
            random: rand::rng().random(),
            type_name: Some(default_type.to_string()),
            q: pre_match_q,
            user: user_and_ts.as_ref().map(|(u, _)| *u),
            timestamp: user_and_ts.map(|(_, t)| t),
            ..Default::default()
        };

        let location = match (item.get("lat").and_then(|v| v.as_f64()), item.get("lon").and_then(|v| v.as_f64())) {
            (Some(lat), Some(lon)) => Some(CoordinateLocation::new(lat, lon)),
            _ => None,
        };

        Some(ExtendedEntry {
            entry,
            location,
            ..Default::default()
        })
    }

    /// `slug` if present, otherwise `{key}_slug`.
    pub(crate) fn resolve_slug(key: &str, item: &serde_json::Value) -> Option<String> {
        if let Some(s) = item.get("slug").and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
        let alt = format!("{key}_slug");
        item.get(&alt).and_then(|v| v.as_str()).map(str::to_string)
    }

    /// `name` if present, otherwise the value at `key`. Mirrors PHP fallback.
    pub(crate) fn resolve_name(key: &str, item: &serde_json::Value) -> Option<String> {
        if let Some(s) = item.get("name").and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
        item.get(key).and_then(|v| v.as_str()).map(str::to_string)
    }

    /// Build "k: v; k: v; …" description from `DESC_FIELDS`. Numbers and
    /// strings are both stringified; nulls and empty strings are skipped.
    pub(crate) fn build_desc(item: &serde_json::Value) -> String {
        let mut parts = vec![];
        for k in DESC_FIELDS {
            let v = match item.get(*k) {
                Some(v) if !v.is_null() => v,
                _ => continue,
            };
            let formatted = if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                v.to_string()
            };
            if formatted.is_empty() || formatted == "\"\"" {
                continue;
            }
            parts.push(format!("{k}: {formatted}"));
        }
        parts.join("; ")
    }

    /// Strict QID parse. Accepts `Q\d+` (case-insensitive), returns the
    /// numeric portion as `isize`. Anything else → None, mirroring the
    /// PHP `preg_match('/^Q\d+$/i', $entry->wp_id)` gate.
    pub(crate) fn parse_wp_id(wp_id: &str) -> Option<isize> {
        lazy_static! {
            static ref RE_QID: Regex = Regex::new(r"^[Qq](\d+)$").expect("regex");
        }
        RE_QID.captures(wp_id)?.get(1)?.as_str().parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2964_resolve_slug_prefers_slug_over_keyed() {
        let item = serde_json::json!({"slug": "alpha", "person_slug": "beta"});
        assert_eq!(
            BespokeScraper2964::resolve_slug("person", &item),
            Some("alpha".to_string())
        );
    }

    #[test]
    fn test_2964_resolve_slug_falls_back_to_keyed() {
        let item = serde_json::json!({"person_slug": "fallback"});
        assert_eq!(
            BespokeScraper2964::resolve_slug("person", &item),
            Some("fallback".to_string())
        );
    }

    #[test]
    fn test_2964_resolve_slug_missing_returns_none() {
        let item = serde_json::json!({"name": "x"});
        assert_eq!(BespokeScraper2964::resolve_slug("person", &item), None);
    }

    #[test]
    fn test_2964_resolve_name_prefers_name_over_keyed() {
        let item = serde_json::json!({"name": "Alpha", "place": "Beta"});
        assert_eq!(
            BespokeScraper2964::resolve_name("place", &item),
            Some("Alpha".to_string())
        );
    }

    #[test]
    fn test_2964_resolve_name_falls_back_to_keyed() {
        let item = serde_json::json!({"place": "Fallback"});
        assert_eq!(
            BespokeScraper2964::resolve_name("place", &item),
            Some("Fallback".to_string())
        );
    }

    #[test]
    fn test_2964_parse_wp_id_uppercase() {
        assert_eq!(BespokeScraper2964::parse_wp_id("Q42"), Some(42));
    }

    #[test]
    fn test_2964_parse_wp_id_lowercase_accepted() {
        // PHP regex was `/^Q\d+$/i` — case-insensitive — so "q42" should match.
        assert_eq!(BespokeScraper2964::parse_wp_id("q42"), Some(42));
    }

    #[test]
    fn test_2964_parse_wp_id_rejects_non_numeric() {
        assert_eq!(BespokeScraper2964::parse_wp_id("Qnope"), None);
        assert_eq!(BespokeScraper2964::parse_wp_id("42"), None);
        assert_eq!(BespokeScraper2964::parse_wp_id(""), None);
    }

    #[test]
    fn test_2964_build_desc_skips_nulls_and_empty() {
        let item = serde_json::json!({
            "id": "x1",
            "description": "",
            "start_year": null,
            "industry": "Science"
        });
        assert_eq!(
            BespokeScraper2964::build_desc(&item),
            "id: x1; industry: Science"
        );
    }

    #[test]
    fn test_2964_build_desc_handles_numeric_values() {
        let item = serde_json::json!({"birthyear": 1879, "deathyear": 1955});
        // serde_json::Value::to_string() for a number gives the bare digits.
        assert_eq!(
            BespokeScraper2964::build_desc(&item),
            "birthyear: 1879; deathyear: 1955"
        );
    }

    #[test]
    fn test_2964_build_desc_preserves_order() {
        // DESC_FIELDS order is fixed; "id" should come before "description".
        let item = serde_json::json!({"description": "second", "id": "first"});
        assert_eq!(
            BespokeScraper2964::build_desc(&item),
            "id: first; description: second"
        );
    }

    #[test]
    fn test_2964_parse_item_person_full() {
        let item = serde_json::json!({
            "slug": "albert-einstein",
            "name": "Albert Einstein",
            "id": "p123",
            "occupation": "Physicist",
            "birthyear": 1879,
            "deathyear": 1955,
            "lat": 48.4,
            "lon": 9.99,
            "wp_id": "Q937"
        });
        let ee = BespokeScraper2964::parse_item(2964, "person", "Q5", &item).unwrap();
        assert_eq!(ee.entry.ext_id, "albert-einstein");
        assert_eq!(ee.entry.ext_name, "Albert Einstein");
        assert_eq!(
            ee.entry.ext_url,
            "https://pantheon.world/profile/person/albert-einstein"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert_eq!(ee.entry.q, Some(937));
        assert_eq!(ee.entry.user, Some(USER_AUX_MATCH));
        assert!(ee.entry.timestamp.is_some());
        let loc = ee.location.as_ref().expect("expected coords");
        assert!((loc.lat() - 48.4).abs() < f64::EPSILON);
        assert!((loc.lon() - 9.99).abs() < f64::EPSILON);
    }

    #[test]
    fn test_2964_parse_item_place_uses_keyed_slug_and_name() {
        let item = serde_json::json!({
            "place_slug": "berlin",
            "place": "Berlin",
            "id": "pl1"
        });
        let ee = BespokeScraper2964::parse_item(2964, "place", "Q2221906", &item).unwrap();
        assert_eq!(ee.entry.ext_id, "berlin");
        assert_eq!(ee.entry.ext_name, "Berlin");
        assert_eq!(ee.entry.type_name, Some("Q2221906".to_string()));
    }

    #[test]
    fn test_2964_parse_item_no_wp_id_no_pre_match() {
        let item = serde_json::json!({"slug": "x", "name": "X"});
        let ee = BespokeScraper2964::parse_item(2964, "person", "Q5", &item).unwrap();
        assert_eq!(ee.entry.q, None);
        assert!(ee.entry.user.is_none());
        assert!(ee.entry.timestamp.is_none());
    }

    #[test]
    fn test_2964_parse_item_invalid_wp_id_no_pre_match() {
        let item = serde_json::json!({"slug": "x", "name": "X", "wp_id": "junk"});
        let ee = BespokeScraper2964::parse_item(2964, "person", "Q5", &item).unwrap();
        assert_eq!(ee.entry.q, None);
    }

    #[test]
    fn test_2964_parse_item_partial_coords_no_location() {
        let item = serde_json::json!({"slug": "x", "name": "X", "lat": 10.0});
        let ee = BespokeScraper2964::parse_item(2964, "place", "Q2221906", &item).unwrap();
        assert!(ee.location.is_none());
    }

    #[test]
    fn test_2964_parse_item_missing_slug_skipped() {
        let item = serde_json::json!({"name": "X"});
        assert!(BespokeScraper2964::parse_item(2964, "person", "Q5", &item).is_none());
    }

    #[test]
    fn test_2964_parse_item_missing_name_skipped() {
        let item = serde_json::json!({"slug": "x"});
        assert!(BespokeScraper2964::parse_item(2964, "person", "Q5", &item).is_none());
    }

    #[test]
    fn test_2964_endpoints_table_intact() {
        // Pin the 5-endpoint shape against accidental edits — the catalog
        // identity is "person + place + country + occupation + era".
        assert_eq!(PANTHEON_ENDPOINTS.len(), 5);
        let keys: Vec<&str> = PANTHEON_ENDPOINTS.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["person", "place", "country", "occupation", "era"]);
    }
}
