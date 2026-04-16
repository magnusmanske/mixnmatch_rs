use crate::{
    app_state::AppState,
    coordinates::CoordinateLocation,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Cyprus Gazetteer

#[derive(Debug)]
pub struct BespokeScraper2670 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper2670 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        2670
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let url = "http://www.cyprusgazetteer.org/map/?&mime_type=application/json&selected_facets=*";
        let client = self.http_client();
        let text = client.get(url).send().await?.text().await?;
        let cleaned = Self::clean_json(&text);
        let json: serde_json::Value = serde_json::from_str(&cleaned)?;
        let entries = Self::parse_features(self.catalog_id(), &json);
        let mut entry_cache = vec![];
        for ee in entries {
            entry_cache.push(ee);
            if entry_cache.len() >= 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper2670 {
    /// Clean the broken JSON response from the Cyprus Gazetteer.
    ///
    /// The raw response contains:
    /// 1. Stray HTML fragments like `", </em></p>",`
    /// 2. Newlines mixed into JSON
    /// 3. Leading/trailing junk around the main JSON object
    /// 4. Excessive whitespace
    /// 5. Trailing commas before `}` or `]`
    pub(crate) fn clean_json(raw: &str) -> String {
        lazy_static! {
            static ref RE_WHITESPACE: Regex = Regex::new(r"\s+").unwrap();
            static ref RE_TRAILING_COMMA_BRACE: Regex = Regex::new(r",\s*\}").unwrap();
            static ref RE_TRAILING_COMMA_BRACKET: Regex = Regex::new(r",\s*\]").unwrap();
        }

        // Step 1: Remove stray HTML fragment
        let s = raw.replace("\", </em></p>\",", "\"");

        // Step 2: Replace newlines with spaces
        let s = s.replace('\n', " ");

        // Step 3: Extract the first top-level `{...}` object
        let s = Self::extract_json_object(&s);

        // Step 4: Collapse whitespace
        let s = RE_WHITESPACE.replace_all(&s, " ").to_string();

        // Step 5: Fix trailing commas
        let s = RE_TRAILING_COMMA_BRACE.replace_all(&s, "}").to_string();
        let s = RE_TRAILING_COMMA_BRACKET.replace_all(&s, "]").to_string();

        s
    }

    /// Extract the substring from the first `{` to the last `}`.
    pub(crate) fn extract_json_object(s: &str) -> String {
        let start = match s.find('{') {
            Some(i) => i,
            None => return s.to_string(),
        };
        let end = match s.rfind('}') {
            Some(i) => i,
            None => return s.to_string(),
        };
        if end >= start {
            s[start..=end].to_string()
        } else {
            s.to_string()
        }
    }

    /// Parse the GeoJSON `features` array into extended entries.
    pub(crate) fn parse_features(
        catalog_id: usize,
        json: &serde_json::Value,
    ) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ID: Regex =
                Regex::new(r"<a href='/(\d+/\d+)'>Full record</a>").unwrap();
            static ref RE_LATLON: Regex =
                Regex::new(r"Lat:\s*([0-9.\-]+),\s*Lng:\s*([0-9.\-]+)").unwrap();
        }

        let features = match json["features"].as_array() {
            Some(f) => f,
            None => return vec![],
        };

        features
            .iter()
            .filter_map(|f| {
                let props = f.get("properties")?;
                let full_name = props.get("name")?.as_str()?.to_string();
                let popup = props.get("popupContent")?.as_str().unwrap_or_default();

                // Extract ID from popup link
                let id = RE_ID.captures(popup)?.get(1)?.as_str().to_string();

                // Split name on first `/` for name/description
                let (ext_name, ext_desc) = Self::split_name(&full_name);

                let ext_url = format!("http://www.cyprusgazetteer.org/{}", id);

                let mut ee = ExtendedEntry {
                    entry: Entry {
                        catalog: catalog_id,
                        ext_id: id,
                        ext_url,
                        ext_name,
                        ext_desc,
                        random: rand::rng().random(),
                        ..Default::default()
                    },
                    ..Default::default()
                };

                // Extract lat/lon from popup
                if let Some(caps) = RE_LATLON.captures(popup) {
                    if let (Ok(lat), Ok(lon)) = (
                        caps[1].parse::<f64>(),
                        caps[2].parse::<f64>(),
                    ) {
                        ee.location = Some(CoordinateLocation::new(lat, lon));
                    }
                }

                Some(ee)
            })
            .collect()
    }

    /// Split a name on the first `/` into (name, description).
    /// If there is no `/`, description is empty.
    pub(crate) fn split_name(name: &str) -> (String, String) {
        match name.find('/') {
            Some(pos) => (
                name[..pos].trim().to_string(),
                name[pos + 1..].trim().to_string(),
            ),
            None => (name.trim().to_string(), String::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_2670_clean_json_removes_html_fragment() {
        let input = r#"junk{"key": "value", </em></p>", "other": 1}trailing"#;
        let cleaned = BespokeScraper2670::clean_json(input);
        assert!(cleaned.contains(r#""key": "value""#));
        assert!(!cleaned.contains("</em>"));
    }

    #[test]
    fn test_2670_clean_json_replaces_newlines() {
        let input = "{\"a\":\n\"b\"}";
        let cleaned = BespokeScraper2670::clean_json(input);
        assert!(!cleaned.contains('\n'));
    }

    #[test]
    fn test_2670_clean_json_fixes_trailing_commas() {
        let input = r#"{"a": [1, 2, ], "b": {"c": 3, }}"#;
        let cleaned = BespokeScraper2670::clean_json(input);
        assert!(!cleaned.contains(",]"));
        assert!(!cleaned.contains(",}"));
        assert!(cleaned.contains("[1, 2]"));
        assert!(cleaned.contains(r#""c": 3}"#));
    }

    #[test]
    fn test_2670_clean_json_extracts_json_object() {
        let input = "some leading text {\"key\": \"val\"} trailing text";
        let cleaned = BespokeScraper2670::clean_json(input);
        assert_eq!(cleaned, r#"{"key": "val"}"#);
    }

    #[test]
    fn test_2670_clean_json_collapses_whitespace() {
        let input = "{\"a\":   \"b\",   \"c\":   \"d\"}";
        let cleaned = BespokeScraper2670::clean_json(input);
        assert!(!cleaned.contains("  "));
    }

    #[test]
    fn test_2670_extract_json_object_basic() {
        assert_eq!(
            BespokeScraper2670::extract_json_object("abc{\"x\":1}def"),
            "{\"x\":1}"
        );
    }

    #[test]
    fn test_2670_extract_json_object_no_braces() {
        assert_eq!(
            BespokeScraper2670::extract_json_object("no json here"),
            "no json here"
        );
    }

    #[test]
    fn test_2670_extract_json_object_nested() {
        let input = "prefix{\"a\":{\"b\":1}}suffix";
        assert_eq!(
            BespokeScraper2670::extract_json_object(input),
            "{\"a\":{\"b\":1}}"
        );
    }

    #[test]
    fn test_2670_split_name_with_slash() {
        let (name, desc) = BespokeScraper2670::split_name("Nicosia/Capital of Cyprus");
        assert_eq!(name, "Nicosia");
        assert_eq!(desc, "Capital of Cyprus");
    }

    #[test]
    fn test_2670_split_name_without_slash() {
        let (name, desc) = BespokeScraper2670::split_name("Paphos");
        assert_eq!(name, "Paphos");
        assert_eq!(desc, "");
    }

    #[test]
    fn test_2670_split_name_multiple_slashes() {
        // Only splits on the first `/`
        let (name, desc) = BespokeScraper2670::split_name("A/B/C");
        assert_eq!(name, "A");
        assert_eq!(desc, "B/C");
    }

    #[test]
    fn test_2670_split_name_trims_whitespace() {
        let (name, desc) = BespokeScraper2670::split_name(" Larnaca / Coastal city ");
        assert_eq!(name, "Larnaca");
        assert_eq!(desc, "Coastal city");
    }

    #[test]
    fn test_2670_parse_features_full() {
        let json = serde_json::json!({
            "features": [
                {
                    "properties": {
                        "name": "Nicosia/Capital city",
                        "popupContent": "<a href='/123/456'>Full record</a> Lat: 35.1856, Lng: 33.3823"
                    }
                },
                {
                    "properties": {
                        "name": "Paphos",
                        "popupContent": "<a href='/789/012'>Full record</a> Lat: 34.7754, Lng: 32.4245"
                    }
                }
            ]
        });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert_eq!(entries.len(), 2);

        let e0 = &entries[0];
        assert_eq!(e0.entry.ext_id, "123/456");
        assert_eq!(e0.entry.ext_name, "Nicosia");
        assert_eq!(e0.entry.ext_desc, "Capital city");
        assert_eq!(e0.entry.ext_url, "http://www.cyprusgazetteer.org/123/456");
        assert_eq!(e0.entry.catalog, 2670);
        let loc = e0.location.unwrap();
        assert!((loc.lat() - 35.1856).abs() < 0.0001);
        assert!((loc.lon() - 33.3823).abs() < 0.0001);

        let e1 = &entries[1];
        assert_eq!(e1.entry.ext_id, "789/012");
        assert_eq!(e1.entry.ext_name, "Paphos");
        assert_eq!(e1.entry.ext_desc, "");
    }

    #[test]
    fn test_2670_parse_features_missing_id() {
        let json = serde_json::json!({
            "features": [
                {
                    "properties": {
                        "name": "Unknown Place",
                        "popupContent": "No link here"
                    }
                }
            ]
        });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_2670_parse_features_missing_properties() {
        let json = serde_json::json!({
            "features": [
                { "geometry": {} }
            ]
        });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_2670_parse_features_no_latlon() {
        let json = serde_json::json!({
            "features": [
                {
                    "properties": {
                        "name": "Limassol",
                        "popupContent": "<a href='/111/222'>Full record</a>"
                    }
                }
            ]
        });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].location.is_none());
    }

    #[test]
    fn test_2670_parse_features_empty() {
        let json = serde_json::json!({ "features": [] });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_2670_parse_features_no_features_key() {
        let json = serde_json::json!({ "type": "FeatureCollection" });
        let entries = BespokeScraper2670::parse_features(2670, &json);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_2670_clean_json_full_pipeline() {
        // Simulate a realistic broken response with the exact fragment the PHP replaces
        let raw = r#"var data = {"type":"FeatureCollection","features":[{"properties":{"name":"Test","popupContent":"<a href='/1/2'>Full record</a>",}},]}; var x = 1;"#;
        let cleaned = BespokeScraper2670::clean_json(raw);
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(&cleaned);
        assert!(parsed.is_ok(), "Cleaned JSON should parse: {}", cleaned);
    }
}
