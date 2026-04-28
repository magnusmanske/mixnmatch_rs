use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    auxiliary_data::AuxiliaryRow,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use wikimisc::timestamp::TimeStamp;

use super::BespokeScraper;

// ______________________________________________________
// featherbase.info - Bird Feather Database

#[derive(Debug)]
pub struct BespokeScraper7697 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7697 {

    scraper_boilerplate!(7697);

    async fn run(&self) -> Result<()> {
        let sparql = "SELECT ?item ?id WHERE { ?item wdt:P12589 ?id }";
        let client = self.http_client();
        let json: serde_json::Value = client
            .get("https://query.wikidata.org/sparql")
            .query(&[("query", sparql), ("format", "json")])
            .header(
                "User-Agent",
                "Mix-n-Match/1.0 (https://mix-n-match.toolforge.org)",
            )
            .send()
            .await?
            .json()
            .await?;
        let bindings = json["results"]["bindings"]
            .as_array()
            .ok_or_else(|| anyhow!("No SPARQL bindings in Featherbase response"))?;
        let mut entry_cache = vec![];
        for binding in bindings {
            if let Some(ee) = Self::binding_to_entry(self.catalog_id(), binding) {
                entry_cache.push(ee);
            self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper7697 {
    pub(crate) fn binding_to_entry(
        catalog_id: usize,
        binding: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let fb_id = binding["id"]["value"].as_str()?;
        let ext_name = Self::featherbase_id_to_name(fb_id)?;
        if ext_name.is_empty() {
            return None;
        }
        let ext_desc = Self::featherbase_id_to_description(fb_id).to_string();
        let ext_url = format!("https://www.featherbase.info/en/{}", fb_id);
        let item_uri = binding["item"]["value"].as_str().unwrap_or_default();
        let q = AppState::item2numeric(item_uri);
        let entry = Entry {
            catalog: catalog_id,
            ext_id: fb_id.to_string(),
            ext_name,
            ext_desc,
            ext_url,
            q,
            user: if q.is_some() {
                Some(USER_AUX_MATCH)
            } else {
                None
            },
            timestamp: if q.is_some() {
                Some(TimeStamp::now())
            } else {
                None
            },
            random: rand::rng().random(),
            type_name: Some("Q16521".to_string()),
            ..Default::default()
        };
        let mut ee = ExtendedEntry {
            entry,
            ..Default::default()
        };
        ee.aux.insert(AuxiliaryRow::new(12589, fb_id.to_string()));
        Some(ee)
    }

    pub(crate) fn capitalize(s: &str) -> String {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            None => String::new(),
        }
    }

    pub(crate) fn featherbase_id_to_name(id: &str) -> Option<String> {
        let parts: Vec<&str> = id.split('/').collect();
        match parts.as_slice() {
            ["species", genus, epithet] => Some(format!("{} {}", Self::capitalize(genus), epithet)),
            ["genus", name] | ["family", name] | ["order", name] => Some(Self::capitalize(name)),
            _ => None,
        }
    }

    pub(crate) fn featherbase_id_to_description(id: &str) -> &'static str {
        if id.starts_with("species/") {
            "Species"
        } else if id.starts_with("genus/") {
            "Genus"
        } else if id.starts_with("family/") {
            "Family"
        } else if id.starts_with("order/") {
            "Order"
        } else {
            ""
        }
    }

    #[allow(dead_code)]
    pub(crate) fn parse_common_name(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_COMMON: Regex = Regex::new(r"<h1[^>]*>\s*([A-Z][^<]+?)\s*<").unwrap();
        }
        let name = RE_COMMON
            .captures(html)?
            .get(1)?
            .as_str()
            .trim()
            .to_string();
        if name.is_empty() { None } else { Some(name) }
    }

    #[allow(dead_code)]
    pub(crate) fn parse_family_from_html(html: &str) -> Option<String> {
        lazy_static! {
            static ref RE_FAMILY: Regex = Regex::new(r"species\s+of\s+[^>]*>(\w+)<").unwrap();
        }
        Some(RE_FAMILY.captures(html)?.get(1)?.as_str().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_7697_capitalize() {
        assert_eq!(BespokeScraper7697::capitalize("buteo"), "Buteo");
        assert_eq!(BespokeScraper7697::capitalize("ALREADY"), "ALREADY");
        assert_eq!(BespokeScraper7697::capitalize(""), "");
        assert_eq!(BespokeScraper7697::capitalize("a"), "A");
    }

    #[test]
    fn test_7697_capitalize_unicode() {
        // Multi-byte first character
        assert_eq!(BespokeScraper7697::capitalize("über"), "Über");
    }

    #[test]
    fn test_7697_featherbase_id_to_name_species() {
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("species/buteo/buteo"),
            Some("Buteo buteo".to_string())
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("species/corvus/albus"),
            Some("Corvus albus".to_string())
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("species/aegolius/funereus"),
            Some("Aegolius funereus".to_string())
        );
    }

    #[test]
    fn test_7697_featherbase_id_to_name_higher_taxa() {
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("genus/amazilis"),
            Some("Amazilis".to_string())
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("family/apodidae"),
            Some("Apodidae".to_string())
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("order/ciconiiformes"),
            Some("Ciconiiformes".to_string())
        );
    }

    #[test]
    fn test_7697_featherbase_id_to_name_non_taxon() {
        // Country and unknown patterns should return None
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("country/FJ"),
            None
        );
        assert_eq!(BespokeScraper7697::featherbase_id_to_name("unknown"), None);
        assert_eq!(BespokeScraper7697::featherbase_id_to_name(""), None);
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("species/buteo"),
            None
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_name("species/a/b/c"),
            None
        );
    }

    #[test]
    fn test_7697_featherbase_id_to_description() {
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("species/buteo/buteo"),
            "Species"
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("genus/amazilis"),
            "Genus"
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("family/apodidae"),
            "Family"
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("order/ciconiiformes"),
            "Order"
        );
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("country/FJ"),
            ""
        );
        assert_eq!(BespokeScraper7697::featherbase_id_to_description(""), "");
    }

    #[test]
    fn test_7697_featherbase_id_to_description_unknown_prefix() {
        assert_eq!(
            BespokeScraper7697::featherbase_id_to_description("unknown/something"),
            ""
        );
    }

    #[test]
    fn test_7697_binding_to_entry_species() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q25351" },
            "id": { "type": "literal", "value": "species/buteo/buteo" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(ee.entry.catalog, 7697);
        assert_eq!(ee.entry.ext_id, "species/buteo/buteo");
        assert_eq!(ee.entry.ext_name, "Buteo buteo");
        assert_eq!(ee.entry.ext_desc, "Species");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.featherbase.info/en/species/buteo/buteo"
        );
        assert_eq!(ee.entry.q, Some(25351));
        assert_eq!(ee.entry.user, Some(USER_AUX_MATCH));
        assert!(ee.entry.timestamp.is_some());
        assert_eq!(ee.entry.type_name, Some("Q16521".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(12589, "species/buteo/buteo".to_string())));
    }

    #[test]
    fn test_7697_binding_to_entry_genus() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q123" },
            "id": { "type": "literal", "value": "genus/amazilis" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(ee.entry.ext_name, "Amazilis");
        assert_eq!(ee.entry.ext_desc, "Genus");
        assert!(ee.aux.contains(&AuxiliaryRow::new(12589, "genus/amazilis".to_string())));
    }

    #[test]
    fn test_7697_binding_to_entry_family() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q456" },
            "id": { "type": "literal", "value": "family/apodidae" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(ee.entry.ext_name, "Apodidae");
        assert_eq!(ee.entry.ext_desc, "Family");
    }

    #[test]
    fn test_7697_binding_to_entry_order() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q789" },
            "id": { "type": "literal", "value": "order/ciconiiformes" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(ee.entry.ext_name, "Ciconiiformes");
        assert_eq!(ee.entry.ext_desc, "Order");
    }

    #[test]
    fn test_7697_binding_to_entry_country_skipped() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q712" },
            "id": { "type": "literal", "value": "country/FJ" }
        });
        assert!(BespokeScraper7697::binding_to_entry(7697, &binding).is_none());
    }

    #[test]
    fn test_7697_binding_to_entry_no_wikidata_match() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "" },
            "id": { "type": "literal", "value": "species/corvus/albus" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(ee.entry.ext_name, "Corvus albus");
        assert!(ee.entry.q.is_none());
        assert!(ee.entry.user.is_none());
        assert!(ee.entry.timestamp.is_none());
    }

    #[test]
    fn test_7697_binding_to_entry_missing_id() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q1" }
        });
        assert!(BespokeScraper7697::binding_to_entry(7697, &binding).is_none());
    }

    #[test]
    fn test_7697_binding_to_entry_always_inserts_aux_p12589() {
        // Every valid entry should carry an auxiliary P12589 value equal to the fb_id
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q999" },
            "id": { "type": "literal", "value": "order/passeriformes" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert!(
            ee.aux.contains(&AuxiliaryRow::new(12589, "order/passeriformes".to_string())),
            "Expected P12589 aux to be present"
        );
    }

    #[test]
    fn test_7697_binding_to_entry_ext_url_format() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q1" },
            "id": { "type": "literal", "value": "species/pica/pica" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        assert_eq!(
            ee.entry.ext_url,
            "https://www.featherbase.info/en/species/pica/pica"
        );
    }

    #[test]
    fn test_7697_binding_to_entry_type_name_is_taxon() {
        let binding = serde_json::json!({
            "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q1" },
            "id": { "type": "literal", "value": "species/pica/pica" }
        });
        let ee = BespokeScraper7697::binding_to_entry(7697, &binding).unwrap();
        // Q16521 = taxon
        assert_eq!(ee.entry.type_name, Some("Q16521".to_string()));
    }

    #[test]
    fn test_7697_parse_common_name() {
        let html = r#"<h1 class="title">Common Buzzard <i>Buteo buteo</i></h1>"#;
        assert_eq!(
            BespokeScraper7697::parse_common_name(html),
            Some("Common Buzzard".to_string())
        );
        let html2 = r#"<h1>Pied Crow <i>Corvus albus</i></h1>"#;
        assert_eq!(
            BespokeScraper7697::parse_common_name(html2),
            Some("Pied Crow".to_string())
        );
        assert_eq!(BespokeScraper7697::parse_common_name("<h1></h1>"), None);
        assert_eq!(
            BespokeScraper7697::parse_common_name("no heading here"),
            None
        );
    }

    #[test]
    fn test_7697_parse_common_name_lowercase_start_not_matched() {
        // The regex requires the name to start with an uppercase letter
        let html = r#"<h1>lowercase name</h1>"#;
        assert_eq!(BespokeScraper7697::parse_common_name(html), None);
    }

    #[test]
    fn test_7697_parse_family_from_html() {
        let html = r#"species of <i>Accipitridae</i>"#;
        assert_eq!(
            BespokeScraper7697::parse_family_from_html(html),
            Some("Accipitridae".to_string())
        );
        let html2 = r#"species of <em>Corvidae</em>"#;
        assert_eq!(
            BespokeScraper7697::parse_family_from_html(html2),
            Some("Corvidae".to_string())
        );
        assert_eq!(
            BespokeScraper7697::parse_family_from_html("no family info"),
            None
        );
    }

    #[test]
    fn test_7697_parse_family_from_html_multiword_not_matched() {
        // The regex requires [^>]*>(\w+)< so there must be a closing > before the word.
        // "species of <em>Not Valid</em>" — the `<em>` tag contains a space in "Not Valid"
        // so \w+ only matches "Not", but the full regex fails to find a closing `<` right
        // after the first word because there is a space. Result depends on engine; actual
        // result is None because serde matches stop before space.
        // Actually: the regex is `species\s+of\s+[^>]*>(\w+)<` which matches
        // "species of " then [^>]* (greedy, stops at >) then > then \w+ then <.
        // With "<em>Not Valid</em>": [^>]*> matches "<em>", then \w+ matches "Not",
        // then < must follow — but " Valid</em>" has a space. So it does NOT match.
        let html = r#"species of <em>Not Valid</em>"#;
        assert_eq!(BespokeScraper7697::parse_family_from_html(html), None);
    }

    #[test]
    fn test_7697_catalog_id() {
        let s = BespokeScraper7697 {
            app: crate::app_state::get_test_app(),
        };
        assert_eq!(s.catalog_id(), 7697);
    }

    #[test]
    fn test_7697_keep_existing_names_default_false() {
        let s = BespokeScraper7697 {
            app: crate::app_state::get_test_app(),
        };
        assert!(!s.keep_existing_names());
    }
}
