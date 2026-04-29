use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashSet;

use super::BespokeScraper;

// ______________________________________________________
// Classical Archives — composer index (4589)
//
// Per-letter A-Z fetch. Every row is by definition a composer, so we
// attach P106=Q36834 (composer) to every entry. `n` is the composer's
// name in "Lastname, Firstname" form; `d` (dates) and `nat` (nationality)
// fold into the description if present.

#[derive(Debug)]
pub struct BespokeScraper4589 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4589 {
    scraper_boilerplate!(4589);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in 'A'..='Z' {
            let url = format!(
                "https://www.classicalarchives.com/api/composer_list_all.json?letter={letter}"
            );
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

impl BespokeScraper4589 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        v: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = Self::id_as_string(v.get("id")?)?;
        let raw_name = v.get("n")?.as_str()?;
        if raw_name.is_empty() {
            return None;
        }
        let ext_name = Self::flip_lastname_first(raw_name);

        let mut desc_parts: Vec<String> = vec![];
        if let Some(d) = v.get("d").and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
            desc_parts.push(d.to_string());
        }
        if let Some(nat) = v.get("nat").and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
            desc_parts.push(nat.to_string());
        }
        let ext_desc = desc_parts.join(" | ");

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        // P106 = occupation; Q36834 = composer.
        aux.insert(AuxiliaryRow::new(106, "Q36834".to_string()));

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!(
                "https://www.classicalarchives.com/newca/#!/Composer/{id}"
            ),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            ..Default::default()
        })
    }

    /// PHP `|^(.+?), (.+)$|` flips on the FIRST comma (lazy on group 1).
    /// Names without a comma are returned unchanged.
    pub(crate) fn flip_lastname_first(name: &str) -> String {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^(.+?), (.+)$").expect("regex");
        }
        match RE.captures(name) {
            Some(caps) => format!("{} {}", &caps[2], &caps[1]),
            None => name.to_string(),
        }
    }

    /// PHP coerces numeric/string ids identically. Mirror that — accept
    /// either JSON form. Returns `None` for nulls or unsupported types.
    fn id_as_string(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) if !s.is_empty() => Some(s.to_string()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4589_flip_lastname_first() {
        assert_eq!(
            BespokeScraper4589::flip_lastname_first("Bach, Johann Sebastian"),
            "Johann Sebastian Bach"
        );
    }

    #[test]
    fn test_4589_flip_lastname_first_no_comma() {
        assert_eq!(
            BespokeScraper4589::flip_lastname_first("Hildegard"),
            "Hildegard"
        );
    }

    #[test]
    fn test_4589_parse_item_full() {
        let v = serde_json::json!({
            "id": "bach-js",
            "n": "Bach, Johann Sebastian",
            "d": "1685-1750",
            "nat": "German"
        });
        let ee = BespokeScraper4589::parse_item(4589, &v).unwrap();
        assert_eq!(ee.entry.ext_id, "bach-js");
        assert_eq!(ee.entry.ext_name, "Johann Sebastian Bach");
        assert_eq!(ee.entry.ext_desc, "1685-1750 | German");
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert_eq!(
            ee.entry.ext_url,
            "https://www.classicalarchives.com/newca/#!/Composer/bach-js"
        );
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q36834".to_string())));
    }

    #[test]
    fn test_4589_parse_item_numeric_id() {
        let v = serde_json::json!({"id": 12345, "n": "Anonymous"});
        let ee = BespokeScraper4589::parse_item(4589, &v).unwrap();
        assert_eq!(ee.entry.ext_id, "12345");
    }

    #[test]
    fn test_4589_parse_item_nat_only() {
        let v = serde_json::json!({"id": "x", "n": "Foo", "nat": "Italian"});
        let ee = BespokeScraper4589::parse_item(4589, &v).unwrap();
        assert_eq!(ee.entry.ext_desc, "Italian");
    }

    #[test]
    fn test_4589_parse_item_empty_dates_skipped() {
        let v = serde_json::json!({"id": "x", "n": "Foo", "d": "", "nat": "Italian"});
        let ee = BespokeScraper4589::parse_item(4589, &v).unwrap();
        assert_eq!(ee.entry.ext_desc, "Italian");
    }

    #[test]
    fn test_4589_parse_item_missing_id_skipped() {
        let v = serde_json::json!({"n": "Foo"});
        assert!(BespokeScraper4589::parse_item(4589, &v).is_none());
    }

    #[test]
    fn test_4589_parse_item_empty_name_skipped() {
        let v = serde_json::json!({"id": "x", "n": ""});
        assert!(BespokeScraper4589::parse_item(4589, &v).is_none());
    }

    #[test]
    fn test_4589_parse_item_composer_aux_attached_unconditionally() {
        // Even without dates / nationality the P106=Q36834 must be set —
        // every row in this catalog is a composer by definition.
        let v = serde_json::json!({"id": "x", "n": "Foo, Bar"});
        let ee = BespokeScraper4589::parse_item(4589, &v).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q36834".to_string())));
        assert_eq!(ee.aux.len(), 1);
    }
}
