use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry, extended_entry::ExtendedEntry,
    person_date::PersonDate,
};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;
use std::collections::HashSet;

use super::BespokeScraper;

// ______________________________________________________
// PhotoLondon — photographers (4681)
//
// Single bulk fetch of up to 1000 people. Every entry is by definition
// a photographer (P106=Q33231); P21 (sex/gender) is set when the API
// reports `male` or `female`. Birth/death are year-only fields, with
// `'0'` used as the "unknown" sentinel — those are dropped.

#[derive(Debug)]
pub struct BespokeScraper4681 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4681 {
    scraper_boilerplate!(4681);

    async fn run(&self) -> Result<()> {
        let url = "https://www.photolondon.org.uk/api/public/people/a/0/1000";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let arr = match json["people"].as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for p in arr {
            if let Some(ee) = Self::parse_item(self.catalog_id(), p) {
                entry_cache.push(ee);
                self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4681 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        p: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = Self::stringify(p.get("id")?)?;
        if id.is_empty() {
            return None;
        }
        let forenames = p.get("p_forenames").and_then(|x| x.as_str()).unwrap_or("");
        let lastname = p.get("p_lastname").and_then(|x| x.as_str()).unwrap_or("");
        let ext_name = format!("{forenames} {lastname}").trim().to_string();
        if ext_name.is_empty() {
            return None;
        }
        let ext_desc = p
            .get("p_briefBio")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        // P106 = occupation; Q33231 = photographer.
        aux.insert(AuxiliaryRow::new(106, "Q33231".to_string()));
        match p.get("p_gender").and_then(|x| x.as_str()) {
            Some("male") => {
                aux.insert(AuxiliaryRow::new(21, "Q6581097".to_string()));
            }
            Some("female") => {
                aux.insert(AuxiliaryRow::new(21, "Q6581072".to_string()));
            }
            _ => {}
        }

        let born = Self::sanitise_year(p.get("p_birthYearOnly"));
        let died = Self::sanitise_year(p.get("p_deathYearOnly"));

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!("https://www.photolondon.org.uk/#/details?id={id}"),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            born: PersonDate::from_db_string(&born),
            died: PersonDate::from_db_string(&died),
            ..Default::default()
        })
    }

    /// PHP treats `'0'` as the "unknown year" sentinel — strip it. Empty
    /// strings stay empty. Numeric values are stringified to mirror the
    /// PHP coercion.
    pub(crate) fn sanitise_year(v: Option<&serde_json::Value>) -> String {
        let s = match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            _ => return String::new(),
        };
        if s == "0" { String::new() } else { s }
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
    fn test_4681_sanitise_year_zero_treated_as_empty() {
        let v = serde_json::json!("0");
        assert_eq!(BespokeScraper4681::sanitise_year(Some(&v)), "");
    }

    #[test]
    fn test_4681_sanitise_year_numeric_zero_treated_as_empty() {
        let v = serde_json::json!(0);
        assert_eq!(BespokeScraper4681::sanitise_year(Some(&v)), "");
    }

    #[test]
    fn test_4681_sanitise_year_real_year() {
        let v = serde_json::json!("1880");
        assert_eq!(BespokeScraper4681::sanitise_year(Some(&v)), "1880");
    }

    #[test]
    fn test_4681_sanitise_year_missing() {
        assert_eq!(BespokeScraper4681::sanitise_year(None), "");
    }

    #[test]
    fn test_4681_parse_item_full_male() {
        let p = serde_json::json!({
            "id": 100,
            "p_forenames": "Julia Margaret",
            "p_lastname": "Cameron",
            "p_briefBio": "Pioneering portraitist.",
            "p_gender": "female",
            "p_birthYearOnly": "1815",
            "p_deathYearOnly": "1879"
        });
        let ee = BespokeScraper4681::parse_item(4681, &p).unwrap();
        assert_eq!(ee.entry.ext_id, "100");
        assert_eq!(ee.entry.ext_name, "Julia Margaret Cameron");
        assert_eq!(ee.entry.ext_desc, "Pioneering portraitist.");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.photolondon.org.uk/#/details?id=100"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q33231".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581072".to_string())));
        assert_eq!(ee.born, Some(PersonDate::year_only(1815)));
        assert_eq!(ee.died, Some(PersonDate::year_only(1879)));
    }

    #[test]
    fn test_4681_parse_item_male_gender_aux() {
        let p = serde_json::json!({
            "id": "x",
            "p_forenames": "John",
            "p_lastname": "Doe",
            "p_gender": "male"
        });
        let ee = BespokeScraper4681::parse_item(4681, &p).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581097".to_string())));
    }

    #[test]
    fn test_4681_parse_item_unknown_gender_no_p21() {
        let p = serde_json::json!({
            "id": "x",
            "p_forenames": "X",
            "p_lastname": "Y",
            "p_gender": "other"
        });
        let ee = BespokeScraper4681::parse_item(4681, &p).unwrap();
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 21));
        // P106 photographer aux is still present unconditionally.
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q33231".to_string())));
    }

    #[test]
    fn test_4681_parse_item_zero_dates_dropped() {
        let p = serde_json::json!({
            "id": "x",
            "p_forenames": "X",
            "p_lastname": "Y",
            "p_birthYearOnly": "0",
            "p_deathYearOnly": "0"
        });
        let ee = BespokeScraper4681::parse_item(4681, &p).unwrap();
        assert!(ee.born.is_none());
        assert!(ee.died.is_none());
    }

    #[test]
    fn test_4681_parse_item_missing_id_skipped() {
        let p = serde_json::json!({"p_forenames": "X", "p_lastname": "Y"});
        assert!(BespokeScraper4681::parse_item(4681, &p).is_none());
    }

    #[test]
    fn test_4681_parse_item_empty_name_skipped() {
        let p = serde_json::json!({"id": 1, "p_forenames": "", "p_lastname": ""});
        assert!(BespokeScraper4681::parse_item(4681, &p).is_none());
    }
}
