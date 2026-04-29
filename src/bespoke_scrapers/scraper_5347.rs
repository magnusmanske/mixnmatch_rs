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
// Bauhaus community — persons (5347)
//
// Single bulk JSON fetch. Description is built from a fixed pattern
// "born X in Y [Z]; died X in Y [Z]" even when individual fields are
// missing — matches PHP behaviour so re-runs don't churn descriptions.
// `gender` is integer-coded: 1=male, 2=female. `gnd` becomes a P227
// auxiliary value.

#[derive(Debug)]
pub struct BespokeScraper5347 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5347 {
    scraper_boilerplate!(5347);

    async fn run(&self) -> Result<()> {
        let url = "https://bauhaus.community/bn_portal_data/indices/search_index.json";
        let json: serde_json::Value = self.http_client().get(url).send().await?.json().await?;
        let arr = match json.as_array() {
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

impl BespokeScraper5347 {
    pub(crate) fn parse_item(catalog_id: usize, p: &serde_json::Value) -> Option<ExtendedEntry> {
        let id = Self::stringify(p.get("id")?)?;
        if id.is_empty() {
            return None;
        }
        let given = p.get("given_names").and_then(|x| x.as_str()).unwrap_or("");
        let surname = p.get("surname").and_then(|x| x.as_str()).unwrap_or("");
        let ext_name = format!("{given} {surname}").trim().to_string();
        if ext_name.is_empty() {
            return None;
        }

        let dob = p
            .get("date_of_birth")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let dod = p
            .get("date_of_death")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let pob = p
            .get("place_of_birth")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let place_of_birth_country = p
            .get("place_of_birth_country")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let pod = p
            .get("place_of_death")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let place_of_death_country = p
            .get("place_of_death_country")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let ext_desc = format!(
            "born {dob} in {pob} [{place_of_birth_country}]; died {dod} in {pod} [{place_of_death_country}]"
        );

        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        match Self::gender_code(p.get("gender")) {
            Some(1) => {
                aux.insert(AuxiliaryRow::new(21, "Q6581097".to_string()));
            }
            Some(2) => {
                aux.insert(AuxiliaryRow::new(21, "Q6581072".to_string()));
            }
            _ => {}
        }
        if let Some(gnd) = p
            .get("gnd")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            aux.insert(AuxiliaryRow::new(227, gnd.to_string()));
        }

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            // Note: the PHP `url` here has a leading space — it's a
            // typo in the source. We strip it so all entries are
            // emitted with valid URLs.
            ext_url: format!("https://bauhaus.community/person/{id}"),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            born: PersonDate::from_db_string(dob),
            died: PersonDate::from_db_string(dod),
            ..Default::default()
        })
    }

    /// PHP compares `$p->gender == 1` with loose equality, so both the
    /// JSON int 1 and string "1" should map to male. Same for 2/female.
    fn gender_code(v: Option<&serde_json::Value>) -> Option<i64> {
        match v {
            Some(serde_json::Value::Number(n)) => n.as_i64(),
            Some(serde_json::Value::String(s)) => s.trim().parse().ok(),
            _ => None,
        }
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
    fn test_5347_parse_item_full() {
        let p = serde_json::json!({
            "id": 100,
            "given_names": "Walter",
            "surname": "Gropius",
            "date_of_birth": "1883-05-18",
            "date_of_death": "1969-07-05",
            "place_of_birth": "Berlin",
            "place_of_birth_country": "DE",
            "place_of_death": "Boston",
            "place_of_death_country": "US",
            "gender": 1,
            "gnd": "118542842"
        });
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert_eq!(ee.entry.ext_id, "100");
        assert_eq!(ee.entry.ext_name, "Walter Gropius");
        assert_eq!(
            ee.entry.ext_desc,
            "born 1883-05-18 in Berlin [DE]; died 1969-07-05 in Boston [US]"
        );
        assert_eq!(ee.entry.ext_url, "https://bauhaus.community/person/100");
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(21, "Q6581097".to_string()))
        );
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(227, "118542842".to_string()))
        );
        assert_eq!(ee.born, Some(PersonDate::year_month_day(1883, 5, 18)));
        assert_eq!(ee.died, Some(PersonDate::year_month_day(1969, 7, 5)));
    }

    #[test]
    fn test_5347_parse_item_female_gender() {
        let p = serde_json::json!({
            "id": 1, "given_names": "Anni", "surname": "Albers", "gender": 2
        });
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(21, "Q6581072".to_string()))
        );
    }

    #[test]
    fn test_5347_parse_item_string_gender_accepted() {
        let p = serde_json::json!({
            "id": 1, "given_names": "X", "surname": "Y", "gender": "1"
        });
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(21, "Q6581097".to_string()))
        );
    }

    #[test]
    fn test_5347_parse_item_unknown_gender_no_p21() {
        let p = serde_json::json!({
            "id": 1, "given_names": "X", "surname": "Y", "gender": 9
        });
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 21));
    }

    #[test]
    fn test_5347_parse_item_empty_gnd_skipped() {
        let p = serde_json::json!({
            "id": 1, "given_names": "X", "surname": "Y", "gnd": ""
        });
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 227));
    }

    #[test]
    fn test_5347_parse_item_desc_pattern_with_empty_fields() {
        // Even when every place / date is empty, the desc shape must be
        // stable — that's the PHP behaviour and updating descriptions
        // on existing entries would churn rows for no semantic gain.
        let p = serde_json::json!({"id": 1, "given_names": "X", "surname": "Y"});
        let ee = BespokeScraper5347::parse_item(5347, &p).unwrap();
        assert_eq!(ee.entry.ext_desc, "born  in  []; died  in  []");
    }

    #[test]
    fn test_5347_parse_item_missing_id_skipped() {
        let p = serde_json::json!({"given_names": "X", "surname": "Y"});
        assert!(BespokeScraper5347::parse_item(5347, &p).is_none());
    }
}
