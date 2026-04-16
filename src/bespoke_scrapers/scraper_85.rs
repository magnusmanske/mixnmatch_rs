use crate::{
    app_state::AppState, entry::Entry, extended_entry::ExtendedEntry, person_date::PersonDate,
};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// Hoogleraren RUG - Dutch Professors at University of Groningen (85)

#[derive(Debug)]
pub struct BespokeScraper85 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper85 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        85
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        let mut start: u64 = 0;
        loop {
            let url = format!(
                "https://hoogleraren.ub.rug.nl/search/json?searchfields=all&all=*:*&sort=score&order=asc&start={}",
                start
            );
            let resp = client.get(&url).send().await?;
            let text = resp.text().await?;
            if text.is_empty() {
                break;
            }
            let json: serde_json::Value = match serde_json::from_str(&text) {
                Ok(j) => j,
                Err(_) => break,
            };
            let arr = match json.as_array() {
                Some(arr) if !arr.is_empty() => arr,
                _ => break,
            };
            for item in arr {
                if let Some(ee) = Self::parse_item(self.catalog_id(), item) {
                    entry_cache.push(ee);
                }
            }
            if entry_cache.len() >= 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
            start += 30;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper85 {
    pub(crate) fn parse_item(
        catalog_id: usize,
        item: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = item["hoogleraar_id"].as_str().or_else(|| {
            // Could also be a number
            None
        }).or_else(|| {
            item["hoogleraar_id"].as_i64().map(|_| "") // handled below
        })?;
        let id_str = if id.is_empty() {
            item["hoogleraar_id"].as_i64()?.to_string()
        } else {
            id.to_string()
        };
        if id_str.is_empty() {
            return None;
        }

        let voornamen = item["hoogleraar_voornamen"].as_str().unwrap_or_default();
        let voorvoegsels = item["hoogleraar_voorvoegsels"].as_str().unwrap_or_default();
        let achternaam = item["hoogleraar_achternaam"].as_str().unwrap_or_default();
        let name = Self::build_name(voornamen, voorvoegsels, achternaam);
        if name.is_empty() {
            return None;
        }

        let desc = item["benoeming_leeropdracht"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let ext_url = format!("http://hoogleraren.ub.rug.nl/hoogleraren/{}", id_str);

        let born = item["hoogleraar_geboortedatum"]
            .as_str()
            .and_then(|s| Self::parse_date(s));
        let died = item["hoogleraar_overlijdensdatum"]
            .as_str()
            .and_then(|s| Self::parse_date(s));

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id_str,
            ext_name: name,
            ext_desc: desc,
            ext_url,
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            born,
            died,
            ..Default::default()
        })
    }

    /// Build a name from first names, prefix (tussenvoegsels), and last name.
    /// Collapse multiple spaces into one.
    pub(crate) fn build_name(voornamen: &str, voorvoegsels: &str, achternaam: &str) -> String {
        let parts = [voornamen, voorvoegsels, achternaam];
        let joined: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).copied().collect();
        joined.join(" ")
    }

    /// Parse an ISO datetime string like "1990-01-15T00:00:00" into a PersonDate.
    /// Strips the "T..." suffix, then delegates to PersonDate.
    pub(crate) fn parse_date(s: &str) -> Option<PersonDate> {
        if s.is_empty() {
            return None;
        }
        // Strip the time portion after 'T'
        let date_part = s.split('T').next().unwrap_or(s);
        if date_part.is_empty() {
            return None;
        }
        PersonDate::from_db_string(date_part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper85 {
        BespokeScraper85 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_85_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 85);
    }

    #[test]
    fn test_85_build_name_full() {
        assert_eq!(
            BespokeScraper85::build_name("Johannes", "van der", "Berg"),
            "Johannes van der Berg"
        );
    }

    #[test]
    fn test_85_build_name_no_prefix() {
        assert_eq!(
            BespokeScraper85::build_name("Pieter", "", "Groot"),
            "Pieter Groot"
        );
    }

    #[test]
    fn test_85_build_name_only_lastname() {
        assert_eq!(BespokeScraper85::build_name("", "", "Janssen"), "Janssen");
    }

    #[test]
    fn test_85_build_name_empty() {
        assert_eq!(BespokeScraper85::build_name("", "", ""), "");
    }

    #[test]
    fn test_85_parse_date_iso_datetime() {
        assert_eq!(
            BespokeScraper85::parse_date("1990-01-15T00:00:00"),
            Some(PersonDate::year_month_day(1990, 1, 15))
        );
    }

    #[test]
    fn test_85_parse_date_iso_date_only() {
        assert_eq!(
            BespokeScraper85::parse_date("1850-06-23"),
            Some(PersonDate::year_month_day(1850, 6, 23))
        );
    }

    #[test]
    fn test_85_parse_date_year_only() {
        assert_eq!(
            BespokeScraper85::parse_date("1900"),
            Some(PersonDate::year_only(1900))
        );
    }

    #[test]
    fn test_85_parse_date_empty() {
        assert!(BespokeScraper85::parse_date("").is_none());
    }

    #[test]
    fn test_85_parse_date_just_t() {
        // Edge case: string is just "T..."
        assert!(BespokeScraper85::parse_date("T12:00:00").is_none());
    }

    #[test]
    fn test_85_parse_item_full() {
        let item = serde_json::json!({
            "hoogleraar_id": "12345",
            "hoogleraar_voornamen": "Johannes",
            "hoogleraar_voorvoegsels": "van",
            "hoogleraar_achternaam": "Berg",
            "hoogleraar_geboortedatum": "1850-03-15T00:00:00",
            "hoogleraar_overlijdensdatum": "1920-11-01T00:00:00",
            "benoeming_leeropdracht": "Professor of Theology"
        });
        let ee = BespokeScraper85::parse_item(85, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "12345");
        assert_eq!(ee.entry.ext_name, "Johannes van Berg");
        assert_eq!(ee.entry.ext_desc, "Professor of Theology");
        assert_eq!(
            ee.entry.ext_url,
            "http://hoogleraren.ub.rug.nl/hoogleraren/12345"
        );
        assert_eq!(ee.entry.catalog, 85);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert_eq!(ee.born, Some(PersonDate::year_month_day(1850, 3, 15)));
        assert_eq!(ee.died, Some(PersonDate::year_month_day(1920, 11, 1)));
    }

    #[test]
    fn test_85_parse_item_no_dates() {
        let item = serde_json::json!({
            "hoogleraar_id": "999",
            "hoogleraar_voornamen": "Pieter",
            "hoogleraar_achternaam": "Groot"
        });
        let ee = BespokeScraper85::parse_item(85, &item).unwrap();
        assert_eq!(ee.entry.ext_name, "Pieter Groot");
        assert!(ee.born.is_none());
        assert!(ee.died.is_none());
        assert_eq!(ee.entry.ext_desc, "");
    }

    #[test]
    fn test_85_parse_item_missing_id() {
        let item = serde_json::json!({
            "hoogleraar_voornamen": "Test",
            "hoogleraar_achternaam": "User"
        });
        assert!(BespokeScraper85::parse_item(85, &item).is_none());
    }

    #[test]
    fn test_85_parse_item_empty_name_parts() {
        // All name parts missing => empty name => None
        let item = serde_json::json!({
            "hoogleraar_id": "111"
        });
        assert!(BespokeScraper85::parse_item(85, &item).is_none());
    }

    #[test]
    fn test_85_parse_item_numeric_id() {
        let item = serde_json::json!({
            "hoogleraar_id": 42,
            "hoogleraar_voornamen": "Jan",
            "hoogleraar_achternaam": "Vries"
        });
        let ee = BespokeScraper85::parse_item(85, &item).unwrap();
        assert_eq!(ee.entry.ext_id, "42");
    }

    #[test]
    fn test_85_parse_item_ext_url_format() {
        let item = serde_json::json!({
            "hoogleraar_id": "7890",
            "hoogleraar_voornamen": "A",
            "hoogleraar_achternaam": "B"
        });
        let ee = BespokeScraper85::parse_item(85, &item).unwrap();
        assert_eq!(
            ee.entry.ext_url,
            "http://hoogleraren.ub.rug.nl/hoogleraren/7890"
        );
    }

    #[test]
    fn test_85_json_array_response() {
        // Simulate the API returning an array at top level
        let json: serde_json::Value = serde_json::json!([
            {
                "hoogleraar_id": "1",
                "hoogleraar_voornamen": "A",
                "hoogleraar_achternaam": "B"
            },
            {
                "hoogleraar_id": "2",
                "hoogleraar_voornamen": "C",
                "hoogleraar_achternaam": "D"
            }
        ]);
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        let entries: Vec<ExtendedEntry> = arr
            .iter()
            .filter_map(|item| BespokeScraper85::parse_item(85, item))
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.ext_id, "1");
        assert_eq!(entries[1].entry.ext_id, "2");
    }

    #[test]
    fn test_85_empty_array_response() {
        let json: serde_json::Value = serde_json::json!([]);
        let arr = json.as_array().unwrap();
        assert!(arr.is_empty());
    }
}
