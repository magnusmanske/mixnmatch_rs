use crate::{
    app_state::AppState, auxiliary_data::AuxiliaryRow, entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// ToposText - Ancient People (5103)

#[derive(Debug)]
pub struct BespokeScraper5103 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper5103 {

    scraper_boilerplate!(5103);

    async fn run(&self) -> Result<()> {
        let url = "https://topostext.org/api/people/readweb.php";
        let client = self.http_client();
        let json: serde_json::Value = client.get(url).send().await?.json().await?;
        let records = match json["records"].as_array() {
            Some(r) => r,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for record in records {
            if let Some(ee) = Self::parse_record(self.catalog_id(), record) {
                entry_cache.push(ee);
            self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper5103 {
    pub(crate) fn parse_record(
        catalog_id: usize,
        record: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = record["ID"].as_str().or({
            // Could be numeric
            None
        }).or_else(|| {
            record["ID"].as_i64().map(|_| "")
        })?;
        let id_str = if id.is_empty() {
            record["ID"].as_i64()?.to_string()
        } else {
            id.to_string()
        };
        if id_str.is_empty() {
            return None;
        }

        // Name: prefer "link", then "concat", then "searchname"
        let link = record["link"].as_str().unwrap_or_default();
        let concat = record["concat"].as_str().unwrap_or_default();
        let searchname = record["searchname"].as_str().unwrap_or_default();
        let name = if !link.is_empty() {
            link.to_string()
        } else if !concat.is_empty() {
            concat.to_string()
        } else if !searchname.is_empty() {
            searchname.to_string()
        } else {
            return None; // no usable name
        };

        let ext_url = format!("https://topostext.org/people/{}", id_str);

        // Build description
        let wikidesc = record["wikidesc"].as_str().unwrap_or_default();
        let period = record["period"].as_str().unwrap_or_default();
        let century = record["century"].as_str().unwrap_or_default();
        let gender = record["gender"].as_str().unwrap_or_default();
        let desc_parts = [
            wikidesc.to_string(),
            concat.to_string(),
            format!("Period: {}", period),
            format!("Century: {}", century),
            format!("Gender: {}", gender),
            searchname.to_string(),
        ];
        let desc = desc_parts.join("; ");

        // Type: Q22988604 (mythical character) for Myth period, Q5 (human) otherwise
        let type_name = if period == "Myth" {
            "Q22988604"
        } else {
            "Q5"
        };

        // Aux: gender (P21)
        let mut aux = std::collections::HashSet::new();
        match gender {
            "male" => {
                aux.insert(AuxiliaryRow::new(21, "Q6581097".to_string()));
            }
            "female" => {
                aux.insert(AuxiliaryRow::new(21, "Q6581072".to_string()));
            }
            _ => {}
        }

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id_str,
            ext_name: name,
            ext_desc: desc,
            ext_url,
            random: rand::rng().random(),
            type_name: Some(type_name.to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            aux,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scraper() -> BespokeScraper5103 {
        BespokeScraper5103 {
            app: crate::app_state::get_test_app(),
        }
    }

    #[test]
    fn test_5103_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 5103);
    }

    #[test]
    fn test_5103_parse_record_full() {
        let record = serde_json::json!({
            "ID": "12345",
            "link": "Socrates",
            "wikidesc": "Greek philosopher",
            "concat": "Socrates of Athens",
            "period": "Classical",
            "century": "5th BC",
            "gender": "male",
            "searchname": "socrates"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(ee.entry.ext_id, "12345");
        assert_eq!(ee.entry.ext_name, "Socrates");
        assert_eq!(
            ee.entry.ext_url,
            "https://topostext.org/people/12345"
        );
        assert_eq!(ee.entry.catalog, 5103);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        // Gender aux: P21 = Q6581097 (male)
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581097".to_string())));
        // Description should contain all parts
        assert!(ee.entry.ext_desc.contains("Greek philosopher"));
        assert!(ee.entry.ext_desc.contains("Period: Classical"));
        assert!(ee.entry.ext_desc.contains("Century: 5th BC"));
        assert!(ee.entry.ext_desc.contains("Gender: male"));
    }

    #[test]
    fn test_5103_parse_record_mythical() {
        let record = serde_json::json!({
            "ID": "100",
            "link": "Achilles",
            "wikidesc": "Greek hero",
            "concat": "Achilles",
            "period": "Myth",
            "century": "",
            "gender": "male",
            "searchname": "achilles"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(ee.entry.type_name, Some("Q22988604".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581097".to_string())));
    }

    #[test]
    fn test_5103_parse_record_female() {
        let record = serde_json::json!({
            "ID": "200",
            "link": "Sappho",
            "wikidesc": "Greek poet",
            "concat": "Sappho of Lesbos",
            "period": "Archaic",
            "century": "7th-6th BC",
            "gender": "female",
            "searchname": "sappho"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581072".to_string())));
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_5103_parse_record_unknown_gender() {
        let record = serde_json::json!({
            "ID": "300",
            "link": "Unknown Person",
            "wikidesc": "",
            "concat": "",
            "period": "Roman",
            "century": "1st",
            "gender": "",
            "searchname": "unknown"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert!(ee.aux.is_empty());
    }

    #[test]
    fn test_5103_parse_record_name_fallback_to_concat() {
        let record = serde_json::json!({
            "ID": "400",
            "link": "",
            "wikidesc": "",
            "concat": "Fallback Name",
            "period": "Classical",
            "century": "",
            "gender": "",
            "searchname": ""
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(ee.entry.ext_name, "Fallback Name");
    }

    #[test]
    fn test_5103_parse_record_name_fallback_to_searchname() {
        let record = serde_json::json!({
            "ID": "500",
            "link": "",
            "wikidesc": "",
            "concat": "",
            "period": "Classical",
            "century": "",
            "gender": "",
            "searchname": "last resort"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(ee.entry.ext_name, "last resort");
    }

    #[test]
    fn test_5103_parse_record_no_name_at_all() {
        let record = serde_json::json!({
            "ID": "600",
            "link": "",
            "concat": "",
            "searchname": "",
            "period": "",
            "century": "",
            "gender": "",
            "wikidesc": ""
        });
        assert!(BespokeScraper5103::parse_record(5103, &record).is_none());
    }

    #[test]
    fn test_5103_parse_record_missing_id() {
        let record = serde_json::json!({
            "link": "No ID Person",
            "concat": "",
            "searchname": "",
            "period": "",
            "century": "",
            "gender": "",
            "wikidesc": ""
        });
        assert!(BespokeScraper5103::parse_record(5103, &record).is_none());
    }

    #[test]
    fn test_5103_parse_record_numeric_id() {
        let record = serde_json::json!({
            "ID": 42,
            "link": "Numeric ID",
            "wikidesc": "",
            "concat": "",
            "period": "",
            "century": "",
            "gender": "",
            "searchname": ""
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(ee.entry.ext_id, "42");
    }

    #[test]
    fn test_5103_parse_record_description_format() {
        let record = serde_json::json!({
            "ID": "700",
            "link": "Test",
            "wikidesc": "A desc",
            "concat": "Full Name",
            "period": "Roman",
            "century": "2nd",
            "gender": "male",
            "searchname": "test"
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert_eq!(
            ee.entry.ext_desc,
            "A desc; Full Name; Period: Roman; Century: 2nd; Gender: male; test"
        );
    }

    #[test]
    fn test_5103_records_array_parsing() {
        let json = serde_json::json!({
            "records": [
                {
                    "ID": "1",
                    "link": "Person One",
                    "wikidesc": "",
                    "concat": "",
                    "period": "",
                    "century": "",
                    "gender": "",
                    "searchname": ""
                },
                {
                    "ID": "2",
                    "link": "Person Two",
                    "wikidesc": "",
                    "concat": "",
                    "period": "Myth",
                    "century": "",
                    "gender": "female",
                    "searchname": ""
                }
            ]
        });
        let records = json["records"].as_array().unwrap();
        let entries: Vec<ExtendedEntry> = records
            .iter()
            .filter_map(|r| BespokeScraper5103::parse_record(5103, r))
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry.type_name, Some("Q5".to_string()));
        assert_eq!(entries[1].entry.type_name, Some("Q22988604".to_string()));
        assert!(entries[1].aux.contains(&AuxiliaryRow::new(21, "Q6581072".to_string())));
    }

    #[test]
    fn test_5103_parse_record_ext_url_format() {
        let record = serde_json::json!({
            "ID": "999",
            "link": "URL Test",
            "wikidesc": "",
            "concat": "",
            "period": "",
            "century": "",
            "gender": "",
            "searchname": ""
        });
        let ee = BespokeScraper5103::parse_record(5103, &record).unwrap();
        assert!(ee.entry.ext_url.starts_with("https://topostext.org/people/"));
        assert!(ee.entry.ext_url.ends_with("999"));
    }
}
