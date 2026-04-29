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
// Foto CH — Swiss photographers (4600)
//
// Single bulk fetch from foto-ch.ch with two upstream quirks:
//
// 1. The endpoint sometimes returns the same JSON object concatenated
//    twice (`{...}{...}`). The PHP splits on `}{` and keeps only the
//    first half — we mirror that defensively before parsing.
//
// 2. Date fields come in two flavours decided by a `gen_*` flag:
//    flag=0 → full geburtsdatum string, flag=1 → year-only (first 4
//    chars). Date strings starting with `'0'` ("0000-…") are treated
//    as placeholders and skipped. PHP also replaces the default
//    P106=Q33231 (photographer) aux with P227=GND when `pnd` is
//    present — preserving that "replace, not append" behaviour here.

#[derive(Debug)]
pub struct BespokeScraper4600 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4600 {
    scraper_boilerplate!(4600);

    async fn run(&self) -> Result<()> {
        let url =
            "https://en.foto-ch.ch/api/?a=streamsearch&type=photographer&limit=25000&offset=0&lang=en";
        let text = self.http_client().get(url).send().await?.text().await?;
        let cleaned = Self::dedup_concatenated_json(&text);
        let json: serde_json::Value = match serde_json::from_str(&cleaned) {
            Ok(j) => j,
            Err(_) => return Ok(()),
        };
        let arr = match json["photographer_results"].as_array() {
            Some(arr) => arr,
            None => return Ok(()),
        };
        let mut entry_cache = vec![];
        for v in arr {
            if let Some(ee) = Self::parse_item(self.catalog_id(), v) {
                entry_cache.push(ee);
                self.maybe_flush_cache(&mut entry_cache).await?;
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4600 {
    /// Mirror PHP `$parts = explode('}{', $text); if (count($parts) > 1)
    /// $text = $parts[0] . '}';`. Defensive — the upstream sometimes
    /// emits two concatenated copies of the same JSON object; we keep
    /// only the first.
    pub(crate) fn dedup_concatenated_json(text: &str) -> String {
        if let Some(idx) = text.find("}{") {
            let mut out = text[..idx].to_string();
            out.push('}');
            out
        } else {
            text.to_string()
        }
    }

    pub(crate) fn parse_item(
        catalog_id: usize,
        v: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = Self::stringify(v.get("id")?)?;
        if id.is_empty() {
            return None;
        }
        let vorname = v.get("vorname").and_then(|x| x.as_str()).unwrap_or("");
        let nachname = v.get("nachname").and_then(|x| x.as_str()).unwrap_or("");
        let ext_name = format!("{vorname} {nachname}").trim().to_string();
        if ext_name.is_empty() {
            return None;
        }

        let born = Self::extract_date(v, "gen_geburtsdatum", "geburtsdatum");
        let died = Self::extract_date(v, "gen_todesdatum", "todesdatum");

        let mut desc_parts: Vec<String> = vec![];
        if !born.is_empty() || !died.is_empty() {
            desc_parts.push(format!("{born} - {died}"));
        }
        for k in ["namenszusatz", "titel", "fotografengattungen_set", "arbeitsorte"] {
            if let Some(s) = v.get(k).and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
                desc_parts.push(format!("{k}: {s}"));
            }
        }
        let ext_desc = desc_parts.join(" | ");

        // PHP starts with `aux = [['P106', 'Q33231']]` and then *replaces*
        // the whole array with `['P227', $pnd]` when pnd is non-empty.
        // We preserve that replace semantic (not append) so behaviour is
        // 1:1 with the source script.
        let mut aux: HashSet<AuxiliaryRow> = HashSet::new();
        let pnd = v.get("pnd").and_then(|x| x.as_str()).filter(|s| !s.is_empty());
        match pnd {
            Some(g) => {
                // P227 = GND identifier
                aux.insert(AuxiliaryRow::new(227, g.to_string()));
            }
            None => {
                // P106 = occupation; Q33231 = photographer
                aux.insert(AuxiliaryRow::new(106, "Q33231".to_string()));
            }
        }

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!(
                "https://en.foto-ch.ch/photographer?detail={id}&type=photographer"
            ),
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

    /// Apply the `gen_*` flag to pick between the full date string and
    /// the year-only first 4 chars. Empty / `'0…'` placeholder values
    /// produce an empty result.
    pub(crate) fn extract_date(
        v: &serde_json::Value,
        flag_field: &str,
        value_field: &str,
    ) -> String {
        let raw = match v.get(value_field).and_then(|x| x.as_str()) {
            Some(s) if !s.is_empty() && !s.starts_with('0') => s,
            _ => return String::new(),
        };
        let flag = Self::truthy(v.get(flag_field));
        if flag {
            // Year only — first 4 chars (PHP `substr($x, 0, 4)`).
            raw.chars().take(4).collect()
        } else {
            raw.to_string()
        }
    }

    /// PHP `$x * 1 == 1` after stringifying. Returns true for numeric 1
    /// or string "1"; everything else is false.
    fn truthy(v: Option<&serde_json::Value>) -> bool {
        match v {
            Some(serde_json::Value::Number(n)) => n.as_i64() == Some(1),
            Some(serde_json::Value::String(s)) => s.trim() == "1",
            _ => false,
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
    fn test_4600_dedup_concatenated_json_splits() {
        let dup = r#"{"a":1}{"a":1}"#;
        assert_eq!(BespokeScraper4600::dedup_concatenated_json(dup), r#"{"a":1}"#);
    }

    #[test]
    fn test_4600_dedup_passthrough_when_single() {
        let single = r#"{"a":1}"#;
        assert_eq!(
            BespokeScraper4600::dedup_concatenated_json(single),
            r#"{"a":1}"#
        );
    }

    #[test]
    fn test_4600_extract_date_full_when_flag_zero() {
        let v = serde_json::json!({"gen_geburtsdatum": "0", "geburtsdatum": "1850-03-15"});
        assert_eq!(
            BespokeScraper4600::extract_date(&v, "gen_geburtsdatum", "geburtsdatum"),
            "1850-03-15"
        );
    }

    #[test]
    fn test_4600_extract_date_year_only_when_flag_one() {
        let v = serde_json::json!({"gen_geburtsdatum": "1", "geburtsdatum": "1850-03-15"});
        assert_eq!(
            BespokeScraper4600::extract_date(&v, "gen_geburtsdatum", "geburtsdatum"),
            "1850"
        );
    }

    #[test]
    fn test_4600_extract_date_skips_zero_prefix_placeholder() {
        let v = serde_json::json!({"gen_geburtsdatum": "0", "geburtsdatum": "0000-00-00"});
        assert_eq!(
            BespokeScraper4600::extract_date(&v, "gen_geburtsdatum", "geburtsdatum"),
            ""
        );
    }

    #[test]
    fn test_4600_extract_date_empty_when_field_missing() {
        let v = serde_json::json!({"gen_geburtsdatum": "0"});
        assert_eq!(
            BespokeScraper4600::extract_date(&v, "gen_geburtsdatum", "geburtsdatum"),
            ""
        );
    }

    #[test]
    fn test_4600_parse_item_pnd_replaces_default_aux() {
        let v = serde_json::json!({
            "id": 100,
            "vorname": "Hans",
            "nachname": "Müller",
            "pnd": "12345"
        });
        let ee = BespokeScraper4600::parse_item(4600, &v).unwrap();
        // PHP behaviour: pnd replaces the default photographer aux entirely.
        assert!(ee.aux.contains(&AuxiliaryRow::new(227, "12345".to_string())));
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 106));
    }

    #[test]
    fn test_4600_parse_item_no_pnd_keeps_photographer_aux() {
        let v = serde_json::json!({
            "id": 200,
            "vorname": "Anna",
            "nachname": "Bühler"
        });
        let ee = BespokeScraper4600::parse_item(4600, &v).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(106, "Q33231".to_string())));
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 227));
    }

    #[test]
    fn test_4600_parse_item_full_with_dates_and_metadata() {
        let v = serde_json::json!({
            "id": 300,
            "vorname": "Max",
            "nachname": "Werner",
            "gen_geburtsdatum": "0",
            "geburtsdatum": "1850-03-15",
            "gen_todesdatum": "1",
            "todesdatum": "1920-07-22",
            "namenszusatz": "Sr.",
            "titel": "Dr.",
            "fotografengattungen_set": "Portrait",
            "arbeitsorte": "Bern, Zürich"
        });
        let ee = BespokeScraper4600::parse_item(4600, &v).unwrap();
        assert_eq!(ee.entry.ext_id, "300");
        assert_eq!(ee.entry.ext_name, "Max Werner");
        assert_eq!(
            ee.entry.ext_desc,
            "1850-03-15 - 1920 | namenszusatz: Sr. | titel: Dr. | fotografengattungen_set: Portrait | arbeitsorte: Bern, Zürich"
        );
        assert_eq!(
            ee.born,
            Some(PersonDate::year_month_day(1850, 3, 15))
        );
        assert_eq!(ee.died, Some(PersonDate::year_only(1920)));
    }

    #[test]
    fn test_4600_parse_item_missing_id_skipped() {
        let v = serde_json::json!({"vorname": "X", "nachname": "Y"});
        assert!(BespokeScraper4600::parse_item(4600, &v).is_none());
    }

    #[test]
    fn test_4600_parse_item_empty_name_skipped() {
        let v = serde_json::json!({"id": 1, "vorname": "", "nachname": ""});
        assert!(BespokeScraper4600::parse_item(4600, &v).is_none());
    }

    #[test]
    fn test_4600_parse_item_only_first_name() {
        let v = serde_json::json!({"id": 1, "vorname": "Mononymous"});
        let ee = BespokeScraper4600::parse_item(4600, &v).unwrap();
        assert_eq!(ee.entry.ext_name, "Mononymous");
    }
}
