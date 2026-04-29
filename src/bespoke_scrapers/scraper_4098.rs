use crate::{app_state::AppState, entry::Entry, person_date::PersonDate};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Studium parisiense (4098) — description-only enricher
//
// Unlike the other json_scraper.php branches this one does NOT create
// new entries. It iterates over every entry in catalog 4098 whose
// `ext_desc` is empty and fills the gap from the prosopography API.
// In addition to the description string, it sets person dates (P569
// born, P570 died) and an inferred P21 gender aux when the API
// surfaces one.

const ENTRY_BATCH_SIZE: usize = 5000;

#[derive(Debug)]
pub struct BespokeScraper4098 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4098 {
    scraper_boilerplate!(4098);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut offset = 0;
        loop {
            let entries = self
                .app()
                .storage()
                .get_entry_batch(self.catalog_id(), ENTRY_BATCH_SIZE, offset)
                .await?;
            if entries.is_empty() {
                break;
            }
            let batch_len = entries.len();
            for entry in entries {
                if !entry.ext_desc.is_empty() {
                    continue;
                }
                let url = format!(
                    "http://studium.univ-paris1.fr/api/prosopography/{}",
                    entry.ext_id
                );
                let json = match client.get(&url).send().await {
                    Ok(resp) => match resp.json::<serde_json::Value>().await {
                        Ok(j) => j,
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };
                let parsed = Self::parse_response(&json);
                self.apply_to_entry(entry, parsed).await?;
            }
            offset += batch_len;
        }
        Ok(())
    }
}

impl BespokeScraper4098 {
    /// Apply the parsed response to a live entry. `Entry::set_*` mutators
    /// take `&mut self` and need an `app` reference — wire it before
    /// calling so the storage round-trip happens against the right
    /// connection pool.
    async fn apply_to_entry(&self, mut entry: Entry, parsed: ParsedProsopography) -> Result<()> {
        entry.set_app(self.app());
        if !parsed.desc.is_empty() {
            entry.set_ext_desc(&parsed.desc).await?;
        }
        if parsed.born.is_some() || parsed.died.is_some() {
            entry.set_person_dates(&parsed.born, &parsed.died).await?;
        }
        for (prop, value) in parsed.aux {
            let _ = entry.set_auxiliary(prop, Some(value)).await;
        }
        Ok(())
    }

    /// Pure parsing of the prosopography JSON. Extracted into its own
    /// function so the JSON-shape handling can be unit-tested without
    /// a database.
    pub(crate) fn parse_response(j: &serde_json::Value) -> ParsedProsopography {
        let mut desc_parts: Vec<String> = vec![];
        let mut born = String::new();
        let mut died = String::new();
        let mut aux: Vec<(usize, String)> = vec![];

        for v in j["identity"]["shortDescription"].as_array().into_iter().flatten() {
            if let Some(s) = v.get("value").and_then(|x| x.as_str()) {
                desc_parts.push(s.to_string());
            }
        }
        for v in j["identity"]["datesOfLife"].as_array().into_iter().flatten() {
            if let Some(s) = v.get("value").and_then(|x| x.as_str()) {
                desc_parts.push(Self::strip_backslash_percent(s.trim()));
            }
            for date in v["meta"]["dates"].as_array().into_iter().flatten() {
                if let Some(s) = date["startDate"]["date"].as_str() {
                    born = s.to_string();
                }
                if let Some(s) = date["endDate"]["date"].as_str() {
                    died = s.to_string();
                }
            }
        }
        for bp in j["origin"]["birthPlace"].as_array().into_iter().flatten() {
            if let Some(s) = bp.get("value").and_then(|x| x.as_str()) {
                desc_parts.push(format!("Born in {s}"));
            }
        }
        for v in j["identity"]["gender"].as_array().into_iter().flatten() {
            if let Some(s) = v.get("value").and_then(|x| x.as_str()) {
                desc_parts.push(format!("gender: {s}"));
                match s {
                    "male" => aux.push((21, "Q6581097".to_string())),
                    "female" => aux.push((21, "Q6581072".to_string())),
                    _ => {}
                }
            }
        }

        let desc = desc_parts.join("; ").trim().to_string();
        ParsedProsopography {
            desc,
            born: PersonDate::from_db_string(&born),
            died: PersonDate::from_db_string(&died),
            aux,
        }
    }

    /// Mirrors PHP `preg_replace('/[\\\\%]/', '', $v->value)` —
    /// strips backslash and percent characters from the value. The
    /// percent sometimes appears in API output as a placeholder for
    /// characters the source database couldn't render.
    pub(crate) fn strip_backslash_percent(s: &str) -> String {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"[\\%]").expect("regex");
        }
        RE.replace_all(s, "").to_string()
    }
}

/// Decoded prosopography response. Fields mirror the four kinds of
/// downstream write the PHP performs: description string, person
/// dates, and auxiliary props.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ParsedProsopography {
    pub(crate) desc: String,
    pub(crate) born: Option<PersonDate>,
    pub(crate) died: Option<PersonDate>,
    pub(crate) aux: Vec<(usize, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4098_strip_backslash_percent() {
        assert_eq!(
            BespokeScraper4098::strip_backslash_percent("a%b\\c"),
            "abc"
        );
        assert_eq!(BespokeScraper4098::strip_backslash_percent("plain"), "plain");
    }

    #[test]
    fn test_4098_parse_response_full() {
        let j = serde_json::json!({
            "identity": {
                "shortDescription": [{"value": "scholar"}],
                "datesOfLife": [{
                    "value": "1100-1180",
                    "meta": {"dates": [{
                        "startDate": {"date": "1100"},
                        "endDate": {"date": "1180"}
                    }]}
                }],
                "gender": [{"value": "male"}]
            },
            "origin": {
                "birthPlace": [{"value": "Paris"}]
            }
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(
            p.desc,
            "scholar; 1100-1180; Born in Paris; gender: male"
        );
        assert_eq!(p.born, Some(PersonDate::year_only(1100)));
        assert_eq!(p.died, Some(PersonDate::year_only(1180)));
        assert_eq!(p.aux, vec![(21, "Q6581097".to_string())]);
    }

    #[test]
    fn test_4098_parse_response_female_aux() {
        let j = serde_json::json!({
            "identity": {"gender": [{"value": "female"}]}
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p.aux, vec![(21, "Q6581072".to_string())]);
    }

    #[test]
    fn test_4098_parse_response_unknown_gender_no_aux() {
        let j = serde_json::json!({
            "identity": {"gender": [{"value": "other"}]}
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert!(p.aux.is_empty());
    }

    #[test]
    fn test_4098_parse_response_dates_strip_backslash_percent() {
        let j = serde_json::json!({
            "identity": {"datesOfLife": [{"value": "1100\\-1180%"}]}
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p.desc, "1100-1180");
    }

    #[test]
    fn test_4098_parse_response_dates_only_no_meta() {
        let j = serde_json::json!({
            "identity": {"datesOfLife": [{"value": "fl. 1100"}]}
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p.desc, "fl. 1100");
        assert!(p.born.is_none());
        assert!(p.died.is_none());
    }

    #[test]
    fn test_4098_parse_response_empty_arrays() {
        let j = serde_json::json!({"identity": {}, "origin": {}});
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p, ParsedProsopography::default());
    }

    #[test]
    fn test_4098_parse_response_birthplace_only() {
        let j = serde_json::json!({
            "origin": {"birthPlace": [{"value": "Reims"}]}
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p.desc, "Born in Reims");
    }

    #[test]
    fn test_4098_parse_response_multiple_short_descriptions_joined() {
        let j = serde_json::json!({
            "identity": {
                "shortDescription": [{"value": "scholar"}, {"value": "theologian"}]
            }
        });
        let p = BespokeScraper4098::parse_response(&j);
        assert_eq!(p.desc, "scholar; theologian");
    }
}
