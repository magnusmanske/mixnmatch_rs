use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use rand::RngExt;

use super::BespokeScraper;

// ______________________________________________________
// UZH Herbaria — collectors (4966)
//
// Per-letter A-Z scrape of a Kendo-style backend. Unlike the
// classicalarchives / Meyers loops this one POSTs a fixed
// `_class/_action/filter` body for each letter; the upstream returns
// `data: [...]` of collector records. PHP unconditionally writes
// "born: …; died: …" into the description even when the API leaves
// those fields empty — preserved here so the Mix'n'match descriptions
// remain stable across reruns.

#[derive(Debug)]
pub struct BespokeScraper4966 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4966 {
    scraper_boilerplate!(4966);

    async fn run(&self) -> Result<()> {
        let url = "https://www.herbarien.uzh.ch/static/manager/app/index.php";
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in 'A'..='Z' {
            let body = Self::build_post_body(letter);
            let response = match client
                .post(url)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };
            let json: serde_json::Value = match response.json().await {
                Ok(j) => j,
                Err(_) => continue,
            };
            let arr = match json["data"].as_array() {
                Some(arr) => arr,
                None => continue,
            };
            for d in arr {
                if let Some(ee) = Self::parse_item(self.catalog_id(), d) {
                    entry_cache.push(ee);
                    self.maybe_flush_cache(&mut entry_cache).await?;
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4966 {
    /// Build the form-urlencoded POST body. The shape is fixed except
    /// for the surname-prefix letter; everything else (the `_class`,
    /// `_action`, and second filter hardcoding `publish_on_herbarien=1`)
    /// is verbatim PHP.
    pub(crate) fn build_post_body(letter: char) -> String {
        format!(
            "_class=Herbar%5CController%5CWebCollectorsController\
             &_action=readList\
             &filter%5Blogic%5D=and\
             &filter%5Bfilters%5D%5B0%5D%5Bfield%5D=last_name\
             &filter%5Bfilters%5D%5B0%5D%5Boperator%5D=startswith\
             &filter%5Bfilters%5D%5B0%5D%5Bvalue%5D={letter}\
             &filter%5Bfilters%5D%5B1%5D%5Bfield%5D=publish_on_herbarien\
             &filter%5Bfilters%5D%5B1%5D%5Boperator%5D=eq\
             &filter%5Bfilters%5D%5B1%5D%5Bvalue%5D=1"
        )
    }

    pub(crate) fn parse_item(
        catalog_id: usize,
        d: &serde_json::Value,
    ) -> Option<ExtendedEntry> {
        let id = Self::stringify(d.get("id")?)?;
        if id.is_empty() {
            return None;
        }
        let first = d.get("first_name").and_then(|x| x.as_str()).unwrap_or("");
        let last = d.get("last_name").and_then(|x| x.as_str()).unwrap_or("");
        let ext_name = format!("{first} {last}").trim().to_string();
        if ext_name.is_empty() {
            return None;
        }

        let dob = d.get("date_of_birth").and_then(|x| x.as_str()).unwrap_or("");
        let dod = d.get("date_of_death").and_then(|x| x.as_str()).unwrap_or("");
        let mut desc_parts: Vec<String> = vec![
            format!("born: {dob}"),
            format!("died: {dod}"),
        ];
        if let Some(p) = d.get("profession").and_then(|x| x.as_str()).filter(|s| !s.is_empty()) {
            desc_parts.push(format!("profession: {p}"));
        }
        if let Some(p) = d
            .get("place_of_activity")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            desc_parts.push(format!("place of activity: {p}"));
        }
        let ext_desc = desc_parts.join("; ");

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id.clone(),
            ext_name,
            ext_desc,
            ext_url: format!(
                "https://www.herbarien.uzh.ch/en/herbarien-zzt/sammler-details.html?id={id}"
            ),
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
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
    fn test_4966_post_body_substitutes_letter() {
        let body = BespokeScraper4966::build_post_body('K');
        assert!(body.contains("%5Bvalue%5D=K"));
        assert!(body.contains("publish_on_herbarien"));
    }

    #[test]
    fn test_4966_parse_item_full() {
        let d = serde_json::json!({
            "id": 100,
            "first_name": "Carl",
            "last_name": "Linnaeus",
            "date_of_birth": "1707-05-23",
            "date_of_death": "1778-01-10",
            "profession": "botanist",
            "place_of_activity": "Uppsala"
        });
        let ee = BespokeScraper4966::parse_item(4966, &d).unwrap();
        assert_eq!(ee.entry.ext_id, "100");
        assert_eq!(ee.entry.ext_name, "Carl Linnaeus");
        assert_eq!(
            ee.entry.ext_desc,
            "born: 1707-05-23; died: 1778-01-10; profession: botanist; place of activity: Uppsala"
        );
        assert_eq!(
            ee.entry.ext_url,
            "https://www.herbarien.uzh.ch/en/herbarien-zzt/sammler-details.html?id=100"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_4966_parse_item_unconditional_born_died_in_desc() {
        // Even when both date strings are empty, the desc still has
        // "born: ; died: " — preserves the PHP "always present" shape
        // so existing entries keep matching.
        let d = serde_json::json!({
            "id": 1,
            "first_name": "X",
            "last_name": "Y",
            "date_of_birth": "",
            "date_of_death": ""
        });
        let ee = BespokeScraper4966::parse_item(4966, &d).unwrap();
        assert_eq!(ee.entry.ext_desc, "born: ; died: ");
    }

    #[test]
    fn test_4966_parse_item_skips_empty_optional_fields() {
        let d = serde_json::json!({
            "id": 1,
            "first_name": "X",
            "last_name": "Y",
            "date_of_birth": "1900",
            "date_of_death": "1950",
            "profession": "",
            "place_of_activity": ""
        });
        let ee = BespokeScraper4966::parse_item(4966, &d).unwrap();
        assert_eq!(ee.entry.ext_desc, "born: 1900; died: 1950");
    }

    #[test]
    fn test_4966_parse_item_missing_id_skipped() {
        let d = serde_json::json!({"first_name": "X", "last_name": "Y"});
        assert!(BespokeScraper4966::parse_item(4966, &d).is_none());
    }

    #[test]
    fn test_4966_parse_item_empty_name_skipped() {
        let d = serde_json::json!({"id": 1, "first_name": "", "last_name": ""});
        assert!(BespokeScraper4966::parse_item(4966, &d).is_none());
    }
}
