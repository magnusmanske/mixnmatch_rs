use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry, person_date::PersonDate};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::{Captures, Regex};

use super::BespokeScraper;

// ______________________________________________________
//  Zurich Kantonsrat and Regierungsrat member ID (P13468)

#[derive(Debug)]
pub struct BespokeScraper6975 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6975 {

    scraper_boilerplate!(6975);

    async fn run(&self) -> Result<()> {
        let url = "https://www.web.statistik.zh.ch/webapp/KRRRPublic/app?page=json&nachname=&vorname=&geburtsjahr=&wohnort=&beruf=&geschlecht=&partei=&parteigruppe=&wk_periode_von=2025&wk_periode_bis=2025&wahlkreis=1.+Wahlkreis+(Z%C3%BCrich+1%2B2)&bemerkungen=&einsitztag=1&einsitzmonat=1&einsitzjahr=2025";
        let client = self.http_client();
        let json: serde_json::Value = client.get(url).send().await?.json().await?;
        let mut entry_cache = vec![];
        let arr = json["data"]
            .as_array()
            .ok_or_else(|| anyhow!("expected json array from https://www.web.statistik.zh.ch"))?;
        for record in arr {
            let ext_entry = match self.record2ext_entry(record) {
                Some(entry) => entry,
                None => continue,
            };

            entry_cache.push(ext_entry);
            self.maybe_flush_cache(&mut entry_cache).await?;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper6975 {
    pub(crate) fn record2ext_entry(&self, record: &serde_json::Value) -> Option<ExtendedEntry> {
        let last_name = record[0].as_str().unwrap_or_default();
        let first_name = record[1].as_str().unwrap_or_default();
        let born = record[3].as_str().unwrap_or_default();
        let id = record[4].as_str().unwrap_or_default();

        lazy_static! {
            static ref re_ext_id: Regex = Regex::new(r"^.*?open_person\('(\d+)'\).*$").unwrap();
        }
        if !re_ext_id.is_match(id) {
            return None;
        }
        let ext_id = re_ext_id.replace(id, |caps: &Captures| caps[1].to_string());

        let ext_name = format!("{first_name} {last_name}");
        let ext_url =
            format!("https://www.wahlen.zh.ch/krdaten_staatsarchiv/abfrage.php?id={ext_id}");

        let ext_entry = ExtendedEntry {
            entry: Entry {
                catalog: self.catalog_id(),
                ext_id: ext_id.to_string(),
                ext_url,
                ext_name,
                ext_desc: String::new(),
                random: rand::rng().random(),
                type_name: Some("Q5".to_string()),
                ..Default::default()
            },
            born: Self::fix_date(born),
            ..Default::default()
        };
        Some(ext_entry)
    }

    pub(crate) fn fix_date(s: &str) -> Option<PersonDate> {
        lazy_static! {
            static ref re_zero: Regex = Regex::new(r"^(\d{3,4})\.00\.00$").unwrap();
            static ref re_dmy: Regex = Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{3,4})$").unwrap();
            static ref re_ymd: Regex = Regex::new(r"^(\d{3,4})\.(\d{1,2})\.(\d{1,2})$").unwrap();
            static ref re_iso: Regex = Regex::new(r"^\d{3,4}(-\d{2}){0,2}$").unwrap();
        }
        let d = re_zero.replace(s, |caps: &Captures| format!("{:0>4}", &caps[1]));
        let d = re_dmy.replace(&d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}-{:0>2}", &caps[3], &caps[2], &caps[1])
        });
        let d = re_ymd.replace(&d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}-{:0>2}", &caps[1], &caps[2], &caps[3])
        });
        if re_iso.is_match(&d) {
            PersonDate::from_db_string(&d)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #lizard forgives the complexity
    #[test]
    fn test_6975_fix_date() {
        assert_eq!(
            BespokeScraper6975::fix_date("16.06.1805"),
            Some(PersonDate::year_month_day(1805, 6, 16))
        );
        assert_eq!(
            BespokeScraper6975::fix_date("1805.06.16"),
            Some(PersonDate::year_month_day(1805, 6, 16))
        );
        assert_eq!(
            BespokeScraper6975::fix_date("1805-06-16"),
            Some(PersonDate::year_month_day(1805, 6, 16))
        );
        assert_eq!(BespokeScraper6975::fix_date("1805.00.00"), Some(PersonDate::year_only(1805)));
        assert_eq!(BespokeScraper6975::fix_date("1805"), Some(PersonDate::year_only(1805)));
    }

    #[test]
    fn test_6975_fix_date_edge_cases() {
        // Day-month-year with single digit day/month
        assert_eq!(
            BespokeScraper6975::fix_date("1.2.1900"),
            Some(PersonDate::year_month_day(1900, 2, 1))
        );
        // Year.00.00 collapses to year only (dot-separated zeros)
        assert_eq!(BespokeScraper6975::fix_date("1900.00.00"), Some(PersonDate::year_only(1900)));
        // ISO with dashes and zeroes is kept as-is (re_zero only matches dot format)
        // "1900-00-00" has invalid month 0, so PersonDate rejects it
        assert!(BespokeScraper6975::fix_date("1900-00-00").is_none());
        // Year-month-day dot format
        assert_eq!(
            BespokeScraper6975::fix_date("1900.6.16"),
            Some(PersonDate::year_month_day(1900, 6, 16))
        );
        // Invalid input returns None
        assert!(BespokeScraper6975::fix_date("not-a-date").is_none());
        assert!(BespokeScraper6975::fix_date("").is_none());
    }

    #[test]
    fn test_6975_fix_date_year_month_only() {
        // ISO year-month (no day) should be valid
        assert_eq!(BespokeScraper6975::fix_date("1900-06"), Some(PersonDate::year_month(1900, 6)));
    }

    #[test]
    fn test_6975_fix_date_three_digit_year() {
        // 3-digit years are accepted by the regexes
        assert_eq!(BespokeScraper6975::fix_date("800"), Some(PersonDate::year_only(800)));
        // re_zero uses {:0>4} which zero-pads to 4 digits
        assert_eq!(BespokeScraper6975::fix_date("800.00.00"), Some(PersonDate::year_only(800)));
    }

    #[test]
    fn test_6975_record2ext_entry_valid() {
        let scraper = BespokeScraper6975 {
            app: crate::app_state::get_test_app(),
        };
        let record = serde_json::json!([
            "Müller",
            "Hans",
            "Zürich",
            "16.06.1970",
            "onclick=\"open_person('12345')\""
        ]);
        let ee = scraper.record2ext_entry(&record).unwrap();
        assert_eq!(ee.entry.ext_id, "12345");
        assert_eq!(ee.entry.ext_name, "Hans Müller");
        assert_eq!(
            ee.entry.ext_url,
            "https://www.wahlen.zh.ch/krdaten_staatsarchiv/abfrage.php?id=12345"
        );
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert_eq!(ee.born, Some(PersonDate::year_month_day(1970, 6, 16)));
        assert_eq!(ee.entry.catalog, 6975);
    }

    #[test]
    fn test_6975_record2ext_entry_no_id_field() {
        let scraper = BespokeScraper6975 {
            app: crate::app_state::get_test_app(),
        };
        // record[4] does not contain the open_person pattern
        let record = serde_json::json!(["Müller", "Hans", "Zürich", "16.06.1970", "no-id-here"]);
        assert!(scraper.record2ext_entry(&record).is_none());
    }

    #[test]
    fn test_6975_record2ext_entry_empty_record() {
        let scraper = BespokeScraper6975 {
            app: crate::app_state::get_test_app(),
        };
        let record = serde_json::json!([]);
        assert!(scraper.record2ext_entry(&record).is_none());
    }

    #[test]
    fn test_6975_record2ext_entry_invalid_date_still_creates_entry() {
        let scraper = BespokeScraper6975 {
            app: crate::app_state::get_test_app(),
        };
        let record = serde_json::json!([
            "Schmidt",
            "Anna",
            "Bern",
            "not-a-date",
            "onclick=\"open_person('99')\""
        ]);
        let ee = scraper.record2ext_entry(&record).unwrap();
        // Entry is still created, but born is None
        assert_eq!(ee.entry.ext_id, "99");
        assert!(ee.born.is_none());
    }

    #[test]
    fn test_6975_record2ext_entry_name_formatting() {
        let scraper = BespokeScraper6975 {
            app: crate::app_state::get_test_app(),
        };
        let record = serde_json::json!([
            "von Arx",
            "Maria",
            "Basel",
            "1980",
            "onclick=\"open_person('7')\""
        ]);
        let ee = scraper.record2ext_entry(&record).unwrap();
        assert_eq!(ee.entry.ext_name, "Maria von Arx");
    }
}
