use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    entry::Entry,
    extended_entry::ExtendedEntry,
    person_date::PersonDate,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::{Captures, Regex};
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

use super::BespokeScraper;

// ______________________________________________________
// SIKART

#[derive(Debug)]
pub struct BespokeScraper121 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper121 {

    scraper_boilerplate!(121);

    async fn run(&self) -> Result<()> {
        let url = "https://www.sikart.ch/personen_export.aspx";
        let client = self.http_client();
        let text = client.get(url).send().await?.text().await?;
        let file = std::io::Cursor::new(text);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b';')
            .from_reader(file);
        type Record = HashMap<String, String>;
        let mut entry_cache = vec![];
        for result in reader.deserialize() {
            let record: Record = match result {
                Ok(record) => record,
                Err(e) => {
                    self.log(format!("Error reading record: {e}"));
                    continue;
                }
            };
            let ext_entry = match self.record2ext_entry(record) {
                Some(ext_entry) => ext_entry,
                None => continue,
            };
            entry_cache.push(ext_entry);
            self.maybe_flush_cache(&mut entry_cache).await?;
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper121 {
    pub(crate) fn record2ext_entry(
        &self,
        record: HashMap<String, String>,
    ) -> Option<ExtendedEntry> {
        let q = match record.get("WIKIDATA_ID") {
            Some(q) => AppState::item2numeric(q),
            None => return None,
        };
        let ext_entry = ExtendedEntry {
            entry: Entry {
                catalog: self.catalog_id(),
                ext_id: record.get("HAUPTNR")?.to_string(),
                ext_url: record.get("LINK_RECHERCHEPORTAL")?.to_string(),
                ext_name: record.get("NAMIDENT")?.to_string(),
                ext_desc: format!(
                    "{}; {}",
                    record.get("LEBENSDATEN")?,
                    record.get("VITAZEILE")?
                ),
                q,
                user: if q.is_none() {
                    None
                } else {
                    Some(USER_AUX_MATCH)
                },
                timestamp: if q.is_none() {
                    None
                } else {
                    Some(TimeStamp::now())
                },
                random: rand::rng().random(),
                type_name: Some("Q5".to_string()),
                ..Default::default()
            },
            born: record.get("GEBURTSDATUM").and_then(|s| Self::parse_date(s)),
            died: record.get("STERBEDATUM").and_then(|s| Self::parse_date(s)),
            ..Default::default()
        };
        Some(ext_entry)
    }

    pub(crate) fn parse_date(d: &str) -> Option<PersonDate> {
        lazy_static! {
            static ref re_dmy: Regex = Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{3,})").unwrap();
            static ref re_dm: Regex = Regex::new(r"^(\d{1,2})\.(\d{1,2})").unwrap();
        }
        let d = re_dmy.replace(d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}-{:0>2}", &caps[3], &caps[2], &caps[1])
        });
        let d = re_dm.replace(&d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}", &caps[2], &caps[1])
        });
        ExtendedEntry::parse_date(&d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_121_parse_date() {
        // Standard DMY format
        assert_eq!(
            BespokeScraper121::parse_date("16.06.1805"),
            Some(PersonDate::year_month_day(1805, 6, 16))
        );
        // Single digit day and month
        assert_eq!(
            BespokeScraper121::parse_date("1.2.1900"),
            Some(PersonDate::year_month_day(1900, 2, 1))
        );
        // Empty string
        assert_eq!(BespokeScraper121::parse_date(""), None);
        // Just a year (no dots) — passed through to ExtendedEntry::parse_date
        assert_eq!(
            BespokeScraper121::parse_date("1805"),
            Some(PersonDate::year_only(1805))
        );
    }

    #[test]
    fn test_121_parse_date_day_month_no_year() {
        // Day.Month with no year: re_dm matches and produces "0006-16".
        // PersonDate parses this as year 6, month 16 which is invalid.
        assert_eq!(
            BespokeScraper121::parse_date("16.06"),
            None
        );
    }

    #[test]
    fn test_121_parse_date_year_only_with_extra_text() {
        // "1900 something" does not match any regex and ExtendedEntry::parse_date
        // cannot parse it, so None is returned.
        assert_eq!(BespokeScraper121::parse_date("1900 something"), None);
    }
}
