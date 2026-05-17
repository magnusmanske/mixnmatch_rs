use std::sync::Arc;
use crate::{
    app_state::{AppContext, USER_AUX_MATCH, item2numeric},
    entry::Entry,
    meta_entry::MetaEntry,
    person_date::PersonDate,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::LazyLock;
use rand::RngExt;
use regex::{Captures, Regex};
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

use super::BespokeScraper;

// ______________________________________________________
// SIKART

#[derive(Debug)]
pub struct BespokeScraper121 {
    pub(super) app: Arc<dyn AppContext>,
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
    ) -> Option<MetaEntry> {
        let q = match record.get("WIKIDATA_ID") {
            Some(q) => item2numeric(q),
            None => return None,
        };
        let ext_entry = MetaEntry {
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
            person_dates: crate::meta_entry::MetaPersonDates::new_or_none(
                record.get("GEBURTSDATUM").and_then(|s| Self::parse_date(s)),
                record.get("STERBEDATUM").and_then(|s| Self::parse_date(s)),
            ),
            ..Default::default()
        };
        Some(ext_entry)
    }

    pub(crate) fn parse_date(d: &str) -> Option<PersonDate> {
        static RE_DMY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{3,})").unwrap());
        static RE_DM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d{1,2})\.(\d{1,2})").unwrap());
        let d = RE_DMY.replace(d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}-{:0>2}", &caps[3], &caps[2], &caps[1])
        });
        let d = RE_DM.replace(&d, |caps: &Captures| {
            format!("{:0>4}-{:0>2}", &caps[2], &caps[1])
        });
        MetaEntry::parse_date(&d)
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
        // Just a year (no dots) — passed through to MetaEntry::parse_date
        assert_eq!(
            BespokeScraper121::parse_date("1805"),
            Some(PersonDate::year_only(1805))
        );
    }

    #[test]
    fn test_121_parse_date_day_month_no_year() {
        // Day.Month with no year: RE_DM matches and produces "0006-16".
        // PersonDate parses this as year 6, month 16 which is invalid.
        assert_eq!(
            BespokeScraper121::parse_date("16.06"),
            None
        );
    }

    #[test]
    fn test_121_parse_date_year_only_with_extra_text() {
        // "1900 something" does not match any regex and MetaEntry::parse_date
        // cannot parse it, so None is returned.
        assert_eq!(BespokeScraper121::parse_date("1900 something"), None);
    }
}
