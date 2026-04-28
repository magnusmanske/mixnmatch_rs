use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    auxiliary_data::AuxiliaryRow,
    coordinates::CoordinateLocation,
    entry::Entry,
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

use super::BespokeScraper;

// ______________________________________________________
// Münzkabinett

#[derive(Debug)]
pub struct BespokeScraper6479 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6479 {
    fn keep_existing_names(&self) -> bool {
        true
    }


    scraper_boilerplate!(6479);

    async fn run(&self) -> Result<()> {
        let url = "https://www.sikart.ch/personen_export.aspx";
        let client = reqwest::Client::new();
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

impl BespokeScraper6479 {
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn record2ext_entry(
        &self,
        record: HashMap<String, String>,
    ) -> Option<ExtendedEntry> {
        lazy_static! {
            static ref re_uri: Regex = Regex::new(r"^https://ikmk.smb.museum/ndp/(.+?)$").unwrap();
            static ref re_loc_name: Regex = Regex::new(r"^(.+?) * \| *(.+)$").unwrap();
            static ref re_wd: Regex =
                Regex::new(r"^https?://www.wikidata.org/(wiki|entity)/Q(\d+)").unwrap();
            static ref re_wp: Regex =
                Regex::new(r"^https?://([a-z]+).wikipedia.org/wiki/(.+)$").unwrap();
            static ref re_gnd: Regex = Regex::new(r"^https?://d-nb.info/gnd/([^#]+)").unwrap();
            static ref re_viaf: Regex = Regex::new(r"^https?://viaf.org/viaf/(.+)$").unwrap();
            static ref re_nomisma: Regex = Regex::new(r"^https?://nomisma.org/id/(.+)$").unwrap();
            static ref re_bm: Regex =
                Regex::new(r"^https?://www.britishmuseum.org/collection/term/BIOG(.+)$").unwrap();
            static ref re_zdb: Regex =
                Regex::new(r"^https?://ld.zdb-services.de/resource/(.+)$").unwrap();
            static ref re_md: Regex =
                Regex::new(r"^https?://term.museum-digital.de/md-de/persinst/(\d+)$").unwrap();
            static ref re_geonames: Regex =
                Regex::new(r"^https?://www.geonames.org/(\d+)$").unwrap();
            static ref re_mmlo: Regex = Regex::new(r"^https?://(www.)?mmlo.de/(\d+)$").unwrap();
            static ref re_rpc: Regex = Regex::new(r"^https?://rpc.ashmus.ox.ac.uk/(.+)$").unwrap();
            static ref re_lgpn: Regex =
                Regex::new(r"^https?://www.lgpn.ox.ac.uk/id/(.+?)$").unwrap();
        }
        let uri = record.get("uri")?.to_string();
        let ext_id = re_uri.captures(&uri)?[1].to_string();
        let mut ext_entry = ExtendedEntry {
            entry: Entry {
                catalog: self.catalog_id(),
                ext_id,
                ext_url: uri,
                ext_name: record.get("label_de")?.to_string(),
                ext_desc: record.get("description_de")?.to_string(),
                random: rand::rng().random(),
                type_name: Some("Q5".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        match record.get("gender_en").map(|s| s.as_str()) {
            Some("male") => {
                ext_entry.aux.insert(AuxiliaryRow::new(21, "Q6581097".to_string()));
            }
            Some("female") => {
                ext_entry.aux.insert(AuxiliaryRow::new(21, "Q6581072".to_string()));
            }
            Some("") => {}
            Some(other) => {
                self.log(format!("Unknown gender {other}"));
            }
            None => {}
        }
        ext_entry.entry.type_name = match record.get("type").map(|s| s.as_str()) {
            Some("https://ikmk.smb.museum/ndp/category/mk_person") => Some("Q5".to_string()),
            Some("https://ikmk.smb.museum/ndp/category/mk_corporation") => {
                Some("Q167037".to_string())
            }
            Some("https://ikmk.smb.museum/ndp/category/mk_owner") => None, // ??
            Some("https://ikmk.smb.museum/ndp/category/mk_mstand") => None, // ??
            Some("https://ikmk.smb.museum/ndp/category/mk_herstellungtype") => None, // ??
            Some("https://ikmk.smb.museum/ndp/category/mk_land") => Some("Q6256".to_string()),
            Some("https://ikmk.smb.museum/ndp/category/mk_material") => Some("Q214609".to_string()),
            Some("https://ikmk.smb.museum/ndp/category/mk_periode") => {
                Some("Q11514315".to_string())
            }
            Some("https://ikmk.smb.museum/ndp/category/mk_staette") => {
                if let Some(name) = re_loc_name.captures(&ext_entry.entry.ext_name) {
                    ext_entry.entry.ext_name = name[1].to_string();
                };
                if let Some(desc) = re_loc_name.captures(&ext_entry.entry.ext_desc) {
                    let lat = desc[1].parse::<f64>();
                    let lon = desc[2].parse::<f64>();
                    if let (Ok(lat), Ok(lon)) = (lat, lon) {
                        ext_entry.location = Some(CoordinateLocation::new(lat, lon));
                    }
                }
                Some("Q3257686".to_string())
            }
            Some(other) => {
                self.log(format!("Unknown type {other}"));
                None
            }
            None => None,
        };

        // Other external IDs
        let lods: Vec<&str> = record
            .get("LOD")
            .map(|s| s.as_str())
            .unwrap_or("")
            .split('|')
            .collect();
        for lod in lods {
            if lod.is_empty() || lod.ends_with('/') {
                // Ignore
            } else if let Some(id) = re_wd.captures(lod) {
                if let Ok(q) = id[2].parse::<isize>() {
                    ext_entry.entry.q = Some(q);
                    ext_entry.entry.user = Some(USER_AUX_MATCH);
                    ext_entry.entry.timestamp = Some(TimeStamp::now());
                }
            } else if let Some(_lang_title) = re_wp.captures(lod) {
                // Wikipedia article, ignore, wikidata should cover it
            } else if let Some(id) = re_gnd.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(227, id[1].to_string()));
            } else if let Some(id) = re_viaf.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(214, id[1].to_string()));
            } else if let Some(id) = re_nomisma.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(2950, id[1].to_string()));
            } else if let Some(id) = re_bm.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(1711, id[1].to_string()));
            } else if let Some(_id) = re_zdb.captures(lod) {
                // Ignore, no property
            } else if let Some(_id) = re_rpc.captures(lod) {
                // Ignore, no property
            } else if let Some(_id) = re_lgpn.captures(lod) {
                // Ignore, no property
            } else if let Some(id) = re_md.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(12597, id[1].to_string()));
            } else if let Some(id) = re_geonames.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(1566, id[1].to_string()));
            } else if let Some(id) = re_mmlo.captures(lod) {
                ext_entry.aux.insert(AuxiliaryRow::new(6240, id[2].to_string()));
            } else {
                self.log(format!("Unknown URL pattern {lod}"));
            }
        }
        Some(ext_entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    fn make_record(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn make_scraper() -> BespokeScraper6479 {
        BespokeScraper6479 {
            app: get_test_app(),
        }
    }

    #[test]
    fn test_6479_keep_existing_names() {
        let s = make_scraper();
        assert!(s.keep_existing_names());
    }

    #[test]
    fn test_6479_catalog_id() {
        let s = make_scraper();
        assert_eq!(s.catalog_id(), 6479);
    }

    #[test]
    fn test_6479_record2ext_entry_missing_uri() {
        let s = make_scraper();
        let record = make_record(&[("label_de", "Test"), ("description_de", "Desc")]);
        assert!(s.record2ext_entry(record).is_none());
    }

    #[test]
    fn test_6479_record2ext_entry_uri_not_matching_pattern() {
        let s = make_scraper();
        // URI does not match ikmk.smb.museum/ndp/…
        let record = make_record(&[
            ("uri", "https://example.com/something"),
            ("label_de", "Test"),
            ("description_de", "Desc"),
        ]);
        assert!(s.record2ext_entry(record).is_none());
    }

    #[test]
    fn test_6479_record2ext_entry_missing_label() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/12345"),
            ("description_de", "Desc"),
        ]);
        assert!(s.record2ext_entry(record).is_none());
    }

    #[test]
    fn test_6479_record2ext_entry_basic() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/12345"),
            ("label_de", "Caesar"),
            ("description_de", "Roman dictator"),
            ("type", "https://ikmk.smb.museum/ndp/category/mk_person"),
            ("gender_en", "male"),
            ("LOD", ""),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert_eq!(ee.entry.ext_id, "person/12345");
        assert_eq!(ee.entry.ext_name, "Caesar");
        assert_eq!(ee.entry.ext_desc, "Roman dictator");
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581097".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_female_gender() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/99"),
            ("label_de", "Cleopatra"),
            ("description_de", "Queen of Egypt"),
            ("gender_en", "female"),
            ("LOD", ""),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(21, "Q6581072".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_wikidata() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/42"),
            ("label_de", "Caesar"),
            ("description_de", ""),
            ("gender_en", ""),
            (
                "LOD",
                "https://www.wikidata.org/wiki/Q1268|https://viaf.org/viaf/99999",
            ),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert_eq!(ee.entry.q, Some(1268));
        assert!(ee.aux.contains(&AuxiliaryRow::new(214, "99999".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_gnd() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/7"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://d-nb.info/gnd/118522426"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(227, "118522426".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_nomisma() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/8"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://nomisma.org/id/caesar"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(2950, "caesar".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_british_museum() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/9"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            (
                "LOD",
                "https://www.britishmuseum.org/collection/term/BIOG12345",
            ),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(1711, "12345".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_geonames() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/location/5"),
            ("label_de", "Berlin"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://www.geonames.org/2950159"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(1566, "2950159".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_mmlo() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/10"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://www.mmlo.de/4567"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(6240, "4567".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_museum_digital() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/11"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://term.museum-digital.de/md-de/persinst/7890"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(12597, "7890".to_string())));
    }

    #[test]
    fn test_6479_record2ext_entry_type_corporation() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/corp/1"),
            ("label_de", "Senate"),
            ("description_de", ""),
            ("gender_en", ""),
            (
                "type",
                "https://ikmk.smb.museum/ndp/category/mk_corporation",
            ),
            ("LOD", ""),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert_eq!(ee.entry.type_name, Some("Q167037".to_string()));
    }

    #[test]
    fn test_6479_record2ext_entry_type_land() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/land/1"),
            ("label_de", "Rome"),
            ("description_de", ""),
            ("gender_en", ""),
            ("type", "https://ikmk.smb.museum/ndp/category/mk_land"),
            ("LOD", ""),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert_eq!(ee.entry.type_name, Some("Q6256".to_string()));
    }

    #[test]
    fn test_6479_record2ext_entry_lod_trailing_slash_ignored() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/20"),
            ("label_de", "Test"),
            ("description_de", ""),
            ("gender_en", ""),
            ("LOD", "https://viaf.org/viaf/12345/"),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        // Trailing slash means no VIAF aux added
        assert!(!ee.aux.iter().any(|a| a.prop_numeric() == 214));
    }

    #[test]
    fn test_6479_record2ext_entry_multiple_lods() {
        let s = make_scraper();
        let record = make_record(&[
            ("uri", "https://ikmk.smb.museum/ndp/person/30"),
            ("label_de", "Multi"),
            ("description_de", ""),
            ("gender_en", ""),
            (
                "LOD",
                "https://d-nb.info/gnd/118522426|https://viaf.org/viaf/54321|https://nomisma.org/id/example",
            ),
        ]);
        let ee = s.record2ext_entry(record).unwrap();
        assert!(ee.aux.contains(&AuxiliaryRow::new(227, "118522426".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(214, "54321".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(2950, "example".to_string())));
    }
}
