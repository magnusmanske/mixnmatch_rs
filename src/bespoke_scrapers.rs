use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    entry::{CoordinateLocation, Entry, ENTRY_NEW_ID},
    extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use log::info;
use rand::Rng;
use regex::{Captures, Regex};
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

#[async_trait]
pub trait BespokeScraper {
    fn new(app: &AppState) -> Self;
    fn catalog_id(&self) -> usize;
    fn app(&self) -> &AppState;
    async fn run(&self) -> Result<()>;

    fn testing(&self) -> bool {
        false
    }

    fn keep_existing_names(&self) -> bool {
        false
    }

    fn log(&self, msg: String) {
        if self.testing() {
            info!("{}", msg);
        }
    }

    async fn process_cache(&self, entry_cache: &mut Vec<ExtendedEntry>) -> Result<()> {
        if entry_cache.is_empty() {
            return Ok(());
        }
        let ext_ids: Vec<String> = entry_cache.iter().map(|e| e.entry.ext_id.clone()).collect();
        let ext_id2id: HashMap<String, usize> = self
            .app()
            .storage()
            .autoscrape_get_entry_ids_for_ext_ids(self.catalog_id(), &ext_ids)
            .await?
            .into_iter()
            .collect();
        let entry_ids: Vec<usize> = ext_id2id.values().copied().collect();
        let existing_entries = Entry::multiple_from_ids(&entry_ids, self.app()).await?;
        for ext_entry in entry_cache {
            let ext_id = &ext_entry.entry.ext_id;
            let existing_entry = ext_id2id
                .get(ext_id)
                .map_or_else(|| None, |id| existing_entries.get(id).cloned());
            match existing_entry {
                Some(mut entry) => {
                    if self.keep_existing_names() {
                        ext_entry.entry.ext_name = entry.ext_name.to_string();
                    }
                    if self.testing() {
                        info!("EXISTS: {:?}", ext_entry);
                    } else {
                        ext_entry.update_existing(&mut entry, self.app()).await?;
                    }
                }
                None => {
                    if self.testing() {
                        info!("CREATE: {:?}", ext_entry);
                    } else {
                        ext_entry.insert_new(self.app()).await?;
                    }
                }
            };
        }
        Ok(())
    }
}

// ______________________________________________________
// SIKART

#[derive(Debug)]
pub struct BespokeScraper121 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper121 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        121
    }

    async fn run(&self) -> Result<()> {
        // let filename = "/Users/mm6/Downloads/Sikart_PersonenDaten.csv";
        // let file = File::open(filename)?;
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
            if entry_cache.len() > 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper121 {
    fn record2ext_entry(&self, record: HashMap<String, String>) -> Option<ExtendedEntry> {
        let q = match record.get("WIKIDATA_ID") {
            Some(q) => AppState::item2numeric(q),
            None => return None,
        };
        let ext_entry = ExtendedEntry {
            entry: Entry {
                id: ENTRY_NEW_ID,
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
                app: None, //Some(self.app.clone()),
            },
            born: record.get("GEBURTSDATUM").and_then(|s| Self::parse_date(s)),
            died: record.get("STERBEDATUM").and_then(|s| Self::parse_date(s)),
            ..Default::default()
        };
        Some(ext_entry)
    }

    fn parse_date(d: &str) -> Option<String> {
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

// ______________________________________________________
// Münzkabinett

#[derive(Debug)]
pub struct BespokeScraper6479 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6479 {
    // fn testing(&self) -> bool {
    //     true
    // }

    fn keep_existing_names(&self) -> bool {
        true
    }

    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        6479
    }

    async fn run(&self) -> Result<()> {
        // let filename = "/Users/mm6/Downloads/muenzkabinett.csv";
        // let file = File::open(filename)?;
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
            if entry_cache.len() > 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper6479 {
    fn record2ext_entry(&self, record: HashMap<String, String>) -> Option<ExtendedEntry> {
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
                id: ENTRY_NEW_ID,
                catalog: self.catalog_id(),
                ext_id,
                ext_url: uri,
                ext_name: record.get("label_de")?.to_string(),
                ext_desc: record.get("description_de")?.to_string(),
                q: None,
                user: None,
                timestamp: None,
                random: rand::rng().random(),
                type_name: Some("Q5".to_string()),
                app: None, //Some(self.app.clone()),
            },
            ..Default::default()
        };
        match record.get("gender_en").map(|s| s.as_str()) {
            Some("male") => {
                ext_entry.aux.insert((21, "Q6581097".to_string()));
            }
            Some("female") => {
                ext_entry.aux.insert((21, "Q6581072".to_string()));
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
                        ext_entry.location = Some(CoordinateLocation { lat, lon });
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
                ext_entry.aux.insert((227, id[1].to_string()));
            } else if let Some(id) = re_viaf.captures(lod) {
                ext_entry.aux.insert((214, id[1].to_string()));
            } else if let Some(id) = re_nomisma.captures(lod) {
                ext_entry.aux.insert((2950, id[1].to_string()));
            } else if let Some(id) = re_bm.captures(lod) {
                ext_entry.aux.insert((1711, id[1].to_string()));
            } else if let Some(_id) = re_zdb.captures(lod) {
                // Ignore, no property
            } else if let Some(_id) = re_rpc.captures(lod) {
                // Ignore, no property
            } else if let Some(_id) = re_lgpn.captures(lod) {
                // Ignore, no property
            } else if let Some(id) = re_md.captures(lod) {
                ext_entry.aux.insert((12597, id[1].to_string()));
            } else if let Some(id) = re_geonames.captures(lod) {
                ext_entry.aux.insert((1566, id[1].to_string()));
            } else if let Some(id) = re_mmlo.captures(lod) {
                ext_entry.aux.insert((6240, id[2].to_string()));
            } else {
                self.log(format!("Unknown URL pattern {lod}"));
            }
        }
        // println!("{:?}", &ext_entry.aux);
        Some(ext_entry)
    }
}
