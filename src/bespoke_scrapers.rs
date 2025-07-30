use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    entry::{CoordinateLocation, Entry},
    extended_entry::ExtendedEntry,
    php_wrapper::PhpWrapper,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use lazy_static::lazy_static;
use log::info;
use rand::Rng;
use regex::{Captures, Regex};
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

/** WHEN YOU CREATE A NEW `BespokeScraper`, ALSO ADD IT HERE TO BE CALLED! **/
pub async fn run_bespoke_scraper(catalog_id: usize, app: &AppState) -> Result<()> {
    match catalog_id {
        121 => BespokeScraper121::new(app).run().await,
        6479 => BespokeScraper6479::new(app).run().await,
        6794 => BespokeScraper6794::new(app).run().await,
        6975 => BespokeScraper6975::new(app).run().await,
        6976 => BespokeScraper6976::new(app).run().await,
        7043 => BespokeScraper7043::new(app).run().await,
        other => PhpWrapper::bespoke_scraper(other).await, // PHP fallback
    }
}

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
            info!("{msg}");
        }
    }

    fn http_client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }

    async fn load_single_line_text_from_url(&self, url: &str) -> Result<String> {
        let text = self
            .http_client()
            .get(url.to_owned())
            .send()
            .await?
            .text()
            .await?
            .replace("\n", ""); // Single line
        Ok(text)
    }

    async fn add_missing_aux(&self, entry_id: usize, prop_re: &[(usize, Regex)]) -> Result<()> {
        let entry = Entry::from_id(entry_id, self.app()).await?;
        let html = self
            .http_client()
            .get(&entry.ext_url)
            .send()
            .await?
            .text()
            .await?;

        let mut new_aux: Vec<(usize, String)> = vec![];

        for (property, re) in prop_re.iter() {
            if let Some(caps) = re.captures(&html) {
                if let Some(id) = caps.get(1) {
                    new_aux.push((*property, id.as_str().to_string()));
                }
            }
        }

        if !new_aux.is_empty() {
            let existing_aux = entry.get_aux().await?;
            for (aux_p, aux_name) in new_aux {
                if !existing_aux
                    .iter()
                    .any(|a| a.prop_numeric() == aux_p && a.value() == aux_name)
                {
                    let _ = entry.set_auxiliary(aux_p, Some(aux_name)).await;
                }
            }
        }
        Ok(())
    }

    async fn process_cache(&self, entry_cache: &mut Vec<ExtendedEntry>) -> Result<()> {
        if entry_cache.is_empty() {
            return Ok(());
        }
        let ext_ids: Vec<String> = entry_cache.iter().map(|e| e.entry.ext_id.clone()).collect();
        let ext_id2id: HashMap<String, usize> = self
            .app()
            .storage()
            .get_entry_ids_for_ext_ids(self.catalog_id(), &ext_ids)
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
                        info!("EXISTS: {ext_entry:?}");
                    } else {
                        ext_entry.update_existing(&mut entry, self.app()).await?;
                    }
                }
                None => {
                    if self.testing() {
                        info!("CREATE: {ext_entry:?}");
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

// ______________________________________________________
//  Zurich Kantonsrat and Regierungsrat member ID (P13468)

#[derive(Debug)]
pub struct BespokeScraper6975 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6975 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        6975
    }

    async fn run(&self) -> Result<()> {
        let url = "https://www.web.statistik.zh.ch/webapp/KRRRPublic/app?page=json&nachname=&vorname=&geburtsjahr=&wohnort=&beruf=&geschlecht=&partei=&parteigruppe=&wk_periode_von=2025&wk_periode_bis=2025&wahlkreis=1.+Wahlkreis+(Z%C3%BCrich+1%2B2)&bemerkungen=&einsitztag=1&einsitzmonat=1&einsitzjahr=2025";
        let client = reqwest::Client::new();
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
            if entry_cache.len() > 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }

    // FOR TESTING ONLY
    // async fn process_cache(&self, entry_cache: &mut Vec<ExtendedEntry>) -> Result<()> {
    //     println!("{entry_cache:#?}");
    //     entry_cache.clear();
    //     Ok(())
    // }
}

impl BespokeScraper6975 {
    fn record2ext_entry(&self, record: &serde_json::Value) -> Option<ExtendedEntry> {
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

    fn fix_date(s: &str) -> Option<String> {
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
            Some(d.to_string())
        } else {
            None
        }
    }
}

// ______________________________________________________
// BMLO ID (P865)

#[derive(Debug)]
pub struct BespokeScraper7043 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7043 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        7043
    }

    async fn run(&self) -> Result<()> {
        // TODO add new?

        lazy_static! {
            static ref PROP_RE: Vec<(usize, Regex)> = {
                vec![
                    (
                        214,
                        Regex::new(r#"href="http://viaf.org/viaf/(\d+)"#).unwrap(),
                    ),
                    (227, Regex::new(r#"\?gnd=(\d+X?)"#).unwrap()),
                ]
            };
        }

        // Run all existing entries for metadata
        let ext_ids = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        for (_ext_id, entry_id) in ext_ids {
            let _ = self.add_missing_aux(entry_id, &PROP_RE).await;
        }
        Ok(())
    }
}

// ______________________________________________________
// Zentrales Personenregister aus den Beständen des Herder-Instituts (6794)

#[derive(Debug)]
pub struct BespokeScraper6794 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6794 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        6794
    }

    async fn run(&self) -> Result<()> {
        // Run all existing entries for metadata
        let ext_id2entry_id = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        let futures = ext_id2entry_id
            .into_values()
            .map(|entry_id| self.add_missing_aux(entry_id))
            .collect::<Vec<_>>();

        // Run 5 in parallel
        let stream = futures::stream::iter(futures).buffer_unordered(5);
        let _ = stream.collect::<Vec<_>>().await;
        Ok(())
    }
}

impl BespokeScraper6794 {
    async fn add_missing_aux(&self, entry_id: usize) -> Result<()> {
        let entry = Entry::from_id(entry_id, &self.app).await?;
        let existing_aux = entry.get_aux().await?;
        let url = &entry.ext_url;
        let text = self.load_single_line_text_from_url(url).await?;
        if !existing_aux.iter().any(|aux| aux.prop_numeric() == 227) {
            if let Some(gnd) = Self::get_main_gnd_from_text(&text) {
                entry.set_auxiliary(227, Some(gnd)).await?;
            }
        }
        Ok(())
    }

    fn get_main_gnd_from_text(text: &str) -> Option<String> {
        lazy_static! {
            static ref RE_GND: Regex =
                Regex::new(r#"<a href="http://d-nb.info/gnd/(.+?)""#).unwrap();
        }
        let captures = RE_GND.captures(text)?;
        Some(captures[1].to_string())
    }
}
// ______________________________________________________
// Hessian Biography person (6976)

#[derive(Debug)]
pub struct BespokeScraper6976 {
    app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper6976 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    fn catalog_id(&self) -> usize {
        6976
    }

    async fn run(&self) -> Result<()> {
        // TODO add new?

        // Run all existing entries for metadata
        let ext_id2entry_id = self
            .app()
            .storage()
            .get_all_external_ids(self.catalog_id())
            .await?;
        let futures = ext_id2entry_id
            .into_values()
            .map(|entry_id| self.add_missing_aux(entry_id))
            .collect::<Vec<_>>();

        // Run 5 in parallel
        let stream = futures::stream::iter(futures).buffer_unordered(5);
        let _ = stream.collect::<Vec<_>>().await;
        Ok(())
    }
}

impl BespokeScraper6976 {
    async fn add_missing_aux(&self, entry_id: usize) -> Result<()> {
        const KEYS2PROP: &[(&str, usize)] = &[
            ("<h3>Vater:</h3>", 22),
            ("<h3>Mutter:</h3>", 25),
            ("<h3>Partner:</h3>", 26),
            ("<h3>Verwandte:</h3>", 1038),
        ];
        lazy_static! {
            static ref RE_DD: Regex = Regex::new(r#"<dd>(.+?)</dd>"#).unwrap();
            static ref RE_SUBJECT: Regex =
                Regex::new(r#"<a href="/[a-z]+/subjects/idrec/sn/bio/id/(\d+)""#).unwrap();
        }
        let entry = Entry::from_id(entry_id, &self.app).await?;
        let existing_aux = entry.get_aux().await?;
        let url = &entry.ext_url;
        let text = self.load_single_line_text_from_url(url).await?;

        if !existing_aux.iter().any(|aux| aux.prop_numeric() == 227) {
            if let Some(gnd) = Self::get_main_gnd_from_text(&text) {
                entry.set_auxiliary(227, Some(gnd)).await?;
            }
        }

        for cap_dd_group in RE_DD.captures_iter(&text) {
            let cap_dd = cap_dd_group.get(1).unwrap().as_str();
            let subject_ids = RE_SUBJECT
                .captures_iter(cap_dd)
                .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
                .collect::<Vec<String>>();
            if subject_ids.is_empty() {
                continue;
            }
            for (key, prop_numeric) in KEYS2PROP {
                if cap_dd.contains(key) {
                    let _ = self
                        .attach_subjects_as_aux(*prop_numeric, &subject_ids, &entry)
                        .await;
                }
            }
        }
        Ok(())
    }

    async fn attach_subjects_as_aux(
        &self,
        prop_numeric: usize,
        subject_ids: &[String],
        entry: &Entry,
    ) -> Result<()> {
        for subject_id in subject_ids {
            if let Some(gnd) = self.get_subject_gnd(subject_id).await {
                let query = format!("haswbstatement:P227={gnd}");
                let items_with_gnd = self
                    .app
                    .wikidata()
                    .search_api(&query)
                    .await
                    .unwrap_or_default();
                if items_with_gnd.len() == 1 {
                    let item = items_with_gnd[0].clone();
                    let _ = entry.set_auxiliary(prop_numeric, Some(item)).await;
                } else if let Ok(target_entry) =
                    Entry::from_ext_id(self.catalog_id(), &gnd, &self.app).await
                {
                    if let Ok(target_entry_id) = target_entry.get_valid_id() {
                        let _ = entry.add_mnm_relation(prop_numeric, target_entry_id).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_subject_gnd(&self, subject_id: &str) -> Option<String> {
        let url = format!("https://www.lagis-hessen.de/de/subjects/idrec/sn/bio/id/{subject_id}");
        let text = self.load_single_line_text_from_url(&url).await.ok()?;
        Self::get_main_gnd_from_text(&text)
    }

    fn get_main_gnd_from_text(text: &str) -> Option<String> {
        lazy_static! {
            static ref RE_GND: Regex = Regex::new(r#"<h2>GND-Nummer</h2>\s*<p>(.+?)</p>"#).unwrap();
        }
        let captures = RE_GND.captures(text)?;
        Some(captures[1].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #lizard forgives the complexity
    #[test]
    fn test_6975_fix_date() {
        assert_eq!(
            BespokeScraper6975::fix_date("16.06.1805").unwrap(),
            "1805-06-16"
        );
        assert_eq!(
            BespokeScraper6975::fix_date("1805.06.16").unwrap(),
            "1805-06-16"
        );
        assert_eq!(
            BespokeScraper6975::fix_date("1805-06-16").unwrap(),
            "1805-06-16"
        );
        assert_eq!(BespokeScraper6975::fix_date("1805.00.00").unwrap(), "1805");
        assert_eq!(BespokeScraper6975::fix_date("1805").unwrap(), "1805");
    }
}
