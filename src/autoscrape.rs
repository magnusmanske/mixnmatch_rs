use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::prelude::*;
use regex::RegexBuilder;
use serde_json::{json, Value};
use regex::Regex;
use std::collections::HashMap;
use mysql_async::from_row;
use std::error::Error;
use std::fmt;
use mysql_async::prelude::*;
use crate::update_catalog::ExtendedEntry;
use crate::entry::*;
use crate::job::*;
use crate::app_state::*;
use crate::mixnmatch::MixNMatch;

const AUTOSCRAPER_USER_AGENT: &str = "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion";
const AUTOSCRAPE_ENTRY_BATCH_SIZE: usize = 100;

lazy_static!{
    static ref RE_SIMPLE_SPACE : Regex = RegexBuilder::new(r"\s+").multi_line(true).ignore_whitespace(true).build().expect("Regex error") ;
    static ref RE_HTML: Regex = Regex::new(r"(<.*?>)").expect("Regex error");
}

#[derive(Debug, Clone)]
enum AutoscrapeError {
    NoAutoscrapeForCatalog,
    UnknownLevelType(Value),
    BadType(Value),
    MediawikiFailure(String)
}

impl Error for AutoscrapeError {}

impl fmt::Display for AutoscrapeError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}

trait JsonStuff {
    //TODO test
    fn json_as_str(json: &Value, key: &str) -> Result<String,AutoscrapeError> {
        Ok(json.get(key)
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .as_str()
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .to_string())
    }

    //TODO test
    fn json_as_u64(json: &Value, key: &str) -> Result<u64,AutoscrapeError> {
        let value = json.get(key).ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
        if value.is_string() {
            let s = value.as_str().ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
            match s.parse::<u64>() {
                Ok(ret) => Ok(ret),
                _ => Err(AutoscrapeError::BadType(json.to_owned()))
            }
        } else {
            value.as_u64().ok_or_else(||AutoscrapeError::BadType(json.to_owned()))
        }
    }

    //TODO test
    fn fix_regex(s: &str) -> String {
        s.replace("\\/","/").to_string()
    }
}

#[async_trait]
trait Level {
    //TODO test
    async fn init(&mut self, autoscrape: &Autoscrape);

    /// Returns true if this level has been completed, false if there was at least one more result.
    //TODO test
    async fn tick(&mut self) -> bool;

    //TODO test
    fn current(&self) -> String;
    //TODO test
    fn get_state(&self) -> Value;
    //TODO test
    fn set_state(&mut self, json: &Value);
}

#[derive(Debug, Clone)]
struct AutoscrapeKeys {
    keys: Vec<String>,
    position: usize
}

#[async_trait]
impl Level for AutoscrapeKeys {
    //TODO test
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.position = 0;
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        self.position += 1 ;
        self.position >= self.keys.len()
    }

    //TODO test
    fn current(&self) -> String {
        match self.keys.get(self.position) {
            Some(v) => v.to_owned(),
            None => String::new()
        }
    }

    //TODO test
    fn get_state(&self) -> Value {
        json!({"position":self.position})
    }

    //TODO test
    fn set_state(&mut self, json: &Value) {
        if let Some(position) = json.get("position") {
            if let Some(position) = position.as_u64() { self.position = position as usize}
        }
    }
}

impl AutoscrapeKeys {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        let keys = json
            .get("keys")
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .as_array()
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .iter()
            .filter_map(|s|s.as_str())
            .map(|s|s.to_string())
            .collect();
        Ok(Self{keys,position:0})
    }
}

#[derive(Debug, Clone)]
struct AutoscrapeRange {
    start: u64,
    end: u64,
    step: u64,
    current_value: u64,
}

impl JsonStuff for AutoscrapeRange {}

#[async_trait]
impl Level for AutoscrapeRange {
    //TODO test
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.current_value = self.start;
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        self.current_value += self.step ;
        self.current_value > self.end
    }

    //TODO test
    fn current(&self) -> String {
        format!("{}",self.current_value)
    }

    //TODO test
    fn get_state(&self) -> Value {
        json!({"current_value":self.current_value})
    }

    //TODO test
    fn set_state(&mut self, json: &Value) {
        if let Some(current_value) = json.get("current_value") {
            if let Some(current_value) = current_value.as_u64() { self.current_value = current_value}
        }
    }
}

impl AutoscrapeRange {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{
            start: Self::json_as_u64(json,"start")?,
            end: Self::json_as_u64(json,"end")?,
            step: Self::json_as_u64(json,"step")?,
            current_value: Self::json_as_u64(json,"start")?,
        })
    }
}


#[derive(Debug, Clone)]
struct AutoscrapeFollow {
    url: String,
    regex: String,
    cache: Vec<String>,
    current_key: String,
}

impl JsonStuff for AutoscrapeFollow {}

#[async_trait]
impl Level for AutoscrapeFollow {
    //TODO test
    async fn init(&mut self, autoscrape: &Autoscrape) {
        let _ = self.refill_cache(autoscrape).await;
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        match self.cache.pop() {
            Some(key) => {
                self.current_key = key.into();
                false
            }
            None => true
        }
    }

    //TODO test
    fn current(&self) -> String {
        self.current_key.to_owned()
    }

    //TODO test
    fn get_state(&self) -> Value {
        json!({"url":self.url.to_owned(),"regex":self.regex.to_owned()})
    }

    //TODO test
    fn set_state(&mut self, json: &Value) {
        if let Some(url) = json.get("url") {
            if let Some(url) = url.as_str() { self.url = url.to_string()}
        }
        if let Some(regex) = json.get("regex") {
            if let Some(regex) = regex.as_str() { self.regex = regex.to_string()}
        }
    }
}

impl AutoscrapeFollow {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{
            url: Self::json_as_str(json,"url")?,
            regex: Self::fix_regex(&Self::json_as_str(json,"rx")?),
            cache: vec![],
            current_key: String::new(),
        })
    }


    /// Follows the next URL
    //TODO test
    async fn refill_cache(&mut self, autoscrape: &Autoscrape) -> Result<(),GenericError> {
        // Construct URL with level values
        let mut url = self.url.clone();
        let level2value: HashMap<String,String> = autoscrape.current().iter().enumerate().map(|(num,value)|{
            (format!("{num}"),value.to_owned())
        }).collect();
        for (key,value) in level2value {
            url = url.replace(&key,&value);
        }

        // Load next URL
        let text = 
            match reqwest::get(&url).await {
                Ok(x) => x.text().await.ok(),
                _ => None
            }
        .ok_or_else(||AutoscrapeError::MediawikiFailure(url.clone()))?;

        // Find new URLs to follow
        let regex = Regex::new(&self.regex)?;
        self.cache = regex
            .captures_iter(&text)
            .map(|cap|cap.get(1))
            .filter_map(|url|url)
            .map(|url|url.as_str().to_string())
            .collect();

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct AutoscrapeMediaWiki {
    url: String,
    apfrom: String,
    title_cache: Vec<String>,
    last_url: Option<String>,
}

impl JsonStuff for AutoscrapeMediaWiki {}

#[async_trait]
impl Level for AutoscrapeMediaWiki {
    //TODO test
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.title_cache.clear();
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        if self.title_cache.is_empty() {
            if let Err(_) = self.refill_cache().await {
                return true
            }
        }
        match self.title_cache.pop() {
            Some(title) => {
                self.apfrom = title.into();
                false
            }
            None => true
        }
    }

    //TODO test
    fn current(&self) -> String {
        self.apfrom.to_owned()
    }

    //TODO test
    fn get_state(&self) -> Value {
        json!({"url":self.url.to_owned(),"apfrom":self.apfrom.to_owned()})
    }

    //TODO test
    fn set_state(&mut self, json: &Value) {
        self.title_cache.clear();
        if let Some(url) = json.get("url") {
            if let Some(url) = url.as_str() { self.url = url.to_string()}
        }
        if let Some(apfrom) = json.get("apfrom") {
            if let Some(apfrom) = apfrom.as_str() { self.apfrom = apfrom.to_string()}
        }
    }
}


impl AutoscrapeMediaWiki {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{url: Self::json_as_str(json,"url")?,apfrom:String::new(),title_cache:vec![],last_url:None})
    }

    /// Returns an allpages query result. Order is reversed so A->Z works via pop().
    //TODO test
    async fn refill_cache(&mut self) -> Result<(),GenericError> {
        let url = format!("{}?action=query&format=json&list=allpages&apnamespace=0&aplimit=500&apfilterredir=nonredirects&apfrom={}",&self.url,&self.apfrom) ;
        if Some(url.to_owned())==self.last_url {
            return Ok(()); // Empty cache, will trigger end-of-the-line
        }
        self.last_url = Some(url.to_owned());
        let text =
            match reqwest::get(&url).await {
                Ok(x) => {
                    if let Ok(text) = x.text().await {
                        Some(text)
                    } else {
                        None
                    }
                }
                _ => None
            }
        .ok_or_else(||AutoscrapeError::MediawikiFailure(url.to_owned()))?;
        let json: Value = serde_json::from_str(&text)?;
        self.title_cache = json
            .get("query").ok_or_else(||AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .get("allpages").ok_or_else(||AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .as_array().ok_or_else(||AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .iter()
            .filter_map(|v|v.get("title"))
            .filter_map(|v|v.as_str())
            .map(|s|s.to_string())
            .rev()
            .collect();
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum AutoscrapeLevelType {
    Keys(AutoscrapeKeys),
    Range(AutoscrapeRange),
    Follow(AutoscrapeFollow),
    MediaWiki(AutoscrapeMediaWiki),
}

impl AutoscrapeLevelType {
    //TODO test
    async fn init(&mut self, autoscrape: &Autoscrape) {
        match self {
            AutoscrapeLevelType::Keys(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::Range(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::Follow(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::MediaWiki(x) => x.init(autoscrape).await,
        }
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        match self {
            AutoscrapeLevelType::Keys(x) => x.tick().await,
            AutoscrapeLevelType::Range(x) => x.tick().await,
            AutoscrapeLevelType::Follow(x) => x.tick().await,
            AutoscrapeLevelType::MediaWiki(x) => x.tick().await,
        }
    }

    //TODO test
    fn current(&self) -> String {
        match self {
            AutoscrapeLevelType::Keys(x) => x.current(),
            AutoscrapeLevelType::Range(x) => x.current(),
            AutoscrapeLevelType::Follow(x) => x.current(),
            AutoscrapeLevelType::MediaWiki(x) => x.current(),
        }
    }

    //TODO test
    fn get_state(&self) -> Value {
        match self {
            AutoscrapeLevelType::Keys(x) => x.get_state(),
            AutoscrapeLevelType::Range(x) => x.get_state(),
            AutoscrapeLevelType::Follow(x) => x.get_state(),
            AutoscrapeLevelType::MediaWiki(x) => x.get_state(),
        }
    }

    //TODO test
    fn set_state(&mut self, json: &Value) {
        match self {
            AutoscrapeLevelType::Keys(x) => x.set_state(json),
            AutoscrapeLevelType::Range(x) => x.set_state(json),
            AutoscrapeLevelType::Follow(x) => x.set_state(json),
            AutoscrapeLevelType::MediaWiki(x) => x.set_state(json),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeLevel {
    level_type: AutoscrapeLevelType,
}

impl AutoscrapeLevel {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        let level_type = match json.get("mode").ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?.as_str().unwrap_or("") {
            "keys" => AutoscrapeLevelType::Keys(AutoscrapeKeys::from_json(json)?),
            "range" => AutoscrapeLevelType::Range(AutoscrapeRange::from_json(json)?),
            "follow" => AutoscrapeLevelType::Follow(AutoscrapeFollow::from_json(json)?),
            "mediawiki" => AutoscrapeLevelType::MediaWiki(AutoscrapeMediaWiki::from_json(json)?),
            _ => return Err(AutoscrapeError::UnknownLevelType(json.to_owned()))
        };
        Ok(Self {
            level_type,
        })
    }

    //TODO test
    async fn init(&mut self, autoscrape: &Autoscrape) {
        self.level_type.init(autoscrape).await
    }

    //TODO test
    async fn tick(&mut self) -> bool {
        self.level_type.tick().await
    }

    //TODO test
    fn current(&self) -> String {
        self.level_type.current()
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeResolve {
    use_pattern: String,
    regexs: Vec<(Regex,String)>,
}

impl JsonStuff for AutoscrapeResolve {}

impl AutoscrapeResolve {
    //TODO test
    fn from_json(json: &Value, key: &str) -> Result<Self,AutoscrapeError> {
        let json = match json.get(key) {
            Some(json) => json,
            None => return Ok(Self{use_pattern:String::new(),regexs:vec![]})
        };
        //.ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
        let regexs_str = json
            .get("rx")
            .map(|x|x.to_owned())
            .unwrap_or_else(|| json!([]))
            .as_array()
            .map(|x|x.to_owned())
            .unwrap_or_else(|| vec![]);
        let mut regexs= vec![];
        for regex in regexs_str {
            let arr = regex
                .as_array()
                .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
            let pattern = arr
                .get(0)
                .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?
                .as_str()
                .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
            let replacement = arr
                .get(1)
                .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?
                .as_str()
                .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
            regexs.push((
                Regex::new(&Self::fix_regex(pattern)).ok().ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?,
                replacement.to_string()
            ));
        }
        Ok(Self{
            use_pattern: Self::json_as_str(&json,"use")?,
            regexs,
        })
    }

    //TODO test
    fn replace_vars(&self, map: &HashMap<String,String>) -> String {
        let mut ret = self.use_pattern.to_owned();
        for (key,value) in map {
            ret = ret.replace(key,value);
        }
        for regex in &self.regexs {
            ret = regex.0.replace_all(&ret, &regex.1).into();
        }
        Self::fix_html(&ret).trim().into()
    }

    //TODO test
    fn fix_html(s: &str) -> String {
        let ret = html_escape::decode_html_entities(s);
        let ret = RE_HTML.replace_all(&ret, " ");
        RE_SIMPLE_SPACE.replace_all(&ret, " ").trim().into()
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeResolveAux {
    property: usize,
    id: String,
}

impl JsonStuff for AutoscrapeResolveAux {}

impl AutoscrapeResolveAux {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        let property = Self::json_as_str(json, "prop")?.replace("P","");
        let property = match property.parse::<usize>() {
            Ok(property) => property,
            _ => return Err(AutoscrapeError::BadType(json.to_owned()))
        } ;
        let id = Self::json_as_str(json, "id")?;
        Ok(Self{property,id})
    }

    //TODO test
    fn replace_vars(&self, map: &HashMap<String,String>) -> (usize,String) {
        let mut ret = self.id.to_owned();
        for (key,value) in map {
            ret = ret.replace(key,value);
        }
        let ret = AutoscrapeResolve::fix_html(&ret);
        (self.property, ret)
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeScraper {
    url: String,
    regex_block: Option<Regex>,
    regex_entry: Vec<Regex>,
    resolve_id: AutoscrapeResolve,
    resolve_name: AutoscrapeResolve,
    resolve_desc: AutoscrapeResolve,
    resolve_url: AutoscrapeResolve,
    resolve_type: AutoscrapeResolve,
    resolve_aux: Vec<AutoscrapeResolveAux>,
}

impl JsonStuff for AutoscrapeScraper {}

impl AutoscrapeScraper {
    //TODO test
    fn from_json(json: &Value) -> Result<Self,GenericError> {
        let resolve = json.get("resolve").ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
        Ok(Self{
            url: Self::json_as_str(json,"url")?,
            regex_block: Self::regex_block_from_json(json)?,
            regex_entry: Self::regex_entry_from_json(json)?,
            resolve_id: AutoscrapeResolve::from_json(resolve,"id")?,
            resolve_name: AutoscrapeResolve::from_json(resolve,"name")?,
            resolve_desc: AutoscrapeResolve::from_json(resolve,"desc")?,
            resolve_url: AutoscrapeResolve::from_json(resolve,"url")?,
            resolve_type: AutoscrapeResolve::from_json(resolve,"type")?,
            resolve_aux: Self::resolve_aux_from_json(json)?,
        })
    }

    //TODO test
    fn resolve_aux_from_json(json: &Value) -> Result<Vec<AutoscrapeResolveAux>,GenericError> {
        Ok(
            json // TODO test aux, eg catalog 287
            .get("aux")
            .map(|x|x.to_owned())
            .unwrap_or_else(||json!([]))
            .as_array()
            .map(|x|x.to_owned())
            .unwrap_or_else(||vec![])
            .iter()
            .filter_map(|x|AutoscrapeResolveAux::from_json(x).ok())
            .collect()
        )
    }

    //TODO test
    fn regex_entry_from_json(json: &Value) -> Result<Vec<Regex>,GenericError> {
        let rx_entry = json.get("rx_entry").ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
        if rx_entry.is_string() {
            let s = rx_entry.as_str().ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
            Ok(vec![RegexBuilder::new(&Self::fix_regex(s)).multi_line(true).build()?])
        } else { // Assuming array
            let arr = rx_entry.as_array().ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
            let mut ret = vec![];
            for x in arr {
                if let Some(s) = x.as_str() {
                    ret.push(RegexBuilder::new(&Self::fix_regex(s)).multi_line(true).build()?)
                }
            }
            Ok(ret)
        }
        //RegexBuilder::new(&Self::json_as_str(json,"rx_entry")?).multi_line(true).build()?
        
    }

    //TODO test
    fn regex_block_from_json(json: &Value) -> Result<Option<Regex>,GenericError> {
        Ok( // TODO test
        if let Some(v) = json.get("rx_block") {
            if let Some(s) = v.as_str() {
                if s.is_empty() {
                    None
                } else {
                    let r = RegexBuilder::new(&Self::fix_regex(s))
                        .multi_line(true)
                        .build()?;
                    Some(r)
                }
            } else {
                None
            }
        } else {
            None
        })
    }

    //TODO test
    fn process_html_page(&self, html: &str, autoscrape: &Autoscrape) -> Vec<ExtendedEntry> {
        match &self.regex_block {
            Some(regex_block) => {
                regex_block
                    .captures_iter(html)
                    .filter_map(|cap|cap.get(1))
                    .map(|s|s.as_str().to_string())
                    .flat_map(|s|self.process_html_block(&s, autoscrape))
                    .collect()
            }
            None => {
                self.process_html_block(html, autoscrape)
            }
        }
    }

    //TODO test
    fn process_html_block(&self, html: &str, autoscrape: &Autoscrape) -> Vec<ExtendedEntry> {
        let mut ret = vec![];
        for regex_entry in &self.regex_entry {
            if !regex_entry.is_match(html) {
                continue;
            }            
            for cap in regex_entry.captures_iter(html) {
                let values: Vec<String> = cap.iter().map(|v|v.map(|x|x.as_str().to_string()).unwrap_or(String::new())).collect();
                let mut map: HashMap<String,String> = values.iter().enumerate().skip(1).map(|(num,value)|(format!("${}",num),value.to_owned())).collect();
                for (num,level) in autoscrape.levels.iter().enumerate() {
                    map.insert(format!("$L{}",num+1), level.current());
                }
                let type_name = self.resolve_type.replace_vars(&map);
                let type_name = if type_name.is_empty() {None} else {Some(type_name)};
                let entry_ex = ExtendedEntry {
                    entry : Entry {
                        id: ENTRY_NEW_ID,
                        catalog: autoscrape.catalog_id,
                        ext_id: self.resolve_id.replace_vars(&map),
                        ext_url: self.resolve_url.replace_vars(&map),
                        ext_name: self.resolve_name.replace_vars(&map),
                        ext_desc: self.resolve_desc.replace_vars(&map),
                        q: None,
                        user: None,
                        timestamp: None,
                        random: rand::thread_rng().gen(),
                        type_name,
                        mnm: Some(autoscrape.mnm.clone())
                    },
                    aux: self.resolve_aux.iter().map(|aux|aux.replace_vars(&map)).collect(),
                    born: None,
                    died: None,
                    aliases: vec![],
                    descriptions: HashMap::new(),
                    location: None,
                };
                ret.push(entry_ex);
            }
            break; // First regexp to match wins
        }
        ret
    }
}

#[derive(Debug, Clone)]
pub struct Autoscrape {
    autoscrape_id: usize,
    catalog_id: usize,
    //json: Value,
    simple_space: bool,
    skip_failed: bool,
    utf8_encode: bool,
    levels: Vec<AutoscrapeLevel>,
    scraper: AutoscrapeScraper,
    mnm: MixNMatch,
    job: Option<Job>,
    client: reqwest::Client,
    urls_loaded: usize,
    entry_batch: Vec<ExtendedEntry>,
}

impl Jobbable for Autoscrape {
    //TODO test
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    //TODO test
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}

impl Autoscrape {
    //TODO test
    pub async fn new(catalog_id: usize, mnm: &MixNMatch) -> Result<Self,GenericError> {
        let results = mnm.app.get_mnm_conn().await?
            .exec_iter("SELECT `id`,`json` FROM `autoscrape` WHERE `catalog`=:catalog_id",params! {catalog_id}).await?
            .map_and_drop(from_row::<(usize,String)>).await?;
        let (id,json) = results.get(0).ok_or_else(||AutoscrapeError::NoAutoscrapeForCatalog)?;
        let json: Value = serde_json::from_str(json)?;
        let mut ret = Self {
            autoscrape_id:*id,
            catalog_id,
            mnm: mnm.clone(), 
            simple_space:false,
            skip_failed:false,
            utf8_encode:false,
            levels:vec![],
            scraper: AutoscrapeScraper::from_json(json.get("scraper").ok_or_else(||AutoscrapeError::NoAutoscrapeForCatalog)?)?,
            job: None,
            client : reqwest::Client::builder().user_agent(AUTOSCRAPER_USER_AGENT).build()?,
            urls_loaded: 0,
            entry_batch: vec![],
        };
        if let Some(options) = json.get("options") { // Options in main JSON
            ret.options_from_json(options);
        } else if let Some(scraper) = json.get("scraper") { // Options in scraper
            if let Some(options) = scraper.get("options") {
                ret.options_from_json(options);
            }
        }
        if let Some(levels) = json.get("levels") {
            for level in levels.as_array().unwrap_or(&vec![]).into_iter() {
                ret.levels.push(AutoscrapeLevel::from_json(level)?);
            }
        }
        Ok(ret)
    }

    //TODO test
    fn options_from_json(&mut self, json: &Value) {
        self.simple_space = json.get("simple_space").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
        self.skip_failed = json.get("skip_failed").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
        self.utf8_encode = json.get("utf8_encode").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
    }

    //TODO test
    pub async fn init(&mut self) {
        let mut levels = self.levels.clone();
        for level in &mut levels {
            level.init(&self).await
        }
        self.levels = levels;
    }

    /// Iterates one permutation. Returns true if all possible permutations have been done.
    //TODO test
    pub async fn tick(&mut self) -> bool {
        let mut l = self.levels.len() ; // start with deepest level; level numbers starting at 1
        while l>0 {
            let mut level = self.levels[l-1].clone();
            if level.tick().await {
                level.init(&self).await;
                self.levels[l-1] = level;
                l -= 1;
            } else {
                self.levels[l-1] = level;
                return false;
            }
        }
        true
    }

    /// Returns the current values of all levels.
    //TODO test
    fn current(&self) -> Vec<String> {
        self.levels.iter().map(|level|level.current()).collect()
    }

    //TODO test
    async fn load_url(&mut self, url: &str) -> Option<String> {
        self.urls_loaded += 1;
        if self.urls_loaded % 1000 == 0 {
            let _ = self.remember_state().await;
        }
        // TODO POST
        self.client.get(url)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()
    }

    //TODO test
    async fn iterate_one(&mut self) -> bool {
        // Run current permutation
        let current = self.current();
        let mut url = self.scraper.url.to_owned();
        current.iter().enumerate().for_each(|(l0,s)| url = url.replace(&format!("${}",l0+1),s));
        if let Some(mut html) = self.load_url(&url).await {
            if self.simple_space {
                html = RE_SIMPLE_SPACE.replace_all(&html," ").to_string();
            }
            if self.utf8_encode {
                // TODO
            }
            self.entry_batch.append(&mut self.scraper.process_html_page(&html,&self));
            if self.entry_batch.len()>= AUTOSCRAPE_ENTRY_BATCH_SIZE {
                let _ = self.add_batch().await;
            }
        }

        // Next permutation
        self.tick().await
    }

    //TODO test
    async fn add_batch(&mut self) -> Result<(),GenericError> {
        if self.entry_batch.is_empty() {
            let _ = self.remember_state().await;
            return Ok(())
        }
        let ext_ids: Vec<String> = self.entry_batch.iter().map(|e|e.entry.ext_id.to_owned()).collect();
        let placeholders = MixNMatch::sql_placeholders(ext_ids.len());
        let sql = format!("SELECT `ext_id`,`id` FROM entry WHERE `ext_id` IN ({}) AND `catalog`={}",&placeholders,self.catalog_id);
        let existing_ext_ids: Vec<(String,usize)> = sql.with(ext_ids.clone())
            .map(self.mnm.app.get_mnm_conn().await?, |(id,ext_id)|(id,ext_id))
            .await?;
        let existing_ext_ids: HashMap<String,usize> = existing_ext_ids.into_iter().collect();
        for ex in &mut self.entry_batch {
            match existing_ext_ids.get(&ex.entry.ext_id) {
                Some(entry_id) => { // Entry already exists
                    ex.entry.id = *entry_id;
                    // TODO update?
                }
                None => {
                    let _ = ex.insert_new(&self.mnm).await;
                }
            }
        }
        self.entry_batch.clear();
        let _ = self.remember_state().await;
        Ok(())
    }

    //TODO test
    pub async fn remember_state(&self) -> Result<(),GenericError> {
        let json: Vec<Value> = self.levels.iter().map(|level|level.level_type.get_state()).collect();
        let json = json!(json);
        self.remember_job_data(&json).await?;
        Ok(())
    }

    //TODO test
    pub async fn run(&mut self) -> Result<(),GenericError> {
        self.init().await;
        let _ = self.start().await;
        while !self.iterate_one().await {}
        let _ = self.finish().await;
        Ok(())
    }

    //TODO test
    pub async fn start(&mut self) -> Result<(),GenericError> {
        let autoscrape_id = self.autoscrape_id;
        let sql = "UPDATE `autoscrape` SET `status`='RUNNING'`last_run_min`=NULL,`last_run_urls`=NULL WHERE `id`=:autoscrape_id" ;
        if let Ok(mut conn) = self.mnm.app.get_mnm_conn().await {
            let _ = conn.exec_drop(sql, params! {autoscrape_id}).await;
        }
        if let Some(json) = self.get_last_job_data() {
            match json.as_array() {
                Some(arr) => {
                    if arr.len()==self.levels.len() {
                        arr.iter().enumerate().for_each(|(num,j)|self.levels[num].level_type.set_state(j));
                    }
                }
                None => {}
            }
        }
        Ok(())
    }

    //TODO test
    pub async fn finish(&mut self) -> Result<(),GenericError> {
        let _ = self.add_batch().await; // Flush
        let autoscrape_id = self.autoscrape_id;
        let last_run_urls = self.urls_loaded;
        let sql = "UPDATE `autoscrape` SET `status`='OK',`last_run_min`=NULL,`last_run_urls`=:last_run_urls WHERE `id`=:autoscrape_id" ;
        if let Ok(mut conn) = self.mnm.app.get_mnm_conn().await {
            let _ = conn.exec_drop(sql, params! {autoscrape_id,last_run_urls}).await;
        }
        let _ = self.mnm.refresh_overview_table(self.catalog_id).await;
        let _ = self.clear_offset().await;
        let _ = Job::queue_simple_job(&self.mnm,self.catalog_id,"automatch_by_search",None).await;
        let _ = Job::queue_simple_job(&self.mnm,self.catalog_id,"microsync",None).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::mixnmatch::*;

    const TEST_CATALOG_ID: usize = 91;//5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;
    const _TEST_ITEM_ID: usize = 13520818 ; // Q13520818

    #[tokio::test]
    async fn test_autoscrape() {
        let mnm = get_test_mnm();
        let mut autoscrape = Autoscrape::new(TEST_CATALOG_ID,&mnm).await.unwrap();
        let mut cnt: usize = 1;
        autoscrape.init().await;
        while !autoscrape.tick().await { cnt += 1 }
        assert_eq!(cnt,319);
    }
}