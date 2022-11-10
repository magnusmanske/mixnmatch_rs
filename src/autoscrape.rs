use lazy_static::lazy_static;
use regex::RegexBuilder;
use serde_json::{json, Value};
use regex::Regex;
use mysql_async::from_row;
use std::error::Error;
use std::fmt;
use mysql_async::prelude::*;
use crate::entry::*;
use crate::job::*;
use crate::app_state::*;
use crate::mixnmatch::MixNMatch;

const AUTOSCRAPER_USER_AGENT: &str = "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion";

lazy_static!{
    static ref RE_SIMPLE_SPACE : Regex = RegexBuilder::new(r"\s+").multi_line(true).ignore_whitespace(true).build().unwrap() ;
}

#[derive(Debug, Clone)]
enum AutoscrapeError {
    NoAutoscrapeForCatalog,
    UnknownLevelType(Value),
    BadType(Value),
}

impl Error for AutoscrapeError {}

impl fmt::Display for AutoscrapeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}

trait JsonStuff {
    fn json_as_str(json: &Value, key: &str) -> Result<String,AutoscrapeError> {
        Ok(json.get(key)
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .as_str()
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .to_string())
    }

    fn json_as_u64(json: &Value, key: &str) -> Result<u64,AutoscrapeError> {
        Ok(json.get(key)
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?
            .as_u64()
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?)
    }
}

trait Level {
    fn init(&mut self);
    fn tick(&mut self) -> bool;
    fn current(&self) -> String;
}

#[derive(Debug, Clone)]
struct AutoscrapeKeys {
    keys: Vec<String>,
    position: usize
}

impl Level for AutoscrapeKeys {
    fn init(&mut self) {
        self.position = 0;
    }

    fn tick(&mut self) -> bool {
        self.position += 1 ;
        self.position >= self.keys.len()
    }

    fn current(&self) -> String {
        match self.keys.get(self.position) {
            Some(v) => v.to_owned(),
            None => String::new()
        }
    }
}

impl AutoscrapeKeys {
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

impl Level for AutoscrapeRange {
    fn init(&mut self) {
        self.current_value = self.start;
    }

    fn tick(&mut self) -> bool {
        self.current_value += self.step ;
        self.current_value > self.end
    }

    fn current(&self) -> String {
        format!("{}",self.current_value)
    }
}

impl AutoscrapeRange {
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{
            start: Self::json_as_u64(json,"start")?,
            end: Self::json_as_u64(json,"end")?,
            step: Self::json_as_u64(json,"step")?,
            current_value: 0, // Gets overwritten by init()
        })
    }
}


#[derive(Debug, Clone)]
struct AutoscrapeFollow {
    url: String,
    regex: String
}

impl JsonStuff for AutoscrapeFollow {}

impl Level for AutoscrapeFollow {
    fn init(&mut self) {
        // TODO
    }

    fn tick(&mut self) -> bool {
        false // TODO
    }

    fn current(&self) -> String {
        String::new() // TODO
    }
}

impl AutoscrapeFollow {
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{
            url: Self::json_as_str(json,"url")?,
            regex: Self::json_as_str(json,"rx")?,
        })
    }
}

#[derive(Debug, Clone)]
struct AutoscrapeMediaWiki {
    url: String
}

impl JsonStuff for AutoscrapeMediaWiki {}

impl Level for AutoscrapeMediaWiki {
    fn init(&mut self) {
        // TODO
    }

    fn tick(&mut self) -> bool {
        false // TODO
    }

    fn current(&self) -> String {
        String::new() // TODO
    }
}


impl AutoscrapeMediaWiki {
    fn from_json(json: &Value) -> Result<Self,AutoscrapeError> {
        Ok(Self{url: Self::json_as_str(json,"url")?})
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
    fn init(&mut self) {
        match self {
            AutoscrapeLevelType::Keys(x) => x.init(),
            AutoscrapeLevelType::Range(x) => x.init(),
            AutoscrapeLevelType::Follow(x) => x.init(),
            AutoscrapeLevelType::MediaWiki(x) => x.init(),
        }
    }

    fn tick(&mut self) -> bool {
        match self {
            AutoscrapeLevelType::Keys(x) => x.tick(),
            AutoscrapeLevelType::Range(x) => x.tick(),
            AutoscrapeLevelType::Follow(x) => x.tick(),
            AutoscrapeLevelType::MediaWiki(x) => x.tick(),
        }
    }

    fn current(&self) -> String {
        match self {
            AutoscrapeLevelType::Keys(x) => x.current(),
            AutoscrapeLevelType::Range(x) => x.current(),
            AutoscrapeLevelType::Follow(x) => x.current(),
            AutoscrapeLevelType::MediaWiki(x) => x.current(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeLevel {
    level_type: AutoscrapeLevelType,
    last_value: Option<Value>,
}

impl AutoscrapeLevel {
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
            last_value: None,
        })
    }

    fn init(&mut self) {
        self.level_type.init()
    }

    fn tick(&mut self) -> bool {
        self.level_type.tick()
    }

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
    fn from_json(json: &Value, key: &str) -> Result<Self,AutoscrapeError> {
        let json = json
            .get(key)
            .ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
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
                Regex::new(pattern).ok().ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?,
                replacement.to_string()
            ));
        }
        Ok(Self{
            use_pattern: Self::json_as_str(&json,"use")?,
            regexs,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeScraper {
    url: String,
    regex_block: Option<Regex>,
    regex_entry: Regex, // TODO mioght be an array, first matching one is used?
    resolve_id: AutoscrapeResolve,
    resolve_name: AutoscrapeResolve,
    resolve_desc: AutoscrapeResolve,
    resolve_url: AutoscrapeResolve,
    resolve_type: AutoscrapeResolve,
}

impl JsonStuff for AutoscrapeScraper {}

impl AutoscrapeScraper {
    fn from_json(json: &Value) -> Result<Self,GenericError> {
        let resolve = json
            .get("resolve")
            .ok_or_else(||AutoscrapeError::BadType(json.to_owned()))?;
        let regex_block = match json.get("rx_block") {
            Some(v) => {
                match v.as_str() {
                    Some(s) => {
                        if s.is_empty() {
                            None
                        } else {
                            let r = RegexBuilder::new(s)
                                .multi_line(true)
                                .build()?;
                            Some(r)
                        }
                    }
                    None => None
                }
            },
            None => None
        } ;
        let regex_entry = RegexBuilder::new(&Self::json_as_str(json,"rx_entry")?)
            .multi_line(true)
            .build()?;
        Ok(Self{
            url: Self::json_as_str(json,"url")?,
            regex_block,
            regex_entry,
            resolve_id: AutoscrapeResolve::from_json(resolve,"id")?,
            resolve_name: AutoscrapeResolve::from_json(resolve,"name")?,
            resolve_desc: AutoscrapeResolve::from_json(resolve,"desc")?,
            resolve_url: AutoscrapeResolve::from_json(resolve,"url")?,
            resolve_type: AutoscrapeResolve::from_json(resolve,"type")?,
        })
    }

    fn process_html_page(&self, html: &str) -> Vec<Entry> {
        match &self.regex_block {
            Some(regex_block) => {
                regex_block
                    .captures_iter(html)
                    .filter_map(|cap|cap.get(1))
                    .map(|s|s.as_str().to_string())
                    .flat_map(|s|self.process_html_block(&s))
                    .collect()
            }
            None => {
                self.process_html_block(html)
            }
        }
    }

    fn process_html_block(&self, html: &str) -> Vec<Entry> {
        println!("\n{}\n{:?}",&html,&self.regex_entry);
        let mut ret = vec![];
        println!("{:?}",self.regex_entry.captures(html));
        for cap in self.regex_entry.captures_iter(html) {
            println!("{:?}",&cap);
            let values: Vec<String> = cap.iter().map(|v|v.map(|x|x.as_str().to_string()).unwrap_or(String::new())).collect();
            println!("{:?}",&values);
        }
        ret
    }
}

#[derive(Debug, Clone)]
pub struct Autoscrape {
    id: usize,
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
}

impl Jobbable for Autoscrape {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}

impl Autoscrape {
    pub async fn new(catalog_id: usize, mnm: &MixNMatch) -> Result<Self,GenericError> {
        let results = mnm.app.get_mnm_conn().await?
            .exec_iter("SELECT `id`,`json` FROM `autoscrape` WHERE `catalog`=:catalog_id",params! {catalog_id}).await?
            .map_and_drop(from_row::<(usize,String)>).await?;
        let (id,json) = results.get(0).ok_or_else(||AutoscrapeError::NoAutoscrapeForCatalog)?;
        let json: Value = serde_json::from_str(json)?;
        let mut ret = Self {
            id:*id,
            mnm: mnm.clone(), 
            catalog_id,
            simple_space:false,
            skip_failed:false,
            utf8_encode:false,
            levels:vec![],
            scraper: AutoscrapeScraper::from_json(json.get("scraper").ok_or_else(||AutoscrapeError::NoAutoscrapeForCatalog)?)?,
            job: None,
            client : reqwest::Client::builder().user_agent(AUTOSCRAPER_USER_AGENT).build()?
    
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

    fn options_from_json(&mut self, json: &Value) {
        self.simple_space = json.get("simple_space").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
        self.skip_failed = json.get("skip_failed").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
        self.utf8_encode = json.get("utf8_encode").map(|x|x.as_u64().unwrap_or(0)).unwrap_or(0)==1;
    }

    pub fn init(&mut self) {
        self.levels.iter_mut().for_each(|level|level.init());
    }

    /// Iterates one permutation. Returns true if all possible permutations have been done.
    pub fn tick(&mut self) -> bool {
        if self.levels.is_empty() {
            return true;
        }
        let mut l = self.levels.len() ; // lowest level, starting at 1
        while l>0 {
            if self.levels[l-1].tick() {
                self.levels[l-1].init();
                l -= 1;
            } else {
                return false;
            }
        }
        true
    }

    fn current(&self) -> Vec<String> {
        self.levels.iter().map(|level|level.current()).collect()
    }

    async fn load_url(&self, url: &str) -> Option<String> {
        // TODO POST
        self.client.get(url)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()
    }

    async fn iterate_one(&mut self) -> bool {
        let current = self.current();
        let mut url = self.scraper.url.to_owned();
        current.iter().enumerate().for_each(|(l0,s)| url = url.replace(&format!("${}",l0+1),s));
        println!("{}",&url);
        if let Some(mut html) = self.load_url(&url).await {
            if self.simple_space {
                html = RE_SIMPLE_SPACE.replace_all(&html," ").to_string();
            }
            // TODO simple_space
            // TODO UTF8-encode
            let results = self.scraper.process_html_page(&html);
        }
        self.tick()
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use crate::mixnmatch::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;
    const _TEST_ITEM_ID: usize = 13520818 ; // Q13520818

    #[tokio::test]
    async fn test_autoscrape() {
        let mnm = get_test_mnm();
        let mut autoscrape = Autoscrape::new(91,&mnm).await.unwrap();
        let mut cnt: usize = 1;
        autoscrape.init();
        while !autoscrape.iterate_one().await { cnt += 1 } // tick
        assert_eq!(cnt,319);
    }
}