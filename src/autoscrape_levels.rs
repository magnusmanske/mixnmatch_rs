use crate::autoscrape::{Autoscrape, AutoscrapeError, AutoscrapeRegex, JsonStuff};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;

#[async_trait]
trait Level {
    async fn init(&mut self, autoscrape: &Autoscrape);

    /// Returns true if this level has been completed, false if there was at least one more result.
    async fn tick(&mut self) -> bool;

    fn current(&self) -> String;
    fn get_state(&self) -> Value;
    fn set_state(&mut self, json: &Value);
}

#[derive(Debug, Clone)]
pub struct AutoscrapeKeys {
    keys: Vec<String>,
    position: usize,
}

#[async_trait]
impl Level for AutoscrapeKeys {
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.position = 0;
    }

    async fn tick(&mut self) -> bool {
        self.position += 1;
        self.position >= self.keys.len()
    }

    fn current(&self) -> String {
        self.keys
            .get(self.position)
            .map_or_else(String::new, |v| v.to_owned())
    }

    fn get_state(&self) -> Value {
        json!({"position":self.position})
    }

    fn set_state(&mut self, json: &Value) {
        if let Some(position) = json.get("position") {
            if let Some(position) = position.as_u64() {
                self.position = position as usize;
            }
        }
    }
}

impl AutoscrapeKeys {
    fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        let keys = json
            .get("keys")
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .as_array()
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .iter()
            .filter_map(|s| s.as_str())
            .map(|s| s.to_string())
            .collect();
        Ok(Self { keys, position: 0 })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AutoscrapeRange {
    start: u64,
    end: u64,
    step: u64,
    current_value: u64,
}

impl JsonStuff for AutoscrapeRange {}

#[async_trait]
impl Level for AutoscrapeRange {
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.current_value = self.start;
    }

    async fn tick(&mut self) -> bool {
        self.current_value += self.step;
        self.current_value > self.end
    }

    fn current(&self) -> String {
        format!("{}", self.current_value)
    }

    fn get_state(&self) -> Value {
        json!({"current_value":self.current_value})
    }

    fn set_state(&mut self, json: &Value) {
        if let Some(current_value) = json.get("current_value") {
            if let Some(current_value) = current_value.as_u64() {
                self.current_value = current_value;
            }
        }
    }
}

impl AutoscrapeRange {
    fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        Ok(Self {
            start: Self::json_as_u64(json, "start")?,
            end: Self::json_as_u64(json, "end")?,
            step: Self::json_as_u64(json, "step")?,
            current_value: Self::json_as_u64(json, "start")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeFollow {
    url: String,
    regex: String,
    cache: Vec<String>,
    current_key: String,
}

impl JsonStuff for AutoscrapeFollow {}

#[async_trait]
impl Level for AutoscrapeFollow {
    async fn init(&mut self, autoscrape: &Autoscrape) {
        let _ = self.refill_cache(autoscrape).await;
    }

    async fn tick(&mut self) -> bool {
        match self.cache.pop() {
            Some(key) => {
                self.current_key = key;
                false
            }
            None => true,
        }
    }

    fn current(&self) -> String {
        self.current_key.to_owned()
    }

    fn get_state(&self) -> Value {
        json!({"url":self.url.to_owned(),"regex":self.regex.to_owned()})
    }

    fn set_state(&mut self, json: &Value) {
        if let Some(url) = json.get("url") {
            if let Some(url) = url.as_str() {
                self.url = url.to_string();
            }
        }
        if let Some(regex) = json.get("regex") {
            if let Some(regex) = regex.as_str() {
                self.regex = regex.to_string();
            }
        }
    }
}

impl AutoscrapeFollow {
    fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        Ok(Self {
            url: Self::json_as_str(json, "url")?,
            regex: Self::fix_regex(&Self::json_as_str(json, "rx")?),
            cache: vec![],
            current_key: String::new(),
        })
    }

    /// Follows the next URL
    async fn refill_cache(&mut self, autoscrape: &Autoscrape) -> Result<()> {
        // Load next URL
        let text = self.refill_cache_get_text(autoscrape).await?;

        // Find new URLs to follow
        self.refill_cache_text_to_cache(text)?;
        Ok(())
    }

    fn refill_cache_text_to_cache(&mut self, text: String) -> Result<()> {
        let regex = AutoscrapeRegex::new(&self.regex)?;
        self.cache = regex
            .captures_iter(&text)
            //.filter_map(|caps|caps.ok())
            .filter_map(|cap| cap.get(1))
            .map(|url| url.as_str().to_string())
            .collect();
        Ok(())
    }

    async fn refill_cache_get_text(&mut self, autoscrape: &Autoscrape) -> Result<String> {
        let url = self.refill_cache_get_url(autoscrape);
        let client = Autoscrape::reqwest_client_external()?;
        let text = match client.get(url.to_owned()).send().await {
            Ok(x) => x.text().await.ok(),
            _ => None,
        }
        .ok_or_else(|| AutoscrapeError::MediawikiFailure(url.clone()))?;
        Ok(text)
    }

    fn refill_cache_get_url(&mut self, autoscrape: &Autoscrape) -> String {
        // Construct URL with level values
        let mut url = self.url.clone();
        let level2value: HashMap<String, String> = autoscrape
            .current()
            .iter()
            .enumerate()
            .map(|(num, value)| (format!("{num}"), value.to_owned()))
            .collect();
        for (key, value) in level2value {
            url = url.replace(&key, &value);
        }
        url
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeMediaWiki {
    url: String,
    apfrom: String,
    title_cache: Vec<String>,
    last_url: Option<String>,
}

impl JsonStuff for AutoscrapeMediaWiki {}

#[async_trait]
impl Level for AutoscrapeMediaWiki {
    async fn init(&mut self, _autoscrape: &Autoscrape) {
        self.title_cache.clear();
    }

    async fn tick(&mut self) -> bool {
        if self.title_cache.is_empty() && self.refill_cache().await.is_err() {
            return true;
        }
        match self.title_cache.pop() {
            Some(title) => {
                self.apfrom = title;
                false
            }
            None => true,
        }
    }

    fn current(&self) -> String {
        self.apfrom.to_owned()
    }

    fn get_state(&self) -> Value {
        json!({"url":self.url.to_owned(),"apfrom":self.apfrom.to_owned()})
    }

    fn set_state(&mut self, json: &Value) {
        self.title_cache.clear();
        if let Some(url) = json.get("url") {
            if let Some(url) = url.as_str() {
                self.url = url.to_string();
            }
        }
        if let Some(apfrom) = json.get("apfrom") {
            if let Some(apfrom) = apfrom.as_str() {
                self.apfrom = apfrom.to_string();
            }
        }
    }
}

impl AutoscrapeMediaWiki {
    fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        Ok(Self {
            url: Self::json_as_str(json, "url")?,
            apfrom: String::new(),
            title_cache: vec![],
            last_url: None,
        })
    }

    /// Returns an allpages query result. Order is reversed so A->Z works via `pop()`.
    async fn refill_cache(&mut self) -> Result<()> {
        let url = format!("{}?action=query&format=json&list=allpages&apnamespace=0&aplimit=500&apfilterredir=nonredirects&apfrom={}",&self.url,&self.apfrom) ;
        if Some(url.to_owned()) == self.last_url {
            return Ok(()); // Empty cache, will trigger end-of-the-line
        }
        self.last_url = Some(url.to_owned());

        let json = Self::refill_cache_load_json(&url).await?;
        self.refill_cache_set_from_json(json, url)?;
        Ok(())
    }

    fn refill_cache_set_from_json(&mut self, json: Value, url: String) -> Result<()> {
        self.title_cache = json
            .get("query")
            .ok_or_else(|| AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .get("allpages")
            .ok_or_else(|| AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .as_array()
            .ok_or_else(|| AutoscrapeError::MediawikiFailure(url.to_owned()))?
            .iter()
            .filter_map(|v| v.get("title"))
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .rev()
            .collect();
        Ok(())
    }

    async fn refill_cache_load_json(url: &String) -> Result<Value> {
        let client = Autoscrape::reqwest_client_external()?;
        let text = match client.get(url.to_owned()).send().await {
            Ok(x) => x.text().await.ok(),
            _ => None,
        }
        .ok_or_else(|| AutoscrapeError::MediawikiFailure(url.clone()))?;
        let json: Value = serde_json::from_str(&text)?;
        Ok(json)
    }
}

#[derive(Debug, Clone)]
pub enum AutoscrapeLevelType {
    Keys(AutoscrapeKeys),
    Range(AutoscrapeRange),
    Follow(AutoscrapeFollow),
    MediaWiki(AutoscrapeMediaWiki),
}

impl AutoscrapeLevelType {
    async fn init(&mut self, autoscrape: &Autoscrape) {
        match self {
            AutoscrapeLevelType::Keys(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::Range(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::Follow(x) => x.init(autoscrape).await,
            AutoscrapeLevelType::MediaWiki(x) => x.init(autoscrape).await,
        }
    }

    async fn tick(&mut self) -> bool {
        match self {
            AutoscrapeLevelType::Keys(x) => x.tick().await,
            AutoscrapeLevelType::Range(x) => x.tick().await,
            AutoscrapeLevelType::Follow(x) => x.tick().await,
            AutoscrapeLevelType::MediaWiki(x) => x.tick().await,
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

    pub fn get_state(&self) -> Value {
        match self {
            AutoscrapeLevelType::Keys(x) => x.get_state(),
            AutoscrapeLevelType::Range(x) => x.get_state(),
            AutoscrapeLevelType::Follow(x) => x.get_state(),
            AutoscrapeLevelType::MediaWiki(x) => x.get_state(),
        }
    }

    pub fn set_state(&mut self, json: &Value) {
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
    pub fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        let level_type = match json
            .get("mode")
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?
            .as_str()
            .unwrap_or("")
        {
            "keys" => AutoscrapeLevelType::Keys(AutoscrapeKeys::from_json(json)?),
            "range" => AutoscrapeLevelType::Range(AutoscrapeRange::from_json(json)?),
            "follow" => AutoscrapeLevelType::Follow(AutoscrapeFollow::from_json(json)?),
            "mediawiki" => AutoscrapeLevelType::MediaWiki(AutoscrapeMediaWiki::from_json(json)?),
            _ => return Err(AutoscrapeError::UnknownLevelType(json.to_string())),
        };
        Ok(Self { level_type })
    }

    pub const fn level_type(&self) -> &AutoscrapeLevelType {
        &self.level_type
    }

    pub const fn level_type_mut(&mut self) -> &mut AutoscrapeLevelType {
        &mut self.level_type
    }

    pub async fn init(&mut self, autoscrape: &Autoscrape) {
        self.level_type.init(autoscrape).await;
    }

    pub async fn tick(&mut self) -> bool {
        self.level_type.tick().await
    }

    pub fn current(&self) -> String {
        self.level_type.current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_autoscrape_level_keys() {
        let json = json!({
            "mode": "keys",
            "keys": ["a", "b", "c"]
        });
        let mut level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.current(), "a");
        assert!(!level.tick().await);
        assert_eq!(level.current(), "b");
        assert!(!level.tick().await);
        assert_eq!(level.current(), "c");
        assert!(level.tick().await);
        assert_eq!(level.current(), "");
    }

    #[tokio::test]
    async fn test_autoscrape_level_range() {
        let json = json!({
            "mode": "range",
            "start": 1,
            "end": 3,
            "step": 1
        });
        let mut level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.current(), "1");
        assert!(!level.tick().await);
        assert_eq!(level.current(), "2");
        assert!(!level.tick().await);
        assert_eq!(level.current(), "3");
        assert!(level.tick().await);
        assert_eq!(level.current(), "4");
    }
}
