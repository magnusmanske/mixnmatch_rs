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

    /// Total number of `tick()` outcomes this level will yield, if it can
    /// be determined ahead of time. `None` for data-driven levels (Follow,
    /// MediaWiki) whose size depends on a yet-to-be-fetched response.
    fn size_hint(&self) -> Option<usize>;

    /// Zero-based index of the current `tick()` within `size_hint()`. `None`
    /// when `size_hint()` is `None`. Used together with `size_hint` to
    /// compute the flat mixed-radix index into the cartesian product of
    /// levels.
    fn position_hint(&self) -> Option<usize>;
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

    fn size_hint(&self) -> Option<usize> {
        Some(self.keys.len())
    }

    fn position_hint(&self) -> Option<usize> {
        Some(self.position.min(self.keys.len()))
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

    fn size_hint(&self) -> Option<usize> {
        // Range is inclusive of `end` (tick returns true only after
        // current_value > end), so e.g. start=1,end=3,step=1 yields three
        // values: 1, 2, 3. Defensive against zero/inverted ranges that
        // would be configuration bugs but shouldn't panic the worker.
        if self.step == 0 || self.start > self.end {
            return None;
        }
        Some(((self.end - self.start) / self.step) as usize + 1)
    }

    fn position_hint(&self) -> Option<usize> {
        if self.step == 0 || self.current_value < self.start {
            return None;
        }
        let size = self.size_hint()?;
        let pos = ((self.current_value - self.start) / self.step) as usize;
        Some(pos.min(size))
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

    fn size_hint(&self) -> Option<usize> {
        // Size is data-driven (depends on the response of an HTTP fetch
        // performed on each outer-level tick), so it cannot be known up
        // front. Treated as opaque by the percentage calculation.
        None
    }

    fn position_hint(&self) -> Option<usize> {
        None
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
        let regex = AutoscrapeRegex::new(&self.regex)
            .map_err(crate::autoscrape::AutoscrapeError::from)?;
        self.cache = regex
            .captures_iter(&text)
            .into_iter()
            .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
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

    fn size_hint(&self) -> Option<usize> {
        // Streamed via apfrom; total unknown until the last page is
        // empty. Treated as opaque by the percentage calculation.
        None
    }

    fn position_hint(&self) -> Option<usize> {
        None
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

    pub fn size_hint(&self) -> Option<usize> {
        match self {
            AutoscrapeLevelType::Keys(x) => x.size_hint(),
            AutoscrapeLevelType::Range(x) => x.size_hint(),
            AutoscrapeLevelType::Follow(x) => x.size_hint(),
            AutoscrapeLevelType::MediaWiki(x) => x.size_hint(),
        }
    }

    pub fn position_hint(&self) -> Option<usize> {
        match self {
            AutoscrapeLevelType::Keys(x) => x.position_hint(),
            AutoscrapeLevelType::Range(x) => x.position_hint(),
            AutoscrapeLevelType::Follow(x) => x.position_hint(),
            AutoscrapeLevelType::MediaWiki(x) => x.position_hint(),
        }
    }
}

/// Returns `(flat_index, total)` over the cartesian product of the
/// longest contiguous prefix of levels whose size is knowable up front
/// (Keys, Range). Inner levels with unknown size (Follow, MediaWiki)
/// terminate the prefix; their state contributes nothing.
///
/// Returns `None` when no level has a size hint or when the prefix's
/// total iteration count would be zero. The percent is derived by
/// [`crate::job_progress::JobProgress::from_counts`].
///
/// Math: a standard mixed-radix decode over the sized prefix —
/// `flat = Σᵢ positionᵢ · Πⱼ₍ⱼ>ᵢ₎ sizeⱼ`, with `total = Πᵢ sizeᵢ`. This
/// matches the autoscrape outer-to-inner cartesian iteration order
/// (`Autoscrape::tick` resets level[N-1] when it exhausts, ticking
/// level[N-2]).
pub fn sized_prefix_index(levels: &[AutoscrapeLevel]) -> Option<(u64, u64)> {
    let mut sizes: Vec<u64> = Vec::with_capacity(levels.len());
    let mut positions: Vec<u64> = Vec::with_capacity(levels.len());
    for level in levels {
        match (level.size_hint(), level.position_hint()) {
            (Some(s), Some(p)) => {
                sizes.push(s as u64);
                positions.push(p as u64);
            }
            _ => break,
        }
    }
    if sizes.is_empty() {
        return None;
    }
    let total: u64 = sizes.iter().product();
    if total == 0 {
        return None;
    }
    let mut flat: u64 = 0;
    for i in 0..sizes.len() {
        let inner_product: u64 = sizes[i + 1..].iter().product();
        flat += positions[i] * inner_product;
    }
    Some((flat, total))
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

    /// Total number of `tick()` outcomes this level will yield, or `None`
    /// if the level's size is data-driven (Follow, MediaWiki).
    pub fn size_hint(&self) -> Option<usize> {
        self.level_type.size_hint()
    }

    /// Zero-based index of the current `tick()` within `size_hint()`, or
    /// `None` if the level is unsized.
    pub fn position_hint(&self) -> Option<usize> {
        self.level_type.position_hint()
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

    // size_hint / position_hint ─────────────────────────────────────────

    #[test]
    fn keys_size_and_position_hints() {
        let json = json!({"mode": "keys", "keys": ["a", "b", "c"]});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), Some(3));
        assert_eq!(level.position_hint(), Some(0));
    }

    #[tokio::test]
    async fn keys_position_hint_advances_with_tick() {
        let json = json!({"mode": "keys", "keys": ["a", "b", "c"]});
        let mut level = AutoscrapeLevel::from_json(&json).unwrap();
        level.tick().await;
        assert_eq!(level.position_hint(), Some(1));
        level.tick().await;
        assert_eq!(level.position_hint(), Some(2));
    }

    #[test]
    fn range_size_hint_inclusive_of_end() {
        // start=1, end=3, step=1 yields 3 values: 1, 2, 3.
        let json = json!({"mode": "range", "start": 1, "end": 3, "step": 1});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), Some(3));
    }

    #[test]
    fn range_size_hint_with_step() {
        // start=0, end=6, step=2 yields 4 values: 0, 2, 4, 6.
        let json = json!({"mode": "range", "start": 0, "end": 6, "step": 2});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), Some(4));
    }

    #[test]
    fn range_position_hint_zero_at_start() {
        let json = json!({"mode": "range", "start": 2000, "end": 2025, "step": 1});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.position_hint(), Some(0));
    }

    #[tokio::test]
    async fn range_position_hint_advances() {
        let json = json!({"mode": "range", "start": 2000, "end": 2005, "step": 1});
        let mut level = AutoscrapeLevel::from_json(&json).unwrap();
        level.tick().await; // 2001
        level.tick().await; // 2002
        assert_eq!(level.position_hint(), Some(2));
    }

    #[test]
    fn range_size_hint_none_on_zero_step() {
        // Defensive: step=0 would be a configuration bug; don't crash.
        let json = json!({"mode": "range", "start": 0, "end": 5, "step": 0});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), None);
    }

    #[test]
    fn follow_returns_none_for_both_hints() {
        // Follow is data-driven; size/position cannot be known ahead.
        let json = json!({"mode": "follow", "url": "http://example.com/$1", "rx": "(.+)"});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), None);
        assert_eq!(level.position_hint(), None);
    }

    #[test]
    fn mediawiki_returns_none_for_both_hints() {
        let json = json!({"mode": "mediawiki", "url": "https://en.wikipedia.org/w/api.php"});
        let level = AutoscrapeLevel::from_json(&json).unwrap();
        assert_eq!(level.size_hint(), None);
        assert_eq!(level.position_hint(), None);
    }

    // sized_prefix_percent ──────────────────────────────────────────────

    fn keys_level(n: usize) -> AutoscrapeLevel {
        let keys: Vec<Value> = (0..n).map(|i| json!(format!("k{i}"))).collect();
        AutoscrapeLevel::from_json(&json!({"mode": "keys", "keys": keys})).unwrap()
    }

    fn range_level(start: u64, end: u64, step: u64) -> AutoscrapeLevel {
        AutoscrapeLevel::from_json(&json!({
            "mode": "range", "start": start, "end": end, "step": step,
        }))
        .unwrap()
    }

    fn follow_level() -> AutoscrapeLevel {
        AutoscrapeLevel::from_json(&json!({
            "mode": "follow", "url": "http://example.com/", "rx": "(.+)",
        }))
        .unwrap()
    }

    #[test]
    fn prefix_index_empty_levels_is_none() {
        assert_eq!(sized_prefix_index(&[]), None);
    }

    #[test]
    fn prefix_index_outermost_unsized_is_none() {
        let levels = vec![follow_level(), keys_level(10)];
        assert_eq!(sized_prefix_index(&levels), None);
    }

    #[test]
    fn prefix_index_at_start_is_zero_flat() {
        // Two levels of size 10 × 20 = 200 total iterations, all at position 0.
        let levels = vec![keys_level(10), range_level(0, 19, 1)];
        assert_eq!(sized_prefix_index(&levels), Some((0, 200)));
    }

    #[tokio::test]
    async fn prefix_index_mid_iteration() {
        // outer keys=10, inner range=20 (start 0, end 19, step 1)
        // Tick outer 3 times, inner 7 times → positions = (3, 7).
        // flat = 3*20 + 7 = 67, total = 200.
        let mut levels = vec![keys_level(10), range_level(0, 19, 1)];
        for _ in 0..3 {
            levels[0].tick().await;
        }
        for _ in 0..7 {
            levels[1].tick().await;
        }
        assert_eq!(sized_prefix_index(&levels), Some((67, 200)));
    }

    #[tokio::test]
    async fn prefix_index_coarse_when_inner_unsized() {
        // Outer keys=10 (sized), inner follow (unsized). After ticking
        // outer 3 times, prefix is just keys: flat=3, total=10.
        let mut levels = vec![keys_level(10), follow_level()];
        for _ in 0..3 {
            levels[0].tick().await;
        }
        assert_eq!(sized_prefix_index(&levels), Some((3, 10)));
    }

    #[test]
    fn prefix_index_zero_total_is_none() {
        // A 0-key list yields total=0; avoid div by zero.
        let levels = vec![keys_level(0)];
        assert_eq!(sized_prefix_index(&levels), None);
    }
}
