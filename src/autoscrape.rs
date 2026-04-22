use crate::app_state::AppState;
use crate::autoscrape_levels::AutoscrapeLevel;
use crate::autoscrape_resolve::RE_SIMPLE_SPACE;
use crate::autoscrape_scraper::AutoscrapeScraper;
use crate::catalog::Catalog;
use crate::extended_entry::ExtendedEntry;
use crate::job::{Job, Jobbable};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

//pub type AutoscrapeRegex = fancy_regex::Regex;
//pub type AutoscrapeRegexBuilder = fancy_regex::RegexBuilder;

pub type AutoscrapeRegex = regex::Regex;
pub type AutoscrapeRegexBuilder = regex::RegexBuilder;

/// Return value of `Autoscrape::test_fetch`, used by the scraper-builder
/// wizard. Keeps `url`/`html`/`results` for the success path (unchanged
/// wire shape) and adds `diagnostics` so the UI can explain *why* a test
/// returned zero rows.
#[derive(Debug)]
pub struct TestFetchResult {
    pub url: String,
    pub html: String,
    pub results: Vec<Value>,
    pub diagnostics: Value,
}

const AUTOSCRAPER_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0";
const AUTOSCRAPE_ENTRY_BATCH_SIZE: usize = 100;
const AUTOSCRAPE_URL_LOAD_TIMEOUT_SEC: u64 = 60;

#[derive(Debug, Clone)]
pub enum AutoscrapeError {
    NoAutoscrapeForCatalog(usize),
    UnknownLevelType(String),
    BadType(Value),
    MediawikiFailure(String),
}

impl Error for AutoscrapeError {}

impl fmt::Display for AutoscrapeError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AutoscrapeError::UnknownLevelType(s) => write!(f, "{s}"), // user-facing output
            AutoscrapeError::BadType(v) => write!(f, "{v}"),
            AutoscrapeError::MediawikiFailure(v) => write!(f, "{v}"),
            AutoscrapeError::NoAutoscrapeForCatalog(catalog_id) => {
                write!(f, "No Autoscraper for catalog {catalog_id}")
            }
        }
    }
}

pub trait JsonStuff {
    //TODO test
    fn json_as_str(json: &Value, key: &str) -> Result<String, AutoscrapeError> {
        Ok(json
            .get(key)
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .as_str()
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .to_string())
    }

    //TODO test
    fn json_as_u64(json: &Value, key: &str) -> Result<u64, AutoscrapeError> {
        let value = json
            .get(key)
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
        if value.is_string() {
            let s = value
                .as_str()
                .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
            s.parse::<u64>()
                .map_or_else(|_| Err(AutoscrapeError::BadType(json.to_owned())), Ok)
        } else {
            value
                .as_u64()
                .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))
        }
    }

    fn fix_regex(s: &str) -> String {
        s.replace("\\/", "/")
            .replace("\\\"", "\"")
            .replace("\\:", ":")
            .to_string()
    }
}

#[derive(Debug)]
pub struct Autoscrape {
    autoscrape_id: usize,
    catalog_id: usize,
    simple_space: bool,
    skip_failed: bool,
    utf8_encode: bool,
    levels: Vec<AutoscrapeLevel>,
    scraper: AutoscrapeScraper,
    app: AppState,
    job: Option<Job>,
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

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl Autoscrape {
    //TODO test
    pub async fn new(catalog_id: usize, app: &AppState) -> Result<Self> {
        let results = app.storage().autoscrape_get_for_catalog(catalog_id).await?;
        let (id, json) = results
            .first()
            .ok_or(AutoscrapeError::NoAutoscrapeForCatalog(catalog_id))?;
        let json: Value = serde_json::from_str(json)?;
        let mut ret = Self::new_basic(id, catalog_id, app, &json)?;
        Self::initialize_with_options(json, &mut ret)?;
        Ok(ret)
    }

    pub const fn catalog_id(&self) -> usize {
        self.catalog_id
    }

    pub const fn app(&self) -> &AppState {
        &self.app
    }

    pub fn levels(&self) -> &[AutoscrapeLevel] {
        &self.levels
    }

    //TODO test
    fn options_from_json(&mut self, json: &Value) {
        // Accept bool, number, or string for each flag. The scraper wizard
        // sends JS booleans on test (via generateJSON) but stores numbers
        // on save — the two paths used to disagree, silently forcing every
        // option off during tests. Treat anything truthy/non-zero as on.
        self.simple_space = json_flag(json.get("simple_space"));
        self.skip_failed = json_flag(json.get("skip_failed"));
        self.utf8_encode = json_flag(json.get("utf8_encode"));
    }

    //TODO test
    pub async fn init(&mut self) {
        let mut levels = self.levels.clone();
        for level in &mut levels {
            level.init(self).await;
        }
        self.levels = levels;
    }

    /// Iterates one permutation. Returns true if all possible permutations have been done.
    pub async fn tick(&mut self) -> bool {
        let mut l = self.levels.len(); // start with deepest level; level numbers starting at 1
        while l > 0 {
            let mut level = self.levels[l - 1].clone();
            if level.tick().await {
                level.init(self).await;
                self.levels[l - 1] = level;
                l -= 1;
            } else {
                self.levels[l - 1] = level;
                return false;
            }
        }
        true
    }

    /// Returns the current values of all levels.
    //TODO test
    pub fn current(&self) -> Vec<String> {
        self.levels.iter().map(|level| level.current()).collect()
    }

    //TODO test
    async fn load_url(&mut self, url: &str) -> Option<String> {
        self.urls_loaded += 1;
        let crosses_threshold = self.urls_loaded.is_multiple_of(1000);
        if crosses_threshold {
            let _ = self.remember_state().await;
        }
        // TODO POST
        Self::reqwest_client_external()
            .ok()?
            .get(url)
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()
    }

    /// Verbose counterpart to `load_url`, used by the scraper-test UI.
    /// Preserves the HTTP status and content-type alongside the body, and
    /// surfaces the underlying reqwest error message on failure instead of
    /// collapsing everything to `None`. The runner itself still calls the
    /// quiet `load_url`; we only pay for this on one-shot tests.
    async fn load_url_verbose(
        &mut self,
        url: &str,
    ) -> Result<(String, u16, Option<String>), String> {
        self.urls_loaded += 1;
        let client = Self::reqwest_client_external()
            .map_err(|e| format!("HTTP client setup: {e}"))?;
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("request failed: {e}"))?;
        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let body = resp
            .text()
            .await
            .map_err(|e| format!("reading body: {e}"))?;
        Ok((body, status, content_type))
    }

    async fn get_current_url(&self) -> String {
        let current = self.current();
        let mut url = self.scraper.url().to_string();
        current
            .iter()
            .enumerate()
            .for_each(|(l0, s)| url = url.replace(&format!("${}", l0 + 1), s));
        url
    }

    async fn get_patched_html(&mut self, url: String) -> Option<String> {
        let mut html = self.load_url(&url).await?;
        if self.simple_space {
            html = RE_SIMPLE_SPACE.replace_all(&html, " ").to_string();
        }
        if self.utf8_encode {
            // TODO
        }
        Some(html)
    }

    //TODO test
    async fn iterate_one(&mut self) {
        // Run current permutation
        let url = self.get_current_url().await;
        if let Some(html) = self.get_patched_html(url).await {
            let mut extended_entries = self.scraper.process_html_page(&html, self);
            self.entry_batch.append(&mut extended_entries);
            let entry_batch_len = self.entry_batch.len();
            if entry_batch_len >= AUTOSCRAPE_ENTRY_BATCH_SIZE {
                let _ = self.add_batch().await;
            }
        }
    }

    //TODO test
    // async fn iterate_batch(&mut self, batch_size: usize) -> bool {
    //     let mut futures = vec![];
    //     let mut ret = true;
    //     for i in 1..batch_size {
    //         let url = self.get_current_url().await;
    //         let future = self.get_patched_html(url);
    //         futures.push(future);
    //         ret = self.tick().await;
    //         if !ret {
    //             break;
    //         }
    //     }
    //     let htmls: Vec<ExtendedEntry> = join_all(futures).await
    //         .into_iter()
    //         .filter_map(|html|html)
    //         .map(|html| self.scraper.process_html_page(&html,&self))
    //         .flatten()
    //         .collect();
    //     ret
    // }

    //TODO test
    async fn add_batch(&mut self) -> Result<()> {
        if self.entry_batch.is_empty() {
            let _ = self.remember_state().await;
            return Ok(());
        }

        let ext_ids: Vec<String> = self
            .entry_batch
            .iter()
            .map(|e| e.entry.ext_id.to_owned())
            .collect();
        let existing_ext_ids = self
            .app
            .storage()
            .get_entry_ids_for_ext_ids(self.catalog_id, &ext_ids)
            .await?;
        let existing_ext_ids: HashMap<String, usize> = existing_ext_ids.into_iter().collect();
        for ex in &mut self.entry_batch {
            match existing_ext_ids.get(&ex.entry.ext_id) {
                Some(entry_id) => {
                    // Entry already exists
                    ex.entry.id = Some(*entry_id);
                    // TODO update?
                }
                None => {
                    let _ = ex.insert_new(&self.app).await;
                }
            }
        }
        self.entry_batch.clear();
        let _ = self.remember_state().await;
        Ok(())
    }

    //TODO test
    pub async fn remember_state(&mut self) -> Result<()> {
        let json: Vec<Value> = self
            .levels
            .iter()
            .map(|level| level.level_type().get_state())
            .collect();
        let json = json!(json);
        self.remember_job_data(&json).await?;
        Ok(())
    }

    //TODO test
    pub async fn run(&mut self) -> Result<()> {
        self.init().await;
        let _ = self.start().await;
        loop {
            self.iterate_one().await;
            if self.tick().await {
                break;
            }
        }
        let _ = self.finish().await;
        Ok(())
    }

    //TODO test
    pub async fn start(&mut self) -> Result<()> {
        let autoscrape_id = self.autoscrape_id;
        self.app.storage().autoscrape_start(autoscrape_id).await?;
        if let Some(json) = self.get_last_job_data().await {
            if let Some(arr) = json.as_array() {
                if arr.len() == self.levels.len() {
                    arr.iter()
                        .enumerate()
                        .for_each(|(num, j)| self.levels[num].level_type_mut().set_state(j));
                }
            }
        }
        Ok(())
    }

    //TODO test
    pub async fn finish(&mut self) -> Result<()> {
        let _ = self.add_batch().await; // Flush
        let autoscrape_id = self.autoscrape_id;
        let last_run_urls = self.urls_loaded;
        self.app
            .storage()
            .autoscrape_finish(autoscrape_id, last_run_urls)
            .await?;
        let catalog = Catalog::from_id(self.catalog_id, &self.app).await?;
        let _ = catalog.refresh_overview_table().await;
        let _ = self.clear_offset().await;
        let _ =
            Job::queue_simple_job(&self.app, self.catalog_id, "automatch_by_search", None).await;
        let _ = Job::queue_simple_job(&self.app, self.catalog_id, "microsync", None).await;
        Ok(())
    }

    pub fn reqwest_client_external() -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .user_agent(AUTOSCRAPER_USER_AGENT)
            .timeout(core::time::Duration::from_secs(
                AUTOSCRAPE_URL_LOAD_TIMEOUT_SEC,
            ))
            .connection_verbose(true)
            .gzip(true)
            .deflate(true)
            .brotli(true)
            .build()?)
    }

    fn new_basic(
        id: &usize,
        catalog_id: usize,
        app: &AppState,
        json: &Value,
    ) -> Result<Autoscrape> {
        let ret = Self {
            autoscrape_id: *id,
            catalog_id,
            app: app.clone(),
            simple_space: false,
            skip_failed: false,
            utf8_encode: false,
            levels: vec![],
            scraper: AutoscrapeScraper::from_json(
                json.get("scraper")
                    .ok_or(AutoscrapeError::NoAutoscrapeForCatalog(catalog_id))?,
            )?,
            job: None,
            urls_loaded: 0,
            entry_batch: vec![],
        };
        Ok(ret)
    }

    /// One-shot test for the scraper-builder UI: construct an in-memory
    /// Autoscrape from a JSON blob (no DB lookup), initialize the levels to
    /// their starting values, fetch the resulting URL, run the scraper's
    /// regex pass once, and return the URL, HTML, extracted entries, and a
    /// structured `diagnostics` blob the frontend can render to help the
    /// user see where a "zero results" test went wrong (bad fetch, block
    /// regex mismatch, entry regex mismatch, …). No state is persisted.
    pub async fn test_fetch(app: &AppState, json: &Value) -> Result<TestFetchResult> {
        // Compile errors (malformed regex, bad scraper JSON) still bubble
        // up as hard errors — the UI will surface them as the test status.
        let mut autoscrape = Self::new_basic(&0, 0, app, json)?;
        Self::initialize_with_options(json.clone(), &mut autoscrape)?;
        autoscrape.init().await;
        let url = autoscrape.get_current_url().await;
        let mut warnings: Vec<String> = vec![];
        // Pre-compile warnings: check the raw regex source strings for
        // features Rust's `regex` crate doesn't support. These are a very
        // common source of "works in my browser but not here" reports.
        Self::collect_regex_feature_warnings(json, &mut warnings);

        let options_json = serde_json::json!({
            "simple_space": autoscrape.simple_space,
            "utf8_encode":  autoscrape.utf8_encode,
            "skip_failed":  autoscrape.skip_failed,
        });

        // Fetch with diagnostics. On failure we still return Ok with a
        // populated diagnostics blob so the UI can render the "here's what
        // went wrong" panel rather than just a terse error string.
        let (raw_html, http_info) = match autoscrape.load_url_verbose(&url).await {
            Ok((body, status, content_type)) => {
                if !(200..300).contains(&status) {
                    warnings.push(format!(
                        "HTTP {status} from the target — non-success responses often explain empty results."
                    ));
                }
                if let Some(ct) = content_type.as_deref() {
                    if !ct.to_ascii_lowercase().starts_with("text/html")
                        && !ct.to_ascii_lowercase().contains("xml")
                        && !ct.to_ascii_lowercase().contains("json")
                    {
                        warnings.push(format!(
                            "Content-Type '{ct}' is not text/html — regex might be matching the wrong representation."
                        ));
                    }
                }
                let info = serde_json::json!({
                    "status": status,
                    "content_type": content_type,
                    "body_length": body.len(),
                });
                (body, info)
            }
            Err(e) => {
                let info = serde_json::json!({ "error": e });
                warnings.push(format!("Fetch failed: {e}"));
                return Ok(TestFetchResult {
                    url,
                    html: String::new(),
                    results: vec![],
                    diagnostics: serde_json::json!({
                        "options":  options_json,
                        "http":     info,
                        "regex":    serde_json::json!({}),
                        "warnings": warnings,
                    }),
                });
            }
        };

        let raw_len = raw_html.len();
        let html = if autoscrape.simple_space {
            RE_SIMPLE_SPACE.replace_all(&raw_html, " ").to_string()
        } else {
            raw_html
        };

        // Analyse the scraper pass so we can report per-regex match counts,
        // which is the single most useful signal when the user's regex
        // compiles but returns zero rows.
        let analysis = autoscrape.scraper.analyze_html_page(&html, &autoscrape);
        let total_entries = analysis.entries.len();

        // Translate post-pass observations into human-readable warnings.
        if let Some(0) = analysis.block_match_count {
            warnings.push(
                "Block regex matched zero blocks in the fetched HTML — entry regex is never consulted."
                    .to_string(),
            );
        } else if let Some(n) = analysis.block_match_count {
            if n > 0 && total_entries == 0 {
                warnings.push(format!(
                    "Block regex matched {n} block(s), but no entry regex matched inside any of them."
                ));
            }
        }
        if analysis.regex_block_source.is_none() && total_entries == 0 && !html.is_empty() {
            warnings.push(
                "Entry regex compiled but matched nothing in the fetched HTML."
                    .to_string(),
            );
        }

        let regex_diag = serde_json::json!({
            "block": analysis.regex_block_source.as_ref().map(|src| serde_json::json!({
                "source":      src,
                "match_count": analysis.block_match_count,
            })),
            "entries": analysis.regex_entry_sources.iter().enumerate().map(|(i, src)| {
                serde_json::json!({
                    "source":      src,
                    "match_count": analysis.regex_entry_match_counts.get(i).copied().unwrap_or(0),
                })
            }).collect::<Vec<_>>(),
            "entry_total": total_entries,
        });

        let diagnostics = serde_json::json!({
            "options":                          options_json,
            "http":                             http_info,
            "html_length_before_compression":   raw_len,
            "html_length_after_compression":    html.len(),
            "regex":                            regex_diag,
            "warnings":                         warnings,
        });

        let results: Vec<Value> = analysis
            .entries
            .into_iter()
            .map(|ex| {
                let e = &ex.entry;
                serde_json::json!({
                    "id": e.ext_id,
                    "name": e.ext_name,
                    "desc": e.ext_desc,
                    "url": e.ext_url,
                    "type": e.type_name.clone().unwrap_or_default(),
                })
            })
            .collect();

        Ok(TestFetchResult { url, html, results, diagnostics })
    }

    /// Surface regex features Rust's `regex` crate doesn't support. The
    /// scraper wizard's browser-side preview uses JavaScript's richer
    /// regex engine, so a pattern that works there but silently matches
    /// nothing here is almost always one of these.
    fn collect_regex_feature_warnings(json: &Value, warnings: &mut Vec<String>) {
        let scraper = match json.get("scraper") {
            Some(v) => v,
            None => return,
        };
        let mut sources: Vec<String> = vec![];
        if let Some(s) = scraper.get("rx_block").and_then(|v| v.as_str()) {
            if !s.is_empty() {
                sources.push(s.to_string());
            }
        }
        match scraper.get("rx_entry") {
            Some(v) if v.is_string() => {
                if let Some(s) = v.as_str() {
                    if !s.is_empty() {
                        sources.push(s.to_string());
                    }
                }
            }
            Some(v) if v.is_array() => {
                for item in v.as_array().unwrap_or(&vec![]) {
                    if let Some(s) = item.as_str() {
                        if !s.is_empty() {
                            sources.push(s.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
        for src in sources {
            if src.contains("(?=")
                || src.contains("(?!")
                || src.contains("(?<=")
                || src.contains("(?<!")
            {
                warnings.push(
                    "One of your regexes uses a lookbehind/lookahead — Rust's regex engine doesn't support those. The server-side test won't match even if your browser does."
                        .into(),
                );
                break;
            }
        }
    }

    fn initialize_with_options(json: Value, ret: &mut Autoscrape) -> Result<()> {
        if let Some(options) = json.get("options") {
            // Options in main JSON
            ret.options_from_json(options);
        } else if let Some(scraper) = json.get("scraper") {
            // Options in scraper
            if let Some(options) = scraper.get("options") {
                ret.options_from_json(options);
            }
        }
        if let Some(levels) = json.get("levels") {
            for level in levels.as_array().unwrap_or(&vec![]).iter() {
                ret.levels.push(AutoscrapeLevel::from_json(level)?);
            }
        }
        Ok(())
    }
}

/// Interpret a JSON value as an on/off flag. Accepts bool, number
/// (non-zero = on), and string ("1"/"true"/"yes"/"on" case-insensitive).
/// Missing / null / anything else = off.
fn json_flag(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().map(|x| x != 0).unwrap_or(false)
            || n.as_f64().map(|x| x != 0.0).unwrap_or(false),
        Some(Value::String(s)) => {
            matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{app_state::get_test_app, autoscrape_levels::AutoscrapeRange};

    const TEST_CATALOG_ID: usize = 91; //5526 ;
    const _TEST_ENTRY_ID: usize = 143962196;
    const _TEST_ITEM_ID: usize = 13520818; // Q13520818

    #[test]
    fn json_flag_accepts_bool_number_string() {
        use serde_json::json;
        // Falsy / missing
        assert!(!json_flag(None));
        assert!(!json_flag(Some(&json!(false))));
        assert!(!json_flag(Some(&json!(0))));
        assert!(!json_flag(Some(&json!("0"))));
        assert!(!json_flag(Some(&json!("false"))));
        assert!(!json_flag(Some(&json!(""))));
        assert!(!json_flag(Some(&json!(null))));
        // Truthy
        assert!(json_flag(Some(&json!(true))));
        assert!(json_flag(Some(&json!(1))));
        assert!(json_flag(Some(&json!(42))));
        assert!(json_flag(Some(&json!("1"))));
        assert!(json_flag(Some(&json!("true"))));
        assert!(json_flag(Some(&json!("TRUE"))));
        assert!(json_flag(Some(&json!("yes"))));
        assert!(json_flag(Some(&json!("on"))));
    }

    #[test]
    fn test_fix_regex() {
        let s = r#"<input type=\"checkbox\" name=\"genre\" id=\"(|sub)genreid\\:D[+]+([\\d]+)\" aria-label=\"Filter by (genre|style): (.+?)\" value=\"(.+?)\">"#;
        let s = AutoscrapeRange::fix_regex(s); // impl of JsonStuff
        let _r = AutoscrapeRegex::new(&s).expect("fix regex fail");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_autoscrape() {
        let mnm = get_test_app();
        let mut autoscrape = Autoscrape::new(TEST_CATALOG_ID, &mnm).await.unwrap();
        let mut cnt: usize = 1;
        autoscrape.init().await;
        while !autoscrape.tick().await {
            cnt += 1;
        }
        assert_eq!(cnt, 319);
    }
}
