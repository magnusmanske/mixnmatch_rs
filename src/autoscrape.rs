use crate::app_state::AppContext;
use crate::autoscrape_levels::{AutoscrapeLevel, sized_prefix_index};
use crate::autoscrape_resolve::RE_SIMPLE_SPACE;
use crate::autoscrape_scraper::AutoscrapeScraper;
use crate::catalog::Catalog;
use crate::extended_entry::ExtendedEntry;
use crate::job::{Job, Jobbable};
use crate::job_progress::{JobProgress, merge_progress_into_json};
use anyhow::Result;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

// Re-exports of the two-engine regex facade. The wrapper compiles every
// pattern with the linear-time `regex` crate first and on a parse error
// falls back to backtracking `fancy_regex` (lookaround, literal `{`,
// etc.). See `autoscrape_regex.rs`.
pub use crate::autoscrape_regex::{
    AutoscrapeCaptures, AutoscrapeRegex, AutoscrapeRegexBuilder, AutoscrapeRegexError,
};

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
const AUTOSCRAPE_MAX_RETRIES: u32 = 3;
const AUTOSCRAPE_DEFAULT_RETRY_AFTER_SECS: u64 = 5;
const AUTOSCRAPE_MAX_RETRY_AFTER_SECS: u64 = 300;
const AUTOSCRAPE_INCREASE_AFTER_SUCCESSES: usize = 50;
const AUTOSCRAPE_DEFAULT_MAX_CONCURRENCY: usize = 8;
const AUTOSCRAPE_DEFAULT_MIN_CONCURRENCY: usize = 1;

/// AIMD concurrency controller for the autoscrape page-fetch loop.
///
/// Starts at `max` concurrent requests. On rate-limiting (HTTP 429/503)
/// halves the limit (floor: `min`). After every `AUTOSCRAPE_INCREASE_AFTER_SUCCESSES`
/// consecutive successful fetches, increments by one (ceiling: `max`).
#[derive(Debug)]
struct ConcurrencyController {
    current: usize,
    min: usize,
    max: usize,
    successes_since_last_increase: usize,
}

impl ConcurrencyController {
    fn new(max: usize, min: usize) -> Self {
        let max = max.max(min).max(1);
        let min = min.max(1);
        Self {
            current: max,
            min,
            max,
            successes_since_last_increase: 0,
        }
    }

    fn current(&self) -> usize {
        self.current
    }

    fn on_rate_limit(&mut self) {
        self.current = (self.current / 2).max(self.min);
        self.successes_since_last_increase = 0;
    }

    fn on_success(&mut self, count: usize) {
        self.successes_since_last_increase += count;
        while self.successes_since_last_increase >= AUTOSCRAPE_INCREASE_AFTER_SUCCESSES
            && self.current < self.max
        {
            self.current += 1;
            self.successes_since_last_increase -= AUTOSCRAPE_INCREASE_AFTER_SUCCESSES;
        }
        if self.current >= self.max {
            self.successes_since_last_increase = 0;
        }
    }
}

/// Outcome of a single HTTP page fetch.
#[derive(Debug)]
enum FetchOutcome {
    /// 2xx — body text ready for scraping.
    Success { body: String },
    /// 429 or 503 — server asked us to slow down.
    RateLimit { retry_after_secs: u64 },
    /// Transient failure (5xx, timeout, network error) — worth retrying.
    TransientError,
    /// Permanent client error (4xx other than 429) — do not retry.
    PermanentError,
}

#[derive(Debug, Clone)]
pub enum AutoscrapeError {
    NoAutoscrapeForCatalog(usize),
    UnknownLevelType(String),
    BadType(Value),
    MediawikiFailure(String),
    /// A `resolve` rx pattern failed to compile under both regex engines.
    /// Carries the underlying compile error so the job's `note` field
    /// shows the catalog author what's wrong, rather than the opaque
    /// resolve sub-JSON we used to dump.
    RegexCompile(AutoscrapeRegexError),
}

impl Error for AutoscrapeError {}

impl fmt::Display for AutoscrapeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AutoscrapeError::UnknownLevelType(s) => write!(f, "{s}"), // user-facing output
            AutoscrapeError::BadType(v) => write!(f, "{v}"),
            AutoscrapeError::MediawikiFailure(v) => write!(f, "{v}"),
            AutoscrapeError::NoAutoscrapeForCatalog(catalog_id) => {
                write!(f, "No Autoscraper for catalog {catalog_id}")
            }
            AutoscrapeError::RegexCompile(e) => write!(f, "{e}"),
        }
    }
}

impl From<AutoscrapeRegexError> for AutoscrapeError {
    fn from(e: AutoscrapeRegexError) -> Self {
        AutoscrapeError::RegexCompile(e)
    }
}

pub trait JsonStuff {
    fn json_as_str(json: &Value, key: &str) -> Result<String, AutoscrapeError> {
        Ok(json
            .get(key)
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .as_str()
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?
            .to_string())
    }

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
    // None only for the test_fetch path, which never calls methods that need app.
    app: Option<Arc<dyn AppContext>>,
    job: Option<Job>,
    urls_loaded: usize,
    entry_batch: Vec<ExtendedEntry>,
}

impl Jobbable for Autoscrape {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }

    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl Autoscrape {
    pub async fn new(catalog_id: usize, app: Arc<dyn AppContext>) -> Result<Self> {
        let results = app.storage().autoscrape_get_for_catalog(catalog_id).await?;
        let (id, json) = results
            .first()
            .ok_or(AutoscrapeError::NoAutoscrapeForCatalog(catalog_id))?;
        let json: Value = serde_json::from_str(json)?;
        let mut ret = Self::new_basic(id, catalog_id, Some(app), &json)?;
        Self::initialize_with_options(json, &mut ret)?;
        Ok(ret)
    }

    pub const fn catalog_id(&self) -> usize {
        self.catalog_id
    }

    fn app_ref(&self) -> &dyn AppContext {
        self.app
            .as_ref()
            .expect("Autoscrape: app accessed in context where it was not set (test_fetch path)")
            .as_ref()
    }

    pub fn levels(&self) -> &[AutoscrapeLevel] {
        &self.levels
    }

    fn options_from_json(&mut self, json: &Value) {
        // Accept bool, number, or string for each flag. The scraper wizard
        // sends JS booleans on test (via generateJSON) but stores numbers
        // on save — the two paths used to disagree, silently forcing every
        // option off during tests. Treat anything truthy/non-zero as on.
        self.simple_space = json_flag(json.get("simple_space"));
        self.skip_failed = json_flag(json.get("skip_failed"));
        self.utf8_encode = json_flag(json.get("utf8_encode"));
    }

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
    pub fn current(&self) -> Vec<String> {
        self.levels.iter().map(|level| level.current()).collect()
    }

    /// Verbose counterpart to the internal page fetcher, used by the scraper-test UI.
    /// Preserves the HTTP status and content-type alongside the body, and
    /// surfaces the underlying reqwest error message on failure instead of
    /// collapsing everything to `None`. The runner itself still calls the
    /// quiet `load_url`; we only pay for this on one-shot tests.
    async fn load_url_verbose(
        &mut self,
        url: &str,
    ) -> Result<(String, u16, Option<String>), String> {
        self.urls_loaded += 1;
        let client =
            Self::reqwest_client_external().map_err(|e| format!("HTTP client setup: {e}"))?;
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
        let app =
            Arc::clone(self.app.as_ref().expect(
                "Autoscrape: app accessed in context where it was not set (test_fetch path)",
            ));
        let existing_ext_ids = app
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
                    let _ = ex.insert_new(app.as_ref()).await;
                }
            }
        }
        self.entry_batch.clear();
        let _ = self.remember_state().await;
        Ok(())
    }

    pub async fn remember_state(&mut self) -> Result<()> {
        // Persist level state under `levels` (new object shape) so the
        // typed progress payload can sit alongside without overwriting it.
        // `start()` keeps a backward-compatible reader for the legacy bare
        // array shape that pre-progress autoscrape runs wrote.
        let levels: Vec<Value> = self
            .levels
            .iter()
            .map(|level| level.level_type().get_state())
            .collect();
        let mut state = json!({ "levels": levels });

        // Coarse percentage over the sized prefix of levels (Keys/Range);
        // None when the outermost level is unsized (Follow/MediaWiki),
        // in which case we still publish a `urls_loaded` counter via
        // `processed` so the UI has something to render.
        let (processed, total) = match sized_prefix_index(&self.levels) {
            Some((flat, total)) => (flat, Some(total)),
            None => (self.urls_loaded as u64, None),
        };
        let progress = JobProgress::from_counts(processed, total);
        state = merge_progress_into_json(Some(&state), &progress);

        self.remember_job_data(&state).await?;
        Ok(())
    }

    fn apply_text_transforms(&self, body: String) -> String {
        if self.simple_space {
            RE_SIMPLE_SPACE.replace_all(&body, " ").to_string()
        } else {
            body
        }
        // utf8_encode: TODO
    }

    pub async fn run(&mut self) -> Result<()> {
        self.init().await;
        let _ = self.start().await;
        let client = Self::reqwest_client_external()?;
        let max_concurrent = *self
            .app_ref()
            .task_specific_usize()
            .get("autoscrape_max_concurrency")
            .unwrap_or(&AUTOSCRAPE_DEFAULT_MAX_CONCURRENCY);
        let min_concurrent = *self
            .app_ref()
            .task_specific_usize()
            .get("autoscrape_min_concurrency")
            .unwrap_or(&AUTOSCRAPE_DEFAULT_MIN_CONCURRENCY);
        let mut ctrl = ConcurrencyController::new(max_concurrent, min_concurrent);

        loop {
            // Pre-generate a window of URLs from the state machine.
            let window_size = ctrl.current();
            let mut url_window: Vec<String> = Vec::with_capacity(window_size);
            let mut exhausted = false;
            for _ in 0..window_size {
                url_window.push(self.get_current_url().await);
                if self.tick().await {
                    exhausted = true;
                    break;
                }
            }

            // Fetch the window concurrently; rate-limited URLs get one retry
            // after the server's requested delay.
            let (bodies, had_rate_limit) =
                fetch_and_retry_window(&client, &url_window, ctrl.current()).await;

            // Process successful responses.
            let success_count = bodies.len();
            for body in bodies {
                let html = self.apply_text_transforms(body);
                let mut entries = self.scraper.process_html_page(&html, self);
                self.entry_batch.append(&mut entries);
                if self.entry_batch.len() >= AUTOSCRAPE_ENTRY_BATCH_SIZE {
                    let _ = self.add_batch().await;
                }
            }
            self.urls_loaded += url_window.len();

            // Adapt concurrency based on this window's outcomes.
            if had_rate_limit {
                ctrl.on_rate_limit();
            } else {
                ctrl.on_success(success_count);
            }

            // Checkpoint after every window so crash-resume skips already-done work.
            let _ = self.remember_state().await;

            if exhausted {
                break;
            }
        }
        let _ = self.finish().await;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let autoscrape_id = self.autoscrape_id;
        self.app_ref()
            .storage()
            .autoscrape_start(autoscrape_id)
            .await?;
        if let Some(json) = self.get_last_job_data().await {
            // Accept both shapes for backward compatibility:
            // - new: object with `levels` array (+ optional `progress`);
            // - legacy: bare array of level states.
            let levels_value = json
                .as_object()
                .and_then(|o| o.get("levels"))
                .cloned()
                .or_else(|| {
                    if json.is_array() {
                        Some(json.clone())
                    } else {
                        None
                    }
                });
            if let Some(Value::Array(arr)) = levels_value {
                if arr.len() == self.levels.len() {
                    arr.iter()
                        .enumerate()
                        .for_each(|(num, j)| self.levels[num].level_type_mut().set_state(j));
                }
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        let _ = self.add_batch().await; // Flush
        let autoscrape_id = self.autoscrape_id;
        let last_run_urls = self.urls_loaded;
        self.app_ref()
            .storage()
            .autoscrape_finish(autoscrape_id, last_run_urls)
            .await?;
        let mut catalog = Catalog::from_id(self.catalog_id, self.app_ref()).await?;
        let _ = catalog.refresh_overview_table(self.app_ref()).await;
        let _ = catalog.check_and_set_person_date(self.app_ref()).await;
        let _ = self.clear_offset().await;
        let _ = Job::queue_simple_job(self.app_ref(), self.catalog_id, "automatch_by_search", None)
            .await;
        let _ = catalog.queue_microsync_if_applicable(self.app_ref()).await;
        Ok(())
    }

    pub fn reqwest_client_external() -> Result<reqwest::Client> {
        // SSRF guard: the resolver drops loopback / private / link-local /
        // reserved addresses before reqwest opens a connection. The filter
        // applies on every connect, so redirects to a private IP are caught
        // too (reqwest re-resolves on each hop). Audit reference: H-2 in
        // `audits/comprehensive_security_report.md`.
        Ok(reqwest::Client::builder()
            .user_agent(AUTOSCRAPER_USER_AGENT)
            .timeout(core::time::Duration::from_secs(
                AUTOSCRAPE_URL_LOAD_TIMEOUT_SEC,
            ))
            .connect_timeout(core::time::Duration::from_secs(5))
            .connection_verbose(true)
            .gzip(true)
            .deflate(true)
            .brotli(true)
            .dns_resolver(std::sync::Arc::new(crate::util::ssrf::PublicOnlyResolver))
            .build()?)
    }

    fn new_basic(
        id: &usize,
        catalog_id: usize,
        app: Option<Arc<dyn AppContext>>,
        json: &Value,
    ) -> Result<Autoscrape> {
        let ret = Self {
            autoscrape_id: *id,
            catalog_id,
            app,
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
    pub async fn test_fetch(json: &Value) -> Result<TestFetchResult> {
        // Compile errors (malformed regex, bad scraper JSON) still bubble
        // up as hard errors — the UI will surface them as the test status.
        let mut autoscrape = Self::new_basic(&0, 0, None, json)?;
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
            warnings
                .push("Entry regex compiled but matched nothing in the fetched HTML.".to_string());
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

        Ok(TestFetchResult {
            url,
            html,
            results,
            diagnostics,
        })
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

/// Performs one HTTP GET, classifying the response into a [`FetchOutcome`].
async fn fetch_url_once(client: &reqwest::Client, url: &str) -> FetchOutcome {
    let resp = match client.get(url).send().await {
        Err(_) => return FetchOutcome::TransientError,
        Ok(r) => r,
    };
    let status = resp.status().as_u16();
    match status {
        200..=299 => match resp.text().await {
            Ok(body) => FetchOutcome::Success { body },
            Err(_) => FetchOutcome::TransientError,
        },
        429 | 503 => {
            let retry_after_secs = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(AUTOSCRAPE_DEFAULT_RETRY_AFTER_SECS)
                .min(AUTOSCRAPE_MAX_RETRY_AFTER_SECS);
            FetchOutcome::RateLimit { retry_after_secs }
        }
        500..=599 => FetchOutcome::TransientError,
        _ => FetchOutcome::PermanentError,
    }
}

/// Fetches a URL, retrying [`FetchOutcome::TransientError`] up to
/// [`AUTOSCRAPE_MAX_RETRIES`] times with exponential back-off.
/// [`FetchOutcome::RateLimit`] is returned immediately so the window-level
/// caller can decide to sleep and retry.
async fn fetch_url_with_backoff(client: &reqwest::Client, url: &str) -> FetchOutcome {
    let mut backoff_secs = 2_u64;
    for attempt in 0..AUTOSCRAPE_MAX_RETRIES {
        let outcome = fetch_url_once(client, url).await;
        if let FetchOutcome::TransientError = outcome {
            if attempt + 1 < AUTOSCRAPE_MAX_RETRIES {
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs *= 2;
                continue;
            }
        }
        return outcome;
    }
    FetchOutcome::TransientError
}

/// Fires `urls` concurrently (up to `concurrent` in-flight at once) using
/// [`fetch_url_with_backoff`] for each URL.
async fn fetch_window(
    client: reqwest::Client,
    urls: Vec<String>,
    concurrent: usize,
) -> Vec<(String, FetchOutcome)> {
    futures::stream::iter(urls.into_iter().map(move |url| {
        let c = client.clone();
        async move {
            let outcome = fetch_url_with_backoff(&c, &url).await;
            (url, outcome)
        }
    }))
    .buffer_unordered(concurrent)
    .collect::<Vec<_>>()
    .await
}

/// Fetches a window of URLs concurrently. Rate-limited URLs are collected,
/// and after sleeping for the requested delay, retried sequentially (concurrency=1).
/// Returns `(successful_bodies, had_any_rate_limit)`.
async fn fetch_and_retry_window(
    client: &reqwest::Client,
    urls: &[String],
    concurrent: usize,
) -> (Vec<String>, bool) {
    if urls.is_empty() {
        return (vec![], false);
    }
    let first_pass = fetch_window(client.clone(), urls.to_vec(), concurrent).await;

    let mut bodies: Vec<String> = Vec::with_capacity(urls.len());
    let mut rate_limited_urls: Vec<String> = vec![];
    let mut max_retry_after = AUTOSCRAPE_DEFAULT_RETRY_AFTER_SECS;

    for (url, outcome) in first_pass {
        match outcome {
            FetchOutcome::Success { body } => bodies.push(body),
            FetchOutcome::RateLimit { retry_after_secs } => {
                rate_limited_urls.push(url);
                max_retry_after = max_retry_after.max(retry_after_secs);
            }
            FetchOutcome::TransientError | FetchOutcome::PermanentError => {}
        }
    }

    let had_rate_limit = !rate_limited_urls.is_empty();
    if had_rate_limit {
        tokio::time::sleep(std::time::Duration::from_secs(max_retry_after)).await;
        let retry_pass = fetch_window(client.clone(), rate_limited_urls, 1).await;
        for (_, outcome) in retry_pass {
            if let FetchOutcome::Success { body } = outcome {
                bodies.push(body);
            }
        }
    }

    (bodies, had_rate_limit)
}

/// Interpret a JSON value as an on/off flag. Accepts bool, number
/// (non-zero = on), and string ("1"/"true"/"yes"/"on" case-insensitive).
/// Missing / null / anything else = off.
fn json_flag(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => {
            n.as_i64().map(|x| x != 0).unwrap_or(false)
                || n.as_f64().map(|x| x != 0.0).unwrap_or(false)
        }
        Some(Value::String(s)) => {
            matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autoscrape_levels::AutoscrapeRange;

    // ── ConcurrencyController ─────────────────────────────────────────────

    #[test]
    fn concurrency_starts_at_max() {
        let ctrl = ConcurrencyController::new(8, 1);
        assert_eq!(ctrl.current(), 8);
    }

    #[test]
    fn concurrency_rate_limit_halves() {
        let mut ctrl = ConcurrencyController::new(8, 1);
        ctrl.on_rate_limit();
        assert_eq!(ctrl.current(), 4);
    }

    #[test]
    fn concurrency_rate_limit_respects_min_floor() {
        let mut ctrl = ConcurrencyController::new(8, 1);
        ctrl.on_rate_limit(); // 4
        ctrl.on_rate_limit(); // 2
        ctrl.on_rate_limit(); // 1
        ctrl.on_rate_limit(); // still 1
        assert_eq!(ctrl.current(), 1);
    }

    #[test]
    fn concurrency_on_success_increases_after_threshold() {
        let mut ctrl = ConcurrencyController::new(8, 1);
        ctrl.on_rate_limit(); // → 4
        ctrl.on_success(49);
        assert_eq!(ctrl.current(), 4, "not yet at threshold");
        ctrl.on_success(1); // 50th
        assert_eq!(ctrl.current(), 5);
    }

    #[test]
    fn concurrency_on_success_does_not_exceed_max() {
        let mut ctrl = ConcurrencyController::new(4, 1);
        ctrl.on_success(200); // many successes
        assert_eq!(ctrl.current(), 4, "must stay at max");
    }

    #[test]
    fn concurrency_rate_limit_resets_success_counter() {
        let mut ctrl = ConcurrencyController::new(8, 1);
        ctrl.on_rate_limit(); // → 4
        ctrl.on_success(45); // progress toward +1, but not there
        ctrl.on_rate_limit(); // → 2, counter resets
        ctrl.on_success(50); // starts fresh from 0 → +1 → 3
        assert_eq!(ctrl.current(), 3);
    }

    #[test]
    fn concurrency_bulk_success_can_add_multiple() {
        let mut ctrl = ConcurrencyController::new(8, 1);
        ctrl.on_rate_limit(); // → 4
        ctrl.on_success(100); // 2 × 50 → +2 → 6
        assert_eq!(ctrl.current(), 6);
    }

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
}
