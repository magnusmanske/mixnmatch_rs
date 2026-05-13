use crate::auth::config::OauthConfig;
use crate::mysql_misc::MySQLMisc;
use crate::storage::Storage;
use crate::storage_mysql::StorageMySQL;
use crate::wdrc::WDRC;
use crate::wikibase::WikiBase;
use crate::wikidata::Wikidata;
use anyhow::{Result, anyhow};
use std::sync::LazyLock;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time;

pub const MICRO_API_PORT: u16 = 8089;

/// Global function for tests.
/// # Panics
/// Used for testing only, panics if the config file is not found.
pub fn get_test_app() -> AppState {
    let ret = AppState::from_config_file("config.json").expect("Cannot create test MnM");
    *TESTING.lock().unwrap() = true;
    ret
}

pub const Q_NA: isize = 0;
pub const Q_NOWD: isize = -1;
pub const USER_AUTO: usize = 0;
pub const USER_DATE_MATCH: usize = 3;
pub const USER_AUX_MATCH: usize = 4;
pub const USER_LOCATION_MATCH: usize = 5;

pub static TESTING: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(false));
// To lock the test entry in the database
pub static TEST_MUTEX: LazyLock<Mutex<bool>> = LazyLock::new(|| Mutex::new(true));
// To lock the test entry in the database
static RE_ITEM2NUMERIC: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(-{0,1}\d+)").expect("Regex failure"));

#[derive(Debug, Clone)]
pub struct AppState {
    wikidata: Wikidata,
    wdt: Wikidata, // To access Wikidata terms DB replica
    wdrc: Arc<WDRC>,
    storage: Arc<Box<dyn Storage>>,
    large_catalogs: Arc<crate::large_catalogs::LargeCatalogs>,
    import_file_path: Arc<String>,
    flickr_key_path: Arc<String>,
    task_specific_usize: Arc<HashMap<String, usize>>,
    max_concurrent_jobs: usize,
    toolforge_php_command: String,
    oauth_config: Option<Arc<OauthConfig>>,
    /// Optional override for the webserver's static-file directory. When
    /// set, the webserver serves files from this path *live* (no in-memory
    /// cache), so HTML/JS edits to a checked-out repo show up immediately.
    /// Unset → fall back to the CLI `--html-dir` argument with caching.
    html_dir_override: Option<Arc<PathBuf>>,
    /// One shared `reqwest::Client` for the default-config HTTP needs that
    /// don't have specialised timeout / header requirements (most bespoke
    /// scrapers, simple HTML fetches, OAuth callbacks). Specialised
    /// callers — WDQS (longer timeout), the API proxy (per-route config),
    /// `wikidata::get_mw_api` (constrained by the mediawiki crate's
    /// builder-only API) — still construct their own.
    ///
    /// `reqwest::Client` is `Clone` and internally `Arc`-shared, so passing
    /// it by clone is cheap; we wrap in `Arc` so AppState clones don't
    /// duplicate the connection pool struct itself.
    http_client: Arc<reqwest::Client>,
}

impl AppState {
    /// Create an `AppState` object from a config JSON file
    pub fn from_config_file(filename: &str) -> Result<Self> {
        let config = Self::load_config(filename)?;
        Self::from_config(&config)
    }

    /// Construct an `AppState` from already-built dependency instances.
    ///
    /// This is the injection seam for tests and embedders that want to
    /// substitute specific implementations (e.g. a fake `Storage` for a
    /// unit test, or a wiremock-backed `Wikidata` for HTTP fixtures).
    /// [`from_config`](Self::from_config) hard-wires `StorageMySQL` and
    /// `Wikidata::new`, which is correct for production but doesn't leave
    /// a seam — `from_parts` does.
    ///
    /// The six parameters are the dependencies that have no sensible
    /// default. The remaining configuration fields — paths, the
    /// `max_concurrent_jobs` cap, OAuth, etc. — take inert defaults
    /// matching what a fresh test process needs:
    /// - `import_file_path` / `flickr_key_path`: empty strings
    /// - `task_specific_usize`: empty map
    /// - `max_concurrent_jobs`: 10 (matches the `from_config` fallback)
    /// - `toolforge_php_command`: `"php8.3"`
    /// - `oauth_config` / `html_dir_override`: `None`
    ///
    /// **For production code:** prefer [`from_config`](Self::from_config) /
    /// [`from_config_file`](Self::from_config_file). They read everything
    /// from disk in one shot. **For tests:** prefer
    /// `test_support::test_app()` unless you specifically need to
    /// substitute a dependency.
    ///
    /// See `audits/code_solid.md` #2.
    pub fn from_parts(
        storage: Arc<Box<dyn Storage>>,
        wikidata: Wikidata,
        wdt: Wikidata,
        wdrc: Arc<WDRC>,
        large_catalogs: Arc<crate::large_catalogs::LargeCatalogs>,
        http_client: Arc<reqwest::Client>,
    ) -> Self {
        Self {
            wikidata,
            wdt,
            wdrc,
            storage,
            large_catalogs,
            import_file_path: Arc::new(String::new()),
            flickr_key_path: Arc::new(String::new()),
            task_specific_usize: Arc::new(HashMap::new()),
            max_concurrent_jobs: 10,
            toolforge_php_command: "php8.3".to_string(),
            oauth_config: None,
            html_dir_override: None,
            http_client,
        }
    }

    pub fn import_file_path(&self) -> &str {
        &self.import_file_path
    }

    pub fn flickr_key_path(&self) -> &str {
        &self.flickr_key_path
    }

    pub fn task_specific_usize(&self) -> &HashMap<String, usize> {
        &self.task_specific_usize
    }

    /// Shared HTTP client for default-config requests. Cheap to clone
    /// (internally Arc-shared) so callers can either borrow the
    /// reference or `.clone()` to own it.
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Creatre an `AppState` object from a config JSON object
    pub fn from_config(config: &Value) -> Result<Self> {
        let task_specific_usize = config["task_specific_usize"]
            .as_object()
            .ok_or_else(|| anyhow!("config.task_specific_usize not found, or not an object"))?
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.as_u64().unwrap_or_default() as usize))
            .collect();
        let task_specific_usize = Arc::new(task_specific_usize);
        let max_concurrent_jobs = config["max_concurrent_jobs"].as_u64().unwrap_or(10) as usize;
        let bot_name = config["bot_name"]
            .as_str()
            .ok_or_else(|| anyhow!("config.bot_name not found, or not an object"))?
            .to_string();
        let bot_password = config["bot_password"]
            .as_str()
            .ok_or_else(|| anyhow!("config.bot_password not found, or not an object"))?
            .to_string();
        let import_file_path = config["import_file_path"]
            .as_str()
            .ok_or_else(|| anyhow!("config.import_file_path not found, or not an object"))?
            .to_string();
        let import_file_path = Arc::new(import_file_path);
        // `flickr_key_path` is optional; the Flickr map source is only used
        // by one UI route, so falling back to an empty string keeps CLI
        // deployments (where this file isn't present) from erroring.
        let flickr_key_path =
            Arc::new(config["flickr_key_path"].as_str().unwrap_or("").to_string());
        let toolforge_php_command = config["toolforge_php_command"]
            .as_str()
            .unwrap_or("php8.3")
            .to_string();
        let large_catalogs =
            crate::large_catalogs::LargeCatalogs::from_config(&config["mixnmatch"])?;
        // OAuth is optional at construction: CLI jobs / bot runs don't need it.
        // The webserver entrypoint checks separately that it's present.
        let oauth_config = if config.get("oauth").is_some() {
            Some(Arc::new(OauthConfig::from_app_config(config)?))
        } else {
            None
        };
        // Optional. When set, the webserver serves static files live from
        // this path (no in-memory cache). Lets a production deployment
        // point at a checked-out repo for instant HTML/JS iteration
        // without rebuilding the deploy image.
        let html_dir_override = config["html_dir_override"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Arc::new(PathBuf::from(s)));
        // One default-config HTTP client for the whole process. 30 s
        // timeout matches the previous per-call defaults in
        // bespoke_scrapers / code_fragment HTML fetchers, which is the
        // majority of consumers.
        let http_client = reqwest::Client::builder()
            .timeout(time::Duration::from_secs(30))
            .connect_timeout(time::Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow!("failed to build shared HTTP client: {e}"))?;
        Ok(Self {
            wikidata: Wikidata::new(&config["wikidata"], bot_name.clone(), bot_password.clone()),
            wdt: Wikidata::new(&config["wdt"], bot_name, bot_password),
            wdrc: Arc::new(WDRC::new(&config["wdrc"])),
            storage: Arc::new(Box::new(StorageMySQL::new(
                &config["mixnmatch"],
                &config["mixnmatch_ro"],
            ))),
            large_catalogs: Arc::new(large_catalogs),
            import_file_path,
            flickr_key_path,
            task_specific_usize,
            max_concurrent_jobs,
            toolforge_php_command,
            oauth_config,
            html_dir_override,
            http_client: Arc::new(http_client),
        })
    }

    /// Optional override path for the webserver's static-file directory.
    /// Returns `Some` when set in config; the webserver then serves
    /// that path live (no caching). `None` → use the CLI `--html-dir`.
    pub fn html_dir_override(&self) -> Option<&Path> {
        self.html_dir_override.as_deref().map(PathBuf::as_path)
    }

    pub fn oauth_config(&self) -> Option<&Arc<OauthConfig>> {
        self.oauth_config.as_ref()
    }

    pub async fn get_wikibase_from_config(&self, config: &Value) -> Result<WikiBase> {
        WikiBase::new(&config["wikibase"])
            .await
            .ok_or(anyhow!("Could not create wikibase"))
    }

    pub fn toolforge_php_command(&self) -> &str {
        &self.toolforge_php_command
    }

    pub fn storage(&self) -> &Arc<Box<dyn Storage>> {
        &self.storage
    }

    pub fn large_catalogs(&self) -> &crate::large_catalogs::LargeCatalogs {
        &self.large_catalogs
    }

    pub const fn wikidata(&self) -> &Wikidata {
        &self.wikidata
    }

    pub const fn wdt(&self) -> &Wikidata {
        &self.wdt
    }

    pub fn wikidata_mut(&mut self) -> &mut Wikidata {
        &mut self.wikidata
    }

    pub fn wdrc(&self) -> &WDRC {
        &self.wdrc
    }

    /// Configured cap on concurrent in-flight jobs in the forever-loop.
    /// Read from `config.max_concurrent_jobs`, default 10. Consumed by
    /// [`crate::job_runner::JobRunner`].
    pub const fn max_concurrent_jobs(&self) -> usize {
        self.max_concurrent_jobs
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.wikidata.disconnect_db().await?;
        self.storage.disconnect().await?;
        Ok(())
    }

    pub fn load_config(filename: &str) -> Result<Value> {
        let mut path = env::current_dir()?;
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(config)
    }
}

/// Wikidata access surface: the Wikidata API client, the terms-replica
/// client (`wdt`), and the WDRC change-tracking handle. Consumers that
/// only query Wikidata (no storage, no HTTP, no config) can bound on
/// this trait alone.
pub trait WikidataContext: std::fmt::Debug + Send + Sync {
    fn wikidata(&self) -> &Wikidata;
    fn wdt(&self) -> &Wikidata;
    fn wdrc(&self) -> &WDRC;
}

impl WikidataContext for AppState {
    fn wikidata(&self) -> &Wikidata {
        AppState::wikidata(self)
    }
    fn wdt(&self) -> &Wikidata {
        AppState::wdt(self)
    }
    fn wdrc(&self) -> &WDRC {
        AppState::wdrc(self)
    }
}

/// External-services surface: the database storage handle and the shared
/// HTTP client. Most background-job code only needs these two.
pub trait ExternalServicesContext: std::fmt::Debug + Send + Sync {
    fn storage(&self) -> &Arc<Box<dyn Storage>>;
    fn http_client(&self) -> &reqwest::Client;
}

impl ExternalServicesContext for AppState {
    fn storage(&self) -> &Arc<Box<dyn Storage>> {
        AppState::storage(self)
    }
    fn http_client(&self) -> &reqwest::Client {
        AppState::http_client(self)
    }
}

/// Static runtime configuration: paths, flags, feature toggles, and
/// rarely-changing process-level settings. API handlers and the OAuth
/// flow bind on this; background jobs rarely need it.
pub trait RuntimeConfig: std::fmt::Debug + Send + Sync {
    fn task_specific_usize(&self) -> &HashMap<String, usize>;
    fn import_file_path(&self) -> &str;
    fn flickr_key_path(&self) -> &str;
    fn toolforge_php_command(&self) -> &str;
    fn html_dir_override(&self) -> Option<&Path>;
    fn oauth_config(&self) -> Option<&Arc<OauthConfig>>;
    fn large_catalogs(&self) -> &crate::large_catalogs::LargeCatalogs;
}

impl RuntimeConfig for AppState {
    fn task_specific_usize(&self) -> &HashMap<String, usize> {
        AppState::task_specific_usize(self)
    }
    fn import_file_path(&self) -> &str {
        AppState::import_file_path(self)
    }
    fn flickr_key_path(&self) -> &str {
        AppState::flickr_key_path(self)
    }
    fn toolforge_php_command(&self) -> &str {
        AppState::toolforge_php_command(self)
    }
    fn html_dir_override(&self) -> Option<&Path> {
        AppState::html_dir_override(self)
    }
    fn oauth_config(&self) -> Option<&Arc<OauthConfig>> {
        AppState::oauth_config(self)
    }
    fn large_catalogs(&self) -> &crate::large_catalogs::LargeCatalogs {
        AppState::large_catalogs(self)
    }
}

/// Full read-only view of the application's runtime services. Composed
/// of the three sub-traits above; a leaf type that needs everything can
/// bound on `AppContext`, while one that only queries Wikidata can bound
/// on `WikidataContext` alone. `wikidata_mut` is intentionally absent:
/// the only caller (Wikidata edit pipeline) needs the concrete type.
///
/// New code should prefer `&impl <SubTrait>` (smallest set) or
/// `&impl AppContext` (full set) over `&AppState` at boundaries.
/// Existing call sites keep `&AppState`; since `AppState` satisfies
/// all three sub-traits it automatically satisfies `AppContext` too.
pub trait AppContext:
    WikidataContext + ExternalServicesContext + RuntimeConfig + std::fmt::Debug + Send + Sync
{
}

impl AppContext for AppState {}

/// Converts a string like "Q12345" to the numeric 12345.
pub fn item2numeric(q: &str) -> Option<isize> {
    RE_ITEM2NUMERIC
        .captures_iter(q)
        .next()
        .and_then(|cap| cap[1].parse::<isize>().ok())
}

pub fn tool_root_dir() -> String {
    std::env::var("TOOL_DATA_DIR").unwrap_or("/data/project/mix-n-match".to_string())
}

pub fn is_on_toolforge() -> bool {
    std::path::Path::new("/etc/wmcs-project").exists()
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time + minimal-runtime check that `AppState` correctly
    /// satisfies all three sub-traits and the composed `AppContext`.
    /// Also verifies that `dyn SubTrait` vtables are usable at
    /// heap-erased (dynamic dispatch) boundaries.
    #[tokio::test]
    async fn app_state_satisfies_app_context() {
        fn takes_context<C: AppContext>(c: &C) -> usize {
            Arc::strong_count(c.storage())
        }
        let app = crate::test_support::test_app().await;
        let _ = takes_context(&app);

        let app2 = crate::test_support::test_app().await;
        let dyn_ext: &dyn ExternalServicesContext = &app2;
        let _ = Arc::strong_count(dyn_ext.storage());

        let app3 = crate::test_support::test_app().await;
        let dyn_cfg: &dyn RuntimeConfig = &app3;
        assert_eq!(dyn_cfg.toolforge_php_command(), "php8.3");

        let app4 = crate::test_support::test_app().await;
        let _dyn_ctx: &dyn AppContext = &app4;
    }

    /// Smoke test for the `from_parts` injection seam (audits/code_solid.md
    /// #2). Builds an AppState by handing real dependency instances to
    /// `from_parts` rather than letting `from_config` allocate them, and
    /// verifies:
    /// - the resulting struct satisfies all three sub-traits + AppContext;
    /// - storage is accessible through the trait;
    /// - the non-injected fields received their documented defaults
    ///   (the audit's whole point: ensure the seam doesn't silently
    ///   produce an unusable AppState in the absence of explicit values).
    ///
    /// Construction goes through `test_support::test_app()` to acquire
    /// real handles for the testcontainer-backed dependencies — once a
    /// future `MockStorage` exists, a tighter variant of this test will
    /// stop touching Docker entirely. Today the test still spins the
    /// container but exercises the seam itself.
    #[tokio::test]
    async fn from_parts_yields_app_context_with_defaults() {
        let source = crate::test_support::test_app().await;
        let part_app = AppState::from_parts(
            Arc::clone(&source.storage),
            source.wikidata.clone(),
            source.wdt.clone(),
            Arc::clone(&source.wdrc),
            Arc::clone(&source.large_catalogs),
            Arc::clone(&source.http_client),
        );

        // (a) The struct produced by from_parts satisfies AppContext.
        fn takes_context<C: AppContext>(c: &C) -> usize {
            Arc::strong_count(c.storage())
        }
        let _ = takes_context(&part_app);

        // (b) The three sub-traits each work through dynamic dispatch.
        let dyn_ext: &dyn ExternalServicesContext = &part_app;
        let _ = Arc::strong_count(dyn_ext.storage());
        let dyn_cfg: &dyn RuntimeConfig = &part_app;
        let dyn_wd: &dyn WikidataContext = &part_app;
        let _ = dyn_wd.wikidata();

        // (c) Defaults applied for non-injected fields. Any future change
        // to the from_parts defaults must update this test deliberately,
        // not silently — that's what the assertions are pinning.
        assert_eq!(dyn_cfg.toolforge_php_command(), "php8.3");
        assert_eq!(part_app.max_concurrent_jobs(), 10);
        assert!(dyn_cfg.import_file_path().is_empty());
        assert!(dyn_cfg.flickr_key_path().is_empty());
        assert!(dyn_cfg.task_specific_usize().is_empty());
        assert!(dyn_cfg.oauth_config().is_none());
        assert!(dyn_cfg.html_dir_override().is_none());
    }

    #[test]
    fn test_item2numeric() {
        assert_eq!(item2numeric("foobar"), None);
        assert_eq!(item2numeric("12345"), Some(12345));
        assert_eq!(item2numeric("Q12345"), Some(12345));
        assert_eq!(item2numeric("Q12345X"), Some(12345));
        assert_eq!(item2numeric("Q12345X6"), Some(12345));
    }

    #[test]
    fn test_item2numeric_edge_cases() {
        assert_eq!(item2numeric(""), None);
        assert_eq!(item2numeric("Q"), None);
        assert_eq!(item2numeric("Q-5"), Some(-5));
        assert_eq!(item2numeric("Q0"), Some(0));
        assert_eq!(item2numeric("Q999999999"), Some(999999999));
        assert_eq!(item2numeric(" Q42 "), Some(42));
        assert_eq!(item2numeric("Q1 Q2"), Some(1));
        assert_eq!(item2numeric("q123"), Some(123));
    }

    #[test]
    fn test_tool_root_dir_default() {
        let dir = tool_root_dir();
        assert!(!dir.is_empty());
    }

    #[test]
    fn test_is_on_toolforge() {
        let _result = is_on_toolforge();
    }
}
