use crate::auth::config::OauthConfig;
use crate::job::Job;
use crate::job_status::JobStatus;
use crate::mysql_misc::MySQLMisc;
use crate::storage::Storage;
use crate::storage_mysql::StorageMySQL;
use crate::task_size::TaskSize;
use crate::wdrc::WDRC;
use crate::wikibase::WikiBase;
use crate::wikidata::Wikidata;
use anyhow::{Result, anyhow};
use chrono::Local;
use dashmap::DashMap;
use std::sync::LazyLock;
use log::{error, info, warn};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{thread, time};
use sysinfo::System;
use tokio::time::sleep;
use wikimisc::timestamp::TimeStamp;

pub const MICRO_API_PORT: u16 = 8089;

/// Per-action concurrency caps enforced by the job runner. When a given
/// action has at least this many jobs running, the SQL job picker skips
/// queued rows for that action until a slot frees up.
///
/// `microsync` is capped to keep the user-triggered "manual sync on every
/// catalog containing Q" pattern (GitHub #6) from saturating the Wikidata
/// replica connection pool — each microsync runs `fix_matched_items`,
/// which in turn calls `get_deleted_items` against the replica, and the
/// pool has only a handful of connections in production. Two concurrent
/// microsyncs comfortably fit; a thundering herd does not. Other actions
/// either don't hit the Wikidata replica or aren't user-triggered, so
/// they don't need a cap here.
const ACTION_CONCURRENCY_CAPS: &[(&str, usize)] = &[("microsync", 2)];

/// Append any action whose running count meets or exceeds its
/// `ACTION_CONCURRENCY_CAPS` entry to `skip_actions`. Pulled out of
/// `get_next_job` so the cap policy is independently unit-testable
/// without spinning up an `AppState`.
fn apply_action_concurrency_caps(
    skip_actions: &mut Vec<String>,
    action_counts: &DashMap<String, usize>,
) {
    for (action, cap) in ACTION_CONCURRENCY_CAPS {
        let running = action_counts.get(*action).map(|v| *v).unwrap_or(0);
        if running >= *cap && !skip_actions.iter().any(|a| a == *action) {
            skip_actions.push((*action).to_string());
        }
    }
}

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

impl AppState {

    pub async fn disconnect(&self) -> Result<()> {
        self.wikidata.disconnect_db().await?;
        self.storage.disconnect().await?;
        Ok(())
    }

    pub async fn run_single_hp_job(&self) -> Result<()> {
        let app = self.clone();
        let mut job = Job::new(&app);
        if let Some(job_id) = job.get_next_high_priority_job().await {
            job.set_from_id(job_id).await?;
            job.set_status(JobStatus::Running).await?;
            job.run().await?;
        }
        Ok(())
    }

    pub async fn run_single_job(&self, job_id: usize) -> Result<()> {
        let app = self.clone();
        let handle = tokio::spawn(async move {
            let mut job = Job::new(&app);
            job.set_from_id(job_id).await?;
            if let Err(e) = job.set_status(JobStatus::Running).await {
                error!("ERROR SETTING JOB STATUS: {e}");
            }
            job.run().await
        });
        handle.await?
    }

    // Kills the app if there are jobs running but have no recent activity
    // Toolforge k8s "continuous job" will restart a new instance
    fn seppuku(&self) {
        let check_every_minutes = 5;
        let max_age_min = 20;
        let app = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(tokio::time::Duration::from_secs(60 * check_every_minutes)).await;
                // println!("seppuku check running");
                let min = chrono::Duration::try_minutes(max_age_min).unwrap();
                let utc = chrono::Utc::now() - min;
                let ts = TimeStamp::datetime(&utc);
                let (running, running_recent) =
                    app.storage().app_state_seppuku_get_running(&ts).await;
                if running > 0 && running_recent == 0 {
                    error!(
                        "seppuku: {running} jobs running but no activity within {max_age_min} minutes, commiting seppuku"
                    );
                    std::process::exit(0);
                }
                // println!("seppuku: honor intact");
            }
        });
    }

    pub async fn forever_loop(&self) -> Result<()> {
        let (current_jobs, action_counts) = self.forever_loop_initalize().await?;
        let threshold_job_size = TaskSize::Medium;
        let threshold_percent = 50;

        // TO MANUALLY FIND ACTIONS NOT ASSIGNED A TASK SIZE:
        // select distinct action from jobs where action not in (select action from job_sizes);

        info!(
            "\n=== Starting forever loop with max_concurrent_jobs={}",
            self.max_concurrent_jobs
        );
        loop {
            // HIGH_PRIORITY fast-path: bypass the capacity gate, the big-job
            // size filter, and per-action concurrency caps. A flood of HP
            // jobs all start immediately; they still count toward
            // `current_jobs`, so normal-priority dispatch waits for the cap
            // as usual. The `continue` is intentional — on a successful
            // dispatch we re-check for more HP jobs before doing anything
            // else, so a queued batch drains back-to-back without sleeps.
            match self
                .forever_loop_try_dispatch_high_priority(&current_jobs, &action_counts)
                .await
            {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => error!("HIGH_PRIORITY fast-path error: {e}"),
            }

            let current_jobs_len = current_jobs.len();
            if current_jobs_len >= self.max_concurrent_jobs {
                Self::hold_on();
                continue;
            }
            match self
                .forever_loop_run_job(
                    &current_jobs,
                    &action_counts,
                    &threshold_job_size,
                    threshold_percent,
                )
                .await
            {
                Ok(_) => {}
                Err(e) => error!("Error in forever_loop_run_job: {e}"),
            }
        }
        // self.disconnect().await?; // Never happens
    }

    /// Pick the next HIGH_PRIORITY job ignoring every dispatch gate.
    ///
    /// Unlike [`Job::get_next_high_priority_job`] (which respects the
    /// caller's `skip_actions`, populated from the big-job size filter and
    /// per-action concurrency caps), this passes an empty filter list — by
    /// design, HIGH_PRIORITY means "start now, regardless of capacity, job
    /// size, or per-action caps." Used by the forever-loop fast-path.
    ///
    /// Returns `Ok(None)` when no HP job is pending, or when a row was
    /// found but vanished before it could be loaded (rare; treated as
    /// "no HP available" and falls through to normal dispatch).
    pub(crate) async fn pick_high_priority_job(&self) -> Result<Option<Job>> {
        let Some(job_id) = self
            .storage()
            .jobs_get_next_job(JobStatus::HighPriority, None, &[], None)
            .await
        else {
            return Ok(None);
        };
        let mut job = Job::new(self);
        if !job.set_from_id(job_id).await? {
            return Ok(None);
        }
        Ok(Some(job))
    }

    /// Dispatch a HIGH_PRIORITY job if one is pending. Returns `true` when
    /// a job was dispatched (the caller should loop immediately to check
    /// for more), `false` when nothing was pending.
    async fn forever_loop_try_dispatch_high_priority(
        &self,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        action_counts: &Arc<DashMap<String, usize>>,
    ) -> Result<bool> {
        let Some(job) = self.pick_high_priority_job().await? else {
            return Ok(false);
        };
        let task_size = self.storage().jobs_get_tasks().await.unwrap_or_default();
        let job_id = job.get_id().await?;
        info!("HIGH_PRIORITY fast-path: dispatching job {job_id}");
        Self::run_job(job, task_size, current_jobs, action_counts).await;
        Ok(true)
    }

    async fn forever_loop_initalize(
        &self,
    ) -> Result<(
        Arc<DashMap<usize, TaskSize>>,
        Arc<DashMap<String, usize>>,
    )> {
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());
        // Per-action running counts. Used to enforce per-action concurrency
        // caps via `ACTION_CONCURRENCY_CAPS` — see `get_next_job`.
        let action_counts: Arc<DashMap<String, usize>> = Arc::new(DashMap::new());
        // Cut any query still in flight from a previous instance BEFORE the
        // reset flips those jobs to TODO. If the old process is still alive,
        // killing its query unblocks the connection and its `set_status(Done)`
        // either fails or targets a row we've already re-queued — whichever
        // wins, the job isn't simultaneously running in two places.
        const ORPHAN_QUERY_THRESHOLD_SECS: u64 = 120;
        match self
            .storage()
            .kill_long_running_queries(ORPHAN_QUERY_THRESHOLD_SECS)
            .await
        {
            Ok(ids) if !ids.is_empty() => {
                info!(
                    "forever_loop: killed {} long-running queries (>{}s): {:?}",
                    ids.len(),
                    ORPHAN_QUERY_THRESHOLD_SECS,
                    ids
                );
            }
            Ok(_) => {}
            Err(e) => error!("forever_loop: kill_long_running_queries failed: {e}"),
        }
        self.storage().reset_running_jobs().await?;
        self.storage().reset_failed_jobs().await?;
        // Ensure the global periodic issue-sweep job exists. On first deployment
        // `initial_next_ts` is set to `now` so it runs soon; on subsequent restarts
        // the ON DUPLICATE KEY no-op preserves whatever period operators have set.
        const ISSUE_SWEEP_PERIOD_SECS: usize = 86400; // daily
        let initial_next_ts = wikimisc::timestamp::TimeStamp::now();
        if let Err(e) = self
            .storage()
            .ensure_periodic_global_job("update_issues", ISSUE_SWEEP_PERIOD_SECS, &initial_next_ts)
            .await
        {
            warn!("forever_loop: could not ensure periodic update_issues job: {e}");
        }
        info!("Old jobs reset, starting bot");
        self.seppuku();
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.storage()
            .set_kv_value("forever_loop_start", &current_time_str)
            .await?;
        Ok((current_jobs, action_counts))
    }

    async fn forever_loop_run_job(
        &self,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        action_counts: &Arc<DashMap<String, usize>>,
        threshold_job_size: &TaskSize,
        threshold_percent: usize,
    ) -> Result<()> {
        let (mut job, task_size) = self
            .get_next_job(
                self,
                current_jobs,
                action_counts,
                threshold_job_size,
                threshold_percent,
            )
            .await?;
        match job.set_next().await {
            Ok(true) => {
                Self::run_job(job, task_size, current_jobs, action_counts).await;
                let current_job_ids = current_jobs
                    .iter()
                    .map(|x| x.key().to_owned())
                    .collect::<Vec<_>>();
                info!("JOBS RUNNING: {current_job_ids:?}");
            }
            Ok(false) => {
                // println!("No jobs available, waiting... (not using: {:?})",job.skip_actions);
                Self::hold_on();
            }
            Err(e) => {
                error!("MAIN LOOP: Something went wrong: {e}");
                Self::hold_on();
            }
        }
        Ok(())
    }

    async fn get_next_job(
        &self,
        app: &AppState,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        action_counts: &Arc<DashMap<String, usize>>,
        threshold_job_size: &TaskSize,
        threshold_percent: usize,
    ) -> Result<(Job, HashMap<String, TaskSize>)> {
        let mut job = Job::new(app);
        let task_size = self.storage().jobs_get_tasks().await?;
        let big_jobs_running = Self::count_big_jobs_running(current_jobs, threshold_job_size);
        let max_job_size = if big_jobs_running >= self.max_concurrent_jobs * threshold_percent / 100
        {
            *threshold_job_size
        } else {
            TaskSize::Ginormous
        };
        // println!("JOBSIZE: {max_job_size} ({big_jobs_running} big jobs running, threshold_percent={threshold_percent})");
        job.skip_actions = task_size
            .iter()
            .filter(|(_action, size)| **size > max_job_size)
            .map(|(action, _size)| action.to_string())
            .collect();
        // Per-action concurrency caps. Keeps the user-triggered "manual
        // sync on every catalog containing Q" pattern (GitHub #6) from
        // saturating the Wikidata replica connection pool — when the cap
        // for an action is reached, the SQL job picker skips it and lets
        // the rows wait in TODO until a slot frees up.
        apply_action_concurrency_caps(&mut job.skip_actions, action_counts);
        Ok((job, task_size))
    }

    fn hold_on() {
        thread::sleep(time::Duration::from_secs(5));
    }

    fn print_sysinfo() {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return;
        }
        let sys = System::new_all();
        // println!("Uptime: {:?}", System::uptime());
        info!(
            "Memory: total {}, free {}, used {} MB; ",
            sys.total_memory() / 1024,
            sys.free_memory() / 1024,
            sys.used_memory() / 1024
        );
        info!(
            "Processes: {}, CPUs: {}; ",
            sys.processes().len(),
            sys.cpus().len()
        );
        info!(
            "CPU usage: {}%, Load average: {:?}",
            sys.global_cpu_usage(),
            System::load_average()
        );
    }

    async fn run_job(
        mut job: Job,
        task_size: HashMap<String, TaskSize>,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        action_counts: &Arc<DashMap<String, usize>>,
    ) {
        let _ = job.set_status(JobStatus::Running).await;
        let action = match job.get_action().await {
            Ok(action) => action,
            Err(_) => {
                let _ = job.set_status(JobStatus::Failed).await;
                return;
            }
        };
        let job_size = task_size.get(&action).copied().unwrap_or(TaskSize::Small);
        let job_id = match job.get_id().await {
            Ok(id) => id,
            Err(_e) => {
                error!("No job ID"); //,e);
                return;
            }
        };
        current_jobs.insert(job_id, job_size);
        // Bump per-action running count BEFORE the spawn so the next
        // `get_next_job` call (on the main loop) sees the increment.
        *action_counts.entry(action.clone()).or_insert(0) += 1;
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        info!("{current_time_str}: {} jobs running", current_jobs.len());
        Self::print_sysinfo();
        let current_jobs = current_jobs.clone();
        let action_counts = action_counts.clone();
        tokio::spawn(async move {
            if let Err(e) = job.run().await {
                error!("Job {job_id} failed with error {e}");
            }
            current_jobs.remove(&job_id);
            // Saturating decrement — a panic between the increment and
            // here would leave the count one above reality; the
            // `if > 0` guard keeps a leaked count from going negative
            // on a subsequent successful run.
            if let Some(mut entry) = action_counts.get_mut(&action) {
                if *entry > 0 {
                    *entry -= 1;
                }
            }
        });
    }

    fn count_big_jobs_running(
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        threshold_job_size: &TaskSize,
    ) -> usize {
        current_jobs
            .iter()
            .map(|x| *x.value())
            .filter(|size| *size > *threshold_job_size)
            .count()
    }

    pub fn load_config(filename: &str) -> Result<Value> {
        let mut path = env::current_dir()?;
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(config)
    }
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

    // ──────────────────────────────────────────────────────────────
    // HIGH_PRIORITY fast-path tests
    //
    // The pick query is global (`SELECT ... LIMIT 1` over the whole
    // jobs table) so multiple HP tests in parallel would step on each
    // other's seeded rows. Verified by grep: no other test in the
    // codebase seeds HIGH_PRIORITY, so we only have to serialize HP
    // tests against each other — done via HP_TEST_MUTEX below, with a
    // pre-test purge of any stale HP rows from earlier runs in the same
    // test process.
    // ──────────────────────────────────────────────────────────────

    static HP_TEST_MUTEX: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    async fn purge_hp_jobs() -> anyhow::Result<()> {
        use mysql_async::prelude::Queryable;
        let (pool, mut conn) = crate::test_support::raw_conn().await?;
        conn.exec_drop("DELETE FROM jobs WHERE status='HIGH_PRIORITY'", ())
            .await?;
        drop(conn);
        pool.disconnect().await.ok();
        Ok(())
    }

    /// Seed a job row with a caller-specified status. Mirrors
    /// `test_support::seed_job` (which hard-codes 'TODO') so the HP
    /// fast-path tests can stage rows in HIGH_PRIORITY / LOW_PRIORITY etc.
    async fn seed_job_with_status(
        action: &str,
        catalog_id: usize,
        status: &str,
    ) -> anyhow::Result<usize> {
        use mysql_async::prelude::*;
        let (pool, mut conn) = crate::test_support::raw_conn().await?;
        r"INSERT INTO jobs (action, catalog, status, last_ts, next_ts, user_id)
          VALUES (:action, :catalog, :status, '20220101000000', '', 0)"
            .with(mysql_async::params! {
                "action"  => action,
                "catalog" => catalog_id,
                "status"  => status,
            })
            .ignore(&mut conn)
            .await?;
        let id: u64 = "SELECT LAST_INSERT_ID()".first(&mut conn).await?.unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        Ok(id as usize)
    }

    #[tokio::test]
    async fn pick_high_priority_returns_none_when_no_hp_jobs() {
        let _guard = HP_TEST_MUTEX.lock().await;
        purge_hp_jobs().await.unwrap();

        let app = crate::test_support::test_app().await;
        let result = app.pick_high_priority_job().await.unwrap();
        assert!(result.is_none(), "no HP jobs seeded — must return None");
    }

    #[tokio::test]
    async fn pick_high_priority_returns_hp_job() {
        let _guard = HP_TEST_MUTEX.lock().await;
        purge_hp_jobs().await.unwrap();

        let app = crate::test_support::test_app().await;
        let (catalog_id, _) = crate::test_support::seed_minimal_entry(&app).await.unwrap();
        let hp_id = seed_job_with_status("microsync", catalog_id, "HIGH_PRIORITY")
            .await
            .unwrap();

        let job = app
            .pick_high_priority_job()
            .await
            .unwrap()
            .expect("HP job should be picked");
        assert_eq!(job.get_id().await.unwrap(), hp_id);
    }

    #[tokio::test]
    async fn pick_high_priority_ignores_todo_jobs() {
        let _guard = HP_TEST_MUTEX.lock().await;
        purge_hp_jobs().await.unwrap();

        let app = crate::test_support::test_app().await;
        let (catalog_id, _) = crate::test_support::seed_minimal_entry(&app).await.unwrap();
        let _todo_id = crate::test_support::seed_job("microsync", catalog_id)
            .await
            .unwrap();

        let result = app.pick_high_priority_job().await.unwrap();
        assert!(
            result.is_none(),
            "TODO jobs must not be picked by the HP fast-path"
        );
    }

    #[tokio::test]
    async fn pick_high_priority_picks_only_hp_among_mixed_statuses() {
        let _guard = HP_TEST_MUTEX.lock().await;
        purge_hp_jobs().await.unwrap();

        // Seed one HP job and several other-status jobs; assert HP is the one chosen.
        let app = crate::test_support::test_app().await;
        let (catalog_id, _) = crate::test_support::seed_minimal_entry(&app).await.unwrap();
        let _ = seed_job_with_status("automatch_by_search", catalog_id, "LOW_PRIORITY")
            .await
            .unwrap();
        let _ = seed_job_with_status("aux2wd", catalog_id, "DONE").await.unwrap();
        let hp_id = seed_job_with_status("microsync", catalog_id, "HIGH_PRIORITY")
            .await
            .unwrap();

        let job = app
            .pick_high_priority_job()
            .await
            .unwrap()
            .expect("HP job should be picked among mixed statuses");
        assert_eq!(job.get_id().await.unwrap(), hp_id);
    }

    #[tokio::test]
    async fn try_dispatch_high_priority_returns_false_when_no_hp_jobs() {
        let _guard = HP_TEST_MUTEX.lock().await;
        purge_hp_jobs().await.unwrap();

        let app = crate::test_support::test_app().await;
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());
        let action_counts: Arc<DashMap<String, usize>> = Arc::new(DashMap::new());
        let dispatched = app
            .forever_loop_try_dispatch_high_priority(&current_jobs, &action_counts)
            .await
            .unwrap();
        assert!(!dispatched);
        assert!(current_jobs.is_empty(), "must not have spawned anything");
    }

    #[test]
    fn action_cap_skips_when_at_capacity() {
        let counts: DashMap<String, usize> = DashMap::new();
        // Cap for microsync is 2 in ACTION_CONCURRENCY_CAPS.
        counts.insert("microsync".to_string(), 2);
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(skip.iter().any(|a| a == "microsync"));
    }

    #[test]
    fn action_cap_does_not_skip_below_capacity() {
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("microsync".to_string(), 1);
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(
            !skip.iter().any(|a| a == "microsync"),
            "1 running, cap is 2 — must not skip"
        );
    }

    #[test]
    fn action_cap_does_not_double_add() {
        // If `microsync` is already in skip_actions (e.g. because it was
        // marked too-big upstream), don't append a duplicate.
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("microsync".to_string(), 5);
        let mut skip: Vec<String> = vec!["microsync".to_string()];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert_eq!(
            skip.iter().filter(|a| **a == "microsync").count(),
            1,
            "microsync must appear at most once in skip_actions"
        );
    }

    #[test]
    fn action_cap_handles_unknown_action() {
        // An action that has no entry in ACTION_CONCURRENCY_CAPS must
        // never be added to skip_actions by the cap logic.
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("auxiliary_matcher".to_string(), 100);
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(skip.is_empty(), "uncapped action must not be skipped");
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
