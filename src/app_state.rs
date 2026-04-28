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
use lazy_static::lazy_static;
use log::{error, info};
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

lazy_static! {
    pub static ref TESTING: Mutex<bool> = Mutex::new(false); // To lock the test entry in the database
    pub static ref TEST_MUTEX: Mutex<bool> = Mutex::new(true); // To lock the test entry in the database
    static ref RE_ITEM2NUMERIC: Regex = Regex::new(r"(-{0,1}\d+)").expect("Regex failure");
}

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

    pub async fn disconnect(&self) -> Result<()> {
        self.wikidata.disconnect_db().await?;
        self.storage.disconnect().await?;
        Ok(())
    }

    /// Converts a string like "Q12345" to the numeric 12334
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
        let current_jobs = self.forever_loop_initalize().await?;
        let threshold_job_size = TaskSize::Medium;
        let threshold_percent = 50;

        // TO MANUALLY FIND ACTIONS NOT ASSIGNED A TASK SIZE:
        // select distinct action from jobs where action not in (select action from job_sizes);

        info!(
            "\n=== Starting forever loop with max_concurrent_jobs={}",
            self.max_concurrent_jobs
        );
        loop {
            let current_jobs_len = current_jobs.len();
            if current_jobs_len >= self.max_concurrent_jobs {
                Self::hold_on();
                continue;
            }
            match self
                .forever_loop_run_job(&current_jobs, &threshold_job_size, threshold_percent)
                .await
            {
                Ok(_) => {}
                Err(e) => error!("Error in forever_loop_run_job: {e}"),
            }
        }
        // self.disconnect().await?; // Never happens
    }

    async fn forever_loop_initalize(&self) -> Result<Arc<DashMap<usize, TaskSize>>> {
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());
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
        info!("Old jobs reset, starting bot");
        self.seppuku();
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.storage()
            .set_kv_value("forever_loop_start", &current_time_str)
            .await?;
        Ok(current_jobs)
    }

    async fn forever_loop_run_job(
        &self,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        threshold_job_size: &TaskSize,
        threshold_percent: usize,
    ) -> Result<()> {
        let (mut job, task_size) = self
            .get_next_job(self, current_jobs, threshold_job_size, threshold_percent)
            .await?;
        match job.set_next().await {
            Ok(true) => {
                Self::run_job(job, task_size, current_jobs).await;
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
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        info!("{current_time_str}: {} jobs running", current_jobs.len());
        Self::print_sysinfo();
        let current_jobs = current_jobs.clone();
        tokio::spawn(async move {
            if let Err(e) = job.run().await {
                error!("Job {job_id} failed with error {e}");
            }
            current_jobs.remove(&job_id);
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

    #[test]
    fn test_item2numeric() {
        assert_eq!(AppState::item2numeric("foobar"), None);
        assert_eq!(AppState::item2numeric("12345"), Some(12345));
        assert_eq!(AppState::item2numeric("Q12345"), Some(12345));
        assert_eq!(AppState::item2numeric("Q12345X"), Some(12345));
        assert_eq!(AppState::item2numeric("Q12345X6"), Some(12345));
    }

    #[test]
    fn test_item2numeric_edge_cases() {
        // Empty string
        assert_eq!(AppState::item2numeric(""), None);
        // Just "Q" with no digits
        assert_eq!(AppState::item2numeric("Q"), None);
        // Negative-looking input — regex allows optional minus sign
        assert_eq!(AppState::item2numeric("Q-5"), Some(-5));
        // Zero
        assert_eq!(AppState::item2numeric("Q0"), Some(0));
        // Large number
        assert_eq!(AppState::item2numeric("Q999999999"), Some(999999999));
        // Whitespace around
        assert_eq!(AppState::item2numeric(" Q42 "), Some(42));
        // Multiple Q patterns — only first match
        assert_eq!(AppState::item2numeric("Q1 Q2"), Some(1));
        // Lowercase q — regex matches digits regardless of prefix
        assert_eq!(AppState::item2numeric("q123"), Some(123));
    }

    #[test]
    fn test_tool_root_dir_default() {
        // When TOOL_DATA_DIR is not set, should return the default path
        // We can't guarantee the env var state, but we can verify it returns a non-empty string
        let dir = AppState::tool_root_dir();
        assert!(!dir.is_empty());
    }

    #[test]
    fn test_is_on_toolforge() {
        // On a dev machine, /etc/wmcs-project should not exist
        // This test just verifies the function doesn't panic
        let _result = AppState::is_on_toolforge();
    }
}
