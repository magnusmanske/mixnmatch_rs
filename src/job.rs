use crate::app_state::{AppState, ExternalServicesContext};
use crate::automatch::AutoMatch;
use crate::autoscrape::Autoscrape;
use crate::auxiliary_matcher::AuxiliaryMatcher;
use crate::coordinate_matcher::CoordinateMatcher;
use crate::job_row::JobRow;
use crate::job_status::JobStatus;
use crate::maintenance::Maintenance;
use crate::match_state::MatchState;
use crate::microsync::Microsync;
use crate::code_fragment;
use crate::php_wrapper::PhpWrapper;
use crate::task_size::TaskSize;
use crate::taxon_matcher::TaxonMatcher;
use crate::update_catalog::UpdateCatalog;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Duration;
use chrono::Local;
use futures::future::BoxFuture;
use log::info;
use serde_json::json;
use std::error::Error;
use std::fmt;
use wikimisc::timestamp::TimeStamp;

/// A trait that allows to manage temporary job data (eg offset)
///
/// To use this trait, simply add a `job: Option<Job>` field to your struct
/// and implement only the three required methods. All other methods have default implementations.
#[async_trait]
pub trait Jobbable {
    fn set_current_job(&mut self, job: &Job);
    fn get_current_job(&self) -> Option<&Job>;
    fn get_current_job_mut(&mut self) -> Option<&mut Job>;

    //TODO test
    async fn get_last_job_data(&self) -> Option<serde_json::Value> {
        self.get_current_job()?.get_json_value().await
    }

    //TODO test
    async fn remember_job_data(&mut self, json: &serde_json::Value) -> Result<()> {
        match self.get_current_job_mut() {
            Some(job) => job.set_json(Some(json.to_owned())).await,
            None => return Ok(()),
        }
    }

    //TODO test
    async fn get_last_job_offset(&self) -> usize {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return 0,
        };
        let json = match job.get_json_value().await {
            Some(json) => json,
            None => return 0,
        };
        json.as_object().map_or(0, |o| {
            o.get("offset")
                .map_or(0, |offset| offset.as_u64().unwrap_or(0) as usize)
        })
    }

    //TODO test
    async fn remember_offset(&mut self, offset: usize) -> Result<()> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        // println!("{}: {offset} [{}]",job.get_id().await.unwrap_or(0), Utc::now());
        job.set_json(Some(json!({ "offset": offset }))).await?;
        Ok(())
    }

    //TODO test
    async fn clear_offset(&mut self) -> Result<()> {
        match self.get_current_job_mut() {
            Some(job) => job.set_json(None).await,
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
pub enum JobError {
    S(String),
    TimeError,
}

impl Error for JobError {}

impl fmt::Display for JobError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JobError::S(s) => write!(f, "JobError::S: {s}"),
            JobError::TimeError => write!(f, "JobError::TimeError"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub data: JobRow,
    pub app: AppState,
    pub skip_actions: Vec<String>,
}

impl Job {
    pub fn new(app: &AppState) -> Self {
        Self {
            data: JobRow::default(),
            app: app.clone(),
            skip_actions: vec![],
        }
    }

    //TODO test
    pub async fn set_next(&mut self) -> Result<bool> {
        match self.get_next_job_id().await {
            Some(job_id) => self.set_from_id(job_id).await,
            None => Ok(false),
        }
    }

    pub async fn set_from_id(&mut self, job_id: usize) -> Result<bool> {
        match self.app.storage().jobs_row_from_id(job_id).await {
            Ok(row) => {
                self.data = row;
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    //TODO test
    pub async fn run(&mut self) -> Result<()> {
        let catalog_id = self.get_catalog().await?;
        let action = self.get_action().await?;
        let res = self.run_this_job().await;
        match res {
            Ok(_) => self.run_ok(catalog_id, action).await?,
            Err(e) => self.run_error(catalog_id, &action, &e).await?,
        }
        self.update_next_ts().await
    }

    async fn run_error(
        &mut self,
        catalog_id: usize,
        action: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        match catalog_id {
            0 => self.set_status(JobStatus::Done).await?, // Don't fail
            _ => self.set_status(JobStatus::Failed).await?,
        }
        let note = Some(format!("{error}"));
        self.set_note(note).await?;
        let job_id = self.get_id().await?;
        info!("Job {job_id} catalog {catalog_id}:{action} FAILED: {error}");
        Ok(())
    }

    async fn run_ok(&mut self, catalog_id: usize, action: String) -> Result<(), anyhow::Error> {
        self.set_status(JobStatus::Done).await?;
        info!(
            "Job {} catalog {}:{} completed.",
            self.get_id().await?,
            catalog_id,
            action
        );
        Ok(())
    }

    //TODO test
    pub async fn set_status(&mut self, status: JobStatus) -> Result<()> {
        let job_id = self.get_id().await?;
        let timestamp = TimeStamp::now();
        self.app
            .storage()
            .jobs_set_status(&status, job_id, timestamp)
            .await?;
        self.put_status(status).await?;
        Ok(())
    }

    //TODO test
    pub async fn set_note(&mut self, note: Option<String>) -> Result<()> {
        let job_id = self.get_id().await?;
        let note_cloned = self.app.storage().jobs_set_note(note, job_id).await?;
        self.put_note(note_cloned).await?;
        Ok(())
    }

    //TODO test
    pub async fn get_next_job_id(&self) -> Option<usize> {
        // Tiny TODO / HIGH_PRIORITY jobs are cheap and must never be
        // starved by the big-job gating that populates `skip_actions`.
        // Try them first, bypassing the skip list entirely.
        if let Some(job_id) = self.get_next_tiny_priority_job().await {
            return Some(job_id);
        }

        if let Some(job_id) = self.get_next_high_priority_job().await {
            return Some(job_id);
        }
        if let Some(job_id) = self.get_next_dependent_job().await {
            return Some(job_id);
        }

        let mut tasks = self.app.storage().jobs_get_tasks().await.ok()?;
        let mut level: u8 = 0;
        while !tasks.is_empty() {
            tasks.retain(|_action, size| size.value() > level);
            let avoid: Vec<String> = tasks.keys().cloned().collect();
            if let Some(job_id) = self.get_next_initial_allowed_job(&avoid).await {
                return Some(job_id);
            }
            level += 1;
        }

        if let Some(job_id) = self.get_next_initial_job().await {
            return Some(job_id);
        }
        if let Some(job_id) = self.get_next_low_priority_job().await {
            return Some(job_id);
        }
        if let Some(job_id) = self.get_next_scheduled_job().await {
            return Some(job_id);
        }
        None
    }

    /// Pick a TODO/HIGH_PRIORITY job whose action is configured as
    /// `TaskSize::Tiny`, ignoring `skip_actions`. Used as the first probe
    /// in `get_next_job_id` so tiny jobs can always start.
    async fn get_next_tiny_priority_job(&self) -> Option<usize> {
        let tasks = self.app.storage().jobs_get_tasks().await.ok()?;
        let tiny: Vec<String> = tasks
            .into_iter()
            .filter(|(_a, size)| *size == TaskSize::Tiny)
            .map(|(a, _)| a)
            .collect();
        if tiny.is_empty() {
            return None;
        }
        if let Some(id) = self
            .app
            .storage()
            .jobs_get_next_job_by_actions(JobStatus::HighPriority, &tiny)
            .await
        {
            return Some(id);
        }
        self.app
            .storage()
            .jobs_get_next_job_by_actions(JobStatus::Todo, &tiny)
            .await
    }

    /// Returns the current `json` as an Option<`serde_json::Value`>
    //TODO test
    pub async fn get_json_value(&self) -> Option<serde_json::Value> {
        serde_json::from_str(self.get_json().await.ok()?.as_ref()?).ok()
    }

    //TODO test
    pub async fn queue_simple_job(
        app: &dyn ExternalServicesContext,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
    ) -> Result<usize> {
        app.storage()
            .jobs_queue_simple_job(catalog_id, action, depends_on, "TODO", TimeStamp::now())
            .await
    }

    /// Sets the value for `json` locally and in database, from a `serde_json::Value`
    //TODO test
    pub async fn set_json(&mut self, json: Option<serde_json::Value>) -> Result<()> {
        let job_id = self.get_id().await?;
        let timestamp = TimeStamp::now();
        match json {
            Some(json) => {
                let json_string = json.to_string();
                self.put_json(Some(json_string.clone())).await?;
                self.app
                    .storage()
                    .jobs_set_json(job_id, json_string, &timestamp)
                    .await?;
            }
            None => {
                self.put_json(None).await?;
                self.app
                    .storage()
                    .jobs_reset_json(job_id, timestamp)
                    .await?;
            }
        }
        Ok(())
    }

    // PRIVATE METHODS

    //TODO test
    async fn run_this_job(&mut self) -> Result<()> {
        if self.data.status == JobStatus::Blocked {
            return Err(anyhow!("Job::run_this_job: Blocked"));
        }
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        info!("{current_time_str}: Starting job {:?}", self.get_id().await);
        let catalog_id = self.get_catalog().await?;
        let action = self.get_action().await?;

        let handler = JOB_HANDLER_REGISTRY
            .iter()
            .find(|(name, _)| *name == action)
            .map(|(_, h)| *h)
            .ok_or_else(|| anyhow!("Job::run_this_job: Unknown action '{action}'"))?;
        handler(self, catalog_id).await
    }

    //TODO test
    async fn data(&self) -> Result<JobRow> {
        Ok(self.data.clone())
    }
    //TODO test
    pub async fn get_id(&self) -> Result<usize> {
        Ok(self.data.id)
    }
    //TODO test
    pub async fn get_action(&self) -> Result<String> {
        Ok(self.data.action.clone())
    }
    //TODO test
    async fn get_catalog(&self) -> Result<usize> {
        Ok(self.data.catalog)
    }
    //TODO test
    async fn get_json(&self) -> Result<Option<String>> {
        Ok(self.data.json.clone())
    }

    //TODO test
    async fn put_status(&mut self, status: JobStatus) -> Result<()> {
        self.data.status = status;
        Ok(())
    }

    //TODO test
    async fn put_json(&mut self, json: Option<String>) -> Result<()> {
        self.data.json = json;
        Ok(())
    }

    //TODO test
    async fn put_note(&mut self, note: Option<String>) -> Result<()> {
        self.data.note = note;
        Ok(())
    }

    //TODO test
    async fn put_next_ts(&mut self, next_ts: &str) -> Result<()> {
        self.data.next_ts = next_ts.to_string();
        Ok(())
    }

    async fn get_next_ts(&mut self) -> Result<String> {
        let seconds = match self.data().await?.repeat_after_sec {
            Some(sec) => sec as i64,
            None => return Ok(String::new()),
        };
        let seconds = Duration::try_seconds(seconds)
            .ok_or_else(|| anyhow!("repeat_after_sec out of range: {seconds}"))?;
        let utc = TimeStamp::str2utc(&self.data().await?.last_ts)
            .ok_or(anyhow!("Can't parse timestamp in last_ts"))?
            .checked_add_signed(seconds)
            .ok_or(JobError::TimeError)?;
        let next_ts = utc.format("%Y%m%d%H%M%S").to_string();
        Ok(next_ts)
    }

    //TODO test
    async fn update_next_ts(&mut self) -> Result<()> {
        let next_ts = self.get_next_ts().await?;
        let job_id = self.get_id().await?;
        self.put_next_ts(&next_ts).await?;
        self.app
            .storage()
            .jobs_update_next_ts(job_id, next_ts)
            .await?;
        Ok(())
    }

    //TODO test
    pub async fn get_next_high_priority_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::HighPriority, None, &self.skip_actions, None)
            .await
    }

    //TODO test
    async fn get_next_low_priority_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::LowPriority, None, &self.skip_actions, None)
            .await
    }

    //TODO test
    async fn get_next_dependent_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(
                JobStatus::Todo,
                Some(JobStatus::Done),
                &self.skip_actions,
                None,
            )
            .await
    }

    //TODO test
    async fn get_next_initial_allowed_job(&self, avoid: &[String]) -> Option<usize> {
        if avoid.is_empty() {
            return None;
        }
        let mut skip = avoid.to_vec();
        skip.append(&mut self.skip_actions.clone());
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::Todo, None, &skip, None)
            .await
    }

    //TODO test
    async fn get_next_initial_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::Todo, None, &self.skip_actions, None)
            .await
    }

    //TODO test
    async fn get_next_scheduled_job(&self) -> Option<usize> {
        let timestamp = TimeStamp::now();
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::Done, None, &self.skip_actions, Some(timestamp))
            .await
    }
}

// ---------------------------------------------------------------------------
// Job-handler registry
// ---------------------------------------------------------------------------
//
// Each registry entry is `(action_name, run_fn)` where `run_fn` consumes the
// `&mut Job` long enough to set up the subsystem (most subsystems clone Job
// internally via `set_current_job`, after which the &mut Job borrow ends),
// then awaits the actual handler. The macros below cover the three common
// shapes; one-off handlers use a hand-written closure.
//
// **Adding a job action**: add one line to `JOB_HANDLER_REGISTRY` (in the
// appropriate section, alphabetical within section). The compile-time
// uniqueness test in this file guards against typos; the
// `job_registry_contains_known_actions` test guards against accidental
// deletion.

/// Erased async-fn signature every job handler is wrapped to.
type JobHandlerFn =
    for<'a> fn(&'a mut Job, usize) -> BoxFuture<'a, Result<()>>;

/// Build a `(action, handler)` entry that delegates to the
/// [`automatch::Matcher`] registry. Each `automatch_*` /
/// `match_on_*` action is implemented as a Strategy unit struct in
/// `automatch/matchers.rs`; the dispatcher just looks it up there.
/// Keeping per-action entries here (rather than one wildcard "any
/// matcher") preserves the single-source-of-truth property of
/// `JOB_HANDLER_REGISTRY` — every action is grep-able to one
/// place.
macro_rules! matcher_action {
    ($action:literal) => {
        ($action, ((|job, catalog_id| Box::pin(async move {
            let mut am = AutoMatch::new(&job.app);
            am.set_current_job(job);
            crate::automatch::run_matcher_for_action($action, &mut am, catalog_id)
                .await
                .ok_or_else(|| anyhow!("Matcher registry missing action: {}", $action))?
        })) as JobHandlerFn))
    };
}

/// `(action, |job, cat| ... m.<method>().await)` for the parameter-free
/// Maintenance methods (~70% of the maintenance group).
macro_rules! maintenance_no_arg {
    ($action:literal, $method:ident) => {
        ($action, ((|job, _catalog_id| Box::pin(async move {
            Maintenance::new(&job.app).$method().await
        })) as JobHandlerFn))
    };
}

/// `(action, |job, cat| ... m.<method>(cat).await)` for the Maintenance
/// methods that take catalog_id.
macro_rules! maintenance_with_cat {
    ($action:literal, $method:ident) => {
        ($action, ((|job, catalog_id| Box::pin(async move {
            Maintenance::new(&job.app).$method(catalog_id).await
        })) as JobHandlerFn))
    };
}

/// Single-source-of-truth dispatch table. Add new actions here.
#[rustfmt::skip]
const JOB_HANDLER_REGISTRY: &[(&str, JobHandlerFn)] = &[
    // --- AutoMatch (delegated to automatch::matchers Strategy registry) ---
    matcher_action!("automatch"),
    matcher_action!("automatch_by_search"),
    matcher_action!("automatch_by_sitelink"),
    matcher_action!("automatch_complex"),
    matcher_action!("automatch_creations"),
    matcher_action!("automatch_from_other_catalogs"),
    matcher_action!("automatch_people_with_birth_year"),
    matcher_action!("automatch_people_with_initials"),
    matcher_action!("automatch_sparql"),
    matcher_action!("match_person_dates"),
    matcher_action!("purge_automatches"),
    matcher_action!("match_on_birthdate"),
    matcher_action!("match_on_deathdate"),

    // --- Maintenance (no-arg) ---
    maintenance_no_arg!("automatch_people_via_year_born",          automatch_people_via_year_born),
    maintenance_no_arg!("cleanup_mnm_relations",                   cleanup_mnm_relations),
    maintenance_no_arg!("create_match_person_dates",               create_match_person_dates_jobs_for_catalogs),
    maintenance_no_arg!("maintenance_artwork",                     artwork),
    maintenance_no_arg!("maintenance_automatch",                   automatch),
    maintenance_no_arg!("maintenance_auxiliary_item_values",       fix_auxiliary_item_values),
    maintenance_no_arg!("maintenance_common_aux",                  common_aux),
    maintenance_no_arg!("maintenance_common_names_birth_year",     common_names_birth_year),
    maintenance_no_arg!("maintenance_common_names_dates",          common_names_dates),
    maintenance_no_arg!("maintenance_common_names_human",          common_names_human),
    maintenance_no_arg!("maintenance_delete_multi_match_for_fully_matched", delete_multi_match_for_fully_matched),
    maintenance_no_arg!("maintenance_fix_gnd_undifferentiated_persons", fix_gnd_undifferentiated_persons),
    maintenance_no_arg!("maintenance_fixup_wd_matches",            fixup_wd_matches),
    maintenance_no_arg!("maintenance_inventory_match",             fully_match_via_collection_inventory_number),
    maintenance_no_arg!("maintenance_misc_catalog_things",         misc_catalog_things),
    maintenance_no_arg!("maintenance_name_and_full_dates",         match_by_name_and_full_dates),
    maintenance_no_arg!("maintenance_taxa",                        taxa),
    maintenance_no_arg!("maintenance_update_aux_candidates",       update_aux_candidates),
    maintenance_no_arg!("remove_p17_for_humans",                   remove_p17_for_humans),
    maintenance_no_arg!("update_has_person_date",                  update_has_person_date),
    maintenance_no_arg!("update_iso",                              update_iso_codes),
    maintenance_no_arg!("update_property_cache",                   update_property_cache),
    maintenance_no_arg!("update_props_todo",                       update_props_todo),

    // --- Maintenance (cat-arg) ---
    maintenance_with_cat!("maintenance_apply_description_aux",     apply_description_aux),
    maintenance_with_cat!("maintenance_fix_html_entities",         fix_html_entities_in_catalog),

    // --- Maintenance: bespoke (logging on success or special handling) ---
    ("maintenance_crossmatch_via_aux", (|job, _cat| Box::pin(async move {
        Maintenance::new(&job.app)
            .crossmatch_via_aux()
            .await
            .map(|n| log::info!("crossmatch_via_aux: {n} new match(es)"))
    })) as JobHandlerFn),
    ("maintenance_sanity_check_date_matches_are_human", (|job, _cat| Box::pin(async move {
        Maintenance::new(&job.app)
            .sanity_check_date_matches_are_human()
            .await
            .map(|n| log::info!("sanity_check_date_matches_are_human: removed {n}"))
    })) as JobHandlerFn),
    ("fix_disambig", (|job, catalog_id| Box::pin(async move {
        Maintenance::new(&job.app)
            .unlink_meta_items(catalog_id, &MatchState::any_matched())
            .await
    })) as JobHandlerFn),
    ("fix_redirected_items_in_catalog", (|job, catalog_id| Box::pin(async move {
        // catalog_id=0 means "any catalog" — pick a random active
        // one so the worker does useful work even when the job was
        // queued without a specific catalog in mind.
        let catalog_id = match catalog_id {
            0 => match job.app.storage().get_random_active_catalog_id().await {
                Some(id) => id,
                None => return Ok(()),
            },
            other => other,
        };
        Maintenance::new(&job.app)
            .fix_redirects(catalog_id, &MatchState::any_matched())
            .await
    })) as JobHandlerFn),

    // --- Subsystems with set_current_job + special construction ---
    ("autoscrape", (|job, catalog_id| Box::pin(async move {
        let mut autoscrape = Autoscrape::new(catalog_id, &job.app).await?;
        autoscrape.set_current_job(job);
        autoscrape.run().await
    })) as JobHandlerFn),
    ("aux2wd", (|job, catalog_id| Box::pin(async move {
        let mut am = AuxiliaryMatcher::new(&job.app);
        am.set_current_job(job);
        am.add_auxiliary_to_wikidata(catalog_id).await
    })) as JobHandlerFn),
    ("auxiliary_matcher", (|job, catalog_id| Box::pin(async move {
        let mut am = AuxiliaryMatcher::new(&job.app);
        am.set_current_job(job);
        am.match_via_auxiliary(catalog_id).await
    })) as JobHandlerFn),
    ("taxon_matcher", (|job, catalog_id| Box::pin(async move {
        let mut tm = TaxonMatcher::new(&job.app);
        tm.set_current_job(job);
        tm.match_taxa(catalog_id).await
    })) as JobHandlerFn),
    ("update_from_tabbed_file", (|job, catalog_id| Box::pin(async move {
        let mut uc = UpdateCatalog::new(&job.app);
        uc.set_current_job(job);
        uc.update_from_tabbed_file(catalog_id).await
    })) as JobHandlerFn),
    ("microsync", (|job, catalog_id| Box::pin(async move {
        let mut ms = Microsync::new(&job.app);
        ms.set_current_job(job);
        let catalog_id = match catalog_id {
            0 => match job.app.storage().get_random_active_catalog_id_with_property().await {
                Some(id) => id,
                None => return Ok(()), // Ignore, very unlikely
            },
            other => other,
        };
        ms.check_catalog(catalog_id).await
    })) as JobHandlerFn),

    // --- Free-function dispatch ---
    ("fix_duplicate_issues", (|job, _cat| Box::pin(async move {
        crate::issue::Issue::fix_wd_duplicates(&job.app).await
    })) as JobHandlerFn),
    ("update_issues", (|job, _cat| Box::pin(async move {
        crate::issue::Issue::sweep_open(&job.app).await
    })) as JobHandlerFn),
    ("wdrc_sync", (|job, _cat| Box::pin(async move {
        job.app.wdrc().sync(&job.app).await
    })) as JobHandlerFn),
    ("sync_wd_matches", (|job, _cat| Box::pin(async move {
        crate::wd_match_sync::classify_pending(&job.app, crate::wd_match_sync::DEFAULT_BATCH_SIZE)
            .await
            .map(|stats| log::info!("sync_wd_matches: {stats}"))
    })) as JobHandlerFn),
    ("push_wd_matches_to_wikidata", (|job, _cat| Box::pin(async move {
        crate::wd_match_sync::push_wd_missing(&job.app, crate::wd_match_sync::DEFAULT_BATCH_SIZE)
            .await
            .map(|stats| log::info!("push_wd_matches_to_wikidata: {stats}"))
    })) as JobHandlerFn),
    ("bespoke_scraper", (|job, catalog_id| Box::pin(async move {
        crate::bespoke_scrapers::run_bespoke_scraper(catalog_id, &job.app).await
    })) as JobHandlerFn),
    ("import_aux_from_url", (|job, catalog_id| Box::pin(async move {
        PhpWrapper::import_aux_from_url(catalog_id, &job.app)
    })) as JobHandlerFn),

    // --- Lua-with-PHP-fallback handlers ---
    //
    // Fallback policy: PHP is used only when no Lua code is registered for
    // the catalog (signalled by `LuaJobOutcome::NoLuaCode`). If Lua exists
    // but fails at runtime, the error is returned — falling back to PHP
    // there would mask the bug.
    ("update_person_dates", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_person_dates_job(catalog_id, &job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::update_person_dates(catalog_id, &job.app)
            }
        }
    })) as JobHandlerFn),
    ("generate_aux_from_description", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_aux_from_desc_job(catalog_id, &job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::generate_aux_from_description(catalog_id, &job.app)
            }
        }
    })) as JobHandlerFn),
    ("update_descriptions_from_url", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_desc_from_html_job(catalog_id, &job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::update_descriptions_from_url(catalog_id, &job.app)
            }
        }
    })) as JobHandlerFn),

    // --- Misc ---
    ("match_by_coordinates", (|job, catalog_id| Box::pin(async move {
        let cm = CoordinateMatcher::new(&job.app, Some(catalog_id)).await?;
        cm.run().await
    })) as JobHandlerFn),
    ("sync_from_cersei", (|job, _cat| Box::pin(async move {
        let cs = crate::cersei::CerseiSync::new(&job.app)?;
        cs.sync().await
    })) as JobHandlerFn),
    ("reference_fixer", (|job, _cat| Box::pin(async move {
        // Rewrites free-form reference-URL references into typed
        // external-id references on Wikidata. Drains the
        // `reference_fixer` queue populated by every successful
        // `entry.set_match`. Catalog id is 0 (not catalog-scoped).
        let mut rf = crate::reference_fixer::ReferenceFixer::new(&job.app)?;
        rf.run().await.map(|_| ())
    })) as JobHandlerFn),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;
    use crate::test_support;
    use std::collections::HashSet;

    /// Catches a copy-paste typo in the registry that would silently
    /// shadow an existing action with a second handler that's never
    /// invoked (the lookup picks the first match).
    #[test]
    fn job_registry_action_names_are_unique() {
        let mut seen: HashSet<&'static str> = HashSet::new();
        for (name, _) in JOB_HANDLER_REGISTRY {
            assert!(seen.insert(name), "duplicate job action registered: {name}");
        }
    }

    /// Spot-check critical actions are registered. Catches accidental
    /// deletion that the build wouldn't flag — runtime would just
    /// start saying "Unknown action 'X'".
    #[test]
    fn job_registry_contains_known_actions() {
        let names: HashSet<&'static str> = JOB_HANDLER_REGISTRY.iter().map(|(n, _)| *n).collect();
        for required in [
            // sample one from each subsystem cluster
            "automatch",
            "automatch_by_search",
            "match_on_birthdate",
            "maintenance_taxa",
            "maintenance_apply_description_aux",
            "maintenance_crossmatch_via_aux",
            "fix_disambig",
            "fix_redirected_items_in_catalog",
            "autoscrape",
            "aux2wd",
            "auxiliary_matcher",
            "taxon_matcher",
            "microsync",
            "wdrc_sync",
            "sync_wd_matches",
            "bespoke_scraper",
            "update_person_dates",
            "match_by_coordinates",
            "sync_from_cersei",
            "reference_fixer",
        ] {
            assert!(
                names.contains(required),
                "JOB_HANDLER_REGISTRY missing required action: {required}"
            );
        }
    }

    #[tokio::test]
    async fn test_set_from_id() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::unique_catalog_id();
        let job_id = test_support::seed_job("automatch_by_search", catalog_id)
            .await
            .unwrap();
        let mut job = Job::new(&app);
        job.set_from_id(job_id).await.unwrap();
        assert_eq!(job.get_id().await.unwrap(), job_id);
        assert_eq!(job.get_catalog().await.unwrap(), catalog_id);
        assert_eq!(job.get_action().await.unwrap(), "automatch_by_search");
    }

    #[tokio::test]
    async fn test_get_next_ts() {
        let app = get_test_app();
        let mut job = Job::new(&app);
        let mut job_row = JobRow::new("test_action", 0);
        job_row.last_ts = "20221027000000".to_string();
        job_row.repeat_after_sec = Some(61);
        job.data = job_row;
        let next_ts = job.get_next_ts().await.unwrap();
        assert_eq!(next_ts, "20221027000101");
    }

    #[test]
    fn test_job_error_display_s() {
        let err = JobError::S("something went wrong".to_string());
        assert_eq!(format!("{err}"), "JobError::S: something went wrong");
    }

    #[test]
    fn test_job_error_display_time_error() {
        let err = JobError::TimeError;
        assert_eq!(format!("{err}"), "JobError::TimeError");
    }
}
