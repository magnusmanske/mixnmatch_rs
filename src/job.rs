use crate::app_state::{AppContext, AppState, ExternalServicesContext};
use crate::automatch::AutoMatch;
use crate::autoscrape::Autoscrape;
use crate::auxiliary_matcher::AuxiliaryMatcher;
use crate::code_fragment;
use crate::coordinate_matcher::CoordinateMatcher;
use crate::job_progress::{
    JobProgress, is_yielded, merge_offset_into_json, merge_progress_into_json,
    merge_progress_with_cursor_into_json, merge_yielded_into_json, strip_yielded_from_json,
};
use crate::job_row::JobRow;
use crate::job_status::JobStatus;
use crate::maintenance::Maintenance;
use crate::match_state::MatchState;
use crate::microsync::Microsync;
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
use std::sync::Arc;
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

    async fn get_last_job_data(&self) -> Option<serde_json::Value> {
        self.get_current_job()?.get_json_value().await
    }

    async fn remember_job_data(&mut self, json: &serde_json::Value) -> Result<()> {
        match self.get_current_job_mut() {
            Some(job) => job.set_json(Some(json.to_owned())).await,
            None => return Ok(()),
        }
    }

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

    /// Persist a resume cursor as `offset` in `jobs.json`, *without*
    /// publishing a progress payload. Use this when the cursor value
    /// isn't a count of processed rows — e.g. an entry_id watermark
    /// (`automatch::match_person_by_dates`). Strategies whose cursor
    /// *is* a row count should call [`report_progress`] instead so
    /// the UI can render a real counter (and a percentage if a total
    /// is known).
    ///
    /// Preserves any other keys in `jobs.json` (e.g. autoscrape's
    /// `levels` array) and clears any stale `progress` payload — see
    /// [`crate::job_progress::merge_offset_into_json`].
    async fn remember_offset(&mut self, offset: usize) -> Result<()> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        let existing = job.get_json_value().await;
        let merged = merge_offset_into_json(existing.as_ref(), offset as u64);
        job.set_json(Some(merged)).await?;
        Ok(())
    }

    /// Persist a typed progress payload to `jobs.json`, merging with the
    /// existing document. The `offset` key is kept in sync with `processed`
    /// for backward compatibility with [`get_last_job_offset`] and any
    /// external readers that already parse it directly.
    async fn report_progress(&mut self, processed: u64, total: Option<u64>) -> Result<()> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        let progress = JobProgress::from_counts(processed, total);
        let existing = job.get_json_value().await;
        let merged = merge_progress_into_json(existing.as_ref(), &progress);
        job.set_json(Some(merged)).await?;
        Ok(())
    }

    /// Like [`report_progress`] but writes an explicit resume cursor
    /// to `offset` instead of mirroring `processed`. Use when the
    /// progress counter and the resume cursor are different quantities
    /// — e.g. `automatch_by_search` shows the running row count in the
    /// UI but resumes on an entry-id watermark (keyset pagination).
    async fn report_progress_with_cursor(
        &mut self,
        processed: u64,
        total: Option<u64>,
        cursor: u64,
    ) -> Result<()> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        let progress = JobProgress::from_counts(processed, total);
        let existing = job.get_json_value().await;
        let merged = merge_progress_with_cursor_into_json(existing.as_ref(), &progress, cursor);
        job.set_json(Some(merged)).await?;
        Ok(())
    }

    async fn clear_offset(&mut self) -> Result<()> {
        match self.get_current_job_mut() {
            Some(job) => job.set_json(None).await,
            None => Ok(()),
        }
    }

    /// True iff the current job's soft deadline (see [`Job::run`]) has
    /// passed. Strategies poll this between batches; on `true` they
    /// should persist their resume cursor (via [`Self::report_progress`],
    /// [`Self::report_progress_with_cursor`], or [`Self::remember_offset`]),
    /// then call [`Self::mark_yielded`] and `return Ok(())` — the job
    /// runner re-queues as TODO from the saved cursor.
    ///
    /// Returns `false` when there is no current job (typical for unit
    /// tests that construct strategies directly).
    fn should_yield(&self) -> bool {
        self.get_current_job()
            .is_some_and(|j| j.soft_deadline_reached())
    }

    /// Flag a cooperative yield in `jobs.json` by writing `"yielded":
    /// true`, preserving every other key. The runner reads this flag
    /// after the handler returns to decide DONE vs re-queue-as-TODO.
    /// No-op when there is no current job.
    async fn mark_yielded(&mut self) -> Result<()> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        let existing = job.get_json_value().await;
        let merged = merge_yielded_into_json(existing.as_ref());
        job.set_json(Some(merged)).await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("JobError::S: {0}")]
    S(String),
    #[error("JobError::TimeError")]
    TimeError,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub data: JobRow,
    pub app: Arc<dyn AppContext>,
    pub skip_actions: Vec<String>,
    /// Wall-clock instant after which long-running handlers should
    /// cooperatively yield (persist progress, return Ok) so the next
    /// scheduling tick can resume them. Set by [`Job::run`] for action
    /// budgets above [`MIN_BUDGET_FOR_SOFT_YIELD_SECS`]; `None` outside
    /// of an active run. The hard `tokio::time::timeout` budget is the
    /// backstop for handlers that don't cooperate.
    soft_deadline: Option<std::time::Instant>,
}

impl Job {
    pub fn new(app: &AppState) -> Self {
        Self {
            data: JobRow::default(),
            app: Arc::new(app.clone()),
            skip_actions: vec![],
            soft_deadline: None,
        }
    }

    /// True iff a soft deadline is set and has been reached. Strategies
    /// poll this between batches via [`Jobbable::should_yield`]; on a
    /// `true` return they persist progress, call
    /// [`Jobbable::mark_yielded`], and `return Ok(())` to let the job
    /// runner re-queue the job from the saved offset.
    pub fn soft_deadline_reached(&self) -> bool {
        match self.soft_deadline {
            Some(d) => std::time::Instant::now() >= d,
            None => false,
        }
    }

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

    pub async fn run(&mut self) -> Result<()> {
        let catalog_id = self.get_catalog().await?;
        let action = self.get_action().await?;
        // DB override (`job_sizes.max_seconds`) wins; the compiled
        // `ACTION_TIMEOUTS_SECS` table is the fallback baseline.
        // Operators can tune any individual action without a recompile
        // by setting (or clearing) the column for that action.
        let budget = match self.app.storage().jobs_get_action_timeout(&action).await {
            Ok(Some(secs)) => secs,
            _ => action_timeout_secs(&action),
        };
        // Soft deadline = budget − grace. Long-running strategies poll
        // `Jobbable::should_yield()` between batches and bail out cleanly
        // before the hard `tokio::time::timeout` fires. The grace gives
        // the strategy time to finish the in-flight batch and persist
        // its resume cursor. Skipped for short-budget actions where the
        // hard timeout is the only sensible cap.
        self.soft_deadline = if budget >= MIN_BUDGET_FOR_SOFT_YIELD_SECS {
            Some(
                std::time::Instant::now()
                    + std::time::Duration::from_secs(budget - SOFT_DEADLINE_GRACE_SECS),
            )
        } else {
            None
        };
        // Wall-clock cap on the handler future. On timeout the inner
        // future is dropped (cancelling at the next .await point); any
        // server-side DB query left behind is mopped up by
        // `max_statement_time` and the periodic reaper. The job is
        // marked Failed via the usual `run_error` path.
        let res =
            match tokio::time::timeout(std::time::Duration::from_secs(budget), self.run_this_job())
                .await
            {
                Ok(r) => r,
                Err(_) => Err(anyhow!(
                    "job exceeded {budget}s wall-clock budget for action '{action}'"
                )),
            };
        match res {
            Ok(_) => {
                // Distinguish natural completion from cooperative yield:
                // a yielding strategy writes `"yielded": true` into
                // `jobs.json` and skips `clear_offset()`. The runner-side
                // job row is stale (the strategy mutated its own clone),
                // so re-read from the DB.
                if self.handler_yielded_in_db().await {
                    self.run_yielded(catalog_id, &action).await?;
                } else {
                    self.run_ok(catalog_id, action).await?;
                }
            }
            Err(e) => self.run_error(catalog_id, &action, &e).await?,
        }
        self.update_next_ts().await
    }

    /// Re-read `jobs.json` from the DB and check for the `"yielded": true`
    /// sentinel a cooperative-yielding handler left behind. Returns false
    /// on any read/parse error (treats unknown as natural completion —
    /// the conservative choice; the worst case is one re-run).
    async fn handler_yielded_in_db(&self) -> bool {
        let job_id = match self.get_id().await {
            Ok(id) => id,
            Err(_) => return false,
        };
        let row = match self.app.storage().jobs_row_from_id(job_id).await {
            Ok(r) => r,
            Err(_) => return false,
        };
        let Some(s) = row.json.as_deref() else {
            return false;
        };
        match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => is_yielded(&v),
            Err(_) => false,
        }
    }

    /// Cooperative yield path: strip the `"yielded"` sentinel from
    /// `jobs.json` (preserving offset/progress/levels), re-set status to
    /// TODO so the next scheduling tick resumes from the saved cursor.
    /// Note (status, not failure) so the UI doesn't render it as a
    /// failure.
    async fn run_yielded(&mut self, catalog_id: usize, action: &str) -> Result<()> {
        let job_id = self.get_id().await?;
        // Refresh `self.data.json` from DB so we strip from the latest
        // persisted JSON (the strategy mutated its own clone).
        if let Ok(row) = self.app.storage().jobs_row_from_id(job_id).await {
            self.data = row;
        }
        let current = self.get_json_value().await;
        let stripped = current
            .as_ref()
            .and_then(strip_yielded_from_json);
        self.set_json(stripped).await?;
        self.set_status(JobStatus::Todo).await?;
        self.set_note(Some(format!(
            "yielded near {action} wall-clock budget; will resume from saved cursor"
        )))
        .await?;
        info!(
            "Job {job_id} catalog {catalog_id}:{action} YIELDED (re-queued as TODO)"
        );
        Ok(())
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
        let note = Some(format!("{error:#}"));
        self.set_note(note).await?;
        let job_id = self.get_id().await?;
        info!("Job {job_id} catalog {catalog_id}:{action} FAILED: {error:#}");
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
        if catalog_id > 0 {
            // Best-effort: announcement failure must never fail the job
            // (the job itself already succeeded). Swallow + log.
            if let Err(e) = self.maybe_announce_first_fill(catalog_id).await {
                info!(
                    "Job catalog {catalog_id} first-fill check skipped: {e:#}"
                );
            }
        }
        Ok(())
    }

    /// Fire the one-shot first-fill announcement iff (a) the catalog was
    /// marked `announce_first_fill=pending` at creation time and (b) it now
    /// has at least one entry. The CAS in
    /// [`Storage::try_consume_first_fill_pending`] guarantees at most one
    /// announcement per catalog under concurrent job completions.
    async fn maybe_announce_first_fill(&self, catalog_id: usize) -> Result<()> {
        let storage = self.app.storage();
        let count = storage.number_of_entries_in_catalog(catalog_id).await?;
        if count == 0 {
            return Ok(());
        }
        if !storage.try_consume_first_fill_pending(catalog_id).await? {
            return Ok(());
        }
        crate::announce::announce_first_fill(self.app.as_ref(), catalog_id, count).await
    }

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

    pub async fn set_note(&mut self, note: Option<String>) -> Result<()> {
        let job_id = self.get_id().await?;
        let note_cloned = self.app.storage().jobs_set_note(note, job_id).await?;
        self.put_note(note_cloned).await?;
        Ok(())
    }

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
    pub async fn get_json_value(&self) -> Option<serde_json::Value> {
        serde_json::from_str(self.get_json().await.ok()?.as_ref()?).ok()
    }

    pub async fn queue_simple_job(
        app: &dyn ExternalServicesContext,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
    ) -> Result<usize> {
        Self::queue_simple_job_for_user(app, catalog_id, action, depends_on, 0).await
    }

    pub async fn queue_simple_job_for_user(
        app: &dyn ExternalServicesContext,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
        user_id: usize,
    ) -> Result<usize> {
        app.storage()
            .jobs_queue_simple_job(
                catalog_id,
                action,
                depends_on,
                "TODO",
                TimeStamp::now(),
                user_id,
            )
            .await
    }

    /// Sets the value for `json` locally and in database, from a `serde_json::Value`
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

    async fn data(&self) -> Result<JobRow> {
        Ok(self.data.clone())
    }
    pub async fn get_id(&self) -> Result<usize> {
        Ok(self.data.id)
    }
    pub async fn get_action(&self) -> Result<String> {
        Ok(self.data.action.clone())
    }
    async fn get_catalog(&self) -> Result<usize> {
        Ok(self.data.catalog)
    }
    async fn get_json(&self) -> Result<Option<String>> {
        Ok(self.data.json.clone())
    }

    async fn put_status(&mut self, status: JobStatus) -> Result<()> {
        self.data.status = status;
        Ok(())
    }

    async fn put_json(&mut self, json: Option<String>) -> Result<()> {
        self.data.json = json;
        Ok(())
    }

    async fn put_note(&mut self, note: Option<String>) -> Result<()> {
        self.data.note = note;
        Ok(())
    }

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

    pub async fn get_next_high_priority_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::HighPriority, None, &self.skip_actions, None)
            .await
    }

    async fn get_next_low_priority_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::LowPriority, None, &self.skip_actions, None)
            .await
    }

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

    async fn get_next_initial_job(&self) -> Option<usize> {
        self.app
            .storage()
            .jobs_get_next_job(JobStatus::Todo, None, &self.skip_actions, None)
            .await
    }

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
type JobHandlerFn = for<'a> fn(&'a mut Job, usize) -> BoxFuture<'a, Result<()>>;

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
        (
            $action,
            ((|job, catalog_id| {
                Box::pin(async move {
                    let mut am = AutoMatch::new(Arc::clone(&job.app));
                    am.set_current_job(job);
                    crate::automatch::run_matcher_for_action($action, &mut am, catalog_id)
                        .await
                        .ok_or_else(|| anyhow!("Matcher registry missing action: {}", $action))?
                })
            }) as JobHandlerFn),
        )
    };
}

/// `(action, |job, cat| ... m.<method>().await)` for the parameter-free
/// Maintenance methods (~70% of the maintenance group).
macro_rules! maintenance_no_arg {
    ($action:literal, $method:ident) => {
        (
            $action,
            ((|job, _catalog_id| {
                Box::pin(async move { Maintenance::new(Arc::clone(&job.app)).$method().await })
            }) as JobHandlerFn),
        )
    };
}

/// `(action, |job, cat| ... m.<method>(cat).await)` for the Maintenance
/// methods that take catalog_id.
macro_rules! maintenance_with_cat {
    ($action:literal, $method:ident) => {
        (
            $action,
            ((|job, catalog_id| {
                Box::pin(async move {
                    Maintenance::new(Arc::clone(&job.app))
                        .$method(catalog_id)
                        .await
                })
            }) as JobHandlerFn),
        )
    };
}

/// Wall-clock budget for the default action. One hour is comfortably
/// above the 99th-percentile runtime of most short actions; anything
/// expected to run longer should be listed in [`ACTION_TIMEOUTS_SECS`].
const DEFAULT_ACTION_TIMEOUT_SECS: u64 = 3_600;

/// Headroom subtracted from the wall-clock budget to compute the soft
/// deadline. Sized to comfortably cover one in-flight batch + a
/// `set_json` round-trip, so a strategy that polls `should_yield()`
/// between batches can persist its cursor and return Ok before the
/// hard `tokio::time::timeout` fires.
const SOFT_DEADLINE_GRACE_SECS: u64 = 60;

/// Below this budget, the soft-deadline yield is disabled — the hard
/// `tokio::time::timeout` is the only cap. Avoids the degenerate case
/// of a 60s job yielding on every batch.
const MIN_BUDGET_FOR_SOFT_YIELD_SECS: u64 = 300;

/// Per-action overrides to [`DEFAULT_ACTION_TIMEOUT_SECS`]. Tunes the
/// budget up for long-running scrape / match jobs and (currently) does
/// not tune anything down — the default is already generous. Operators
/// adding a new long action should add it here rather than relaxing the
/// default.
#[rustfmt::skip]
const ACTION_TIMEOUTS_SECS: &[(&str, u64)] = &[
    ("autoscrape",                   28_800), // 8 h: paginated external sites
    ("bespoke_scraper",              14_400), // 4 h: per-catalog scrapers
    ("automatch",                     7_200), // 2 h: WDQS-heavy
    ("automatch_by_search",           7_200), // 2 h: one Wikidata search API call per unique (label, type) pair
    ("automatch_complex",             7_200),
    ("automatch_from_other_catalogs", 7_200),
    ("auxiliary_matcher",             7_200),
    ("aux2wd",                        7_200),
    ("microsync",                    14_400), // 4 h: SPARQL covers every WD item with the catalog's property
    ("taxon_matcher",                 7_200),
    ("match_by_coordinates",          7_200),
    ("sync_from_cersei",              7_200),
    ("update_from_tabbed_file",      28_800), // 8 h: bulk import of large flat-file catalogs
    ("wdrc_sync",                     7_200),
    ("update_property_cache",         7_200),
];

/// Resolve an action name to its wall-clock budget in seconds. Falls
/// back to [`DEFAULT_ACTION_TIMEOUT_SECS`] for unlisted actions.
fn action_timeout_secs(action: &str) -> u64 {
    ACTION_TIMEOUTS_SECS
        .iter()
        .find(|(a, _)| *a == action)
        .map(|(_, secs)| *secs)
        .unwrap_or(DEFAULT_ACTION_TIMEOUT_SECS)
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
        Maintenance::new(Arc::clone(&job.app))
            .crossmatch_via_aux()
            .await
            .map(|n| log::info!("crossmatch_via_aux: {n} new match(es)"))
    })) as JobHandlerFn),
    ("maintenance_sanity_check_date_matches_are_human", (|job, _cat| Box::pin(async move {
        Maintenance::new(Arc::clone(&job.app))
            .sanity_check_date_matches_are_human()
            .await
            .map(|n| log::info!("sanity_check_date_matches_are_human: removed {n}"))
    })) as JobHandlerFn),
    ("fix_disambig", (|job, catalog_id| Box::pin(async move {
        Maintenance::new(Arc::clone(&job.app))
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
        Maintenance::new(Arc::clone(&job.app))
            .fix_redirects(catalog_id, &MatchState::any_matched())
            .await
    })) as JobHandlerFn),

    // --- Subsystems with set_current_job + special construction ---
    ("autoscrape", (|job, catalog_id| Box::pin(async move {
        let mut autoscrape = Autoscrape::new(catalog_id, Arc::clone(&job.app)).await?;
        autoscrape.set_current_job(job);
        autoscrape.run().await
    })) as JobHandlerFn),
    ("aux2wd", (|job, catalog_id| Box::pin(async move {
        let mut am = AuxiliaryMatcher::new(Arc::clone(&job.app));
        am.set_current_job(job);
        am.add_auxiliary_to_wikidata(catalog_id).await
    })) as JobHandlerFn),
    ("auxiliary_matcher", (|job, catalog_id| Box::pin(async move {
        let mut am = AuxiliaryMatcher::new(Arc::clone(&job.app));
        am.set_current_job(job);
        am.match_via_auxiliary(catalog_id).await
    })) as JobHandlerFn),
    ("taxon_matcher", (|job, catalog_id| Box::pin(async move {
        let mut tm = TaxonMatcher::new(Arc::clone(&job.app));
        tm.set_current_job(job);
        tm.match_taxa(catalog_id).await
    })) as JobHandlerFn),
    ("update_from_tabbed_file", (|job, catalog_id| Box::pin(async move {
        let mut uc = UpdateCatalog::new(Arc::clone(&job.app));
        uc.set_current_job(job);
        uc.update_from_tabbed_file(catalog_id).await
    })) as JobHandlerFn),
    ("microsync", (|job, catalog_id| Box::pin(async move {
        let mut ms = Microsync::new(Arc::clone(&job.app));
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
        crate::issue::Issue::fix_wd_duplicates(&*job.app).await
    })) as JobHandlerFn),
    ("update_issues", (|job, _cat| Box::pin(async move {
        crate::issue::Issue::sweep_open(&*job.app).await
    })) as JobHandlerFn),
    ("wdrc_sync", (|job, _cat| Box::pin(async move {
        job.app.wdrc().sync(&*job.app).await
    })) as JobHandlerFn),
    ("sync_wd_matches", (|job, _cat| Box::pin(async move {
        crate::wd_match_sync::classify_pending(&*job.app, crate::wd_match_sync::DEFAULT_BATCH_SIZE)
            .await
            .map(|stats| log::info!("sync_wd_matches: {stats}"))
    })) as JobHandlerFn),
    ("push_wd_matches_to_wikidata", (|job, _cat| Box::pin(async move {
        crate::wd_match_sync::push_wd_missing(&*job.app, crate::wd_match_sync::DEFAULT_BATCH_SIZE)
            .await
            .map(|stats| log::info!("push_wd_matches_to_wikidata: {stats}"))
    })) as JobHandlerFn),
    ("bespoke_scraper", (|job, catalog_id| Box::pin(async move {
        crate::bespoke_scrapers::run_bespoke_scraper(catalog_id, Arc::clone(&job.app)).await
    })) as JobHandlerFn),
    ("import_aux_from_url", (|job, catalog_id| Box::pin(async move {
        PhpWrapper::import_aux_from_url(catalog_id, &*job.app)
    })) as JobHandlerFn),

    // --- Lua-with-PHP-fallback handlers ---
    //
    // Fallback policy: PHP is used only when no Lua code is registered for
    // the catalog (signalled by `LuaJobOutcome::NoLuaCode`). If Lua exists
    // but fails at runtime, the error is returned — falling back to PHP
    // there would mask the bug.
    ("update_person_dates", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_person_dates_job(catalog_id, &*job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::update_person_dates(catalog_id, &*job.app)
            }
        }
    })) as JobHandlerFn),
    ("generate_aux_from_description", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_aux_from_desc_job(catalog_id, &*job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::generate_aux_from_description(catalog_id, &*job.app)
            }
        }
    })) as JobHandlerFn),
    ("update_descriptions_from_url", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_desc_from_html_job(catalog_id, &*job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::update_descriptions_from_url(catalog_id, &*job.app)
            }
        }
    })) as JobHandlerFn),
    ("update_coordinates_from_url", (|job, catalog_id| Box::pin(async move {
        match code_fragment::run_coords_from_html_job(catalog_id, &*job.app).await? {
            code_fragment::LuaJobOutcome::Done => Ok(()),
            code_fragment::LuaJobOutcome::NoLuaCode => {
                PhpWrapper::update_coordinates_from_url(catalog_id, &*job.app)
            }
        }
    })) as JobHandlerFn),

    // --- Misc ---
    ("match_by_coordinates", (|job, catalog_id| Box::pin(async move {
        let cm = CoordinateMatcher::new(Arc::clone(&job.app), Some(catalog_id)).await?;
        cm.run().await
    })) as JobHandlerFn),
    ("sync_from_cersei", (|job, _cat| Box::pin(async move {
        let cs = crate::cersei::CerseiSync::new(Arc::clone(&job.app))?;
        cs.sync().await
    })) as JobHandlerFn),
    ("reference_fixer", (|job, _cat| Box::pin(async move {
        // Rewrites free-form reference-URL references into typed
        // external-id references on Wikidata. Drains the
        // `reference_fixer` queue populated by every successful
        // `entry.set_match`. Catalog id is 0 (not catalog-scoped).
        let mut rf = crate::reference_fixer::ReferenceFixer::new(Arc::clone(&job.app))?;
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
            "update_coordinates_from_url",
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
    fn action_timeout_secs_returns_override_for_listed_action() {
        assert_eq!(action_timeout_secs("autoscrape"), 28_800);
        assert_eq!(action_timeout_secs("automatch"), 7_200);
        // Pinned: large catalogs trip the 1-hour default — the SPARQL alone
        // returns every WD item with the catalog's property and the chunked
        // diff walk is then O(n_items).
        assert_eq!(action_timeout_secs("microsync"), 14_400);
    }

    #[test]
    fn action_timeout_secs_returns_default_for_unlisted_action() {
        assert_eq!(
            action_timeout_secs("definitely_not_a_real_action"),
            DEFAULT_ACTION_TIMEOUT_SECS
        );
    }

    /// Every override must name an action that actually exists in the
    /// handler registry — otherwise the entry is a typo doing nothing.
    #[test]
    fn action_timeouts_only_reference_registered_actions() {
        let registered: HashSet<&'static str> =
            JOB_HANDLER_REGISTRY.iter().map(|(n, _)| *n).collect();
        for (action, _) in ACTION_TIMEOUTS_SECS {
            assert!(
                registered.contains(action),
                "ACTION_TIMEOUTS_SECS references unknown action: {action}"
            );
        }
    }

    /// Times must be plausibly bounded — neither zero (every job would
    /// instantly time out) nor obviously copy-pasted from a different unit.
    #[test]
    fn action_timeouts_are_within_sane_range() {
        const ONE_MINUTE: u64 = 60;
        const ONE_DAY: u64 = 86_400;
        for (action, secs) in ACTION_TIMEOUTS_SECS {
            assert!(
                *secs >= ONE_MINUTE && *secs <= ONE_DAY,
                "ACTION_TIMEOUTS_SECS[{action}]={secs} is outside [60, 86_400]"
            );
        }
        let default = DEFAULT_ACTION_TIMEOUT_SECS;
        assert!((ONE_MINUTE..=ONE_DAY).contains(&default));
    }

    #[test]
    fn test_job_error_display_time_error() {
        let err = JobError::TimeError;
        assert_eq!(format!("{err}"), "JobError::TimeError");
    }

    // Soft-deadline / cooperative yield ─────────────────────────────────

    #[test]
    fn soft_deadline_reached_false_when_unset() {
        let app = get_test_app();
        let job = Job::new(&app);
        assert!(!job.soft_deadline_reached());
    }

    #[test]
    fn soft_deadline_reached_false_when_in_future() {
        let app = get_test_app();
        let mut job = Job::new(&app);
        job.soft_deadline =
            Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        assert!(!job.soft_deadline_reached());
    }

    #[test]
    fn soft_deadline_reached_true_when_past() {
        let app = get_test_app();
        let mut job = Job::new(&app);
        job.soft_deadline =
            Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        assert!(job.soft_deadline_reached());
    }

    /// Grace must be strictly less than the minimum yield-eligible budget,
    /// otherwise eligible jobs would be born already past the soft deadline.
    /// Compile-time check via a `const` block — also a unit test so a
    /// reader scanning the test list can see the invariant exists.
    #[test]
    fn soft_deadline_grace_is_below_min_budget() {
        const { assert!(SOFT_DEADLINE_GRACE_SECS < MIN_BUDGET_FOR_SOFT_YIELD_SECS) };
    }

    /// Test-only `Jobbable` impl: lets us exercise `should_yield` /
    /// `mark_yielded` defaults without spinning a full subsystem.
    struct YieldHarness {
        job: Option<Job>,
    }

    #[async_trait]
    impl Jobbable for YieldHarness {
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

    #[tokio::test]
    async fn should_yield_false_without_current_job() {
        let h = YieldHarness { job: None };
        assert!(!h.should_yield());
    }

    #[tokio::test]
    async fn should_yield_tracks_jobs_soft_deadline() {
        let app = get_test_app();
        let mut job = Job::new(&app);
        let mut h = YieldHarness { job: None };
        // Deadline in the future → no yield.
        job.soft_deadline =
            Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        h.set_current_job(&job);
        assert!(!h.should_yield());
        // Deadline in the past → yield.
        job.soft_deadline =
            Some(std::time::Instant::now() - std::time::Duration::from_secs(1));
        h.set_current_job(&job);
        assert!(h.should_yield());
    }

    #[tokio::test]
    async fn mark_yielded_is_noop_without_current_job() {
        let mut h = YieldHarness { job: None };
        assert!(h.mark_yielded().await.is_ok());
    }

    /// End-to-end: a yielded handler must leave the job as TODO (not
    /// DONE), strip the `yielded` sentinel from `jobs.json`, and
    /// preserve the resume cursor for the next run.
    #[tokio::test]
    async fn run_yielded_requeues_as_todo_and_strips_flag() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::unique_catalog_id();
        let job_id = test_support::seed_job("automatch_by_search", catalog_id)
            .await
            .unwrap();
        // Seed the job-state a yielding strategy would have written: a
        // resume cursor and a progress payload, plus the `yielded`
        // sentinel.
        let yielded_state = serde_json::json!({
            "offset": 12345,
            "progress": {"processed": 12345, "total": 100000, "percent": 12.345},
            "yielded": true
        });
        app.storage()
            .jobs_set_json(job_id, yielded_state.to_string(), &TimeStamp::now())
            .await
            .unwrap();
        let mut job = Job::new(&app);
        job.set_from_id(job_id).await.unwrap();
        job.run_yielded(catalog_id, "automatch_by_search")
            .await
            .unwrap();

        // Re-read so we observe the persisted state.
        let row = app.storage().jobs_row_from_id(job_id).await.unwrap();
        assert_eq!(row.status, JobStatus::Todo);
        assert!(
            row.note
                .as_deref()
                .unwrap_or("")
                .contains("yielded near automatch_by_search"),
            "note should explain the requeue; got {:?}",
            row.note
        );
        let json: serde_json::Value =
            serde_json::from_str(row.json.as_deref().unwrap_or("null")).unwrap();
        assert_eq!(json.get("offset"), Some(&serde_json::json!(12345)));
        assert!(json.get("progress").is_some());
        assert!(
            json.get("yielded").is_none(),
            "yielded flag must be stripped; got {json:?}"
        );
    }

    /// `handler_yielded_in_db` is the runner's signal to take the
    /// yield branch in `run`. It must return true when `jobs.json`
    /// has the sentinel, false otherwise — including the empty-json
    /// (natural completion) case.
    #[tokio::test]
    async fn handler_yielded_in_db_reflects_sentinel_presence() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::unique_catalog_id();
        let job_id = test_support::seed_job("automatch_by_search", catalog_id)
            .await
            .unwrap();
        let mut job = Job::new(&app);
        job.set_from_id(job_id).await.unwrap();

        // No JSON → no yield.
        assert!(!job.handler_yielded_in_db().await);

        // Yielded sentinel present → yield detected.
        app.storage()
            .jobs_set_json(
                job_id,
                serde_json::json!({"offset": 1, "yielded": true}).to_string(),
                &TimeStamp::now(),
            )
            .await
            .unwrap();
        assert!(job.handler_yielded_in_db().await);

        // Sentinel cleared → no yield.
        app.storage()
            .jobs_set_json(
                job_id,
                serde_json::json!({"offset": 1}).to_string(),
                &TimeStamp::now(),
            )
            .await
            .unwrap();
        assert!(!job.handler_yielded_in_db().await);
    }

    // ---------------------------------------------------------------
    // First-fill announcement orchestration in Job::run_ok
    //
    // `maybe_announce_first_fill` is the load-bearing predicate: it must
    // consume the pending marker iff entries > 0, must be idempotent, and
    // must be a no-op for catalogs created outside the storage layer
    // (which have no marker — covers pre-existing production catalogs).
    //
    // Tests use `seed_minimal_entry` (which inserts an explicit-id catalog
    // *with* one entry) plus a manual `set_catalog_kv` to install the
    // marker. They deliberately avoid `create_catalog_from_meta` because
    // its auto-id INSERT advances `catalog.AUTO_INCREMENT` into the same
    // range as `test_support::unique_catalog_id()`, causing collisions in
    // other tests sharing the MariaDB container.
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn maybe_announce_first_fill_consumes_marker_on_first_call() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        app.storage()
            .set_catalog_kv(catalog_id, "announce_first_fill", "pending")
            .await
            .unwrap();
        let job = Job::new(&app);

        job.maybe_announce_first_fill(catalog_id).await.unwrap();
        let kv_after_first = app
            .storage()
            .get_catalog_key_value_pairs(catalog_id)
            .await
            .unwrap();
        assert_eq!(
            kv_after_first.get("announce_first_fill").map(String::as_str),
            Some("done"),
            "marker must be consumed on the first job completion with entries present"
        );

        // Idempotent: second call must not change anything (no re-announce).
        job.maybe_announce_first_fill(catalog_id).await.unwrap();
        let kv_after_second = app
            .storage()
            .get_catalog_key_value_pairs(catalog_id)
            .await
            .unwrap();
        assert_eq!(
            kv_after_second.get("announce_first_fill").map(String::as_str),
            Some("done")
        );
    }

    #[tokio::test]
    async fn maybe_announce_first_fill_leaves_marker_pending_when_no_entries() {
        // Catalog marked pending but zero entries — the announce must be
        // deferred to a later successful job. The CAS is gated on
        // entries > 0 *before* the flip, so the marker stays pending.
        // Seed via raw_conn to get a catalog *without* any entries.
        let app = test_support::test_app().await;
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let catalog_id = test_support::unique_catalog_id();
        use mysql_async::prelude::*;
        conn.exec_drop(
            "INSERT INTO catalog \
             (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run) \
             VALUES (:id, :name, '', '', 'person', 'en', 1, 0, '', '', 0)",
            mysql_async::params! {
                "id"   => catalog_id,
                "name" => format!("empty_first_fill_{catalog_id}"),
            },
        )
        .await
        .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        app.storage()
            .set_catalog_kv(catalog_id, "announce_first_fill", "pending")
            .await
            .unwrap();

        let job = Job::new(&app);
        job.maybe_announce_first_fill(catalog_id).await.unwrap();
        let kv = app
            .storage()
            .get_catalog_key_value_pairs(catalog_id)
            .await
            .unwrap();
        assert_eq!(
            kv.get("announce_first_fill").map(String::as_str),
            Some("pending"),
            "empty-catalog completion must not consume the marker — try again next job"
        );
    }

    #[tokio::test]
    async fn maybe_announce_first_fill_is_noop_for_unmarked_catalog() {
        // A pre-existing catalog (no marker row) must never trigger the
        // announcement, no matter how many entries it has.
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let job = Job::new(&app);
        job.maybe_announce_first_fill(catalog_id).await.unwrap();
        let kv = app
            .storage()
            .get_catalog_key_value_pairs(catalog_id)
            .await
            .unwrap();
        assert!(
            !kv.contains_key("announce_first_fill"),
            "unmarked catalog must not gain an announce_first_fill row"
        );
    }
}
