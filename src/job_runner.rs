//! Background job runner — `forever_loop` and friends.
//!
//! Previously lived as inherent methods on `AppState`. Lifted here so
//! `AppState` is a *runtime container* (storage handle, Wikidata
//! sessions, config) and `JobRunner` is the *user* of that container
//! that owns the dispatch loop, the periodic supervisors (`seppuku`,
//! `reaper`), and the per-action concurrency-cap policy. A webserver
//! process that never runs jobs no longer drags this code into its
//! call graph by importing `AppState`.
//!
//! See `audits/code_solid.md` #10.

use crate::app_state::AppState;
use crate::job::Job;
use crate::job_status::JobStatus;
use crate::task_size::TaskSize;
use anyhow::Result;
use chrono::Local;
use dashmap::DashMap;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use sysinfo::System;
use tokio::time::sleep;
use wikimisc::timestamp::TimeStamp;

/// Per-action concurrency caps enforced by the job runner. When a given
/// action has at least this many jobs running, the SQL job picker skips
/// queued rows for that action until a slot frees up.
///
/// **`microsync` → 4.** Capped to keep the user-triggered "manual sync on
/// every catalog containing Q" pattern (GitHub #6) from saturating the
/// Wikidata terms-replica pool — each microsync runs `fix_matched_items`,
/// which in turn calls `get_deleted_items` against `wdt`. The production
/// `wdt` pool is currently `max_connections=8`, so the cap is set to half
/// the pool: enough headroom for ad-hoc one-off `wdt` reads from other
/// code paths, while doubling the previous (cap=2) parallelism now that
/// the pool itself grew. Raising further is fine if a load profile shows
/// the cap is the bottleneck.
///
/// **`wdrc_sync` → 1.** The `wdrc` MariaDB pool is intentionally tiny
/// (`max_connections=1` — it's a small auxiliary database). Two
/// concurrent `wdrc_sync` jobs would serialise on the pool anyway; the
/// cap just documents the intent and avoids dispatching a second
/// `wdrc_sync` only for it to block on a connection acquisition.
const ACTION_CONCURRENCY_CAPS: &[(&str, usize)] = &[("microsync", 4), ("wdrc_sync", 1)];

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

/// Owns the job-dispatch loop and the periodic supervisors. Construct
/// once per process (the long-running bot worker); a process that only
/// serves HTTP doesn't need to instantiate this at all.
#[derive(Debug)]
pub struct JobRunner {
    app: AppState,
}

impl JobRunner {
    pub fn new(app: AppState) -> Self {
        Self { app }
    }

    pub async fn run_single_hp_job(&self) -> Result<()> {
        let app = self.app.clone();
        let mut job = Job::new(&app);
        if let Some(job_id) = job.get_next_high_priority_job().await {
            job.set_from_id(job_id).await?;
            job.set_status(JobStatus::Running).await?;
            job.run().await?;
        }
        Ok(())
    }

    pub async fn run_single_job(&self, job_id: usize) -> Result<()> {
        let app = self.app.clone();
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

    /// Kills the app if there are jobs running but have no recent activity.
    /// Toolforge k8s "continuous job" will restart a new instance.
    fn seppuku(&self) {
        let check_every_minutes = 5;
        let max_age_min = 20;
        let app = self.app.clone();
        tokio::spawn(async move {
            loop {
                sleep(tokio::time::Duration::from_secs(60 * check_every_minutes)).await;
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
            }
        });
    }

    /// Periodically kills queries on the main storage pool that have been
    /// running longer than `REAPER_THRESHOLD_SECS`. Acts as a backstop
    /// behind the `max_statement_time` session setting (which only catches
    /// read-only SELECTs): writes that hang and any connections that
    /// somehow bypassed the session setup still get cleaned up here.
    ///
    /// Each iteration is wrapped in a `tokio::time::timeout` strictly
    /// smaller than the interval. The exact failure mode the reaper exists
    /// to recover from is pool exhaustion — but `kill_long_running_queries`
    /// itself acquires a connection from that same pool. Without the
    /// timeout, an exhausted pool blocks the reaper future indefinitely
    /// and the reaper is silently disabled until the binary restarts.
    /// With the timeout, a hung acquire surfaces as a logged error and
    /// the next tick still fires on schedule.
    fn reaper(&self) {
        const REAPER_INTERVAL_SECS: u64 = 300;
        const REAPER_THRESHOLD_SECS: u64 = 300;
        const REAPER_ITERATION_TIMEOUT_SECS: u64 = 60;
        // Compile-time guard: the per-iteration timeout must be strictly
        // less than the inter-tick sleep, otherwise two iterations can
        // overlap and pile up against the pool the timeout exists to
        // protect.
        const _: () = assert!(REAPER_ITERATION_TIMEOUT_SECS < REAPER_INTERVAL_SECS);
        let app = self.app.clone();
        tokio::spawn(async move {
            loop {
                sleep(tokio::time::Duration::from_secs(REAPER_INTERVAL_SECS)).await;
                let iteration = tokio::time::timeout(
                    tokio::time::Duration::from_secs(REAPER_ITERATION_TIMEOUT_SECS),
                    app.storage().kill_long_running_queries(REAPER_THRESHOLD_SECS),
                )
                .await;
                match iteration {
                    Ok(Ok(ids)) if !ids.is_empty() => {
                        info!(
                            "reaper: killed {} long-running queries (>{}s): {:?}",
                            ids.len(),
                            REAPER_THRESHOLD_SECS,
                            ids
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => error!("reaper: kill_long_running_queries failed: {e}"),
                    Err(_) => error!(
                        "reaper: kill_long_running_queries timed out after {REAPER_ITERATION_TIMEOUT_SECS}s — pool likely exhausted"
                    ),
                }
            }
        });
    }

    pub async fn forever_loop(&self) -> Result<()> {
        let (current_jobs, action_counts) = self.forever_loop_initalize().await?;
        let threshold_job_size = TaskSize::Medium;
        let threshold_percent = 80;
        let max_concurrent_jobs = self.app.max_concurrent_jobs();

        // TO MANUALLY FIND ACTIONS NOT ASSIGNED A TASK SIZE:
        // select distinct action from jobs where action not in (select action from job_sizes);

        info!("\n=== Starting forever loop with max_concurrent_jobs={max_concurrent_jobs}");
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
            if current_jobs_len >= max_concurrent_jobs {
                Self::hold_on().await;
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
            .app
            .storage()
            .jobs_get_next_job(JobStatus::HighPriority, None, &[], None)
            .await
        else {
            return Ok(None);
        };
        let mut job = Job::new(&self.app);
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
        let task_size = self
            .app
            .storage()
            .jobs_get_tasks()
            .await
            .unwrap_or_default();
        let job_id = job.get_id().await?;
        info!("HIGH_PRIORITY fast-path: dispatching job {job_id}");
        Self::run_job(job, task_size, current_jobs, action_counts).await;
        Ok(true)
    }

    async fn forever_loop_initalize(
        &self,
    ) -> Result<(Arc<DashMap<usize, TaskSize>>, Arc<DashMap<String, usize>>)> {
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
            .app
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
        self.app.storage().reset_running_jobs().await?;
        self.app.storage().reset_failed_jobs().await?;
        // Ensure the global periodic issue-sweep job exists. On first deployment
        // `initial_next_ts` is set to `now` so it runs soon; on subsequent restarts
        // the ON DUPLICATE KEY no-op preserves whatever period operators have set.
        const ISSUE_SWEEP_PERIOD_SECS: usize = 86400; // daily
        let initial_next_ts = wikimisc::timestamp::TimeStamp::now();
        if let Err(e) = self
            .app
            .storage()
            .ensure_periodic_global_job("update_issues", ISSUE_SWEEP_PERIOD_SECS, &initial_next_ts)
            .await
        {
            warn!("forever_loop: could not ensure periodic update_issues job: {e}");
        }
        info!("Old jobs reset, starting bot");
        self.seppuku();
        self.reaper();
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.app
            .storage()
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
                Self::hold_on().await;
            }
            Err(e) => {
                error!("MAIN LOOP: Something went wrong: {e}");
                Self::hold_on().await;
            }
        }
        Ok(())
    }

    async fn get_next_job(
        &self,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        action_counts: &Arc<DashMap<String, usize>>,
        threshold_job_size: &TaskSize,
        threshold_percent: usize,
    ) -> Result<(Job, HashMap<String, TaskSize>)> {
        let mut job = Job::new(&self.app);
        let task_size = self.app.storage().jobs_get_tasks().await?;
        let big_jobs_running = Self::count_big_jobs_running(current_jobs, threshold_job_size);
        let max_concurrent_jobs = self.app.max_concurrent_jobs();
        let max_job_size = if big_jobs_running >= max_concurrent_jobs * threshold_percent / 100 {
            *threshold_job_size
        } else {
            TaskSize::Ginormous
        };
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

    async fn hold_on() {
        sleep(time::Duration::from_secs(5)).await;
    }

    fn print_sysinfo() {
        if !sysinfo::IS_SUPPORTED_SYSTEM {
            return;
        }
        let sys = System::new_all();
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
                error!("No job ID");
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

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
        let runner = JobRunner::new(app);
        let result = runner.pick_high_priority_job().await.unwrap();
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

        let runner = JobRunner::new(app);
        let job = runner
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

        let runner = JobRunner::new(app);
        let result = runner.pick_high_priority_job().await.unwrap();
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
        let _ = seed_job_with_status("aux2wd", catalog_id, "DONE")
            .await
            .unwrap();
        let hp_id = seed_job_with_status("microsync", catalog_id, "HIGH_PRIORITY")
            .await
            .unwrap();

        let runner = JobRunner::new(app);
        let job = runner
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
        let runner = JobRunner::new(app);
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());
        let action_counts: Arc<DashMap<String, usize>> = Arc::new(DashMap::new());
        let dispatched = runner
            .forever_loop_try_dispatch_high_priority(&current_jobs, &action_counts)
            .await
            .unwrap();
        assert!(!dispatched);
        assert!(current_jobs.is_empty(), "must not have spawned anything");
    }

    /// Reads the cap for an action from the canonical
    /// [`ACTION_CONCURRENCY_CAPS`] table so tests don't drift when the
    /// production value is retuned — the assertion that matters is the
    /// *behaviour at the boundary*, not the literal number.
    fn cap_for(action: &str) -> usize {
        ACTION_CONCURRENCY_CAPS
            .iter()
            .find(|(a, _)| *a == action)
            .map(|(_, c)| *c)
            .expect("test references an action that isn't in ACTION_CONCURRENCY_CAPS")
    }

    #[test]
    fn action_cap_skips_when_at_capacity() {
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("microsync".to_string(), cap_for("microsync"));
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(skip.iter().any(|a| a == "microsync"));
    }

    #[test]
    fn action_cap_does_not_skip_below_capacity() {
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("microsync".to_string(), cap_for("microsync") - 1);
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(
            !skip.iter().any(|a| a == "microsync"),
            "running one below cap — must not skip"
        );
    }

    #[test]
    fn action_cap_does_not_double_add() {
        // If `microsync` is already in skip_actions (e.g. because it was
        // marked too-big upstream), don't append a duplicate.
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("microsync".to_string(), cap_for("microsync") + 3);
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
    fn action_cap_wdrc_sync_skips_at_one_running() {
        // wdrc pool has max_connections=1 in production, so a single
        // running wdrc_sync already saturates it.
        let counts: DashMap<String, usize> = DashMap::new();
        counts.insert("wdrc_sync".to_string(), 1);
        let mut skip: Vec<String> = vec![];
        apply_action_concurrency_caps(&mut skip, &counts);
        assert!(skip.iter().any(|a| a == "wdrc_sync"));
    }
}
