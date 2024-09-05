use crate::app_state::AppState;
use crate::automatch::*;
use crate::autoscrape::*;
use crate::auxiliary_matcher::*;
use crate::coordinate_matcher::CoordinateMatcher;
use crate::maintenance::*;
use crate::match_state::MatchState;
use crate::microsync::*;
use crate::php_wrapper::*;
use crate::taxon_matcher::*;
use crate::update_catalog::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Duration;
use chrono::Local;
use serde_json::json;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use wikimisc::timestamp::TimeStamp;

#[derive(Eq, Clone, Debug)]
pub enum TaskSize {
    TINY,
    SMALL,
    MEDIUM,
    LARGE,
    GINORMOUS,
}

impl Ord for TaskSize {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

impl PartialOrd for TaskSize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TaskSize {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}

impl TaskSize {
    pub fn value(&self) -> u8 {
        match self {
            TaskSize::TINY => 1,
            TaskSize::SMALL => 2,
            TaskSize::MEDIUM => 3,
            TaskSize::LARGE => 4,
            TaskSize::GINORMOUS => 5,
        }
    }

    pub fn new(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "tiny" => Some(Self::TINY),
            "small" => Some(Self::SMALL),
            "medium" => Some(Self::MEDIUM),
            "large" => Some(Self::LARGE),
            "ginormous" => Some(Self::GINORMOUS),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum JobStatus {
    #[default]
    Todo,
    Done,
    Failed,
    Running,
    HighPriority,
    LowPriority,
    Blocked,
}

impl JobStatus {
    pub fn new(s: &str) -> Option<Self> {
        match s {
            "TODO" => Some(JobStatus::Todo),
            "DONE" => Some(JobStatus::Done),
            "FAILED" => Some(JobStatus::Failed),
            "RUNNING" => Some(JobStatus::Running),
            "HIGH_PRIORITY" => Some(JobStatus::HighPriority),
            "LOW_PRIORITY" => Some(JobStatus::LowPriority),
            "BLOCKED" => Some(JobStatus::Blocked),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &str {
        match *self {
            JobStatus::Todo => "TODO",
            JobStatus::Done => "DONE",
            JobStatus::Failed => "FAILED",
            JobStatus::Running => "RUNNING",
            JobStatus::HighPriority => "HIGH_PRIORITY",
            JobStatus::LowPriority => "LOW_PRIORITY",
            JobStatus::Blocked => "BLOCKED",
        }
    }
}

/// A trait that allows to manage temporary job data (eg offset)
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
        match json.as_object() {
            Some(o) => match o.get("offset") {
                Some(offset) => offset.as_u64().unwrap_or(0) as usize,
                None => 0,
            },
            None => 0,
        }
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

type JobRowMySql = (
    usize,
    String,
    usize,
    Option<String>,
    Option<usize>,
    String,
    String,
    Option<String>,
    Option<usize>,
    String,
    usize,
);

#[derive(Debug, Clone, Default)]
pub struct JobRow {
    pub id: usize,
    pub action: String,
    pub catalog: usize,
    pub json: Option<String>,
    pub depends_on: Option<usize>,
    pub status: JobStatus,
    pub last_ts: String,
    pub note: Option<String>,
    pub repeat_after_sec: Option<usize>,
    pub next_ts: String,
    pub user_id: usize,
}

impl JobRow {
    pub fn from_row(x: JobRowMySql) -> Self {
        Self {
            id: x.0,
            action: x.1,
            catalog: x.2,
            json: x.3,
            depends_on: x.4,
            status: JobStatus::new(&x.5).unwrap_or(JobStatus::Todo),
            last_ts: x.6,
            note: x.7,
            repeat_after_sec: x.8,
            next_ts: x.9,
            user_id: x.10,
        }
    }

    pub fn new(action: &str, catalog_id: usize) -> JobRow {
        Self {
            id: 0,
            action: action.to_string(),
            catalog: catalog_id,
            json: None,
            depends_on: None,
            status: JobStatus::Todo,
            last_ts: TimeStamp::now(),
            note: None,
            repeat_after_sec: None,
            next_ts: "".to_string(),
            user_id: 0,
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
        println!("Job {job_id} catalog {catalog_id}:{action} FAILED: {error}");
        Ok(())
    }

    async fn run_ok(&mut self, catalog_id: usize, action: String) -> Result<(), anyhow::Error> {
        self.set_status(JobStatus::Done).await?;
        println!(
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

    /// Returns the current `json` as an Option<serde_json::Value>
    //TODO test
    pub async fn get_json_value(&self) -> Option<serde_json::Value> {
        serde_json::from_str(self.get_json().await.ok()?.as_ref()?).ok()
    }

    //TODO test
    pub async fn queue_simple_job(
        app: &AppState,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
    ) -> Result<usize> {
        app.storage()
            .jobs_queue_simple_job(catalog_id, action, depends_on, "TODO", TimeStamp::now())
            .await
    }

    /// Sets the value for `json` locally and in database, from a serde_json::Value
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
    // #lizard forgives the complexity
    async fn run_this_job(&mut self) -> Result<()> {
        // let json = self.get_json().await;
        // println!("STARTING {:?} with option {:?}", &self.data().await?,&json);
        if self.data.status == JobStatus::Blocked {
            return Err(anyhow!("Job::run_this_job: Blocked"));
        }
        let current_time_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        println!("{current_time_str}: Starting job {:?}", self.get_id().await);
        let catalog_id = self.get_catalog().await?;
        match self.get_action().await?.as_str() {
            "automatch" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_simple(catalog_id).await
            }
            "automatch_by_search" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_by_search(catalog_id).await
            }
            "automatch_from_other_catalogs" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_from_other_catalogs(catalog_id).await
            }
            "automatch_by_sitelink" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_by_sitelink(catalog_id).await
            }
            "automatch_creations" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_creations(catalog_id).await
            }
            "automatch_complex" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.automatch_complex(catalog_id).await
            }
            "purge_automatches" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.purge_automatches(catalog_id).await
            }
            "match_person_dates" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.match_person_by_dates(catalog_id).await
            }
            "match_on_birthdate" => {
                let mut am = AutoMatch::new(&self.app);
                am.set_current_job(self);
                am.match_person_by_single_date(catalog_id).await
            }
            "autoscrape" => {
                let mut autoscrape = Autoscrape::new(catalog_id, &self.app).await?;
                autoscrape.set_current_job(self);
                autoscrape.run().await
            }
            "aux2wd" => {
                let mut am = AuxiliaryMatcher::new(&self.app);
                am.set_current_job(self);
                am.add_auxiliary_to_wikidata(catalog_id).await
            }
            "auxiliary_matcher" => {
                let mut am = AuxiliaryMatcher::new(&self.app);
                am.set_current_job(self);
                am.match_via_auxiliary(catalog_id).await
            }
            "taxon_matcher" => {
                let mut tm = TaxonMatcher::new(&self.app);
                tm.set_current_job(self);
                tm.match_taxa(catalog_id).await
            }
            "update_from_tabbed_file" => {
                let mut uc = UpdateCatalog::new(&self.app);
                uc.set_current_job(self);
                uc.update_from_tabbed_file(catalog_id).await
            }
            "microsync" => {
                let mut ms = Microsync::new(&self.app);
                ms.set_current_job(self);
                let catalog_id = match catalog_id {
                    0 => {
                        match self
                            .app
                            .storage()
                            .get_random_active_catalog_id_with_property()
                            .await
                        {
                            Some(id) => id,
                            None => return Ok(()), // Ignore, very unlikely
                        }
                    }
                    other => other,
                };
                ms.check_catalog(catalog_id).await
            }
            "fix_disambig" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance
                    .unlink_meta_items(catalog_id, &MatchState::any_matched())
                    .await
            }

            "fix_redirected_items_in_catalog" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance
                    .fix_redirects(catalog_id, &MatchState::any_matched())
                    .await
            }

            "maintenance_automatch" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance.automatch().await
            }

            "maintenance_inventory_match" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance
                    .fully_match_via_collection_inventory_number()
                    .await
            }

            "automatch_people_via_year_born" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance.automatch_people_via_year_born().await
            }

            "remove_p17_for_humans" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance.remove_p17_for_humans().await
            }

            "cleanup_mnm_relations" => {
                let maintenance = Maintenance::new(&self.app);
                maintenance.cleanup_mnm_relations().await
            }

            "wdrc_sync" => self.app.wdrc().sync(&self.app).await,
            // Maintenance::new(&self.app).wdrc_sync().await,
            "update_person_dates" => PhpWrapper::update_person_dates(catalog_id),
            "generate_aux_from_description" => {
                PhpWrapper::generate_aux_from_description(catalog_id)
            }
            "bespoke_scraper" => PhpWrapper::bespoke_scraper(catalog_id),
            "import_aux_from_url" => PhpWrapper::import_aux_from_url(catalog_id),
            "update_descriptions_from_url" => PhpWrapper::update_descriptions_from_url(catalog_id),
            "match_by_coordinates" => {
                let cm = CoordinateMatcher::new(&self.app, Some(catalog_id)).await?;
                cm.run().await
            }

            other => Err(anyhow!("Job::run_this_job: Unknown action '{}'", other)),
        }
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
        let seconds = Duration::try_seconds(seconds).unwrap();
        let utc = TimeStamp::from_str(&self.data().await?.last_ts.clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    const _TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_set_from_id() {
        let app = get_test_app();
        let mut job = Job::new(&app);
        job.set_from_id(1).await.unwrap();
        assert_eq!(job.get_id().await.unwrap(), 1);
        assert_eq!(job.get_catalog().await.unwrap(), 2930);
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
    fn test_task_size() {
        assert!(TaskSize::TINY < TaskSize::SMALL);
        assert!(TaskSize::SMALL < TaskSize::MEDIUM);
        assert!(TaskSize::MEDIUM < TaskSize::LARGE);
        assert!(TaskSize::LARGE < TaskSize::GINORMOUS);
    }
}
