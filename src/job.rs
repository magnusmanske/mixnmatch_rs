use crate::app_state::*;
use crate::automatch::*;
use crate::autoscrape::*;
use crate::auxiliary_matcher::*;
use crate::coordinate_matcher::CoordinateMatcher;
use crate::entry::*;
use crate::maintenance::*;
use crate::microsync::*;
use crate::mixnmatch::*;
use crate::php_wrapper::*;
use crate::taxon_matcher::*;
use crate::update_catalog::*;
use async_trait::async_trait;
use chrono::Duration;
use mysql_async::from_row;
use mysql_async::prelude::*;
use serde_json::json;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

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
    async fn remember_job_data(&mut self, json: &serde_json::Value) -> Result<(), GenericError> {
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
    async fn remember_offset(&mut self, offset: usize) -> Result<(), GenericError> {
        let job = match self.get_current_job_mut() {
            Some(job) => job,
            None => return Ok(()),
        };
        // println!("{}: {offset} [{}]",job.get_id().await.unwrap_or(0), Utc::now());
        job.set_json(Some(json!({ "offset": offset }))).await?;
        Ok(())
    }

    //TODO test
    async fn clear_offset(&mut self) -> Result<(), GenericError> {
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
        write!(f, "{}", self) // user-facing output
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
            last_ts: MixNMatch::get_timestamp(),
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
    pub mnm: Arc<MixNMatch>,
    pub skip_actions: Option<Vec<String>>,
}

impl Job {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            data: JobRow::default(),
            mnm: Arc::new(mnm.clone()),
            skip_actions: None,
        }
    }

    pub async fn get_tasks(&self) -> Result<HashMap<String, TaskSize>, GenericError> {
        let sql = "SELECT `action`,`size` FROM `job_sizes`";
        let ret = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?
            .into_iter()
            .map(|(name, size)| (name, TaskSize::new(&size)))
            .filter(|(_name, size)| size.is_some())
            .map(|(name, size)| (name, size.unwrap()))
            .collect();
        Ok(ret)
    }

    //TODO test
    pub async fn set_next(&mut self) -> Result<bool, GenericError> {
        match self.get_next_job_id().await {
            Some(job_id) => self.set_from_id(job_id).await,
            None => Ok(false),
        }
    }

    pub async fn set_from_id(&mut self, job_id: usize) -> Result<bool, GenericError> {
        let sql = r"SELECT id,action,catalog,json,depends_on,status,last_ts,note,repeat_after_sec,next_ts,user_id FROM `jobs` WHERE `id`=:job_id";
        let row = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, params! {job_id})
            .await?
            .map_and_drop(
                from_row::<(
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
                )>,
            )
            .await?
            .pop()
            .ok_or(format!("No job with ID {}", job_id))?;
        let result = JobRow::from_row(row);
        self.data = result;
        Ok(true)
    }

    //TODO test
    pub async fn run(&mut self) -> Result<(), GenericError> {
        let catalog_id = self.get_catalog().await?;
        let action = self.get_action().await?;
        let res = self.run_this_job().await;
        match res {
            Ok(_) => {
                self.set_status(JobStatus::Done).await?;
                println!(
                    "Job {} catalog {}:{} completed.",
                    self.get_id().await?,
                    catalog_id,
                    action
                );
            }
            Err(_e) => {
                match catalog_id {
                    0 => self.set_status(JobStatus::Done).await?, // Don't fail'
                    _ => self.set_status(JobStatus::Failed).await?,
                }
                // let e = e.to_string(); // causes stack overflow!
                let note = Some("ERROR".to_string()); //Some(e.to_owned());
                self.set_note(note).await?;
                let job_id = self.get_id().await?;
                println!("Job {job_id} catalog {catalog_id}:{action} FAILED");
            }
        }
        self.update_next_ts().await
    }

    //TODO test
    pub async fn set_status(&mut self, status: JobStatus) -> Result<(), GenericError> {
        let job_id = self.get_id().await?;
        let timestamp = MixNMatch::get_timestamp();
        let status_str = status.as_str();
        let sql = "UPDATE `jobs` SET `status`=:status_str,`last_ts`=:timestamp,`note`=NULL WHERE `id`=:job_id";
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params! {job_id,timestamp,status_str})
            .await?;
        self.put_status(status).await?;
        Ok(())
    }

    //TODO test
    pub async fn set_note(&mut self, note: Option<String>) -> Result<(), GenericError> {
        let job_id = self.get_id().await?;
        let note_cloned = note.clone().map(|s| s.get(..127).unwrap_or(&s).to_string());
        let sql = "UPDATE `jobs` SET `note`=:note WHERE `id`=:job_id";
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params! {job_id,note})
            .await?;
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

        let mut tasks = self.get_tasks().await.ok()?;
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

    /// Resets all RUNNING jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    pub async fn reset_running_jobs(&self) -> Result<(), GenericError> {
        let sql = format!(
            "UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Running.as_str()
        );
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, ())
            .await?;
        Ok(())
    }

    /// Resets all FAILED jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    pub async fn reset_failed_jobs(&self) -> Result<(), GenericError> {
        let sql = format!(
            "UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Failed.as_str()
        );
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, ())
            .await?;
        Ok(())
    }

    /// Returns the current `json` as an Option<serde_json::Value>
    //TODO test
    pub async fn get_json_value(&self) -> Option<serde_json::Value> {
        serde_json::from_str(self.get_json().await.ok()?.as_ref()?).ok()
    }

    //TODO test
    pub async fn queue_simple_job(
        mnm: &MixNMatch,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
    ) -> Result<usize, GenericError> {
        let timestamp = MixNMatch::get_timestamp();
        let status = "TODO";
        let sql = "INSERT INTO `jobs` (catalog,action,status,depends_on,last_ts) VALUES (:catalog_id,:action,:status,:depends_on,:timestamp)
        ON DUPLICATE KEY UPDATE status=:status,depends_on=:depends_on,last_ts=:timestamp";
        let mut conn = mnm.app.get_mnm_conn().await?;
        conn.exec_drop(sql, params! {catalog_id,action,depends_on,status,timestamp})
            .await?;
        let last_id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(last_id)
    }

    /// Sets the value for `json` locally and in database, from a serde_json::Value
    //TODO test
    pub async fn set_json(&mut self, json: Option<serde_json::Value>) -> Result<(), GenericError> {
        let job_id = self.get_id().await?;
        let timestamp = MixNMatch::get_timestamp();
        match json {
            Some(json) => {
                let json_string = json.to_string();
                self.put_json(Some(json_string.clone())).await?;
                let sql =
                    "UPDATE `jobs` SET `json`=:json_string,last_ts=:timestamp WHERE `id`=:job_id";
                self.mnm
                    .app
                    .get_mnm_conn()
                    .await?
                    .exec_drop(sql, params! {job_id, json_string, timestamp})
                    .await?;
            }
            None => {
                self.put_json(None).await?;
                let sql = "UPDATE `jobs` SET `json`=NULL,last_ts=:timestamp WHERE `id`=:job_id";
                self.mnm
                    .app
                    .get_mnm_conn()
                    .await?
                    .exec_drop(sql, params! {job_id, timestamp})
                    .await?;
            }
        }
        Ok(())
    }

    // PRIVATE METHODS

    //TODO test
    async fn run_this_job(&mut self) -> Result<(), GenericError> {
        // let json = self.get_json().await;
        // println!("STARTING {:?} with option {:?}", &self.data().await?,&json);
        if self.data.status == JobStatus::Blocked {
            return Err(Box::new(JobError::S("Job::run_this_job: Blocked".into())));
        }
        println!("STARTING JOB {:?}", self.get_id().await); // DEACTIVATED VERBOSE OUTPUT FOR FEAR OF STACK OVERFLOW
        let catalog_id = self.get_catalog().await?;
        match self.get_action().await?.as_str() {
            "automatch" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_simple(catalog_id).await
            }
            "automatch_by_search" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_by_search(catalog_id).await
            }
            "automatch_from_other_catalogs" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_from_other_catalogs(catalog_id).await
            }
            "automatch_by_sitelink" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_by_sitelink(catalog_id).await
            }
            "automatch_creations" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_creations(catalog_id).await
            }
            "purge_automatches" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.purge_automatches(catalog_id).await
            }
            "match_person_dates" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.match_person_by_dates(catalog_id).await
            }
            "match_on_birthdate" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.match_person_by_single_date(catalog_id).await
            }
            "autoscrape" => {
                let mut autoscrape = Autoscrape::new(catalog_id, &self.mnm).await?;
                autoscrape.set_current_job(self);
                autoscrape.run().await
            }
            "aux2wd" => {
                let mut am = AuxiliaryMatcher::new(&self.mnm);
                am.set_current_job(self);
                am.add_auxiliary_to_wikidata(catalog_id).await
            }
            "auxiliary_matcher" => {
                let mut am = AuxiliaryMatcher::new(&self.mnm);
                am.set_current_job(self);
                am.match_via_auxiliary(catalog_id).await
            }
            "taxon_matcher" => {
                let mut tm = TaxonMatcher::new(&self.mnm);
                tm.set_current_job(self);
                tm.match_taxa(catalog_id).await
            }
            "update_from_tabbed_file" => {
                let mut uc = UpdateCatalog::new(&self.mnm);
                uc.set_current_job(self);
                uc.update_from_tabbed_file(catalog_id).await
            }
            "microsync" => {
                let mut ms = Microsync::new(&self.mnm);
                ms.set_current_job(self);
                let catalog_id = match catalog_id {
                    0 => {
                        match self.mnm.get_random_active_catalog_id_with_property().await {
                            Some(id) => id,
                            None => return Ok(()), // Ignore, very unlikely
                        }
                    }
                    other => other,
                };
                ms.check_catalog(catalog_id).await
            }
            "fix_disambig" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance
                    .unlink_meta_items(catalog_id, &MatchState::any_matched())
                    .await
            }
            "fix_redirected_items_in_catalog" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance
                    .fix_redirects(catalog_id, &MatchState::any_matched())
                    .await
            }

            "maintenance_automatch" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.maintenance_automatch().await
            }

            "remove_p17_for_humans" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.remove_p17_for_humans().await
            }

            "cleanup_mnm_relations" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.cleanup_mnm_relations().await
            }

            "wdrc_sync" => Maintenance::new(&self.mnm).wdrc_sync().await,

            "update_person_dates" => PhpWrapper::update_person_dates(catalog_id),
            "generate_aux_from_description" => {
                PhpWrapper::generate_aux_from_description(catalog_id)
            }
            "bespoke_scraper" => PhpWrapper::bespoke_scraper(catalog_id),
            "import_aux_from_url" => PhpWrapper::import_aux_from_url(catalog_id),
            "update_descriptions_from_url" => PhpWrapper::update_descriptions_from_url(catalog_id),
            "match_by_coordinates" => {
                let cm = CoordinateMatcher::new(&self.mnm, Some(catalog_id)).await?;
                cm.run().await
            }

            other => Err(Box::new(JobError::S(format!(
                "Job::run_this_job: Unknown action '{}'",
                other
            )))),
        }
    }

    //TODO test
    async fn data(&self) -> Result<JobRow, JobError> {
        Ok(self.data.clone())
    }
    //TODO test
    pub async fn get_id(&self) -> Result<usize, JobError> {
        Ok(self.data.id)
    }
    //TODO test
    pub async fn get_action(&self) -> Result<String, JobError> {
        Ok(self.data.action.clone())
    }
    //TODO test
    async fn get_catalog(&self) -> Result<usize, JobError> {
        Ok(self.data.catalog)
    }
    //TODO test
    async fn get_json(&self) -> Result<Option<String>, JobError> {
        Ok(self.data.json.clone())
    }

    //TODO test
    async fn put_status(&mut self, status: JobStatus) -> Result<(), JobError> {
        self.data.status = status;
        Ok(())
    }

    //TODO test
    async fn put_json(&mut self, json: Option<String>) -> Result<(), JobError> {
        self.data.json = json;
        Ok(())
    }

    //TODO test
    async fn put_note(&mut self, note: Option<String>) -> Result<(), JobError> {
        self.data.note = note;
        Ok(())
    }

    //TODO test
    async fn put_next_ts(&mut self, next_ts: &str) -> Result<(), JobError> {
        self.data.next_ts = next_ts.to_string();
        Ok(())
    }

    async fn get_next_ts(&mut self) -> Result<String, GenericError> {
        let seconds = match self.data().await?.repeat_after_sec {
            Some(sec) => sec as i64,
            None => return Ok(String::new()),
        };
        let utc = MixNMatch::parse_timestamp(&self.data().await?.last_ts.clone())
            .ok_or("Can't parse timestamp in last_ts")?
            .checked_add_signed(Duration::seconds(seconds))
            .ok_or(JobError::TimeError)?;
        let next_ts = utc.format("%Y%m%d%H%M%S").to_string();
        Ok(next_ts)
    }

    //TODO test
    async fn update_next_ts(&mut self) -> Result<(), GenericError> {
        let next_ts = self.get_next_ts().await?;

        let job_id = self.get_id().await?;
        self.put_next_ts(&next_ts).await?;
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(
                "UPDATE `jobs` SET `next_ts`=:next_ts WHERE `id`=:job_id",
                params! {job_id,next_ts},
            )
            .await?;
        Ok(())
    }

    fn add_sql_action_filter(&self, sql: String) -> String {
        match &self.skip_actions {
            Some(actions) => {
                let actions = actions.join("','");
                format!("{sql} AND `action` NOT IN ('{actions}')")
            }
            None => sql,
        }
    }

    //TODO test
    pub async fn get_next_high_priority_job(&self) -> Option<usize> {
        let sql = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",
            JobStatus::HighPriority.as_str()
        );
        // let sql = self.add_sql_action_filter(sql);
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_low_priority_job(&self) -> Option<usize> {
        let sql = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",
            JobStatus::LowPriority.as_str()
        );
        let sql = self.add_sql_action_filter(sql);
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_dependent_job(&self) -> Option<usize> {
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}')",JobStatus::Todo.as_str(),JobStatus::Done.as_str()) ;
        let sql = self.add_sql_action_filter(sql);
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_initial_allowed_job(&self, avoid: &[String]) -> Option<usize> {
        if avoid.is_empty() {
            return None;
        }
        let not_in = avoid.join("','");
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL AND `action` NOT IN ('{}')",JobStatus::Todo.as_str(),&not_in) ;
        let sql = self.add_sql_action_filter(sql);
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_initial_job(&self) -> Option<usize> {
        let sql = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",
            JobStatus::Todo.as_str()
        );
        let sql = self.add_sql_action_filter(sql);
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_scheduled_job(&self) -> Option<usize> {
        let timestamp = MixNMatch::get_timestamp();
        let sql = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' AND `next_ts`!='' AND `next_ts`<='{}'",
            JobStatus::Done.as_str(),
            &timestamp
        );
        let sql = self.add_sql_action_filter(sql);
        let sql = format!("{sql} ORDER BY `next_ts` LIMIT 1");
        self.get_next_job_generic(&sql).await
    }

    //TODO test
    async fn get_next_job_generic(&self, sql: &str) -> Option<usize> {
        let sql = if sql.contains(" ORDER BY ") {
            // self.add_sql_action_filter(sql.to_string())
            sql.to_string()
        } else {
            let sql = self.add_sql_action_filter(sql.to_string());
            format!("{} ORDER BY `last_ts` LIMIT 1", sql)
        };
        // println!("{sql}");
        self.mnm
            .app
            .get_mnm_conn()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .pop()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_set_from_id() {
        let mnm = get_test_mnm();
        let mut job = Job::new(&mnm);
        job.set_from_id(1).await.unwrap();
        assert_eq!(job.get_id().await.unwrap(), 1);
        assert_eq!(job.get_catalog().await.unwrap(), 2930);
        assert_eq!(job.get_action().await.unwrap(), "automatch_by_search");
    }

    #[tokio::test]
    async fn test_get_next_ts() {
        let mnm = get_test_mnm();
        let mut job = Job::new(&mnm);
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
