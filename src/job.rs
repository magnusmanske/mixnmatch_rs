use lazy_static::lazy_static;
use std::error::Error;
use std::sync::{Arc, Mutex};
use serde_json::json;
use mysql_async::prelude::*;
use mysql_async::from_row;
use chrono::Duration;
use std::fmt;
use async_trait::async_trait;
use crate::app_state::*;
use crate::entry::*;
use crate::mixnmatch::*;
use crate::automatch::*;
use crate::auxiliary_matcher::*;
use crate::taxon_matcher::*;
use crate::update_catalog::*;
use crate::autoscrape::*;

pub const STATUS_TODO: &'static str = "TODO";
pub const STATUS_DONE: &'static str = "DONE";
pub const STATUS_FAILED: &'static str = "FAILED";
pub const STATUS_RUNNING: &'static str = "RUNNING";
pub const STATUS_HIGH_PRIORITY: &'static str = "HIGH_PRIORITY";
pub const STATUS_LOW_PRIORITY: &'static str = "LOW_PRIORITY";

lazy_static!{
    pub static ref JOB_SUPPORTED_ACTIONS: Vec<&'static str> = {vec!(
        "autoscrape",
        "automatch_by_search",
        "automatch_from_other_catalogs",
        "taxon_matcher",
        "purge_automatches",
        "match_person_dates",
        "match_on_birthdate",
        "update_from_tabbed_file",
        "automatch_by_sitelink",
        "auxiliary_matcher",
        "aux2wd"
    )};
}


/// A trait that allows to manage temporary job data (eg offset)
#[async_trait]
pub trait Jobbable {
    fn set_current_job(&mut self, job: &Job) ;
    fn get_current_job(&self) -> Option<&Job> ;

    fn get_last_job_data(&self) -> Option<serde_json::Value> {
        self.get_current_job()?.get_json_value()
    }

    async fn remember_job_data(&self, json: &serde_json::Value) -> Result<(),GenericError> {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return Ok(())
        };
        job.set_json(Some(json.to_owned())).await
    }

    fn get_last_job_offset(&self) -> usize {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return 0
        };
        let json = match job.get_json_value() {
            Some(json) => json,
            None => return 0
        };
        match json.as_object() {
            Some(o) => {
                match o.get("offset") {
                    Some(offset) => offset.as_u64().unwrap_or(0) as usize,
                    None => 0
                }
            }
            None => 0
        }
    }

    async fn remember_offset(&self, offset: usize) -> Result<(),GenericError> {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return Ok(())
        };
        job.set_json(Some(json!({"offset":offset}))).await?;
        Ok(())
    }

    async fn clear_offset(&self) -> Result<(),GenericError> {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return Ok(())
        };
        job.set_json(None).await
    }
}

#[derive(Debug)]
enum JobError {
    S(String),
    DataNotSet,
    PoisonedJobRowMutex,
    TimeError
}

impl Error for JobError {}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}


#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: usize,
    pub action: String,
    pub catalog: usize,
    pub json: Option<String>,
    pub depends_on: Option<usize>,
    pub status: String,
    pub last_ts: String,
    pub note: Option<String>,
    pub repeat_after_sec: Option<usize>,
    pub next_ts: String,
    pub user_id: usize
}

impl JobRow {
    pub fn from_row(x: (usize,String,usize,Option<String>,Option<usize>,String,String,Option<String>,Option<usize>,String,usize)) -> Self {
            Self {
                id: x.0,
                action: x.1,
                catalog: x.2,
                json: x.3,
                depends_on: x.4,
                status: x.5,
                last_ts: x.6,
                note: x.7,
                repeat_after_sec: x.8,
                next_ts: x.9,
                user_id: x.10
            }
        }

        pub fn new(action: &str, catalog_id: usize) -> JobRow {
            Self {
                id: 0,
                action: action.to_string(),
                catalog: catalog_id,
                json: None,
                depends_on: None,
                status: STATUS_TODO.to_string(),
                last_ts: MixNMatch::get_timestamp(),
                note: None,
                repeat_after_sec: None,
                next_ts: "".to_string(),
                user_id: 0
            }
        }
    
    }

#[derive(Debug, Clone)]
pub struct Job {
    pub data: Option<Arc<Mutex<JobRow>>>,
    pub mnm: Arc<MixNMatch>
}

impl Job {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            data: None,
            mnm: Arc::new(mnm.clone())
        }
    }

    pub async fn set_next(&mut self, actions: &Option<Vec<&str>>) -> Result<bool,GenericError> {
        match self.get_next_job_id(actions).await {
            Some(job_id) => self.set_from_id(job_id).await,
            None => Ok(false)
        }
    }

    pub async fn set_from_id(&mut self, job_id: usize) -> Result<bool,GenericError> {
        self.data = None;
        let sql = r"SELECT id,action,catalog,json,depends_on,status,last_ts,note,repeat_after_sec,next_ts,user_id FROM `jobs` WHERE `id`=:job_id";
        let row = self.mnm.app.get_mnm_conn().await?
            .exec_iter(sql, params!{job_id}).await?
            .map_and_drop(from_row::<(usize,String,usize,Option<String>,Option<usize>,String,String,Option<String>,Option<usize>,String,usize)>).await?
            .pop().ok_or(format!("No job with ID {}", job_id))?;
        let result = JobRow::from_row(row);
        self.data = Some(Arc::new(Mutex::new(result)));
        Ok(true)
    }
    pub async fn run(&mut self) -> Result<(),GenericError> {
        let catalog_id = self.get_catalog()?;
        let action = self.get_action()?;
        match self.run_this_job().await {
            Ok(_) => {
                self.set_status(STATUS_DONE).await?;
                println!("Job {}:{} completed.",catalog_id,action);
            }
            Err(e) => {
                self.set_status(STATUS_FAILED).await?;
                println!("Job {}:{} FAILED: {:?}",catalog_id,action,&e);
            }
        }
        self.update_next_ts().await
    }

    pub async fn set_status(&mut self, status: &str) -> Result<(),GenericError> {
        let job_id = self.get_id()?;
        let timestamp = MixNMatch::get_timestamp();
        let sql = "UPDATE `jobs` SET `status`=:status,`last_ts`=:timestamp WHERE `id`=:job_id";
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {job_id,timestamp,status}).await?;
        self.put_status(status)?;
        Ok(())
    }

    pub async fn get_next_job_id(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        if let Some(job_id) = self.get_next_high_priority_job(actions).await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_dependent_job(actions).await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_initial_job(actions).await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_low_priority_job(actions).await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_scheduled_job(actions).await {
            return Some(job_id) ;
        }
        None
    }

    /// Resets all RUNNING jobs of certain types to TODO. Used when bot restarts.
    pub async fn reset_running_jobs(&self, actions: &Option<Vec<&str>>) -> Result<(),GenericError> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("UPDATE `jobs` SET `status`='{}' WHERE `status`='{}' {}",STATUS_TODO,STATUS_RUNNING,&conditions) ;
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    /// Resets all FAILED jobs of certain types to TODO. Used when bot restarts.
    pub async fn reset_failed_jobs(&self, actions: &Option<Vec<&str>>) -> Result<(),GenericError> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("UPDATE `jobs` SET `status`='{}' WHERE `status`='{}' {}",STATUS_TODO,STATUS_FAILED,&conditions) ;
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }
    
    /// Returns the current `json` as an Option<serde_json::Value>
    pub fn get_json_value(&self) ->  Option<serde_json::Value> {
        serde_json::from_str(self.get_json().ok()?.as_ref()?).ok()
    }

    pub async fn queue_simple_job(mnm: &MixNMatch, catalog_id: usize, action: &str, depends_on: Option<usize>) -> Result<usize,GenericError> {
        let timestamp = MixNMatch::get_timestamp();
        let status = "TODO";
        let sql = "INSERT INTO `jobs` (catalog,action,status,depends_on,last_ts) VALUES (:catalog_id,:action,:status,:depends_on,:timestamp)
        ON DUPLICATE KEY UPDATE status=:status,depends_on=:depends_on,last_ts=:timestamp";
        let mut conn = mnm.app.get_mnm_conn().await?;
        conn.exec_drop(sql, params!{catalog_id,action,depends_on,status,timestamp}).await?;
        let last_id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(last_id)
    }

    /// Sets the value for `json` locally and in database, from a serde_json::Value
    pub async fn set_json(&self, json: Option<serde_json::Value> ) ->  Result<(),GenericError> {
        let job_id = self.get_id()?;
        match json {
            Some(json) => {
                let json_string = json.to_string();
                self.put_json(Some(json_string.clone()))?;
                let sql = "UPDATE `jobs` SET `json`=:json_string WHERE `id`=:job_id";
                self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params!{job_id, json_string}).await?;
            }
            None => {
                self.put_json(None)?;
                let sql = "UPDATE `jobs` SET `json`=NULL WHERE `id`=:job_id";
                self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params!{job_id}).await?;
            }
        }
        Ok(())
    }

    // PRIVATE METHODS

    async fn run_this_job(&mut self) -> Result<(),GenericError> {
        let json = self.get_json();
        println!("STARTING {:?} with option {:?}", &self.data()?,&json);
        let catalog_id = self.get_catalog()?;
        match self.get_action()?.as_str() {
            "automatch_by_search" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_by_search(catalog_id).await
            },
            "automatch_from_other_catalogs" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_from_other_catalogs(catalog_id).await
            },
            "automatch_by_sitelink" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_by_sitelink(catalog_id).await
            },
            "purge_automatches" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.purge_automatches(catalog_id).await
            },
            "match_person_dates" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.match_person_by_dates(catalog_id).await
            },
            "match_on_birthdate" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.match_person_by_single_date(catalog_id).await
            },
            "autoscrape" => {
                let mut autoscrape = Autoscrape::new(catalog_id, &self.mnm).await?;
                autoscrape.set_current_job(self);
                autoscrape.run().await
            },
            "aux2wd" => {
                let mut am = AuxiliaryMatcher::new(&self.mnm);
                am.set_current_job(self);
                am.add_auxiliary_to_wikidata(catalog_id).await
            },
            "auxiliary_matcher" => {
                let mut am = AuxiliaryMatcher::new(&self.mnm);
                am.set_current_job(self);
                am.match_via_auxiliary(catalog_id).await
            },
            "taxon_matcher" => {
                let mut tm = TaxonMatcher::new(&self.mnm);
                tm.set_current_job(self);
                tm.match_taxa(catalog_id).await
            },
            "update_from_tabbed_file" => {
                let mut uc = UpdateCatalog::new(&self.mnm);
                uc.set_current_job(self);
                uc.update_from_tabbed_file(catalog_id).await
            },

            other => {
                return Err(Box::new(JobError::S(format!("Job::run_this_job: Unknown action '{}'",other))))
            }
        }
    }


    fn data(&self) -> Result<JobRow,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.clone())
    }
    fn get_id(&self) -> Result<usize,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.id)
    }
    fn get_action(&self) -> Result<String,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.action.clone())
    }
    fn get_catalog(&self) -> Result<usize,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.catalog)
    }
    fn get_json(&self) -> Result<Option<String>,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.json.clone())
    }

    fn put_status(&self, status: &str) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).status = status.to_string();
        Ok(())
    }
    fn put_json(&self, json: Option<String>) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).json = json;
        Ok(())
    }
    fn put_next_ts(&self, next_ts: &str) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).next_ts = next_ts.to_string();
        Ok(())
    }

    fn get_next_ts(&mut self) -> Result<String,GenericError> {
        let seconds = match self.data()?.repeat_after_sec {
            Some(sec) => sec as i64,
            None => return Ok(String::new())
        };
        let utc = MixNMatch::parse_timestamp(&self.data()?.last_ts.clone()).ok_or("Can't parse timestamp in last_ts")?
            .checked_add_signed(Duration::seconds(seconds)).ok_or(JobError::TimeError)?;
        let next_ts = utc.format("%Y%m%d%H%M%S").to_string();
        Ok(next_ts)
    }

    async fn update_next_ts(&mut self) -> Result<(),GenericError> {
        let next_ts = self.get_next_ts()?;

        let job_id = self.get_id()?;
        self.put_next_ts(&next_ts)?;
        self.mnm.app.get_mnm_conn().await?.exec_drop("UPDATE `jobs` SET `next_ts`=:next_ts WHERE `id`=:job_id", params! {job_id,next_ts}).await?;
        Ok(())
    }

    async fn get_next_high_priority_job(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL {}",STATUS_HIGH_PRIORITY,&conditions) ;
        self.get_next_job_generic(&sql).await
    }
    
    async fn get_next_low_priority_job(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL {}",STATUS_LOW_PRIORITY,&conditions) ;
        self.get_next_job_generic(&sql).await
    }
    
    async fn get_next_dependent_job(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}') {}",STATUS_TODO,STATUS_DONE,&conditions) ;
        self.get_next_job_generic(&sql).await
    }
    
    async fn get_next_initial_job(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL {}",STATUS_TODO,&conditions) ;
        self.get_next_job_generic(&sql).await
    }
    
    async fn get_next_scheduled_job(&self, actions: &Option<Vec<&str>>) -> Option<usize> {
        let conditions = self.get_action_conditions(actions) ;
        let timestamp =  MixNMatch::get_timestamp();
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `next_ts`!='' AND `next_ts`<='{}' {} ORDER BY `next_ts` LIMIT 1",STATUS_DONE,&timestamp,&conditions) ;
        self.get_next_job_generic(&sql).await
    }
    
    async fn get_next_job_generic(&self, sql: &str) -> Option<usize> {
        let sql = if sql.contains(" ORDER BY ") {
            sql.to_string()
        } else {
            format!("{} ORDER BY `last_ts` LIMIT 1", sql)
        };
        self.mnm.app.get_mnm_conn().await.ok()?
            .exec_iter(sql,()).await.ok()?
            .map_and_drop(from_row::<usize>).await.ok()?.pop()
    }

    fn get_action_conditions(&self, actions: &Option<Vec<&str>>) -> String {
        let actions = match actions {
            Some(a) => a,
            None => return "".to_string()
        };
        if actions.is_empty() {
            return "".to_string() ;
        }
        return format!(" AND `action` IN ('{}') ",actions.join("','"));
    }
}



#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_get_next_ts() {
        let mnm = get_test_mnm();
        let mut job = Job::new(&mnm);
        let mut job_row = JobRow::new("test_action",0);
        job_row.last_ts = "20221027000000".to_string();
        job_row.repeat_after_sec = Some(61);
        job.data = Some(Arc::new(Mutex::new(job_row)));
        let next_ts = job.get_next_ts().unwrap();
        assert_eq!(next_ts,"20221027000101");
    }
 
    #[tokio::test]
    async fn test_job_find() {
        let mnm = get_test_mnm();
        let mut job = Job::new(&mnm);
        // THIS IS NOT A GOOD TEST
        let _success = job.set_next(&Some(vec!("automatch_by_search"))).await.unwrap();
        //println!("{:?}", &job);
    }
}