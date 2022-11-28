use std::error::Error;
use std::sync::{Arc, Mutex};
use serde_json::json;
use mysql_async::prelude::*;
use mysql_async::from_row;
use chrono::Duration;
use std::fmt;
use async_trait::async_trait;
use crate::maintenance::*;
use crate::app_state::*;
use crate::entry::*;
use crate::mixnmatch::*;
use crate::automatch::*;
use crate::auxiliary_matcher::*;
use crate::taxon_matcher::*;
use crate::update_catalog::*;
use crate::autoscrape::*;
use crate::microsync::*;
use crate::php_wrapper::*;

pub const TASK_SIZE: &'static [(&'static str,u8)] = &[
    ("automatch",2),
    ("automatch_by_search",2),
    ("automatch_by_sitelink",2),
    ("automatch_from_other_catalogs",2),
    ("autoscrape",4),
    ("aux2wd",2),
    ("auxiliary_matcher",2),
    ("bespoke_scraper",5),
    ("generate_aux_from_description",5),
    ("import_aux_from_url",5),
    ("match_by_coordinates",5),
    ("match_on_birthdate",1),
    ("match_person_dates",1),
    ("microsync",1),
    ("purge_automatches",1),
    ("taxon_matcher",2),
    ("update_descriptions_from_url",5),
    ("update_from_tabbed_file",3),
    ("update_person_dates",2),
];

#[derive(Debug, Clone)]
pub enum JobStatus {
    Todo,
    Done,
    Failed,
    Running,
    HighPriority,
    LowPriority,
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
            _ => None
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
        }
    }
}


/// A trait that allows to manage temporary job data (eg offset)
#[async_trait]
pub trait Jobbable {
    fn set_current_job(&mut self, job: &Job) ;
    fn get_current_job(&self) -> Option<&Job> ;

    //TODO test
    fn get_last_job_data(&self) -> Option<serde_json::Value> {
        self.get_current_job()?.get_json_value()
    }

    //TODO test
    async fn remember_job_data(&self, json: &serde_json::Value) -> Result<(),GenericError> {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return Ok(())
        };
        job.set_json(Some(json.to_owned())).await
    }

    //TODO test
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

    //TODO test
    async fn remember_offset(&self, offset: usize) -> Result<(),GenericError> {
        let job = match self.get_current_job() {
            Some(job) => job,
            None => return Ok(())
        };
        job.set_json(Some(json!({"offset":offset}))).await?;
        Ok(())
    }

    //TODO test
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
    //TODO test
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
    pub status: JobStatus,
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
                status: JobStatus::new(&x.5).unwrap_or(JobStatus::Todo),
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
                status: JobStatus::Todo,
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

    //TODO test
    pub async fn set_next(&mut self) -> Result<bool,GenericError> {
        match self.get_next_job_id().await {
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
    //TODO test
    pub async fn run(&mut self) -> Result<(),GenericError> {
        let catalog_id = self.get_catalog()?;
        let action = self.get_action()?;
        match self.run_this_job().await {
            Ok(_) => {
                self.set_status(JobStatus::Done).await?;
                println!("Job {} catalog {}:{} completed.",self.get_id()?,catalog_id,action);
            }
            Err(e) => {
                self.set_status(JobStatus::Failed).await?;
                self.set_note(Some(e.to_string())).await?;
                println!("Job {} catalog {}:{} FAILED: {:?}",self.get_id()?,catalog_id,action,&e);
            }
        }
        self.update_next_ts().await
    }

    //TODO test
    pub async fn set_status(&mut self, status: JobStatus) -> Result<(),GenericError> {
        let job_id = self.get_id()?;
        let timestamp = MixNMatch::get_timestamp();
        let status_str = status.as_str();
        let sql = "UPDATE `jobs` SET `status`=:status_str,`last_ts`=:timestamp,`note`=NULL WHERE `id`=:job_id";
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {job_id,timestamp,status_str}).await?;
        self.put_status(status)?;
        Ok(())
    }

    //TODO test
    pub async fn set_note(&mut self, note: Option<String>) -> Result<(),GenericError> {
        let job_id = self.get_id()?;
        let note_cloned = note.clone();
        let sql = "UPDATE `jobs` SET `note`=:note WHERE `id`=:job_id";
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {job_id,note}).await?;
        self.put_note(note_cloned)?;
        Ok(())
    }
    
    //TODO test
    pub async fn get_next_job_id(&self) -> Option<usize> {
        if let Some(job_id) = self.get_next_high_priority_job().await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_dependent_job().await {
            return Some(job_id) ;
        }

        let mut tasks = Vec::from(TASK_SIZE);
        let mut level: u8 = 0;
        while !tasks.is_empty() {
            tasks.retain(|v|v.1>level);
            let avoid: Vec<String> = tasks.iter().map(|v|v.0.to_string()).collect();
            if let Some(job_id) = self.get_next_initial_allowed_job(&avoid).await {
                return Some(job_id) ;
            }
                level += 1;
        }

        /*
        if let Some(job_id) = self.get_next_initial_fast_job().await {
            return Some(job_id) ;
        } */
        if let Some(job_id) = self.get_next_initial_job().await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_low_priority_job().await {
            return Some(job_id) ;
        }
        if let Some(job_id) = self.get_next_scheduled_job().await {
            return Some(job_id) ;
        }
        None
    }

    /// Resets all RUNNING jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    pub async fn reset_running_jobs(&self) -> Result<(),GenericError> {
        let sql = format!("UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",JobStatus::Todo.as_str(),JobStatus::Running.as_str()) ;
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    /// Resets all FAILED jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    pub async fn reset_failed_jobs(&self) -> Result<(),GenericError> {
        let sql = format!("UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",JobStatus::Todo.as_str(),JobStatus::Failed.as_str()) ;
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }
    
    /// Returns the current `json` as an Option<serde_json::Value>
    //TODO test
    pub fn get_json_value(&self) ->  Option<serde_json::Value> {
        serde_json::from_str(self.get_json().ok()?.as_ref()?).ok()
    }

    //TODO test
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
    //TODO test
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

    //TODO test
    async fn run_this_job(&mut self) -> Result<(),GenericError> {
        let json = self.get_json();
        println!("STARTING {:?} with option {:?}", &self.data()?,&json);
        let catalog_id = self.get_catalog()?;
        match self.get_action()?.as_str() {
            "automatch" => {
                let mut am = AutoMatch::new(&self.mnm);
                am.set_current_job(self);
                am.automatch_simple(catalog_id).await
            },
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
            "microsync" => {
                let mut ms = Microsync::new(&self.mnm);
                ms.set_current_job(self);
                let catalog_id = match catalog_id {
                    0 => {
                        match self.mnm.get_random_active_catalog_id_with_property().await {
                            Some(id) => id,
                            None => return Ok(()) // Ignore, very unlikely
                        }
                    }
                    other => other
                };
                ms.check_catalog(catalog_id).await
            },
            "fix_disambig" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.unlink_meta_items(catalog_id,&MatchState::any_matched()).await
            },
            "fix_redirected_items_in_catalog" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.fix_redirects(catalog_id,&MatchState::any_matched()).await
            },

            "maintenance_automatch" => {
                let maintenance = Maintenance::new(&self.mnm);
                maintenance.maintenance_automatch().await
            },

            "update_person_dates" => {
                PhpWrapper::update_person_dates(catalog_id)
            },
            "generate_aux_from_description" => {
                PhpWrapper::generate_aux_from_description(catalog_id)
            },
            "bespoke_scraper" => {
                PhpWrapper::bespoke_scraper(catalog_id)
            },
            "import_aux_from_url" => {
                PhpWrapper::import_aux_from_url(catalog_id)
            },
            "update_descriptions_from_url" => {
                PhpWrapper::update_descriptions_from_url(catalog_id)
            },
            /*"automatch" => { // TODO native
                PhpWrapper::automatch(catalog_id)
            },*/
            "match_by_coordinates" => { // TODO native
                PhpWrapper::match_by_coordinates(catalog_id)
            },
    
            other => {
                return Err(Box::new(JobError::S(format!("Job::run_this_job: Unknown action '{}'",other))))
            }
        }
    }


    //TODO test
    fn data(&self) -> Result<JobRow,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.clone())
    }
    //TODO test
    fn get_id(&self) -> Result<usize,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.id)
    }
    //TODO test
    fn get_action(&self) -> Result<String,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.action.clone())
    }
    //TODO test
    fn get_catalog(&self) -> Result<usize,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.catalog)
    }
    //TODO test
    fn get_json(&self) -> Result<Option<String>,JobError> {
        Ok(self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?.json.clone())
    }

    //TODO test
    fn put_status(&self, status: JobStatus) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).status = status;
        Ok(())
    }

    //TODO test
    fn put_json(&self, json: Option<String>) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).json = json;
        Ok(())
    }

    //TODO test
    fn put_note(&self, note: Option<String>) -> Result<(),JobError> {
        (*self.data.as_ref().ok_or(JobError::DataNotSet)?.lock().map_err(|_|JobError::PoisonedJobRowMutex)?).note = note;
        Ok(())
    }

    //TODO test
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

    //TODO test
    async fn update_next_ts(&mut self) -> Result<(),GenericError> {
        let next_ts = self.get_next_ts()?;

        let job_id = self.get_id()?;
        self.put_next_ts(&next_ts)?;
        self.mnm.app.get_mnm_conn().await?.exec_drop("UPDATE `jobs` SET `next_ts`=:next_ts WHERE `id`=:job_id", params! {job_id,next_ts}).await?;
        Ok(())
    }

    //TODO test
    async fn get_next_high_priority_job(&self) -> Option<usize> {
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",JobStatus::HighPriority.as_str()) ;
        self.get_next_job_generic(&sql).await
    }
    
    //TODO test
    async fn get_next_low_priority_job(&self) -> Option<usize> {
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",JobStatus::LowPriority.as_str()) ;
        self.get_next_job_generic(&sql).await
    }
    
    //TODO test
    async fn get_next_dependent_job(&self) -> Option<usize> {
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}')",JobStatus::Todo.as_str(),JobStatus::Done.as_str()) ;
        self.get_next_job_generic(&sql).await
    }

    
    //TODO test
    async fn get_next_initial_allowed_job(&self, avoid: &Vec<String>) -> Option<usize> {
        if avoid.is_empty() {
            return None;
        }
        let not_in = avoid.join("','");
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL AND `action` NOT IN ('{}')",JobStatus::Todo.as_str(),&not_in) ;
        self.get_next_job_generic(&sql).await
    }
    
    //TODO test
    async fn get_next_initial_job(&self) -> Option<usize> {
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NULL",JobStatus::Todo.as_str()) ;
        self.get_next_job_generic(&sql).await
    }
     
    //TODO test
    async fn get_next_scheduled_job(&self) -> Option<usize> {
        let timestamp =  MixNMatch::get_timestamp();
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `next_ts`!='' AND `next_ts`<='{}' ORDER BY `next_ts` LIMIT 1",JobStatus::Done.as_str(),&timestamp) ;
        self.get_next_job_generic(&sql).await
    }
    
    //TODO test
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

}



#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_set_from_id() {
        let mnm = get_test_mnm();
        let mut job = Job::new(&mnm);
        job.set_from_id(1).await.unwrap();
        assert_eq!(job.get_id().unwrap(),1);
        assert_eq!(job.get_catalog().unwrap(),2930);
        assert_eq!(job.get_action().unwrap(),"automatch_by_search");
    }

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

}