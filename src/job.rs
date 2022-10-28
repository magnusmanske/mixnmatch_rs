use mysql_async::prelude::*;
use mysql_async::from_row;
use chrono::Duration;
use std::error::Error;
use std::fmt;
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::automatch::*;
use crate::taxon_matcher::*;


pub const STATUS_TODO: &'static str = "TODO";
pub const STATUS_DONE: &'static str = "DONE";
pub const STATUS_FAILED: &'static str = "FAILED";
pub const STATUS_RUNNING: &'static str = "RUNNING";
pub const STATUS_HIGH_PRIORITY: &'static str = "HIGH_PRIORITY";
pub const STATUS_LOW_PRIORITY: &'static str = "LOW_PRIORITY";


#[derive(Debug)]
enum JobError {
    S(String)
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
    pub data: Option<JobRow>,
    pub mnm: MixNMatch
}

impl Job {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            data: None,
            mnm: mnm.clone()
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
        self.data = Some(result);
        Ok(true)
    }

    pub async fn run(&mut self) -> Result<(),GenericError> {
        match self.run_this_job().await {
            Ok(_) => {
                self.set_status(STATUS_DONE).await?;
                println!("Job {}:{} completed.",self.data.as_ref().unwrap().catalog,self.data.as_ref().unwrap().action);
            }
            _ => {
                self.set_status(STATUS_FAILED).await?;
                println!("Job {}:{} FAILED.",self.data.as_ref().unwrap().catalog,self.data.as_ref().unwrap().action);
            }
        }
        self.update_next_ts().await
    }

    pub async fn set_status(&mut self, status: &str) -> Result<(),GenericError> {
        let job_id = self.data.as_ref().ok_or("!")?.id;
        let timestamp = MixNMatch::get_timestamp();
        let sql = "UPDATE `jobs` SET `status`=:status,`last_ts`=:timestamp WHERE `id`=:job_id";
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {job_id,timestamp,status}).await?;
        self.data.as_mut().ok_or("!")?.status = status.to_string();
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

    pub async fn reset_running_jobs(&self, actions: &Option<Vec<&str>>) -> Result<(),GenericError> {
        let conditions = self.get_action_conditions(actions) ;
        let sql = format!("UPDATE `jobs` SET `status`='{}' WHERE `status`='{}' {}",STATUS_TODO,STATUS_RUNNING,&conditions) ;
        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    // PRIVATE METHODS

    async fn run_this_job(&mut self) -> Result<(),GenericError> {
        let data = self.data.as_ref().ok_or("Job::run_this_job: No job data set")?.clone();
        println!("STARTING {:?}", &data);
        match data.action.as_str() {
            "automatch_by_search" => {
                let am = AutoMatch::new(&self.mnm);
                am.automatch_by_search(data.catalog).await
            },
            "automatch_from_other_catalogs" => {
                let am = AutoMatch::new(&self.mnm);
                am.automatch_from_other_catalogs(data.catalog).await
            },
            "purge_automatches" => {
                let am = AutoMatch::new(&self.mnm);
                am.purge_automatches(data.catalog).await
            },
            "taxon_matcher" => {
                let tm = TaxonMatcher::new(&self.mnm);
                tm.match_taxa(data.catalog).await
            },
            
            other => {
                return Err(Box::new(JobError::S(format!("Job::run_this_job: Unknown action '{}'",other))))
            }
        }
    }

    fn get_next_ts(&mut self) -> Result<String,GenericError> {
        let data = self.data.as_ref().ok_or("Job::get_next_ts: No job data set")?;
        let seconds = match data.repeat_after_sec {
            Some(sec) => sec as i64,
            None => return Err(Box::new(JobError::S(format!("Job::get_next_ts"))))
        };
        let utc = MixNMatch::parse_timestamp(&data.last_ts).ok_or("Can't parse timestamp in last_ts")?
            .checked_add_signed(Duration::seconds(seconds)).unwrap(); // TODO fix unwrap
        let next_ts = utc.format("%Y%m%d%H%M%S").to_string();
        Ok(next_ts)
    }

    async fn update_next_ts(&mut self) -> Result<(),GenericError> {
        let next_ts = self.get_next_ts()?;

        let job_id = self.data.as_ref().ok_or("Job::update_next_ts: No job data set")?.id;
        self.data.as_mut().ok_or("!")?.next_ts = next_ts.to_string();
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
        job.data = Some(JobRow::new("test_action",0));
        job.data.as_mut().unwrap().last_ts = "20221027000000".to_string();
        job.data.as_mut().unwrap().repeat_after_sec = Some(61);
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