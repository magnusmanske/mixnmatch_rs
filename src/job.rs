use mysql_async::prelude::*;
use mysql_async::from_row;
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::automatch::*;
use std::error::Error;
use std::fmt;


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
                self.update_next_ts().await?;
                self.set_status(STATUS_DONE).await
            }
            _ => {
                self.update_next_ts().await?;
                self.set_status(STATUS_FAILED).await
            }
        }
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
        let data = self.data.as_ref().ok_or("Job::run: No job data set")?.clone();
        println!("STARTING {:?}", &data);
        match data.action.as_str() {
            "automatch_by_search" => {
                let am = AutoMatch::new(&self.mnm);
                am.automatch_by_search(data.catalog).await
            },
            other => {
                return Err(Box::new(JobError::S(format!("Job::run: Unknown action '{}'",other))))
            }
        }
    }

    async fn update_next_ts(&mut self) -> Result<(),GenericError> {
        // TODO
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
        let sql = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}') {}",STATUS_TODO,&conditions,STATUS_DONE) ;
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
    use static_init::dynamic;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;

    #[dynamic(drop)]
    static mut MNM_CACHE: Option<MixNMatch> = None;

    async fn get_mnm() -> MixNMatch {
        if MNM_CACHE.read().is_none() {
            let app = AppState::from_config_file("config.json").await.unwrap();
            let mnm = MixNMatch::new(app.clone());
            (*MNM_CACHE.write()) = Some(mnm);
        }
        MNM_CACHE.read().as_ref().map(|s| s.clone()).unwrap().clone()
    }

    #[tokio::test]
    async fn test_job_find() {
        let mnm = get_mnm().await;
        let mut job = Job::new(&mnm);
        // THIS IS NOT A GOOD TEST
        let _success = job.set_next(&Some(vec!("automatch_by_search"))).await.unwrap();
        println!("{:?}", &job);
    }
}