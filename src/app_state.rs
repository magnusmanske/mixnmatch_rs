use crate::job::*;
use crate::mysql_misc::MySQLMisc;
use crate::storage::Storage;
use crate::storage_mysql::StorageMySQL;
use crate::wdrc::WDRC;
use crate::wikidata::Wikidata;
use anyhow::Result;
use dashmap::DashMap;
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use tokio::time::sleep;
use wikimisc::timestamp::TimeStamp;

/// Global function for tests.
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
    wdrc: Arc<WDRC>,
    storage: Arc<Box<dyn Storage>>,
    import_file_path: Arc<String>,
    task_specific_usize: Arc<HashMap<String, usize>>,
    max_concurrent_jobs: usize,
}

impl AppState {
    /// Create an AppState object from a config JSON file
    pub fn from_config_file(filename: &str) -> Result<Self> {
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(Self::from_config(&config))
    }

    pub fn import_file_path(&self) -> &str {
        &self.import_file_path
    }

    pub fn task_specific_usize(&self) -> &HashMap<String, usize> {
        &self.task_specific_usize
    }

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let task_specific_usize = config["task_specific_usize"]
            .as_object()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.as_u64().unwrap_or_default() as usize))
            .collect();
        let task_specific_usize = Arc::new(task_specific_usize);
        let max_concurrent_jobs = config["max_concurrent_jobs"].as_u64().unwrap_or(10) as usize;
        let bot_name = config["bot_name"].as_str().unwrap().to_string();
        let bot_password = config["bot_password"].as_str().unwrap().to_string();
        let import_file_path = config["import_file_path"].as_str().unwrap().to_string();
        let import_file_path = Arc::new(import_file_path);
        Self {
            wikidata: Wikidata::new(&config["wikidata"], bot_name, bot_password),
            wdrc: Arc::new(WDRC::new(&config["wdrc"])),
            storage: Arc::new(Box::new(StorageMySQL::new(&config["mixnmatch"]))),
            import_file_path,
            task_specific_usize,
            max_concurrent_jobs,
        }
    }

    pub fn storage(&self) -> &Arc<Box<dyn Storage>> {
        &self.storage
    }

    pub fn wikidata(&self) -> &Wikidata {
        &self.wikidata
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

    // pub async fn run_from_props(&self, props: Vec<u32>, min_entries: u16) -> Result<()> {
    //     if props.len() < 2 {
    //         return Err(anyhow!("Minimum of two properties required."));
    //     }
    //     let mut mnm = MixNMatch::new(self.clone());
    //     let first_prop = props.first().unwrap(); // Safe
    //     let mut sql = format!(
    //         r#"SELECT main_ext_id,group_concat(entry_id),count(DISTINCT entry_id) AS cnt
    //         FROM ( SELECT entry_id,aux_name AS main_ext_id FROM auxiliary,entry WHERE aux_p={first_prop} and entry.id=entry_id AND (entry.q is null or entry.user=0)
    //         UNION SELECT entry.id,ext_id FROM entry,catalog WHERE entry.catalog=catalog.id AND catalog.active=1 AND catalog.wd_prop={first_prop} AND (entry.q is null or entry.user=0) ) t1"#
    //     );
    //     for (num, prop) in props.iter().skip(1).enumerate() {
    //         sql += if num == 0 { " WHERE" } else { " AND" };
    //         sql += &format!(
    //             r#" entry_id IN (SELECT entry_id FROM auxiliary,entry WHERE aux_p={prop} and entry.id=entry_id UNION SELECT entry.id FROM entry,catalog WHERE entry.catalog=catalog.id AND catalog.active=1 AND catalog.wd_prop={prop})"#
    //         );
    //     }
    //     sql += &format!(r#"GROUP BY main_ext_id HAVING cnt>={min_entries}"#);
    //     sql = sql.replace(['\n', '\t'], " ");

    //     let mut conn = self
    //         .get_mnm_conn()
    //         .await
    //         .expect("run_from_props: No DB connection");

    //     let results: Vec<_> = conn
    //         .exec_iter(sql, ())
    //         .await
    //         .expect("run_from_props: No results")
    //         .map_and_drop(from_row::<(String, String, usize)>)
    //         .await
    //         .expect("run_from_props: Result retrieval failure");

    //     let props_s = props
    //         .iter()
    //         .map(|p| format!("{p}"))
    //         .collect::<Vec<String>>()
    //         .join(",");
    //     for (_primary_ext_id, entries_s, _cnt) in results {
    //         let entries_v: Vec<_> = entries_s
    //             .split(',')
    //             .filter_map(|s| s.parse::<usize>().ok())
    //             .collect();
    //         self.create_item_from_entries(&entries_v, &props_s, &mut conn, &mut mnm)
    //             .await?;
    //     }
    //     Ok(())
    // }

    // pub async fn create_item_from_entries(
    //     &self,
    //     entries_v: &[usize],
    //     props_s: &str,
    //     conn: &mut Conn,
    //     mnm: &mut MixNMatch,
    // ) -> Result<()> {
    //     let entries_s: Vec<_> = entries_v.iter().map(|id| format!("{id}")).collect();
    //     let entries_s = entries_s.join(",");
    //     let sql = format!(
    //         r#"SELECT entry_id,aux_p,aux_name FROM auxiliary WHERE entry_id IN ({entries_s}) AND aux_p IN ({props_s}) UNION SELECT entry.id,catalog.wd_prop,ext_id FROM entry,catalog WHERE entry.catalog=catalog.id AND entry.id IN ({entries_s}) AND wd_prop IN ({props_s})"#
    //     );

    //     let entry_prop_values: Vec<_> = conn
    //         .exec_iter(sql, ())
    //         .await
    //         .expect("run_from_props: No results")
    //         .map_and_drop(from_row::<(usize, u32, String)>)
    //         .await
    //         .expect("run_from_props: Result retrieval failure");

    //     let prop_values = entry_prop_values
    //         .iter()
    //         .map(|(_entry_id, prop, value)| format!("P{prop}={value}"))
    //         .collect::<Vec<String>>()
    //         .join("|");
    //     let query = format!(r#"haswbstatement:"{prop_values}""#);
    //     let mut qs = mnm.wd_search(&query).await?;
    //     if qs.is_empty() {
    //         println!("Create new item from {entries_s}");
    //         let mut new_item = ItemEntity::new_empty();
    //         for entry_id in entries_v {
    //             let entry = Entry::from_id(*entry_id, mnm).await?;
    //             entry.add_to_item(&mut new_item).await?;
    //         }
    //         // println!("{:#?}", new_item);
    //         match mnm.create_new_wikidata_item(new_item).await {
    //             Ok(q) => {
    //                 println!("Created https://www.wikidata.org/wiki/{q}");
    //                 for entry_id in entries_v {
    //                     let mut entry = Entry::from_id(*entry_id, mnm).await?;
    //                     let _ = entry.set_match(&q, USER_AUX_MATCH).await;
    //                 }
    //             }
    //             Err(e) => {
    //                 // Ignore TODO try again with blank description?
    //                 println!("ERROR: {e}");
    //                 return Ok(());
    //             }
    //         }
    //     } else {
    //         qs.sort();
    //         qs.dedup();
    //         if qs.len() == 1 {
    //             let q = qs.first().unwrap(); // Safe
    //             for entry_id in entries_v {
    //                 let mut entry = Entry::from_id(*entry_id, mnm).await?;
    //                 if !entry.is_fully_matched() {
    //                     let _ = entry.set_match(q, USER_AUX_MATCH).await?;
    //                 }
    //             }
    //         } else {
    //             println!("Multiple potential matches for {entries_s} {qs:?}, skipping");
    //         }
    //     }
    //     Ok(())
    // }

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
                println!("ERROR SETTING JOB STATUS: {e}")
            }
            job.run().await
        });
        handle.await.expect("Handle unwrap failed")
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
                    println!("seppuku: {running} jobs running but no activity within {max_age_min} minutes, commiting seppuku");
                    std::process::exit(0);
                }
                // println!("seppuku: honor intact");
            }
        });
    }

    pub async fn forever_loop(&self) -> Result<()> {
        let app = self.clone();
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());

        // Reset old running&failed jobs
        self.storage().reset_running_jobs().await?;
        self.storage().reset_failed_jobs().await?;
        println!("Old jobs reset, starting bot");

        self.seppuku();

        let threshold_job_size = TaskSize::MEDIUM;
        let threshold_percent = 50;

        // TO MANUALLY FIND ACTIONS NOT ASSIGNED A TASK SIZE:
        // select distinct action from jobs where action not in (select action from job_sizes);

        loop {
            let current_jobs_len = current_jobs.len();
            if current_jobs_len >= self.max_concurrent_jobs {
                self.hold_on();
                continue;
            }
            let (mut job, task_size) = self
                .get_next_job(&app, &current_jobs, &threshold_job_size, threshold_percent)
                .await?;
            match job.set_next().await {
                Ok(true) => {
                    Self::run_job(job, task_size, &current_jobs).await;
                }
                Ok(false) => {
                    // println!("No jobs available, waiting... (not using: {:?})",job.skip_actions);
                    self.hold_on();
                }
                Err(_e) => {
                    // Not writing error, there might be an issue that causes stack overflow
                    println!("MAIN LOOP: Something went wrong!");
                    self.hold_on();
                }
            }
        }
        // self.disconnect().await?; // Never happens
    }

    async fn get_next_job(
        &self,
        app: &AppState,
        current_jobs: &Arc<DashMap<usize, TaskSize>>,
        threshold_job_size: &TaskSize,
        threshold_percent: usize,
    ) -> Result<(Job, HashMap<String, TaskSize>), anyhow::Error> {
        let mut job = Job::new(app);
        let task_size = self.storage().jobs_get_tasks().await?;
        let big_jobs_running = (**current_jobs)
            .clone()
            .into_read_only()
            .iter()
            .map(|(_job_id, size)| size.to_owned())
            .filter(|size| *size > *threshold_job_size)
            .count();
        let max_job_size = if big_jobs_running >= self.max_concurrent_jobs * threshold_percent / 100
        {
            threshold_job_size.to_owned()
        } else {
            TaskSize::GINORMOUS
        };
        job.skip_actions = task_size
            .iter()
            .filter(|(_action, size)| **size > max_job_size)
            .map(|(action, _size)| action.to_string())
            .collect();
        Ok((job, task_size))
    }

    fn hold_on(&self) {
        thread::sleep(time::Duration::from_secs(5));
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
        let job_size = task_size
            .get(&action)
            .unwrap_or(&TaskSize::SMALL)
            .to_owned();
        let job_id = match job.get_id().await {
            Ok(id) => id,
            Err(_e) => {
                eprintln!("No job ID"); //,e);
                return;
            }
        };
        current_jobs.insert(job_id, job_size);
        println!("Now {} jobs running", current_jobs.len());
        let current_jobs = current_jobs.clone();
        tokio::spawn(async move {
            if let Err(_e) = job.run().await {
                println!("Job {job_id} failed with error") // Not writing error, there might be an issue that causes stack overflow
            }
            current_jobs.remove(&job_id);
        });
    }
}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}

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
}
