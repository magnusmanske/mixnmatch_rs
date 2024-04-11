use crate::entry::Entry;
use crate::job::*;
use crate::mixnmatch::*;
use anyhow::{anyhow, Result};
use core::time::Duration;
use dashmap::DashMap;
use mysql_async::prelude::*;
use mysql_async::{from_row, Conn, Opts, OptsBuilder, PoolConstraints, PoolOpts};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::sync::Arc;
use std::{thread, time};
use tokio::time::sleep;
use wikimisc::wikibase::ItemEntity;

pub const DB_POOL_MIN: usize = 0;
pub const DB_POOL_MAX: usize = 3;
pub const DB_POOL_KEEP_SEC: u64 = 120;

#[derive(Debug, Clone)]
pub struct AppState {
    wd_pool: mysql_async::Pool,
    mnm_pool: mysql_async::Pool,
    wdrc_pool: mysql_async::Pool,
    pub import_file_path: String,
    pub bot_name: String,
    pub bot_password: String,
    pub task_specific_usize: HashMap<String, usize>,
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

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let task_specific_usize = config["task_specific_usize"]
            .as_object()
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.as_u64().unwrap_or_default() as usize))
            .collect();
        let max_concurrent_jobs = config["max_concurrent_jobs"].as_u64().unwrap_or(10) as usize;
        // let thread_stack_factor = config["thread_stack_factor"].as_u64().unwrap_or(64) as usize;
        // let default_threads= config["default_threads"].as_u64().unwrap_or(64) as usize;
        let ret = Self {
            wd_pool: Self::create_pool(&config["wikidata"]),
            mnm_pool: Self::create_pool(&config["mixnmatch"]),
            wdrc_pool: Self::create_pool(&config["wdrc"]),
            import_file_path: config["import_file_path"].as_str().unwrap().to_string(),
            bot_name: config["bot_name"].as_str().unwrap().to_string(),
            bot_password: config["bot_password"].as_str().unwrap().to_string(),
            task_specific_usize,
            max_concurrent_jobs,
            // runtime: Arc::new(Self::create_runtime(max_concurrent_jobs, default_threads, thread_stack_factor)),
        };
        ret
    }

    /// Helper function to create a DB pool from a JSON config object
    fn create_pool(config: &Value) -> mysql_async::Pool {
        let min_connections = config["min_connections"]
            .as_u64()
            .expect("No min_connections value") as usize;
        let max_connections = config["max_connections"]
            .as_u64()
            .expect("No max_connections value") as usize;
        let keep_sec = config["keep_sec"].as_u64().expect("No keep_sec value");
        let url = config["url"].as_str().expect("No url value");
        let pool_opts = PoolOpts::default()
            .with_constraints(
                PoolConstraints::new(min_connections, max_connections).expect("Constraints error"),
            )
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let wd_url = url;
        let wd_opts = Opts::from_url(wd_url)
            .unwrap_or_else(|_| panic!("Can not build options from db_wd URL {}", wd_url));
        mysql_async::Pool::new(OptsBuilder::from_opts(wd_opts).pool_opts(pool_opts.clone()))
    }

    /// Returns a connection to the Mix'n'Match tool database
    pub async fn get_mnm_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.mnm_pool.get_conn().await
    }

    /// Returns a connection to the Wikidata DB replica
    pub async fn get_wd_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.wd_pool.get_conn().await
    }

    /// Returns a connection to the WDRC tool database
    pub async fn get_wdrc_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.wdrc_pool.get_conn().await
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.wd_pool.clone().disconnect().await?;
        self.mnm_pool.clone().disconnect().await?;
        Ok(())
    }

    pub async fn run_from_props(&self, props: Vec<u32>, min_entries: u16) -> Result<()> {
        if props.len() < 2 {
            return Err(anyhow!("Minimum of two properties required."));
        }
        let mut mnm = MixNMatch::new(self.clone());
        let first_prop = props.first().unwrap(); // Safe
        let mut sql = format!(
            r#"SELECT main_ext_id,group_concat(entry_id),count(DISTINCT entry_id) AS cnt
            FROM ( SELECT entry_id,aux_name AS main_ext_id FROM auxiliary,entry WHERE aux_p={first_prop} and entry.id=entry_id AND (entry.q is null or entry.user=0)
            UNION SELECT entry.id,ext_id FROM entry,catalog WHERE entry.catalog=catalog.id AND catalog.active=1 AND catalog.wd_prop={first_prop} AND (entry.q is null or entry.user=0) ) t1"#
        );
        for (num, prop) in props.iter().skip(1).enumerate() {
            sql += if num == 0 { " WHERE" } else { " AND" };
            sql += &format!(
                r#" entry_id IN (SELECT entry_id FROM auxiliary,entry WHERE aux_p={prop} and entry.id=entry_id UNION SELECT entry.id FROM entry,catalog WHERE entry.catalog=catalog.id AND catalog.active=1 AND catalog.wd_prop={prop})"#
            );
        }
        sql += &format!(r#"GROUP BY main_ext_id HAVING cnt>={min_entries}"#);
        sql = sql.replace(['\n', '\t'], " ");

        let mut conn = self
            .get_mnm_conn()
            .await
            .expect("run_from_props: No DB connection");

        let results: Vec<_> = conn
            .exec_iter(sql, ())
            .await
            .expect("run_from_props: No results")
            .map_and_drop(from_row::<(String, String, usize)>)
            .await
            .expect("run_from_props: Result retrieval failure");

        let props_s = props
            .iter()
            .map(|p| format!("{p}"))
            .collect::<Vec<String>>()
            .join(",");
        for (_primary_ext_id, entries_s, _cnt) in results {
            let entries_v: Vec<_> = entries_s
                .split(',')
                .filter_map(|s| s.parse::<usize>().ok())
                .collect();
            self.create_item_from_entries(&entries_v, &props_s, &mut conn, &mut mnm)
                .await?;
        }
        Ok(())
    }

    pub async fn create_item_from_entries(
        &self,
        entries_v: &[usize],
        props_s: &str,
        conn: &mut Conn,
        mnm: &mut MixNMatch,
    ) -> Result<()> {
        let entries_s: Vec<_> = entries_v.iter().map(|id| format!("{id}")).collect();
        let entries_s = entries_s.join(",");
        let sql = format!(
            r#"SELECT entry_id,aux_p,aux_name FROM auxiliary WHERE entry_id IN ({entries_s}) AND aux_p IN ({props_s}) UNION SELECT entry.id,catalog.wd_prop,ext_id FROM entry,catalog WHERE entry.catalog=catalog.id AND entry.id IN ({entries_s}) AND wd_prop IN ({props_s})"#
        );

        let entry_prop_values: Vec<_> = conn
            .exec_iter(sql, ())
            .await
            .expect("run_from_props: No results")
            .map_and_drop(from_row::<(usize, u32, String)>)
            .await
            .expect("run_from_props: Result retrieval failure");

        let prop_values = entry_prop_values
            .iter()
            .map(|(_entry_id, prop, value)| format!("P{prop}={value}"))
            .collect::<Vec<String>>()
            .join("|");
        let query = format!(r#"haswbstatement:"{prop_values}""#);
        let mut qs = mnm.wd_search(&query).await?;
        if qs.is_empty() {
            println!("Create new item from {entries_s}");
            let mut new_item = ItemEntity::new_empty();
            for entry_id in entries_v {
                let entry = Entry::from_id(*entry_id, mnm).await?;
                entry.add_to_item(&mut new_item).await?;
            }
            // println!("{:#?}", new_item);
            match mnm.create_new_wikidata_item(new_item).await {
                Ok(q) => {
                    println!("Created https://www.wikidata.org/wiki/{q}");
                    for entry_id in entries_v {
                        let mut entry = Entry::from_id(*entry_id, mnm).await?;
                        let _ = entry.set_match(&q, USER_AUX_MATCH).await;
                    }
                }
                Err(e) => {
                    // Ignore TODO try again with blank description?
                    println!("ERROR: {e}");
                    return Ok(());
                }
            }
        } else {
            qs.sort();
            qs.dedup();
            if qs.len() == 1 {
                let q = qs.first().unwrap(); // Safe
                for entry_id in entries_v {
                    let mut entry = Entry::from_id(*entry_id, mnm).await?;
                    if !entry.is_fully_matched() {
                        let _ = entry.set_match(q, USER_AUX_MATCH).await?;
                    }
                }
            } else {
                println!("Multiple potential matches for {entries_s} {qs:?}, skipping");
            }
        }
        Ok(())
    }

    pub async fn run_single_hp_job(&self) -> Result<()> {
        let mnm = MixNMatch::new(self.clone());
        let mut job = Job::new(&mnm);
        if let Some(job_id) = job.get_next_high_priority_job().await {
            job.set_from_id(job_id).await?;
            job.set_status(JobStatus::Running).await?;
            job.run().await?;
        }
        Ok(())
    }

    pub async fn run_single_job(&self, job_id: usize) -> Result<()> {
        let mnm = MixNMatch::new(self.clone());
        let handle = tokio::spawn(async move {
            let mut job = Job::new(&mnm);
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
        let mnm = MixNMatch::new(self.clone());
        tokio::spawn(async move {
            loop {
                sleep(tokio::time::Duration::from_secs(60 * check_every_minutes)).await;
                // println!("seppuku check running");
                let min = chrono::Duration::try_minutes(max_age_min).unwrap();
                let utc = chrono::Utc::now() - min;
                let ts = MixNMatch::get_timestamp_relative(&utc);
                let sql = format!("SELECT
                    (SELECT count(*) FROM jobs WHERE `status` IN ('RUNNING')) AS running,
                    (SELECT count(*) FROM jobs WHERE `status` IN ('RUNNING') AND last_ts>='{ts}') AS running_recent");
                let (running, running_recent) = *mnm
                    .app
                    .get_mnm_conn()
                    .await
                    .expect("seppuku: No DB connection")
                    .exec_iter(sql, ())
                    .await
                    .expect("seppuku: No results")
                    .map_and_drop(from_row::<(usize, usize)>)
                    .await
                    .expect("seppuku: Result retrieval failure")
                    .first()
                    .expect("seppuku: No DB results");
                if running > 0 && running_recent == 0 {
                    println!("seppuku: {running} jobs running but no activity within {max_age_min} minutes, commiting seppuku");
                    std::process::exit(0);
                }
                // println!("seppuku: honor intact");
            }
        });
    }

    pub async fn forever_loop(&self) -> Result<()> {
        let mnm = MixNMatch::new(self.clone());
        let current_jobs: Arc<DashMap<usize, TaskSize>> = Arc::new(DashMap::new());

        // Reset old running&failed jobs
        Job::new(&mnm).reset_running_jobs().await?;
        Job::new(&mnm).reset_failed_jobs().await?;
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
            let mut job = Job::new(&mnm);
            let task_size = job.get_tasks().await?;
            let big_jobs_running = (*current_jobs)
                .clone()
                .into_read_only()
                .iter()
                .map(|(_job_id, size)| size.to_owned())
                .filter(|size| *size > threshold_job_size)
                .count();
            let max_job_size =
                if big_jobs_running >= self.max_concurrent_jobs * threshold_percent / 100 {
                    threshold_job_size.to_owned()
                } else {
                    TaskSize::GINORMOUS
                };
            job.skip_actions = Some(
                task_size
                    .iter()
                    .filter(|(_action, size)| **size > max_job_size)
                    .map(|(action, _size)| action.to_string())
                    .collect(),
            );
            match job.set_next().await {
                Ok(true) => {
                    let _ = job.set_status(JobStatus::Running).await;
                    let action = match job.get_action().await {
                        Ok(action) => action,
                        Err(_) => {
                            let _ = job.set_status(JobStatus::Failed).await;
                            continue;
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
                            continue;
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

    fn hold_on(&self) {
        thread::sleep(time::Duration::from_secs(5));
    }
}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}
