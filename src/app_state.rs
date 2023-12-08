use std::collections::HashMap;
use std::{thread, time};
use std::sync::{Arc, Mutex};
use std::env;
use std::fs::File;
use serde_json::Value;
use mysql_async::{PoolOpts, PoolConstraints, Opts, OptsBuilder, Conn};
use tokio::runtime::{Runtime, self};
use core::time::Duration;
use crate::mixnmatch::*;
use crate::job::*;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

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
    pub task_specific_usize: HashMap<String,usize>,
    max_concurrent_jobs: usize,
    pub runtime: Arc<Runtime>,
}

impl AppState {
    /// Creatre an AppState object from a config JSON file
    pub fn from_config_file(filename: &str) -> Result<Self,GenericError> {
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(Self::from_config(&config))
    }

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let task_specific_usize=  config["task_specific_usize"].as_object().unwrap()
            .into_iter()
            .map(|(k,v)|(k.to_owned(),v.as_u64().unwrap_or_default() as usize))
            .collect();
        let max_concurrent_jobs = config["max_concurrent_jobs"].as_u64().unwrap_or(10) as usize;
        let thread_stack_factor = config["thread_stack_factor"].as_u64().unwrap_or(64) as usize;
        let default_threads= config["default_threads"].as_u64().unwrap_or(64) as usize;
        let ret = Self {
            wd_pool: Self::create_pool(&config["wikidata"]),
            mnm_pool: Self::create_pool(&config["mixnmatch"]),
            wdrc_pool: Self::create_pool(&config["wdrc"]),
            import_file_path: config["import_file_path"].as_str().unwrap().to_string(),
            bot_name: config["bot_name"].as_str().unwrap().to_string(),
            bot_password: config["bot_password"].as_str().unwrap().to_string(),
            task_specific_usize,
            max_concurrent_jobs,
            runtime: Arc::new(Self::create_runtime(max_concurrent_jobs, default_threads, thread_stack_factor)),
        };
        ret
    }

    fn create_runtime(_max_concurrent_jobs: usize, default_threads: usize, thread_stack_factor: usize) -> Runtime {
        let threads = match env::var("MNM_THREADS") {
            Ok(s) => s.parse::<usize>().unwrap_or(default_threads),
            Err(_) => default_threads,
        };
        // let threads = cmp::min(threads,max_concurrent_jobs+1); // No point having more threads than max concurrent jobs
        println!("Using {threads} threads");
    
        let threaded_rt = runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads)
            .thread_name("mixnmatch")
            .thread_stack_size(thread_stack_factor * 1024 * 1024)
            .build()
            .expect("Could not create tokio runtime");
        threaded_rt
    }

    /// Helper function to create a DB pool from a JSON config object
    fn create_pool(config: &Value) -> mysql_async::Pool {
        let min_connections = config["min_connections"].as_u64().expect("No min_connections value") as usize;
        let max_connections = config["max_connections"].as_u64().expect("No max_connections value") as usize;
        let keep_sec = config["keep_sec"].as_u64().expect("No keep_sec value");
        let url = config["url"].as_str().expect("No url value");
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(min_connections, max_connections).expect("Constraints error"))
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let wd_url = url;
        let wd_opts = Opts::from_url(wd_url).expect(format!("Can not build options from db_wd URL {}",wd_url).as_str());
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

    pub async fn disconnect(&self) -> Result<(),GenericError> {
        self.wd_pool.clone().disconnect().await?;
        self.mnm_pool.clone().disconnect().await?;
        Ok(())
    }

    pub async fn run_single_hp_job(&self) -> Result<(),GenericError> {
        let mnm = MixNMatch::new(self.clone());
        let mut job = Job::new(&mnm);
        if let Some(job_id) = job.get_next_high_priority_job().await {
            job.set_from_id(job_id).await?;
            job.set_status(JobStatus::Running).await?;
            job.run().await?;
        }
        Ok(())
    }

    pub async fn run_single_job(&self, job_id: usize) -> Result<(),GenericError> {
        let mnm = MixNMatch::new(self.clone());
        let handle = self.runtime.spawn(async move {
            let mut job = Job::new(&mnm);
            job.set_from_id(job_id).await?;
            if let Err(e) = job.set_status(JobStatus::Running).await {
                println!("ERROR SETTING JOB STATUS: {e}")
            }
            job.run().await
        });
        handle.await.expect("Handle unwrap failed")
    }

    pub async fn forever_loop(&self) -> Result<(),GenericError> {
        let mnm = MixNMatch::new(self.clone());
        let current_jobs: Arc<Mutex<HashMap<usize,TaskSize>>> = Arc::new(Mutex::new(HashMap::new()));
    
        // Reset old running&failed jobs
        Job::new(&mnm).reset_running_jobs().await?;
        Job::new(&mnm).reset_failed_jobs().await?;
        println!("Old jobs reset, starting bot");

        let threshold_job_size = TaskSize::MEDIUM;
        let threshold_percent = 50;

        // TO MANUALLY FIND ACTIONS NOT ASSIGNED A TASK SIZE:
        // select distinct action from jobs where action not in (select action from job_sizes);
    
        loop {
            let current_jobs_len = current_jobs.lock().unwrap().len();
            if current_jobs_len >= self.max_concurrent_jobs {
                self.hold_on();
                continue;
            }
            let mut job = Job::new(&mnm);
            let task_size = job.get_tasks().await?;
            let big_jobs_running = current_jobs.lock().unwrap().iter()
                .map(|(_job_id,size)|size.to_owned())
                .filter(|size|*size>threshold_job_size)
                .count();
            let max_job_size = if big_jobs_running>=self.max_concurrent_jobs*threshold_percent/100 { threshold_job_size.to_owned() } else { TaskSize::GINORMOUS };
            job.skip_actions = Some(
                task_size.iter()
                    .filter(|(_action,size)| **size>max_job_size)
                    .map(|(action,_size)| action.to_string())
                    .collect()
            );
            match job.set_next().await {
                Ok(true) => {
                    let _ = job.set_status(JobStatus::Running).await;
                    let action = match job.get_action().await {
                        Ok(action) => action,
                        Err(_) => {
                            let _ = job.set_status(JobStatus::Failed).await;
                            continue;
                        },
                    };
                    let job_size = task_size.get(&action).unwrap_or(&TaskSize::SMALL).to_owned();
                    let job_id = match job.get_id().await {
                        Ok(id) => id,
                        Err(_e) => {
                            eprintln!("No job ID");//,e);
                            continue;
                        }
                    };
                    match current_jobs.lock() {
                        Ok(mut cj) => {
                            cj.insert(job_id,job_size);
                            println!("Now {} jobs running",cj.len());
                        }
                        Err(_e) => {
                            panic!("current_jobs mutex poisoned!");
                        }
                    }
                    let current_jobs = current_jobs.clone();
                    self.runtime.spawn(async move {
                        if let Err(_e) = job.run().await {
                            println!("Job {job_id} failed with error") // Not writing error, there might be an issue that causes stack overflow
                        }
                        current_jobs.lock().unwrap().remove(&job_id);
                    });
                }
                Ok(false) => {
                    println!("No jobs available, waiting... (not using: {:?})",job.skip_actions);
                    self.hold_on();
                }
                Err(_e) => { // Not writing error, there might be an issue that causes stack overflow
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
