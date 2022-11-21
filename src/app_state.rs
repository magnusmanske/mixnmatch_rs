use std::{thread, time};
use std::sync::{Arc, Mutex};
use std::env;
use std::fs::File;
use serde_json::Value;
use mysql_async::{PoolOpts, PoolConstraints, Opts, OptsBuilder, Conn};
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
    pub import_file_path: String,
    pub bot_name: String,
    pub bot_password: String
}

impl AppState {
    /// Creatre an AppState object from a config JSION file
    pub fn from_config_file(filename: &str) -> Result<Self,GenericError> {
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(Self::from_config(&config))
    }

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let ret = Self {
            wd_pool: Self::create_pool(&config["wikidata"]),
            mnm_pool: Self::create_pool(&config["mixnmatch"]),
            import_file_path: config["import_file_path"].as_str().unwrap().to_string(),
            bot_name: config["bot_name"].as_str().unwrap().to_string(),
            bot_password: config["bot_password"].as_str().unwrap().to_string(),
        };
        ret
    }

    /// Helper function to create a DB pool from a JSON config object
    fn create_pool(config: &Value) -> mysql_async::Pool {
        let min_connections = config["min_connections"].as_u64().expect("No min_connections value") as usize;
        let max_connections = config["max_connections"].as_u64().expect("No max_connections value") as usize;
        let keep_sec = config["keep_sec"].as_u64().expect("No keep_sec value");
        let url = config["url"].as_str().expect("No url value");
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(min_connections, max_connections).unwrap())
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

    pub async fn disconnect(&self) -> Result<(),GenericError> {
        self.wd_pool.clone().disconnect().await?;
        self.mnm_pool.clone().disconnect().await?;
        Ok(())
    }

    pub async fn run_single_job(&self, job_id: usize) -> Result<(),GenericError> {
        let mnm = MixNMatch::new(self.clone());
        let mut job = Job::new(&mnm);
        job.set_from_id(job_id).await?;
        match job.set_status(STATUS_RUNNING).await {
            Ok(_) => {
                println!("Finished successfully");
            }
            Err(e) => {
                println!("ERROR: {}",e);
            }
        }
        job.run().await
    }

    pub async fn forever_loop(&self, max_concurrent: usize) -> Result<(),GenericError> {
        let mnm = MixNMatch::new(self.clone());
        let concurrent:Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    
        // Reset old running&failed jobs
        Job::new(&mnm).reset_running_jobs().await?;
        Job::new(&mnm).reset_failed_jobs().await?;
        println!("Old jobs reset, starting bot");
    
        loop {
            if *concurrent.lock().unwrap()>=max_concurrent {
                println!("Too many");
                self.hold_on();
                continue;
            }
            let mut job = Job::new(&mnm);
            match job.set_next().await {
                Ok(true) => {
                    let _ = job.set_status(STATUS_RUNNING).await;
                    let concurrent = concurrent.clone();
                    tokio::spawn(async move {
                        *concurrent.lock().unwrap() += 1;
                        println!("Now {} jobs running",concurrent.lock().unwrap());
                        let _ = job.run().await;
                        *concurrent.lock().unwrap() -= 1;
                    });
                }
                Ok(false) => {
                    println!("Wait 5");
                    self.hold_on();
                }
                _ => {
                    println!("MAIN LOOP: Something went wrong");
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
