use std::env;
use std::fs::File;
use serde_json::Value;
use mysql_async::{PoolOpts, PoolConstraints, Opts, OptsBuilder, Conn};
use core::time::Duration;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

pub const DB_POOL_MIN: usize = 0;
pub const DB_POOL_MAX: usize = 3;
pub const DB_POOL_KEEP_SEC: u64 = 120;

#[derive(Debug, Clone)]
pub struct AppState {
    wd_pool: mysql_async::Pool,
    mnm_pool: mysql_async::Pool
}

impl AppState {
    pub fn from_config_file(filename: &str) -> Result<Self,GenericError> {
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(Self::from_config(&config))
    }

    pub fn from_config(config: &Value) -> Self {
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(DB_POOL_MIN, DB_POOL_MAX).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(DB_POOL_KEEP_SEC));
        let wd_url = config["db_wd"].as_str().expect("No db_wd in config") ;
        let wd_opts = Opts::from_url(wd_url).expect(format!("Can not build options from db_wd URL {}",wd_url).as_str());
        let mnm_url = config["db_mnm"].as_str().expect("No db_mnm in config") ;
        let mnm_opts = Opts::from_url(mnm_url).expect(format!("Can not build options from db_mnm URL {}",mnm_url).as_str());
        let ret = Self {
            wd_pool: mysql_async::Pool::new(OptsBuilder::from_opts(wd_opts).pool_opts(pool_opts.clone())),
            mnm_pool: mysql_async::Pool::new(OptsBuilder::from_opts(mnm_opts).pool_opts(pool_opts.clone()))
        };
        ret
    }

    pub async fn get_mnm_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.mnm_pool.get_conn().await
    }

    pub async fn get_wd_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.wd_pool.get_conn().await
    }

    pub async fn disconnect(&self) -> Result<(),GenericError> {
        //self.wd_pool.disconnect().await?;
        //self.mnm_pool.disconnect().await?;
        Ok(())
    }

}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}
