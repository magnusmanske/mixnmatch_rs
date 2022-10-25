//use mysql_async::prelude::*;
use mysql_async::{PoolOpts, PoolConstraints, Opts, OptsBuilder, Conn};
use core::time::Duration;
use serde_json::Value;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone)]
pub struct AppState {
    wd_pool: mysql_async::Pool,
    mnm_pool: mysql_async::Pool
}

impl AppState {
    pub async fn new_from_config(config: &Value) -> Self {
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 3).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(60));
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

}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}
