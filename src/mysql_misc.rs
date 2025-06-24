use anyhow::Result;
use async_trait::async_trait;
use core::time::Duration;
use mysql_async::{futures::GetConn, Opts, OptsBuilder, PoolConstraints, PoolOpts};
use serde_json::Value;

#[async_trait]
pub trait MySQLMisc {
    fn pool(&self) -> &mysql_async::Pool;

    fn get_conn(&self) -> GetConn {
        self.pool().get_conn()
    }

    // TODO FIXME this should return a connection to the x0 (wbt_) cluster
    fn get_conn_wbt(&self) -> GetConn {
        self.pool().get_conn()
    }

    async fn disconnect_db(&self) -> Result<()> {
        self.pool().clone().disconnect().await?;
        Ok(())
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

    fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num, "?".to_string());
        placeholders.join(",")
    }
}
