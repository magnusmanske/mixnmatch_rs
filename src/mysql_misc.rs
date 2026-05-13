use anyhow::Result;
use async_trait::async_trait;
use core::time::Duration;
use mysql_async::{Opts, OptsBuilder, PoolConstraints, PoolOpts, futures::GetConn};
use serde_json::Value;

/// Default per-statement timeout for read-only SELECTs, in seconds.
/// MariaDB enforces this via the `max_statement_time` session variable.
/// 120 s is well above any healthy query budget; legitimate long reports
/// should be moved to a dedicated batch path rather than relaxing this.
const DEFAULT_MAX_STATEMENT_TIME_SECS: u64 = 120;

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
        // Optional per-pool override; defaults to DEFAULT_MAX_STATEMENT_TIME_SECS.
        let max_statement_time_secs = config["max_statement_time_secs"]
            .as_u64()
            .unwrap_or(DEFAULT_MAX_STATEMENT_TIME_SECS);
        let pool_opts = PoolOpts::default()
            .with_constraints(
                PoolConstraints::new(min_connections, max_connections).expect("Constraints error"),
            )
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let wd_url = url;
        let wd_opts = Opts::from_url(wd_url)
            .unwrap_or_else(|_| panic!("Can not build options from db_wd URL {wd_url}"));
        // MariaDB's `max_statement_time` aborts read-only SELECT statements
        // that exceed the configured wall-clock budget (seconds). It does
        // *not* affect writes — those still need the supervisor's periodic
        // kill_long_running_queries reaper as a backstop. Applied via
        // `setup` so it survives `Conn::reset` / `change_user`, not just
        // initial handshake.
        let opts_builder = OptsBuilder::from_opts(wd_opts)
            .pool_opts(pool_opts.clone())
            .setup(vec![format!(
                "SET SESSION max_statement_time={max_statement_time_secs}"
            )]);
        mysql_async::Pool::new(opts_builder)
    }

    fn sql_placeholders(num: usize) -> String {
        vec!["?"; num].join(",")
    }
}
