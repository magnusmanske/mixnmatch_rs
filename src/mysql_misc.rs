use anyhow::{Result, anyhow};
use async_trait::async_trait;
use core::time::Duration;
use mysql_async::{Conn, Opts, OptsBuilder, PoolConstraints, PoolOpts};
use serde_json::Value;

/// Default per-statement timeout for read-only SELECTs, in seconds.
/// MariaDB enforces this via the `max_statement_time` session variable.
/// 120 s is well above any healthy query budget; legitimate long reports
/// should be moved to a dedicated batch path rather than relaxing this.
const DEFAULT_MAX_STATEMENT_TIME_SECS: u64 = 120;

/// Maximum time we wait for a connection to be acquired from the pool.
/// `mysql_async::Pool::get_conn` has no built-in acquisition timeout — under
/// pool exhaustion it blocks indefinitely, which previously made an exhausted
/// pool propagate into a hung reaper (the supervisor that exists to recover
/// from exhaustion). 15 s is comfortably greater than any healthy acquisition
/// (sub-100 ms in steady state, single-digit seconds under burst) while
/// surfacing a real outage in time for the per-request budget to react.
pub const GET_CONN_TIMEOUT_SECS: u64 = 15;

/// Wrap a `GetConn` future in `tokio::time::timeout` and map the two error
/// shapes onto a single `anyhow::Error` chain. Free function so it can be
/// shared between the default trait method and the `StorageMySQL` inherent
/// methods without duplicating the timeout literal.
pub async fn acquire_with_timeout(fut: mysql_async::futures::GetConn) -> Result<Conn> {
    match tokio::time::timeout(Duration::from_secs(GET_CONN_TIMEOUT_SECS), fut).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(anyhow!("DB connection failed: {e}")),
        Err(_) => Err(anyhow!(
            "DB connection acquisition timed out after {GET_CONN_TIMEOUT_SECS}s"
        )),
    }
}

#[async_trait]
pub trait MySQLMisc {
    fn pool(&self) -> &mysql_async::Pool;

    /// Acquire a connection, bounded by `GET_CONN_TIMEOUT_SECS`. The async
    /// signature is compatible with the original `fn -> GetConn` shape at
    /// every call site that does `.get_conn().await?` (the universal form
    /// across the codebase) — the only observable change is that a hung
    /// acquire now surfaces as a logged error after 15 s instead of
    /// blocking the calling future forever.
    async fn get_conn(&self) -> Result<Conn> {
        acquire_with_timeout(self.pool().get_conn()).await
    }

    // TODO FIXME this should return a connection to the x0 (wbt_) cluster
    async fn get_conn_wbt(&self) -> Result<Conn> {
        acquire_with_timeout(self.pool().get_conn()).await
    }

    async fn disconnect_db(&self) -> Result<()> {
        self.pool().clone().disconnect().await?;
        Ok(())
    }

    /// Helper function to create a DB pool from a JSON config object.
    ///
    /// Returns `Err` (rather than panicking) on missing or malformed config
    /// keys. A typo in `config.json` used to crash the binary at startup
    /// with a Rust panic; now it surfaces as an `anyhow::Error` that the
    /// caller (`AppState::from_config`) propagates upwards as a normal
    /// boot-time error message.
    fn create_pool(config: &Value) -> Result<mysql_async::Pool> {
        let min_connections = config["min_connections"]
            .as_u64()
            .ok_or_else(|| anyhow!("DB config missing min_connections"))?
            as usize;
        let max_connections = config["max_connections"]
            .as_u64()
            .ok_or_else(|| anyhow!("DB config missing max_connections"))?
            as usize;
        let keep_sec = config["keep_sec"]
            .as_u64()
            .ok_or_else(|| anyhow!("DB config missing keep_sec"))?;
        let url = config["url"]
            .as_str()
            .ok_or_else(|| anyhow!("DB config missing url"))?;
        // Optional per-pool override; defaults to DEFAULT_MAX_STATEMENT_TIME_SECS.
        let max_statement_time_secs = config["max_statement_time_secs"]
            .as_u64()
            .unwrap_or(DEFAULT_MAX_STATEMENT_TIME_SECS);
        let pool_opts = PoolOpts::default()
            .with_constraints(
                PoolConstraints::new(min_connections, max_connections).ok_or_else(|| {
                    anyhow!(
                        "invalid pool constraints: min={min_connections} max={max_connections}"
                    )
                })?,
            )
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let wd_opts =
            Opts::from_url(url).map_err(|e| anyhow!("invalid DB URL '{url}': {e}"))?;
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
        Ok(mysql_async::Pool::new(opts_builder))
    }

    fn sql_placeholders(num: usize) -> String {
        vec!["?"; num].join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The acquisition timeout must be comfortably greater than any healthy
    /// pool acquire (single-digit seconds even under burst) but small enough
    /// that an exhausted pool surfaces inside a single API request budget
    /// (~30 s for most handlers). 5..=60 seconds is the safe window — drift
    /// outside it is almost certainly a bug.
    #[test]
    fn get_conn_timeout_within_sensible_bounds() {
        assert!(
            (5..=60).contains(&GET_CONN_TIMEOUT_SECS),
            "GET_CONN_TIMEOUT_SECS={GET_CONN_TIMEOUT_SECS} outside sane window [5, 60]"
        );
    }

    /// Pin the failure-mode behaviour: a future that never resolves must
    /// surface as an `Elapsed` error from `tokio::time::timeout` — the
    /// primitive `acquire_with_timeout` is built on. We use a 10ms timeout
    /// against a pending-forever future so the test runs instantly without
    /// needing virtual-time control.
    #[tokio::test]
    async fn pending_future_times_out_not_blocks() {
        let never = std::future::pending::<()>();
        let res = tokio::time::timeout(Duration::from_millis(10), never).await;
        assert!(res.is_err(), "expected Elapsed, got {res:?}");
    }

    // Probe the create_pool config-validation path against a dummy type whose
    // only purpose is to satisfy the trait. We can't (and shouldn't) hit a
    // real database from a unit test, so we feed valid-shape configs that
    // never get used — the test pins the *missing-key* error paths only.
    struct DummyPool {}
    impl MySQLMisc for DummyPool {
        fn pool(&self) -> &mysql_async::Pool {
            unreachable!("test never acquires a connection")
        }
    }

    #[test]
    fn create_pool_returns_err_on_missing_min_connections() {
        let cfg = serde_json::json!({
            "max_connections": 2,
            "keep_sec": 60,
            "url": "mysql://u:p@h/db",
        });
        let err = DummyPool::create_pool(&cfg).expect_err("missing min_connections must error");
        assert!(err.to_string().contains("min_connections"), "got {err}");
    }

    #[test]
    fn create_pool_returns_err_on_missing_url() {
        let cfg = serde_json::json!({
            "min_connections": 0,
            "max_connections": 2,
            "keep_sec": 60,
        });
        let err = DummyPool::create_pool(&cfg).expect_err("missing url must error");
        assert!(err.to_string().contains("url"), "got {err}");
    }

    #[test]
    fn create_pool_returns_err_on_bad_url() {
        let cfg = serde_json::json!({
            "min_connections": 0,
            "max_connections": 2,
            "keep_sec": 60,
            "url": "not a url",
        });
        let err = DummyPool::create_pool(&cfg).expect_err("bad url must error");
        assert!(err.to_string().contains("invalid DB URL"), "got {err}");
    }
}
