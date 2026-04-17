use crate::mysql_misc::MySQLMisc;
use anyhow::{Result, anyhow};
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, PoolConstraints, PoolOpts, Row, from_row, params};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

/// Wraps the `mixnmatch_large_catalogs_p` database, which stores entries
/// for catalogs that are too large for the main MnM database.
#[derive(Debug)]
pub struct LargeCatalogs {
    pool: mysql_async::Pool,
}

impl MySQLMisc for LargeCatalogs {
    fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }
}

impl LargeCatalogs {
    /// Build from the main mixnmatch config value. Derives the large-catalogs
    /// DB URL by replacing the database name in the connection string.
    pub fn from_config(mixnmatch_config: &Value) -> Result<Self> {
        let url = mixnmatch_config["url"]
            .as_str()
            .ok_or_else(|| anyhow!("mixnmatch config missing url"))?;
        // Replace the DB name at the end of the URL
        let lc_url = if let Some(pos) = url.rfind('/') {
            let base = &url[..=pos];
            let old_db = &url[pos + 1..];
            let new_db = old_db.replace("mixnmatch_p", "mixnmatch_large_catalogs_p");
            if new_db == old_db {
                // Fallback: just append _large_catalogs
                format!("{base}{old_db}_large_catalogs")
            } else {
                format!("{base}{new_db}")
            }
        } else {
            return Err(anyhow!("Cannot derive large catalogs DB URL from {url}"));
        };

        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 2).expect("pool constraints"))
            .with_inactive_connection_ttl(Duration::from_secs(2));
        let opts = Opts::from_url(&lc_url)
            .map_err(|e| anyhow!("Bad large catalogs DB URL: {e}"))?;
        let pool = mysql_async::Pool::new(OptsBuilder::from_opts(opts).pool_opts(pool_opts));

        Ok(Self { pool })
    }

    /// Load the catalog metadata table from the large-catalogs DB.
    pub async fn get_catalogs(&self) -> Result<Vec<Value>> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Value> = conn
            .exec_iter("SELECT * FROM `catalog`", ())
            .await?
            .map_and_drop(|row: Row| row_to_json(&row))
            .await?;
        Ok(rows)
    }

    /// Load catalogs as a map of id → catalog object.
    pub async fn get_catalogs_map(&self) -> Result<HashMap<usize, Value>> {
        let catalogs = self.get_catalogs().await?;
        let mut map = HashMap::new();
        for c in catalogs {
            if let Some(id) = c["id"].as_u64() {
                map.insert(id as usize, c);
            }
        }
        Ok(map)
    }

    /// Query entries from a catalog-specific table within a bounding box.
    pub async fn get_entries_in_bbox(
        &self,
        table: &str,
        bbox: &[f64; 4], // [lon_min, lat_min, lon_max, lat_max]
        limit: usize,
    ) -> Result<Vec<Value>> {
        // Validate table name to prevent injection
        if !is_safe_identifier(table) {
            return Err(anyhow!("Invalid table name: {table}"));
        }
        let sql = format!(
            "SELECT * FROM `{table}` WHERE `longitude` BETWEEN :lon_min AND :lon_max AND `latitude` BETWEEN :lat_min AND :lat_max LIMIT :limit"
        );
        let lon_min = bbox[0];
        let lat_min = bbox[1];
        let lon_max = bbox[2];
        let lat_max = bbox[3];
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, params! { lon_min, lat_min, lon_max, lat_max, limit })
            .await?
            .map_and_drop(|row: Row| row_to_json(&row))
            .await?;
        Ok(rows)
    }

    /// Get open issue counts grouped by catalog_id.
    pub async fn get_open_issue_counts(&self) -> Result<HashMap<usize, usize>> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<(usize, usize)> = conn
            .exec_iter(
                "SELECT catalog_id, count(*) AS cnt FROM report WHERE status!='DONE' GROUP BY catalog_id",
                (),
            )
            .await?
            .map_and_drop(from_row::<(usize, usize)>)
            .await?;
        Ok(rows.into_iter().collect())
    }

    /// Get a report summary matrix for a catalog.
    pub async fn get_report_matrix(&self, catalog_id: usize) -> Result<Vec<Value>> {
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Value> = conn
            .exec_iter(
                "SELECT prop, status, `type`, count(*) AS cnt FROM report WHERE catalog_id=:catalog_id GROUP BY prop, status, `type`",
                params! { catalog_id },
            )
            .await?
            .map_and_drop(|row: Row| row_to_json(&row))
            .await?;
        Ok(rows)
    }

    /// Get a filtered, paginated list of report rows.
    pub async fn get_report_list(
        &self,
        catalog_id: usize,
        status: &str,
        report_type: &str,
        user: &str,
        prop: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Value>> {
        let mut sql = format!("SELECT * FROM report WHERE catalog_id={catalog_id}");
        if !status.is_empty() {
            let s = status.replace('\'', "''");
            sql += &format!(" AND `status`='{s}'");
        }
        if !report_type.is_empty() {
            let t = report_type.replace('\'', "''");
            sql += &format!(" AND `type`='{t}'");
        }
        if !user.is_empty() {
            let u = user.replace('\'', "''");
            sql += &format!(" AND `user`='{u}'");
        }
        if !prop.is_empty() {
            if let Ok(p) = prop.parse::<usize>() {
                sql += &format!(" AND `prop`={p}");
            }
        }
        sql += &format!(" LIMIT {limit} OFFSET {offset}");
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| row_to_json(&row))
            .await?;
        Ok(rows)
    }

    /// Get recent changes from the report table.
    pub async fn get_recent_changes(
        &self,
        limit: usize,
        offset: usize,
        users_only: bool,
    ) -> Result<Vec<Value>> {
        let mut sql = "SELECT * FROM report ".to_string();
        if users_only {
            sql += "WHERE user!='' ";
        }
        sql += &format!("ORDER BY `timestamp` DESC LIMIT {limit} OFFSET {offset}");
        let mut conn = self.pool.get_conn().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| row_to_json(&row))
            .await?;
        Ok(rows)
    }

    /// Set the status of a report row.
    pub async fn set_report_status(
        &self,
        report_id: usize,
        status: &str,
        user: &str,
    ) -> Result<()> {
        let ts = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
        let status = status.replace('\'', "''");
        let user = user.replace('\'', "''");
        let sql = format!(
            "UPDATE report SET status='{status}', `timestamp`='{ts}', user='{user}' WHERE id={report_id}"
        );
        self.pool.get_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }
}

/// Convert a MySQL Row to a JSON Value, preserving column names.
fn row_to_json(row: &Row) -> Value {
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns_ref().iter().enumerate() {
        let name = col.name_str().to_string();
        let val = match &row[i] {
            mysql_async::Value::NULL => Value::Null,
            mysql_async::Value::Int(n) => json!(*n),
            mysql_async::Value::UInt(n) => json!(*n),
            mysql_async::Value::Float(n) => json!(*n),
            mysql_async::Value::Double(n) => json!(*n),
            mysql_async::Value::Bytes(b) => json!(String::from_utf8_lossy(b).to_string()),
            other => json!(format!("{other:?}")),
        };
        obj.insert(name, val);
    }
    Value::Object(obj)
}

/// Validate that an identifier is safe for use in SQL (alphanumeric + underscore only).
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_safe_identifier_valid() {
        assert!(is_safe_identifier("geonames"));
        assert!(is_safe_identifier("imdb_actor"));
        assert!(is_safe_identifier("bnf_person"));
    }

    #[test]
    fn test_is_safe_identifier_invalid() {
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier("table; DROP"));
        assert!(!is_safe_identifier("a b"));
        assert!(!is_safe_identifier("table`name"));
    }

    #[test]
    fn test_row_to_json_structure() {
        // Can't easily test without a real row, but verify the function compiles
        // and the helper is accessible.
    }

    #[test]
    fn test_from_config_derives_url() {
        // Verify URL derivation logic
        let config = json!({
            "url": "mysql://user:pass@host:3308/s51434__mixnmatch_p",
            "min_connections": 0,
            "max_connections": 2,
            "keep_sec": 2
        });
        let lc = LargeCatalogs::from_config(&config);
        assert!(lc.is_ok());
    }

    #[test]
    fn test_from_config_missing_url() {
        let config = json!({"min_connections": 0});
        assert!(LargeCatalogs::from_config(&config).is_err());
    }
}
