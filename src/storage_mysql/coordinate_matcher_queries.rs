//! `impl CoordinateMatcherQueries for StorageMySQL`. Two reads used by
//! the coordinate matcher, kept in their own file so callers grepping for
//! the trait surface land here directly.

use super::StorageMySQL;
use crate::coordinates::LocationRow;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_row, prelude::*};

#[async_trait]
impl crate::storage::CoordinateMatcherQueries for StorageMySQL {
    async fn get_coordinate_matcher_rows(
        &self,
        catalog_id: &Option<usize>,
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> Result<Vec<LocationRow>> {
        let sql = Self::coordinate_matcher_main_query_sql(catalog_id, bad_catalogs, max_results);
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<LocationRow> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::location_row_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        Ok(rows)
    }

    async fn get_all_catalogs_key_value_pairs(&self) -> Result<Vec<(usize, String, String)>> {
        let sql = r#"SELECT `catalog_id`,`kv_key`,`kv_value` FROM `kv_catalog`"#;
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, String, String)>)
            .await?;
        Ok(results)
    }
}
