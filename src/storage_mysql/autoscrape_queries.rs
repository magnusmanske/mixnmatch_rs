//! `impl AutoscrapeQueries for StorageMySQL`. Reads + status writes for
//! the autoscrape subsystem (per-catalog scraper config rows).

use super::StorageMySQL;
use crate::entry_query::EntryQuery;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{from_row, params, prelude::*};

#[async_trait]
impl crate::storage::AutoscrapeQueries for StorageMySQL {
    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>> {
        Ok(self
            .get_conn_ro()
            .await?
            .exec_iter(
                "SELECT `id`,`json` FROM `autoscrape` WHERE `catalog`=:catalog_id",
                params! {catalog_id},
            )
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?)
    }

    /// Returns a list of (ext_id,entry_id) values for the given catalog_id and ext_ids.
    async fn get_entry_ids_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<(String, usize)>> {
        let eq = EntryQuery::default()
            .with_catalog_id(catalog_id)
            .with_ext_ids(ext_ids.to_vec());
        let sql = "SELECT ext_id,id FROM entry WHERE".to_string();
        let (sql, parts) = Self::get_entry_query_sql_where(&eq, sql, vec![])?;
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, parts)
            .await?
            .map_and_drop(from_row::<(String, usize)>)
            .await?;
        Ok(ret)
    }

    async fn autoscrape_start(&self, autoscrape_id: usize) -> Result<()> {
        let sql = "UPDATE `autoscrape` SET `status`='RUNNING'`last_run_min`=NULL,`last_run_urls`=NULL WHERE `id`=:autoscrape_id";
        if let Ok(mut conn) = self.get_conn().await {
            let _ = conn.exec_drop(sql, params! {autoscrape_id}).await; // Ignore error
        }
        Ok(())
    }

    async fn autoscrape_finish(&self, autoscrape_id: usize, last_run_urls: usize) -> Result<()> {
        let sql = "UPDATE `autoscrape` SET `status`='OK',`last_run_min`=NULL,`last_run_urls`=:last_run_urls WHERE `id`=:autoscrape_id";
        if let Ok(mut conn) = self.get_conn().await {
            let _ = conn
                .exec_drop(sql, params! {autoscrape_id,last_run_urls})
                .await;
        }
        Ok(())
    }
}
