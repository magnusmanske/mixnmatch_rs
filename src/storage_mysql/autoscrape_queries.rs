//! `impl AutoscrapeQueries for StorageMySQL`. Reads + status writes for
//! the autoscrape subsystem (per-catalog scraper config rows).

use super::StorageMySQL;
use crate::entry_query::EntryQuery;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{params, prelude::*};

#[async_trait]
impl crate::storage::AutoscrapeQueries for StorageMySQL {
    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>> {
        self.query_ro(
            "SELECT `id`,`json` FROM `autoscrape` WHERE `catalog`=:catalog_id",
            params! {catalog_id},
        )
        .await
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
        self.query_ro(&sql, parts).await
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

    async fn delete_autoscraper(&self, catalog_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        conn.exec_drop(
            "DELETE FROM `autoscrape` WHERE `catalog`=:catalog_id",
            params! {catalog_id},
        )
        .await?;
        conn.exec_drop(
            "DELETE FROM `jobs` WHERE `catalog`=:catalog_id AND `action`='autoscrape'",
            params! {catalog_id},
        )
        .await?;
        Ok(())
    }

    async fn get_autoscrape_job_repeat(&self, catalog_id: usize) -> Result<Option<usize>> {
        let rows: Vec<Option<usize>> = self
            .query_ro(
                "SELECT `repeat_after_sec` FROM `jobs` WHERE `catalog`=:catalog_id AND `action`='autoscrape' LIMIT 1",
                params! {catalog_id},
            )
            .await?;
        Ok(rows.into_iter().next().flatten())
    }

    async fn set_autoscrape_job_repeat(
        &self,
        catalog_id: usize,
        repeat_after_sec: Option<usize>,
    ) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `jobs` SET `repeat_after_sec`=:repeat_after_sec WHERE `catalog`=:catalog_id AND `action`='autoscrape'",
                params! {catalog_id, repeat_after_sec},
            )
            .await?;
        Ok(())
    }
}
