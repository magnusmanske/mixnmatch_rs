//! `impl CerseiQueries for StorageMySQL`. Cersei is the upstream
//! scraper-config registry — these queries sync the local cache.

use super::StorageMySQL;
use crate::storage::CurrentScraper;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{Row, params, prelude::*};
use std::collections::HashMap;

#[async_trait]
impl crate::storage::CerseiQueries for StorageMySQL {
    /// Get current scrapers from database
    async fn get_cersei_scrapers(&self) -> Result<HashMap<usize, CurrentScraper>> {
        let mut conn = self.get_conn_ro().await?;
        let sql = "SELECT * FROM `cersei`";
        let rows: Vec<Row> = conn.query(sql).await?;

        let mut scrapers = HashMap::new();
        for row in rows {
            let scraper = CurrentScraper {
                cersei_scraper_id: row
                    .get::<Option<usize>, _>("cersei_scraper_id")
                    .flatten()
                    .unwrap_or(0),
                catalog_id: row
                    .get::<Option<usize>, _>("catalog_id")
                    .flatten()
                    .unwrap_or(0),
                last_sync: row.get::<Option<String>, _>("last_sync").flatten(),
            };
            scrapers.insert(scraper.cersei_scraper_id, scraper);
        }

        Ok(scrapers)
    }

    async fn add_cersei_catalog(&self, catalog_id: usize, scraper_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let sql = "INSERT INTO `cersei` (`catalog_id`, `cersei_scraper_id`) VALUES (:catalog_id, :scraper_id)";
        conn.exec_drop(sql, params! {catalog_id, scraper_id})
            .await?;
        Ok(())
    }

    async fn update_cersei_last_update(&self, scraper_id: usize, last_sync: &str) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `cersei` SET `last_sync`=:last_sync WHERE `cersei_scraper_id`=:scraper_id",
                params! {last_sync, scraper_id},
            )
            .await?;
        Ok(())
    }

    async fn entry_update_cersei(
        &self,
        entry_id: usize,
        ext_name: &str,
        ext_desc: &str,
        type_name: &str,
        ext_url: &str,
    ) -> Result<()> {
        let type_name = crate::entry::normalize_entry_type(Some(type_name));
        let sql = "UPDATE `entry` \
            SET `ext_name`=SUBSTR(:ext_name,1,127), `ext_desc`=SUBSTR(:ext_desc,1,254), \
                `type`=:type_name, `ext_url`=:ext_url \
            WHERE `id`=:entry_id \
            AND (`ext_name`!=SUBSTR(:ext_name,1,127) OR `ext_desc`!=SUBSTR(:ext_desc,1,254) \
                 OR `type`!=:type_name OR `ext_url`!=:ext_url)";
        self.get_conn()
            .await?
            .exec_drop(
                sql,
                params! {ext_name, ext_desc, type_name, ext_url, entry_id},
            )
            .await?;
        Ok(())
    }
}
