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

#[cfg(test)]
mod tests {
    use crate::test_support;
    use mysql_async::prelude::*;

    /// Pick a scraper id no other test in this process is using. The
    /// cersei table's primary key is the scraper id, so collisions across
    /// parallel tests would manifest as duplicate-key errors.
    fn unique_scraper_id() -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(900_000);
        NEXT.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert a cersei row directly with a non-empty last_sync; the trait's
    /// `add_cersei_catalog` doesn't supply last_sync, but the column is
    /// NOT NULL, so for environments where strict mode is enabled we seed
    /// the row by hand instead.
    async fn seed_cersei_row(catalog_id: usize, scraper_id: usize, last_sync: &str) {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        conn.exec_drop(
            "INSERT INTO `cersei` (`catalog_id`,`cersei_scraper_id`,`last_sync`) \
             VALUES (:catalog_id,:scraper_id,:last_sync)",
            params! { catalog_id, scraper_id, last_sync },
        )
        .await
        .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
    }

    async fn fetch_cersei_last_sync(scraper_id: usize) -> Option<String> {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: Option<(Option<String>,)> = conn
            .exec_first(
                "SELECT `last_sync` FROM `cersei` WHERE `cersei_scraper_id`=:scraper_id",
                params! { scraper_id },
            )
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.and_then(|(s,)| s)
    }

    async fn fetch_entry_fields(entry_id: usize) -> (String, String, String, String) {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: (String, String, String, String) = conn
            .exec_first(
                "SELECT `ext_name`,`ext_desc`,`type`,`ext_url` FROM `entry` WHERE `id`=:id",
                params! { "id" => entry_id },
            )
            .await
            .unwrap()
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row
    }

    // ── get_cersei_scrapers ─────────────────────────────────────────────────

    #[tokio::test]
    async fn get_cersei_scrapers_returns_inserted_row() {
        let app = test_support::test_app().await;
        let scraper_id = unique_scraper_id();
        seed_cersei_row(42, scraper_id, "20260101000000").await;

        let scrapers = app.storage().get_cersei_scrapers().await.unwrap();
        let mine = scrapers.get(&scraper_id).expect("scraper missing from result");
        assert_eq!(mine.cersei_scraper_id, scraper_id);
        assert_eq!(mine.catalog_id, 42);
        assert_eq!(mine.last_sync.as_deref(), Some("20260101000000"));
    }

    // ── update_cersei_last_update ───────────────────────────────────────────

    #[tokio::test]
    async fn update_cersei_last_update_overwrites_value() {
        let app = test_support::test_app().await;
        let scraper_id = unique_scraper_id();
        seed_cersei_row(123, scraper_id, "20260101000000").await;

        app.storage()
            .update_cersei_last_update(scraper_id, "20991231235959")
            .await
            .unwrap();

        assert_eq!(
            fetch_cersei_last_sync(scraper_id).await.as_deref(),
            Some("20991231235959"),
        );
    }

    /// Updating a scraper id that doesn't exist must be a no-op, not an error.
    #[tokio::test]
    async fn update_cersei_last_update_missing_scraper_is_silent() {
        let app = test_support::test_app().await;
        // 8_000_000 is far above the unique_scraper_id range and any seeded ids.
        app.storage()
            .update_cersei_last_update(8_000_000, "20260101000000")
            .await
            .unwrap();
    }

    // ── entry_update_cersei ─────────────────────────────────────────────────

    #[tokio::test]
    async fn entry_update_cersei_overwrites_changed_fields() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();

        app.storage()
            .entry_update_cersei(entry_id, "New Name", "New Desc", "Q5", "https://example.org/x")
            .await
            .unwrap();

        let (name, desc, type_name, url) = fetch_entry_fields(entry_id).await;
        assert_eq!(name, "New Name");
        assert_eq!(desc, "New Desc");
        assert_eq!(type_name, "Q5");
        assert_eq!(url, "https://example.org/x");
    }

    /// `SUBSTR(...,1,127)` clamps `ext_name`; `SUBSTR(...,1,254)` clamps
    /// `ext_desc`. Verify the truncation lands at the documented limits.
    #[tokio::test]
    async fn entry_update_cersei_truncates_long_strings() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();

        let long_name = "A".repeat(200);
        let long_desc = "B".repeat(400);
        app.storage()
            .entry_update_cersei(entry_id, &long_name, &long_desc, "Q5", "")
            .await
            .unwrap();

        let (name, desc, _, _) = fetch_entry_fields(entry_id).await;
        assert_eq!(name.len(), 127, "ext_name must be truncated to 127 chars");
        assert_eq!(desc.len(), 254, "ext_desc must be truncated to 254 chars");
    }
}
