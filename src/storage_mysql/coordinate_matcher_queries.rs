//! `impl CoordinateMatcherQueries for StorageMySQL`. Two reads used by
//! the coordinate matcher, kept in their own file so callers grepping for
//! the trait surface land here directly.

use super::StorageMySQL;
use crate::coordinates::LocationRow;
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::prelude::*;

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
        self.query_ro(
            r#"SELECT `catalog_id`,`kv_key`,`kv_value` FROM `kv_catalog`"#,
            (),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support;
    use mysql_async::prelude::*;

    /// Seed a row into the `location` table so `vw_location` exposes it
    /// to the coordinate-matcher query.
    async fn seed_location(entry_id: usize, lat: f64, lon: f64) {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        conn.exec_drop(
            "INSERT INTO `location` (`entry_id`,`lat`,`lon`,`precision`) \
             VALUES (:entry_id,:lat,:lon,1.0)",
            params! { entry_id, lat, lon },
        )
        .await
        .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
    }

    // ── get_all_catalogs_key_value_pairs ────────────────────────────────────

    #[tokio::test]
    async fn get_all_catalogs_key_value_pairs_returns_seeded_rows() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        app.storage().set_catalog_kv(catalog_id, "test_key_one", "value_one").await.unwrap();
        app.storage().set_catalog_kv(catalog_id, "test_key_two", "value_two").await.unwrap();

        let all = app.storage().get_all_catalogs_key_value_pairs().await.unwrap();
        let mine: Vec<_> = all.iter().filter(|(c, _, _)| *c == catalog_id).collect();
        assert_eq!(mine.len(), 2, "both seeded keys must be returned");
        assert!(mine.iter().any(|(_, k, v)| k == "test_key_one" && v == "value_one"));
        assert!(mine.iter().any(|(_, k, v)| k == "test_key_two" && v == "value_two"));
    }

    // ── get_coordinate_matcher_rows ─────────────────────────────────────────

    /// With a high catalog id that has no matching `location` rows, the
    /// query must return an empty vec — verifies the WHERE-clause + view
    /// path runs without error and returns nothing.
    #[tokio::test]
    async fn get_coordinate_matcher_rows_empty_when_no_locations_for_catalog() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        // No location seeded for this catalog's entry.
        let rows = app
            .storage()
            .get_coordinate_matcher_rows(&Some(catalog_id), &[], 100)
            .await
            .unwrap();
        assert!(rows.is_empty(), "no location rows seeded → empty result");
    }

    #[tokio::test]
    async fn get_coordinate_matcher_rows_returns_seeded_location() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        seed_location(entry_id, 51.5074, -0.1278).await;

        let rows = app
            .storage()
            .get_coordinate_matcher_rows(&Some(catalog_id), &[], 100)
            .await
            .unwrap();
        let mine = rows.iter().find(|r| r.entry_id == entry_id);
        assert!(mine.is_some(), "seeded location must surface in result");
    }

    /// `bad_catalogs` filter must exclude listed catalogs even when they
    /// have matching location rows (random-sample branch).
    #[tokio::test]
    async fn get_coordinate_matcher_rows_excludes_bad_catalogs() {
        let app = test_support::test_app().await;
        let (catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        seed_location(entry_id, 40.7128, -74.0060).await;

        let rows = app
            .storage()
            .get_coordinate_matcher_rows(&None, &[catalog_id], 100)
            .await
            .unwrap();
        assert!(
            rows.iter().all(|r| r.catalog_id != catalog_id),
            "bad_catalogs entry must not appear"
        );
    }
}
