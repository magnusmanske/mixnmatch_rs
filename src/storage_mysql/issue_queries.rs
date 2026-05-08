//! `impl IssueQueries for StorageMySQL` — extracted from the monolithic
//! `mod.rs` to give the issue-tracking SQL its own home and to provide a
//! template for migrating the remaining sub-trait impls.

use super::StorageMySQL;
use crate::issue::{Issue, IssueStatus};
use anyhow::Result;
use async_trait::async_trait;
use mysql_async::{params, prelude::*};

#[async_trait]
impl crate::storage::IssueQueries for StorageMySQL {
    async fn issues_close_for_inactive_catalogs(&self) -> Result<usize> {
        // Use a correlated EXISTS so MySQL can short-circuit per row;
        // a JOIN here would force a temp table on a multi-million-row
        // `issues` join `entry`.
        let sql = "UPDATE `issues` SET `status` = 'INACTIVE_CATALOG' \
            WHERE `status` = 'OPEN' \
              AND EXISTS ( \
                SELECT 1 FROM `entry` \
                INNER JOIN `catalog` ON `catalog`.`id` = `entry`.`catalog` \
                WHERE `entry`.`id` = `issues`.`entry_id` \
                  AND `catalog`.`active` != 1 \
              )";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn issues_close_jan01_mismatches(&self) -> Result<usize> {
        // Match the PHP heuristic verbatim: the JSON payload of a
        // MISMATCH_DATES issue stores the MnM date as `mnm_time` and
        // ends with `-01-01` exactly when the entry's date is
        // year-only / Jan 1 placeholder. Anything more elaborate
        // (e.g. JSON_EXTRACT) needs a JSON column and we still have
        // varchar.
        let sql = "UPDATE `issues` SET `status` = 'JAN01' \
            WHERE `status` = 'OPEN' \
              AND `type` = 'MISMATCH_DATES' \
              AND `json` LIKE '%mnm_time%-01-01%'";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn issues_delete_invalid_q_matches(&self) -> Result<usize> {
        // Q ≤ 0 with a positive `user` means the entry is flagged as
        // N/A (Q=0) or "no Wikidata" (Q=-1) by a human — neither has a
        // corresponding Wikidata item, so any open issue against the
        // entry is meaningless and should be removed outright (not
        // just closed — re-running the issue scanner won't recreate
        // these for the same row).
        let sql = "DELETE FROM `issues` \
            WHERE `status` = 'OPEN' \
              AND EXISTS ( \
                SELECT 1 FROM `entry` \
                WHERE `entry`.`id` = `issues`.`entry_id` \
                  AND `entry`.`q` <= 0 \
                  AND `entry`.`user` > 0 \
              )";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn get_open_wd_duplicates(&self) -> Result<Vec<Issue>> {
        let sql = r"SELECT * FROM `issues` WHERE `status`='OPEN' and `type`='WD_DUPLICATE'";
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Issue::from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(ret)
    }

    async fn issue_insert(&self, issue: &Issue) -> Result<()> {
        let sql = "INSERT IGNORE INTO `issues` (`entry_id`,`type`,`json`,`random`,`catalog`)
        SELECT :entry_id,:issue_type,:json,rand(),`catalog` FROM `entry` WHERE `id`=:entry_id";
        let params = params! {
            "entry_id" => issue.entry_id,
            "issue_type" => issue.issue_type.to_str(),
            "json" => issue.json.to_string(),
            "catalog" => issue.catalog_id,
        };
        self.get_conn().await?.exec_drop(sql, params).await?;
        Ok(())
    }

    async fn set_issue_status(&self, issue_id: usize, status: IssueStatus) -> Result<()> {
        let sql = "UPDATE `issues` SET `status`=:status WHERE `id`=:issue_id";
        let params = params! {
            "issue_id" => issue_id,
            "status" => status.to_str(),
        };
        self.get_conn().await?.exec_drop(sql, params).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::issue::{Issue, IssueStatus, IssueType};
    use crate::test_support;
    use mysql_async::prelude::*;
    use serde_json::json;

    async fn fetch_status_for_entry_type(
        entry_id: usize,
        issue_type: &str,
    ) -> Option<String> {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: Option<(String,)> = conn
            .exec_first(
                "SELECT `status` FROM `issues` WHERE `entry_id`=:entry_id AND `type`=:itype",
                params! { entry_id, "itype" => issue_type },
            )
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.map(|(s,)| s)
    }

    async fn fetch_issue_id_for_entry_type(
        entry_id: usize,
        issue_type: &str,
    ) -> usize {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: Option<(usize,)> = conn
            .exec_first(
                "SELECT `id` FROM `issues` WHERE `entry_id`=:entry_id AND `type`=:itype",
                params! { entry_id, "itype" => issue_type },
            )
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.expect("issue row not found").0
    }

    /// Override an entry's q/user so `issues_delete_invalid_q_matches` will pick it up.
    async fn force_entry_q_and_user(entry_id: usize, q: i64, user: i64) {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        conn.exec_drop(
            "UPDATE `entry` SET `q`=:q, `user`=:user WHERE `id`=:id",
            params! { "q" => q, "user" => user, "id" => entry_id },
        )
        .await
        .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
    }

    async fn count_issues_for_entry(entry_id: usize, issue_type: &str) -> usize {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: (usize,) = conn
            .exec_first(
                "SELECT COUNT(*) FROM `issues` WHERE `entry_id`=:entry_id AND `type`=:itype",
                params! { entry_id, "itype" => issue_type },
            )
            .await
            .unwrap()
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.0
    }

    // ── issue_insert + set_issue_status round-trip ──────────────────────────

    #[tokio::test]
    async fn issue_insert_persists_and_set_status_round_trip() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();

        let issue = Issue::new(entry_id, IssueType::WdDuplicate, json!(["Q1", "Q2"]));
        app.storage().issue_insert(&issue).await.unwrap();

        assert_eq!(
            fetch_status_for_entry_type(entry_id, "WD_DUPLICATE").await.as_deref(),
            Some("OPEN"),
            "fresh insert must be OPEN"
        );

        let issue_id = fetch_issue_id_for_entry_type(entry_id, "WD_DUPLICATE").await;
        app.storage()
            .set_issue_status(issue_id, IssueStatus::ResolvedOnWikidata)
            .await
            .unwrap();
        assert_eq!(
            fetch_status_for_entry_type(entry_id, "WD_DUPLICATE").await.as_deref(),
            Some("RESOLVED_ON_WIKIDATA"),
        );
    }

    /// `INSERT IGNORE` swallows the duplicate-key collision when the same
    /// `(entry_id, type)` pair is inserted twice — second call must be a no-op,
    /// not an error, and the row count must stay 1.
    #[tokio::test]
    async fn issue_insert_is_idempotent_on_unique_key() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let issue = Issue::new(entry_id, IssueType::Mismatch, json!({"foo":"bar"}));
        app.storage().issue_insert(&issue).await.unwrap();
        app.storage().issue_insert(&issue).await.unwrap();
        assert_eq!(count_issues_for_entry(entry_id, "MISMATCH").await, 1);
    }

    /// `issue_insert` joins onto `entry` and uses `entry.catalog` for the
    /// issue's `catalog` column. With no entry row, the INSERT…SELECT returns
    /// zero rows and silently inserts nothing.
    #[tokio::test]
    async fn issue_insert_with_missing_entry_inserts_nothing() {
        let app = test_support::test_app().await;
        let issue = Issue::new(99_999_999, IssueType::ItemDeleted, json!(null));
        app.storage().issue_insert(&issue).await.unwrap();
        assert_eq!(count_issues_for_entry(99_999_999, "ITEM_DELETED").await, 0);
    }

    // ── issues_close_for_inactive_catalogs ──────────────────────────────────

    #[tokio::test]
    async fn issues_close_for_inactive_catalogs_flips_status() {
        let app = test_support::test_app().await;
        let inactive_catalog_id = test_support::seed_inactive_catalog().await.unwrap();
        let entry_id = test_support::seed_entry_in_catalog(inactive_catalog_id, "Inactive Cat Entry")
            .await
            .unwrap();
        app.storage()
            .issue_insert(&Issue::new(entry_id, IssueType::Mismatch, json!("inact")))
            .await
            .unwrap();

        app.storage().issues_close_for_inactive_catalogs().await.unwrap();

        assert_eq!(
            fetch_status_for_entry_type(entry_id, "MISMATCH").await.as_deref(),
            Some("INACTIVE_CATALOG")
        );
    }

    /// Active-catalog issues must NOT be flipped.
    #[tokio::test]
    async fn issues_close_for_inactive_catalogs_leaves_active_alone() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        app.storage()
            .issue_insert(&Issue::new(entry_id, IssueType::Mismatch, json!("active")))
            .await
            .unwrap();

        app.storage().issues_close_for_inactive_catalogs().await.unwrap();

        assert_eq!(
            fetch_status_for_entry_type(entry_id, "MISMATCH").await.as_deref(),
            Some("OPEN"),
            "active catalog issue must stay OPEN"
        );
    }

    // ── issues_close_jan01_mismatches ───────────────────────────────────────

    #[tokio::test]
    async fn issues_close_jan01_mismatches_matches_only_mnm_time_01_01() {
        let app = test_support::test_app().await;
        let (_, entry_id_jan01) = test_support::seed_minimal_entry(&app).await.unwrap();
        let (_, entry_id_other) = test_support::seed_minimal_entry(&app).await.unwrap();

        app.storage()
            .issue_insert(&Issue::new(
                entry_id_jan01,
                IssueType::MismatchDates,
                json!({ "mnm_time": "1900-01-01" }),
            ))
            .await
            .unwrap();
        app.storage()
            .issue_insert(&Issue::new(
                entry_id_other,
                IssueType::MismatchDates,
                json!({ "mnm_time": "1900-06-15" }),
            ))
            .await
            .unwrap();

        app.storage().issues_close_jan01_mismatches().await.unwrap();

        assert_eq!(
            fetch_status_for_entry_type(entry_id_jan01, "MISMATCH_DATES").await.as_deref(),
            Some("JAN01"),
        );
        assert_eq!(
            fetch_status_for_entry_type(entry_id_other, "MISMATCH_DATES").await.as_deref(),
            Some("OPEN"),
        );
    }

    // ── issues_delete_invalid_q_matches ─────────────────────────────────────

    #[tokio::test]
    async fn issues_delete_invalid_q_matches_removes_na_flagged_rows() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        force_entry_q_and_user(entry_id, 0, 7).await;
        app.storage()
            .issue_insert(&Issue::new(entry_id, IssueType::WdDuplicate, json!(["Q1"])))
            .await
            .unwrap();

        app.storage().issues_delete_invalid_q_matches().await.unwrap();

        assert!(
            fetch_status_for_entry_type(entry_id, "WD_DUPLICATE").await.is_none(),
            "issue row for N/A entry must be deleted outright"
        );
    }

    /// q>0 entries are real matches — issues against them must NOT be deleted.
    #[tokio::test]
    async fn issues_delete_invalid_q_matches_keeps_real_matches() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        force_entry_q_and_user(entry_id, 42, 7).await;
        app.storage()
            .issue_insert(&Issue::new(entry_id, IssueType::Mismatch, json!("real")))
            .await
            .unwrap();

        app.storage().issues_delete_invalid_q_matches().await.unwrap();

        assert_eq!(
            fetch_status_for_entry_type(entry_id, "MISMATCH").await.as_deref(),
            Some("OPEN"),
            "issue against q>0 entry must survive"
        );
    }

    // ── get_open_wd_duplicates ──────────────────────────────────────────────

    #[tokio::test]
    async fn get_open_wd_duplicates_returns_inserted_open_issue() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let payload = json!(["Q11", "Q22"]);
        app.storage()
            .issue_insert(&Issue::new(entry_id, IssueType::WdDuplicate, payload.clone()))
            .await
            .unwrap();

        let issues = app.storage().get_open_wd_duplicates().await.unwrap();
        let mine = issues.iter().find(|i| i.entry_id == entry_id);
        assert!(mine.is_some(), "freshly-inserted WD_DUPLICATE must appear in get_open_wd_duplicates");
        assert_eq!(mine.unwrap().json, payload, "JSON payload must round-trip");
    }
}
