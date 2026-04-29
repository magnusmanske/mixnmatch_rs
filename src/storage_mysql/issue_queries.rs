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
