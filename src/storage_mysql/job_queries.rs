//! `impl JobQueries for StorageMySQL`. Job-queue table operations: queue
//! a new job, pull the next runnable one, flip status, persist note/json
//! payloads, reset stuck rows on bot restart.

use super::StorageMySQL;
use crate::entry::EntryError;
use crate::job_row::JobRow;
use crate::job_status::JobStatus;
use crate::task_size::TaskSize;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use mysql_async::{from_row, params, prelude::*};
use std::collections::HashMap;

#[async_trait]
impl crate::storage::JobQueries for StorageMySQL {
    async fn jobs_get_tasks(&self) -> Result<HashMap<String, TaskSize>> {
        let sql = "SELECT `action`,`size` FROM `job_sizes`";
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?
            .into_iter()
            .filter_map(|(name, size)| TaskSize::new(&size).map(|s| (name, s)))
            .collect();
        Ok(ret)
    }

    /// Resets all RUNNING jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    async fn reset_running_jobs(&self) -> Result<()> {
        let sql = format!(
            "UPDATE /* reset_running_jobs */ `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Running.as_str()
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn kill_long_running_queries(&self, threshold_secs: u64) -> Result<Vec<u64>> {
        // Toolforge ToolsDB only exposes the caller's own connections via
        // information_schema.processlist, but filter by CURRENT_USER() anyway
        // for defence in depth in case the DB user ever gains PROCESS privilege.
        let select_sql = "SELECT /* kill_long_running_queries */ ID \
             FROM information_schema.processlist \
             WHERE USER=SUBSTRING_INDEX(CURRENT_USER(),'@',1) \
               AND COMMAND='Query' \
               AND TIME >= :t \
               AND ID != CONNECTION_ID()";
        let mut conn = self.get_conn().await?;
        let ids: Vec<u64> = conn
            .exec_iter(select_sql, params! { "t" => threshold_secs })
            .await?
            .map_and_drop(from_row::<u64>)
            .await?;
        for id in &ids {
            // KILL doesn't accept parameters; the id came from the server so it's trusted.
            let _ = conn.exec_drop(format!("KILL QUERY {id}"), ()).await;
        }
        Ok(ids)
    }

    /// Resets all FAILED jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    async fn reset_failed_jobs(&self) -> Result<()> {
        let sql = format!(
            "UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Failed.as_str()
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn jobs_queue_simple_job(
        &self,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
        status: &str,
        timestamp: String,
    ) -> Result<usize> {
        let sql = "INSERT INTO `jobs` (catalog,action,status,depends_on,last_ts) VALUES (:catalog_id,:action,:status,:depends_on,:timestamp)
            ON DUPLICATE KEY UPDATE status=:status,depends_on=:depends_on,last_ts=:timestamp";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id,action,depends_on,status,timestamp})
            .await?;
        let last_id = conn
            .last_insert_id()
            .ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(last_id)
    }

    async fn jobs_reset_json(&self, job_id: usize, timestamp: String) -> Result<()> {
        let sql = "UPDATE `jobs` SET `json`=NULL,last_ts=:timestamp WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id, timestamp}).await?;
        Ok(())
    }

    async fn jobs_set_json(
        &self,
        job_id: usize,
        json_string: String,
        timestamp: &str,
    ) -> Result<()> {
        let sql = "UPDATE `jobs` SET `json`=:json_string,last_ts=:timestamp WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id, json_string, timestamp})
            .await?;
        Ok(())
    }

    async fn jobs_row_from_id(&self, job_id: usize) -> Result<JobRow> {
        let sql = r"SELECT id,action,catalog,json,depends_on,status,last_ts,note,repeat_after_sec,next_ts,user_id FROM `jobs` WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        let row = conn
            .exec_iter(sql, params! {job_id})
            .await?
            .map_and_drop(
                from_row::<(
                    usize,
                    String,
                    usize,
                    Option<String>,
                    Option<usize>,
                    String,
                    String,
                    Option<String>,
                    Option<usize>,
                    String,
                    usize,
                )>,
            )
            .await?
            .pop()
            .ok_or(anyhow!("No job with ID {}", job_id))?;
        let job_row = JobRow::from_row(row);
        Ok(job_row)
    }

    async fn jobs_set_status(
        &self,
        status: &JobStatus,
        job_id: usize,
        timestamp: String,
    ) -> Result<()> {
        let status_str = status.as_str();
        let sql = "UPDATE `jobs` SET `status`=:status_str,`last_ts`=:timestamp,`note`=NULL WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,timestamp,status_str})
            .await?;
        Ok(())
    }

    async fn jobs_set_note(
        &self,
        note: Option<String>,
        job_id: usize,
    ) -> Result<Option<String>> {
        let note_cloned = note
            .clone()
            .map(|s| s.get(..127).unwrap_or(&s).to_string());
        let sql = "UPDATE `jobs` SET `note`=substr(:note,1,250) WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,note}).await?;
        Ok(note_cloned)
    }

    async fn jobs_update_next_ts(&self, job_id: usize, next_ts: String) -> Result<()> {
        let sql = "UPDATE `jobs` SET `next_ts`=:next_ts WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,next_ts}).await?;
        Ok(())
    }

    async fn jobs_get_next_job(
        &self,
        status: JobStatus,
        depends_on: Option<JobStatus>,
        no_actions: &[String],
        next_ts: Option<String>,
    ) -> Option<usize> {
        let sql = Self::jobs_get_next_job_construct_sql(status, depends_on, no_actions, next_ts);
        let mut conn = self.get_conn().await.ok()?;
        conn.exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .pop()
    }

    async fn jobs_get_next_job_by_actions(
        &self,
        status: JobStatus,
        only_actions: &[String],
    ) -> Option<usize> {
        if only_actions.is_empty() {
            return None;
        }
        // Action names come from our own task-size config (never user input),
        // but belt-and-braces: filter to ASCII word chars before interpolating.
        let safe: Vec<String> = only_actions
            .iter()
            .filter(|a| a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
            .cloned()
            .collect();
        if safe.is_empty() {
            return None;
        }
        let actions = safe.join("','");
        let sql = format!(
            "SELECT /* jobs_get_next_job_by_actions */ `id` FROM `jobs` \
             WHERE `status`='{status}' \
             AND NOT EXISTS (SELECT * FROM catalog WHERE catalog.id=jobs.catalog AND active!=1) \
             AND `depends_on` IS NULL \
             AND `action` IN ('{actions}') \
             ORDER BY `last_ts` LIMIT 1",
            status = status.as_str(),
        );
        let mut conn = self.get_conn().await.ok()?;
        conn.exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .pop()
    }
}
