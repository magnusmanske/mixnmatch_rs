use crate::app_state::*;
use crate::mixnmatch::*;
use mysql_async::prelude::*;
use serde_json::Value;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum IssueError {
    UnregognizedType,
    UnregognizedStatus,
    NoIssueWithId(usize),
}

impl Error for IssueError {}

impl fmt::Display for IssueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}

pub enum IssueType {
    WdDuplicate,
    Mismatch,
    ItemDeleted,
    MismatchDates,
    Multiple,
}

impl IssueType {
    pub fn new(s: &str) -> Result<Self, IssueError> {
        match s {
            "WD_DUPLICATE" => Ok(IssueType::WdDuplicate),
            "MISMATCH" => Ok(IssueType::Mismatch),
            "ITEM_DELETED" => Ok(IssueType::ItemDeleted),
            "MISMATCH_DATES" => Ok(IssueType::MismatchDates),
            "MULTIPLE" => Ok(IssueType::Multiple),
            _ => Err(IssueError::UnregognizedType),
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            IssueType::WdDuplicate => "WD_DUPLICATE",
            IssueType::Mismatch => "MISMATCH",
            IssueType::ItemDeleted => "ITEM_DELETED",
            IssueType::MismatchDates => "MISMATCH_DATES",
            IssueType::Multiple => "MULTIPLE",
        }
    }
}

pub enum IssueStatus {
    Open,
    Done,
    InactiveCatalog,
    ResolvedOnWikidata,
    Jan01,
}

impl IssueStatus {
    pub fn new(s: &str) -> Result<Self, IssueError> {
        match s {
            "OPEN" => Ok(IssueStatus::Open),
            "DONE" => Ok(IssueStatus::Done),
            "INACTIVE_CATALOG" => Ok(IssueStatus::InactiveCatalog),
            "RESOLVED_ON_WIKIDATA" => Ok(IssueStatus::ResolvedOnWikidata),
            "JAN01" => Ok(IssueStatus::Jan01),
            _ => Err(IssueError::UnregognizedStatus),
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            IssueStatus::Open => "OPEN",
            IssueStatus::Done => "DONE",
            IssueStatus::InactiveCatalog => "INACTIVE_CATALOG",
            IssueStatus::ResolvedOnWikidata => "RESOLVED_ON_WIKIDATA",
            IssueStatus::Jan01 => "JAN01",
        }
    }
}

pub struct Issue {
    pub entry_id: usize,
    pub issue_type: IssueType,
    pub json: Value,
    pub status: IssueStatus,
    pub user_id: Option<usize>,
    pub resolved_ts: Option<String>,
    pub catalog_id: usize,
    mnm: MixNMatch,
}

impl Issue {
    pub async fn new(
        entry_id: usize,
        issue_type: IssueType,
        json: Value,
        mnm: &MixNMatch,
    ) -> Result<Self, GenericError> {
        Ok(Self {
            mnm: mnm.clone(),
            entry_id,
            issue_type,
            json,
            status: IssueStatus::Open,
            user_id: None,
            resolved_ts: None,
            catalog_id: 0,
        })
    }

    pub async fn insert(&self) -> Result<(), GenericError> {
        let sql = "INSERT IGNORE INTO `issues` (`entry_id`,`type`,`json`,`random`,`catalog`)
            SELECT :entry_id,:issue_type,:json,rand(),`catalog` FROM `entry` WHERE `id`=:entry_id";
        let params = params! {
            "entry_id" => self.entry_id,
            "issue_type" => self.issue_type.to_str(),
            "json" => self.json.to_string(),
            //"status" => self.status.to_str(),
            //"user_id" => self.user_id,
            //"resolved_ts" => &self.resolved_ts,
            "catalog" => self.catalog_id,
        };
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use mysql_async::from_row;
    use serde_json::json;

    const _TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_issue_insert() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let entry_id = TEST_ENTRY_ID;

        // Cleanup
        mnm.app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_drop(
                "DELETE FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap();

        let issues_for_entry = *mnm
            .app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_iter(
                "SELECT count(*) AS `cnt` FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap()
            .map_and_drop(from_row::<usize>)
            .await
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(issues_for_entry, 0);

        let issue = Issue::new(entry_id, IssueType::Mismatch, json!("!"), &mnm)
            .await
            .unwrap();
        issue.insert().await.unwrap();

        let issues_for_entry = *mnm
            .app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_iter(
                "SELECT count(*) AS `cnt` FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap()
            .map_and_drop(from_row::<usize>)
            .await
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(issues_for_entry, 1);

        // Cleanup
        mnm.app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_drop(
                "DELETE FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap();
    }
}
