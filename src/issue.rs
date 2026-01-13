use crate::app_state::AppState;
use anyhow::{Result, anyhow};
use futures::future::join_all;
use mysql_async::Row;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy)]
pub enum IssueError {
    UnregognizedType,
    UnregognizedStatus,
    NoIssueWithId(usize),
}

impl Error for IssueError {}

impl fmt::Display for IssueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IssueError::UnregognizedType => write!(f, "IssueError::UnregognizedType"),
            IssueError::UnregognizedStatus => write!(f, "IssueError::UnregognizedStatus"),
            IssueError::NoIssueWithId(id) => write!(f, "No issue with ID {id}"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

    pub const fn to_str(&self) -> &str {
        match self {
            IssueType::WdDuplicate => "WD_DUPLICATE",
            IssueType::Mismatch => "MISMATCH",
            IssueType::ItemDeleted => "ITEM_DELETED",
            IssueType::MismatchDates => "MISMATCH_DATES",
            IssueType::Multiple => "MULTIPLE",
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

    pub const fn to_str(&self) -> &str {
        match self {
            IssueStatus::Open => "OPEN",
            IssueStatus::Done => "DONE",
            IssueStatus::InactiveCatalog => "INACTIVE_CATALOG",
            IssueStatus::ResolvedOnWikidata => "RESOLVED_ON_WIKIDATA",
            IssueStatus::Jan01 => "JAN01",
        }
    }
}

#[derive(Debug)]
pub struct Issue {
    id: Option<usize>,
    pub entry_id: usize,
    pub issue_type: IssueType,
    pub json: Value,
    pub status: IssueStatus,
    pub user_id: Option<usize>,
    pub resolved_ts: Option<String>,
    pub catalog_id: usize,
    app: Option<AppState>,
}

impl Issue {
    pub async fn new(
        entry_id: usize,
        issue_type: IssueType,
        json: Value,
        app: &AppState,
    ) -> Result<Self> {
        Ok(Self {
            app: Some(app.clone()),
            id: None,
            entry_id,
            issue_type,
            json,
            status: IssueStatus::Open,
            user_id: None,
            resolved_ts: None,
            catalog_id: 0,
        })
    }

    pub async fn insert(&self) -> Result<()> {
        self.app
            .clone()
            .ok_or(anyhow!("No app state provided"))?
            .storage()
            .issue_insert(self)
            .await?;
        Ok(())
    }

    pub fn from_row(row: &Row) -> Option<Self> {
        let issue_type: String = row.get("type")?;
        let json: String = row.get("json")?;
        let status: String = row.get("status")?;
        let user_id = row.get_opt("user_id")?.ok();
        let resolved_ts = row.get_opt("resolved_ts")?.ok();
        Some(Self {
            id: row.get("id"),
            entry_id: row.get("entry_id")?,
            issue_type: IssueType::new(&issue_type).ok()?,
            json: serde_json::from_str(&json).ok()?,
            status: IssueStatus::new(&status).ok()?,
            user_id,
            resolved_ts,
            catalog_id: row.get("catalog")?,
            app: None,
        })
    }

    pub async fn fix_wd_duplicates(app: &AppState) -> Result<()> {
        let issues = app.storage().get_open_wd_duplicates().await?;
        let mut items = issues
            .iter()
            .filter_map(|issue| issue.json.as_array())
            .flatten()
            .filter_map(|q| q.as_str())
            .map(|q| q.to_string())
            .collect::<Vec<_>>();
        items.sort();
        items.dedup();
        let redirected_from_to: HashMap<String, String> = app
            .wikidata()
            .get_redirected_items(&items)
            .await?
            .into_iter()
            .collect();
        let resolved_ids: Vec<usize> = issues
            .iter()
            .filter_map(|issue| {
                let issue_items: Vec<String> = issue
                    .json
                    .as_array()?
                    .iter()
                    .filter_map(|q| q.as_str().map(|q| q.to_string()))
                    .collect();
                // This will leave duplicates with three or more items alone!
                match &issue_items.as_slice() {
                    &[q1, q2] => {
                        if redirected_from_to.get(q1) == Some(q2)
                            || redirected_from_to.get(q2) == Some(q1)
                        {
                            Some(issue.id)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .flatten()
            .collect();
        if resolved_ids.is_empty() {
            return Ok(());
        }

        let mut futures = Vec::new();
        for id in resolved_ids {
            let future = app
                .storage()
                .set_issue_status(id, IssueStatus::ResolvedOnWikidata);
            futures.push(future);
        }
        let _results = join_all(futures).await;
        Ok(())
    }
}
