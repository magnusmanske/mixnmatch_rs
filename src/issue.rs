use crate::mixnmatch::*;
use anyhow::Result;
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
        match self {
            IssueError::UnregognizedType => write!(f, "IssueError::UnregognizedType"),
            IssueError::UnregognizedStatus => write!(f, "IssueError::UnregognizedStatus"),
            IssueError::NoIssueWithId(id) => write!(f, "No issue with ID {id}"),
        }
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
    ) -> Result<Self> {
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

    pub async fn insert(&self) -> Result<()> {
        self.mnm.get_storage().issue_insert(self).await?;
        Ok(())
    }
}
