use crate::app_state::AppState;
use crate::storage::IssueQueries;
use anyhow::Result;
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
}

impl Issue {
    pub fn new(entry_id: usize, issue_type: IssueType, json: Value) -> Self {
        Self {
            id: None,
            entry_id,
            issue_type,
            json,
            status: IssueStatus::Open,
            user_id: None,
            resolved_ts: None,
            catalog_id: 0,
        }
    }

    /// Persist this issue. Takes the narrow `&dyn IssueQueries` view of
    /// storage rather than the full `AppState`, so callers and tests can
    /// supply a fake that implements only the issue-related methods.
    pub async fn insert(&self, queries: &dyn IssueQueries) -> Result<()> {
        queries.issue_insert(self).await
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
        })
    }

    /// Run every periodic issue-table maintenance pass in sequence.
    /// Mirrors PHP `Maintenance::updateIssues`: status-flip issues from
    /// inactive catalogs, suppress Jan 1 placeholder MISMATCH_DATES,
    /// delete issues against entries the user has flagged N/A or
    /// no-Wikidata, then auto-close WD_DUPLICATE pairs that are just
    /// Wikidata-side redirects.
    ///
    /// Each step is best-effort: a transient failure in one pass
    /// surfaces a `warn!` but doesn't keep the rest from running, so
    /// one stuck step can't starve the others on every cron tick.
    pub async fn sweep_open(app: &AppState) -> Result<()> {
        macro_rules! run_step {
            ($label:literal, $call:expr) => {
                match $call.await {
                    Ok(n) => log::info!("issues sweep: {} closed/deleted {} row(s)", $label, n),
                    Err(e) => log::warn!("issues sweep: {} failed: {e}", $label),
                }
            };
        }
        run_step!(
            "inactive_catalog",
            app.storage().issues_close_for_inactive_catalogs()
        );
        run_step!(
            "jan01_mismatch",
            app.storage().issues_close_jan01_mismatches()
        );
        run_step!(
            "invalid_q_match",
            app.storage().issues_delete_invalid_q_matches()
        );
        // The duplicate-resolver hits Wikidata, so a network blip in
        // the middle would otherwise abort the whole sweep — keep it
        // last and tolerate failure the same way as the SQL passes.
        if let Err(e) = Self::fix_wd_duplicates(app).await {
            log::warn!("issues sweep: fix_wd_duplicates failed: {e}");
        }
        Ok(())
    }

    pub async fn fix_wd_duplicates(app: &AppState) -> Result<()> {
        let issues = app.storage().get_open_wd_duplicates().await?;
        let items = collect_unique_qids(&issues);
        let redirected_from_to: HashMap<String, String> = app
            .wikidata()
            .get_redirected_items(&items)
            .await?
            .into_iter()
            .collect();
        let resolved_ids = resolved_duplicate_ids(&issues, &redirected_from_to);
        if resolved_ids.is_empty() {
            return Ok(());
        }

        let queries: &dyn IssueQueries = app.storage().as_ref().as_ref();
        let mut futures = Vec::new();
        for id in resolved_ids {
            futures.push(queries.set_issue_status(id, IssueStatus::ResolvedOnWikidata));
        }
        let _results = join_all(futures).await;
        Ok(())
    }
}

/// All distinct Q-strings referenced by any of the given duplicate-issue
/// payloads. Sorted + deduped so the upstream Wikidata redirect lookup
/// makes one query per unique QID.
fn collect_unique_qids(issues: &[Issue]) -> Vec<String> {
    let mut items: Vec<String> = issues
        .iter()
        .filter_map(|issue| issue.json.as_array())
        .flatten()
        .filter_map(|q| q.as_str())
        .map(|q| q.to_string())
        .collect();
    items.sort();
    items.dedup();
    items
}

/// Pick out the issue ids that can now be auto-resolved: a WD_DUPLICATE
/// pair is moot once one Q has been redirected to the other. Three-or-more
/// duplicates are left alone — the redirect map can't disambiguate which
/// of the items is the canonical survivor.
fn resolved_duplicate_ids(
    issues: &[Issue],
    redirected_from_to: &HashMap<String, String>,
) -> Vec<usize> {
    issues
        .iter()
        .filter_map(|issue| {
            let issue_items: Vec<String> = issue
                .json
                .as_array()?
                .iter()
                .filter_map(|q| q.as_str().map(|q| q.to_string()))
                .collect();
            match issue_items.as_slice() {
                [q1, q2] => {
                    if redirected_from_to.get(q1) == Some(q2)
                        || redirected_from_to.get(q2) == Some(q1)
                    {
                        issue.id
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_issue_type_new_valid() {
        assert!(matches!(
            IssueType::new("WD_DUPLICATE"),
            Ok(IssueType::WdDuplicate)
        ));
        assert!(matches!(
            IssueType::new("MISMATCH"),
            Ok(IssueType::Mismatch)
        ));
        assert!(matches!(
            IssueType::new("ITEM_DELETED"),
            Ok(IssueType::ItemDeleted)
        ));
        assert!(matches!(
            IssueType::new("MISMATCH_DATES"),
            Ok(IssueType::MismatchDates)
        ));
        assert!(matches!(
            IssueType::new("MULTIPLE"),
            Ok(IssueType::Multiple)
        ));
    }

    #[test]
    fn test_issue_type_new_invalid() {
        assert!(IssueType::new("").is_err());
        assert!(IssueType::new("UNKNOWN").is_err());
        assert!(IssueType::new("wd_duplicate").is_err());
    }

    #[test]
    fn test_issue_type_round_trip() {
        let types = [
            IssueType::WdDuplicate,
            IssueType::Mismatch,
            IssueType::ItemDeleted,
            IssueType::MismatchDates,
            IssueType::Multiple,
        ];
        for t in &types {
            let s = t.to_str();
            let round_tripped = IssueType::new(s).unwrap();
            assert_eq!(s, round_tripped.to_str());
        }
    }

    #[test]
    fn test_issue_status_new_valid() {
        assert!(matches!(IssueStatus::new("OPEN"), Ok(IssueStatus::Open)));
        assert!(matches!(IssueStatus::new("DONE"), Ok(IssueStatus::Done)));
        assert!(matches!(
            IssueStatus::new("INACTIVE_CATALOG"),
            Ok(IssueStatus::InactiveCatalog)
        ));
        assert!(matches!(
            IssueStatus::new("RESOLVED_ON_WIKIDATA"),
            Ok(IssueStatus::ResolvedOnWikidata)
        ));
        assert!(matches!(IssueStatus::new("JAN01"), Ok(IssueStatus::Jan01)));
    }

    #[test]
    fn test_issue_status_new_invalid() {
        assert!(IssueStatus::new("").is_err());
        assert!(IssueStatus::new("UNKNOWN").is_err());
        assert!(IssueStatus::new("open").is_err());
    }

    #[test]
    fn test_issue_status_round_trip() {
        let statuses = [
            IssueStatus::Open,
            IssueStatus::Done,
            IssueStatus::InactiveCatalog,
            IssueStatus::ResolvedOnWikidata,
            IssueStatus::Jan01,
        ];
        for s in &statuses {
            let str_val = s.to_str();
            let round_tripped = IssueStatus::new(str_val).unwrap();
            assert_eq!(str_val, round_tripped.to_str());
        }
    }

    #[test]
    fn test_issue_error_display() {
        assert_eq!(
            format!("{}", IssueError::UnregognizedType),
            "IssueError::UnregognizedType"
        );
        assert_eq!(
            format!("{}", IssueError::UnregognizedStatus),
            "IssueError::UnregognizedStatus"
        );
        assert_eq!(
            format!("{}", IssueError::NoIssueWithId(42)),
            "No issue with ID 42"
        );
    }

    fn make_issue(id: usize, qids: &[&str]) -> Issue {
        let mut issue = Issue::new(
            42,
            IssueType::WdDuplicate,
            serde_json::json!(qids.iter().collect::<Vec<_>>()),
        );
        issue.id = Some(id);
        issue
    }

    #[test]
    fn collect_unique_qids_sorts_and_dedups() {
        let issues = vec![
            make_issue(1, &["Q5", "Q10"]),
            make_issue(2, &["Q5", "Q10", "Q20"]),
            make_issue(3, &["Q20", "Q5"]),
        ];
        assert_eq!(
            collect_unique_qids(&issues),
            vec!["Q10".to_string(), "Q20".to_string(), "Q5".to_string()]
        );
    }

    #[test]
    fn collect_unique_qids_handles_empty() {
        assert!(collect_unique_qids(&[]).is_empty());
    }

    #[test]
    fn resolved_duplicate_ids_picks_pair_when_redirect_matches_either_direction() {
        let issues = vec![
            make_issue(1, &["Q10", "Q20"]),
            make_issue(2, &["Q30", "Q40"]),
        ];
        let mut redirects = HashMap::new();
        redirects.insert("Q10".to_string(), "Q20".to_string());
        redirects.insert("Q40".to_string(), "Q30".to_string());
        let mut resolved = resolved_duplicate_ids(&issues, &redirects);
        resolved.sort();
        assert_eq!(resolved, vec![1, 2]);
    }

    #[test]
    fn resolved_duplicate_ids_skips_when_no_redirect_links_pair() {
        let issues = vec![make_issue(1, &["Q10", "Q20"])];
        let mut redirects = HashMap::new();
        redirects.insert("Q10".to_string(), "Q99".to_string()); // unrelated target
        assert!(resolved_duplicate_ids(&issues, &redirects).is_empty());
    }

    #[test]
    fn resolved_duplicate_ids_skips_when_three_or_more_qids() {
        // Three-or-more duplicates can't be auto-resolved by a pairwise
        // redirect map: even if A→B is a redirect, the third item C might
        // still be a real distinct duplicate, so a human has to pick.
        let issues = vec![make_issue(1, &["Q10", "Q20", "Q30"])];
        let mut redirects = HashMap::new();
        redirects.insert("Q10".to_string(), "Q20".to_string());
        assert!(resolved_duplicate_ids(&issues, &redirects).is_empty());
    }

    #[test]
    fn resolved_duplicate_ids_handles_empty_redirect_map() {
        let issues = vec![make_issue(1, &["Q10", "Q20"])];
        assert!(resolved_duplicate_ids(&issues, &HashMap::new()).is_empty());
    }
}
