use crate::job_status::JobStatus;
use wikimisc::timestamp::TimeStamp;

type JobRowMySql = (
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
);

#[derive(Debug, Clone, Default)]
pub struct JobRow {
    pub id: usize,
    pub action: String,
    pub catalog: usize,
    pub json: Option<String>,
    pub depends_on: Option<usize>,
    pub status: JobStatus,
    pub last_ts: String,
    pub note: Option<String>,
    pub repeat_after_sec: Option<usize>,
    pub next_ts: String,
    pub user_id: usize,
}

impl JobRow {
    pub fn from_row(x: JobRowMySql) -> Self {
        Self {
            id: x.0,
            action: x.1,
            catalog: x.2,
            json: x.3,
            depends_on: x.4,
            status: JobStatus::new(&x.5).unwrap_or(JobStatus::Todo),
            last_ts: x.6,
            note: x.7,
            repeat_after_sec: x.8,
            next_ts: x.9,
            user_id: x.10,
        }
    }

    pub fn new(action: &str, catalog_id: usize) -> JobRow {
        Self {
            id: 0,
            action: action.to_string(),
            catalog: catalog_id,
            json: None,
            depends_on: None,
            status: JobStatus::Todo,
            last_ts: TimeStamp::now(),
            note: None,
            repeat_after_sec: None,
            next_ts: "".to_string(),
            user_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let row = JobRow::new("automatch", 42);
        assert_eq!(row.id, 0);
        assert_eq!(row.action, "automatch");
        assert_eq!(row.catalog, 42);
        assert!(row.json.is_none());
        assert!(row.depends_on.is_none());
        assert_eq!(row.status, JobStatus::Todo);
        assert!(!row.last_ts.is_empty());
        assert!(row.note.is_none());
        assert!(row.repeat_after_sec.is_none());
        assert_eq!(row.next_ts, "");
        assert_eq!(row.user_id, 0);
    }

    #[test]
    fn test_from_row() {
        let mysql_row: JobRowMySql = (
            7,
            "microsync".to_string(),
            99,
            Some("{\"offset\":10}".to_string()),
            Some(3),
            "DONE".to_string(),
            "2024-01-01 00:00:00".to_string(),
            Some("a note".to_string()),
            Some(3600),
            "2024-01-02 00:00:00".to_string(),
            5,
        );
        let row = JobRow::from_row(mysql_row);
        assert_eq!(row.id, 7);
        assert_eq!(row.action, "microsync");
        assert_eq!(row.catalog, 99);
        assert_eq!(row.json, Some("{\"offset\":10}".to_string()));
        assert_eq!(row.depends_on, Some(3));
        assert_eq!(row.status, JobStatus::Done);
        assert_eq!(row.last_ts, "2024-01-01 00:00:00");
        assert_eq!(row.note, Some("a note".to_string()));
        assert_eq!(row.repeat_after_sec, Some(3600));
        assert_eq!(row.next_ts, "2024-01-02 00:00:00");
        assert_eq!(row.user_id, 5);
    }

    #[test]
    fn test_from_row_invalid_status_falls_back_to_todo() {
        let mysql_row: JobRowMySql = (
            1,
            "test".to_string(),
            1,
            None,
            None,
            "INVALID_STATUS".to_string(),
            "".to_string(),
            None,
            None,
            "".to_string(),
            0,
        );
        let row = JobRow::from_row(mysql_row);
        assert_eq!(row.status, JobStatus::Todo);
    }

    #[test]
    fn test_default() {
        let row = JobRow::default();
        assert_eq!(row.id, 0);
        assert_eq!(row.action, "");
        assert_eq!(row.catalog, 0);
        assert!(row.json.is_none());
        assert!(row.depends_on.is_none());
        assert_eq!(row.status, JobStatus::Todo);
        assert_eq!(row.last_ts, "");
        assert!(row.note.is_none());
        assert!(row.repeat_after_sec.is_none());
        assert_eq!(row.next_ts, "");
        assert_eq!(row.user_id, 0);
    }
}
