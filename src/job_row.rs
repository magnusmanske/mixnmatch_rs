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
