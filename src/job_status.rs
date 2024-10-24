#[derive(Debug, Clone, Default, PartialEq)]
pub enum JobStatus {
    #[default]
    Todo,
    Done,
    Failed,
    Running,
    HighPriority,
    LowPriority,
    Blocked,
    Deactivated,
}

impl JobStatus {
    pub fn new(s: &str) -> Option<Self> {
        match s {
            "TODO" => Some(JobStatus::Todo),
            "DONE" => Some(JobStatus::Done),
            "FAILED" => Some(JobStatus::Failed),
            "RUNNING" => Some(JobStatus::Running),
            "HIGH_PRIORITY" => Some(JobStatus::HighPriority),
            "LOW_PRIORITY" => Some(JobStatus::LowPriority),
            "BLOCKED" => Some(JobStatus::Blocked),
            "DEACTIVATED" => Some(JobStatus::Deactivated),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &str {
        match *self {
            JobStatus::Todo => "TODO",
            JobStatus::Done => "DONE",
            JobStatus::Failed => "FAILED",
            JobStatus::Running => "RUNNING",
            JobStatus::HighPriority => "HIGH_PRIORITY",
            JobStatus::LowPriority => "LOW_PRIORITY",
            JobStatus::Blocked => "BLOCKED",
            JobStatus::Deactivated => "DEACTIVATED",
        }
    }
}
