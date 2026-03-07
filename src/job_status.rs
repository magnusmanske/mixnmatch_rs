#[derive(Debug, Clone, Default, PartialEq, Copy)]
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
    pub const fn as_str(&self) -> &str {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_valid_statuses() {
        assert_eq!(JobStatus::new("TODO"), Some(JobStatus::Todo));
        assert_eq!(JobStatus::new("DONE"), Some(JobStatus::Done));
        assert_eq!(JobStatus::new("FAILED"), Some(JobStatus::Failed));
        assert_eq!(JobStatus::new("RUNNING"), Some(JobStatus::Running));
        assert_eq!(
            JobStatus::new("HIGH_PRIORITY"),
            Some(JobStatus::HighPriority)
        );
        assert_eq!(JobStatus::new("LOW_PRIORITY"), Some(JobStatus::LowPriority));
        assert_eq!(JobStatus::new("BLOCKED"), Some(JobStatus::Blocked));
        assert_eq!(JobStatus::new("DEACTIVATED"), Some(JobStatus::Deactivated));
    }

    #[test]
    fn test_new_invalid_status() {
        assert_eq!(JobStatus::new(""), None);
        assert_eq!(JobStatus::new("todo"), None);
        assert_eq!(JobStatus::new("UNKNOWN"), None);
    }

    #[test]
    fn test_as_str_round_trip() {
        let statuses = [
            JobStatus::Todo,
            JobStatus::Done,
            JobStatus::Failed,
            JobStatus::Running,
            JobStatus::HighPriority,
            JobStatus::LowPriority,
            JobStatus::Blocked,
            JobStatus::Deactivated,
        ];
        for status in &statuses {
            let s = status.as_str();
            let round_tripped = JobStatus::new(s).unwrap();
            assert_eq!(*status, round_tripped);
        }
    }

    #[test]
    fn test_default_is_todo() {
        assert_eq!(JobStatus::default(), JobStatus::Todo);
    }
}
