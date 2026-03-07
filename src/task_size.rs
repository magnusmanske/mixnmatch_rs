use std::{cmp::Ordering, fmt};

#[derive(Eq, Clone, Copy, Debug)]
pub enum TaskSize {
    Tiny,
    Small,
    Medium,
    Large,
    Ginormous,
}

impl Ord for TaskSize {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().cmp(&other.value())
    }
}

impl PartialOrd for TaskSize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TaskSize {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}

impl TaskSize {
    pub const fn value(&self) -> u8 {
        match self {
            TaskSize::Tiny => 1,
            TaskSize::Small => 2,
            TaskSize::Medium => 3,
            TaskSize::Large => 4,
            TaskSize::Ginormous => 5,
        }
    }

    pub fn new(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "tiny" => Some(Self::Tiny),
            "small" => Some(Self::Small),
            "medium" => Some(Self::Medium),
            "large" => Some(Self::Large),
            "ginormous" => Some(Self::Ginormous),
            _ => None,
        }
    }
}

impl fmt::Display for TaskSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_size_ordering() {
        assert!(TaskSize::Tiny < TaskSize::Small);
        assert!(TaskSize::Small < TaskSize::Medium);
        assert!(TaskSize::Medium < TaskSize::Large);
        assert!(TaskSize::Large < TaskSize::Ginormous);
    }

    #[test]
    fn test_task_size_equality() {
        assert_eq!(TaskSize::Tiny, TaskSize::Tiny);
        assert_eq!(TaskSize::Ginormous, TaskSize::Ginormous);
        assert_ne!(TaskSize::Tiny, TaskSize::Small);
    }

    #[test]
    fn test_task_size_new_valid() {
        assert_eq!(TaskSize::new("tiny"), Some(TaskSize::Tiny));
        assert_eq!(TaskSize::new("small"), Some(TaskSize::Small));
        assert_eq!(TaskSize::new("medium"), Some(TaskSize::Medium));
        assert_eq!(TaskSize::new("large"), Some(TaskSize::Large));
        assert_eq!(TaskSize::new("ginormous"), Some(TaskSize::Ginormous));
    }

    #[test]
    fn test_task_size_new_case_insensitive() {
        assert_eq!(TaskSize::new("TINY"), Some(TaskSize::Tiny));
        assert_eq!(TaskSize::new("Small"), Some(TaskSize::Small));
        assert_eq!(TaskSize::new("MEDIUM"), Some(TaskSize::Medium));
        assert_eq!(TaskSize::new("  large  "), Some(TaskSize::Large));
    }

    #[test]
    fn test_task_size_new_invalid() {
        assert_eq!(TaskSize::new(""), None);
        assert_eq!(TaskSize::new("unknown"), None);
        assert_eq!(TaskSize::new("huge"), None);
    }

    #[test]
    fn test_task_size_display() {
        assert_eq!(format!("{}", TaskSize::Tiny), "1");
        assert_eq!(format!("{}", TaskSize::Small), "2");
        assert_eq!(format!("{}", TaskSize::Medium), "3");
        assert_eq!(format!("{}", TaskSize::Large), "4");
        assert_eq!(format!("{}", TaskSize::Ginormous), "5");
    }

    #[test]
    fn test_task_size_value() {
        assert_eq!(TaskSize::Tiny.value(), 1);
        assert_eq!(TaskSize::Small.value(), 2);
        assert_eq!(TaskSize::Medium.value(), 3);
        assert_eq!(TaskSize::Large.value(), 4);
        assert_eq!(TaskSize::Ginormous.value(), 5);
    }
}
