use std::{cmp::Ordering, fmt};

#[derive(Eq, Clone, Copy, Debug)]
pub enum TaskSize {
    TINY,
    SMALL,
    MEDIUM,
    LARGE,
    GINORMOUS,
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
            TaskSize::TINY => 1,
            TaskSize::SMALL => 2,
            TaskSize::MEDIUM => 3,
            TaskSize::LARGE => 4,
            TaskSize::GINORMOUS => 5,
        }
    }

    pub fn new(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "tiny" => Some(Self::TINY),
            "small" => Some(Self::SMALL),
            "medium" => Some(Self::MEDIUM),
            "large" => Some(Self::LARGE),
            "ginormous" => Some(Self::GINORMOUS),
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
        assert!(TaskSize::TINY < TaskSize::SMALL);
        assert!(TaskSize::SMALL < TaskSize::MEDIUM);
        assert!(TaskSize::MEDIUM < TaskSize::LARGE);
        assert!(TaskSize::LARGE < TaskSize::GINORMOUS);
    }

    #[test]
    fn test_task_size_equality() {
        assert_eq!(TaskSize::TINY, TaskSize::TINY);
        assert_eq!(TaskSize::GINORMOUS, TaskSize::GINORMOUS);
        assert_ne!(TaskSize::TINY, TaskSize::SMALL);
    }

    #[test]
    fn test_task_size_new_valid() {
        assert_eq!(TaskSize::new("tiny"), Some(TaskSize::TINY));
        assert_eq!(TaskSize::new("small"), Some(TaskSize::SMALL));
        assert_eq!(TaskSize::new("medium"), Some(TaskSize::MEDIUM));
        assert_eq!(TaskSize::new("large"), Some(TaskSize::LARGE));
        assert_eq!(TaskSize::new("ginormous"), Some(TaskSize::GINORMOUS));
    }

    #[test]
    fn test_task_size_new_case_insensitive() {
        assert_eq!(TaskSize::new("TINY"), Some(TaskSize::TINY));
        assert_eq!(TaskSize::new("Small"), Some(TaskSize::SMALL));
        assert_eq!(TaskSize::new("MEDIUM"), Some(TaskSize::MEDIUM));
        assert_eq!(TaskSize::new("  large  "), Some(TaskSize::LARGE));
    }

    #[test]
    fn test_task_size_new_invalid() {
        assert_eq!(TaskSize::new(""), None);
        assert_eq!(TaskSize::new("unknown"), None);
        assert_eq!(TaskSize::new("huge"), None);
    }

    #[test]
    fn test_task_size_display() {
        assert_eq!(format!("{}", TaskSize::TINY), "1");
        assert_eq!(format!("{}", TaskSize::SMALL), "2");
        assert_eq!(format!("{}", TaskSize::MEDIUM), "3");
        assert_eq!(format!("{}", TaskSize::LARGE), "4");
        assert_eq!(format!("{}", TaskSize::GINORMOUS), "5");
    }

    #[test]
    fn test_task_size_value() {
        assert_eq!(TaskSize::TINY.value(), 1);
        assert_eq!(TaskSize::SMALL.value(), 2);
        assert_eq!(TaskSize::MEDIUM.value(), 3);
        assert_eq!(TaskSize::LARGE.value(), 4);
        assert_eq!(TaskSize::GINORMOUS.value(), 5);
    }
}
