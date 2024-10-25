use std::{cmp::Ordering, fmt};

#[derive(Eq, Clone, Debug)]
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
    pub fn value(&self) -> u8 {
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
    fn test_task_size() {
        assert!(TaskSize::TINY < TaskSize::SMALL);
        assert!(TaskSize::SMALL < TaskSize::MEDIUM);
        assert!(TaskSize::MEDIUM < TaskSize::LARGE);
        assert!(TaskSize::LARGE < TaskSize::GINORMOUS);
    }
}
