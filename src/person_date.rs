use serde::{Deserialize, Serialize};
use std::fmt;

// ── PersonDate: a date with variable precision ─────────────────────────────

/// A date with variable precision: year-only, year-month, or year-month-day.
/// Negative years represent BCE dates. Serializes to/from the DB string format
/// ("YYYY", "YYYY-MM", "YYYY-MM-DD").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonDate {
    pub year: i32,
    pub month: Option<u8>,
    pub day: Option<u8>,
}

impl PersonDate {
    /// Create a year-only date.
    pub fn year_only(year: i32) -> Self {
        Self {
            year,
            month: None,
            day: None,
        }
    }

    /// Create a year-month date.
    pub fn year_month(year: i32, month: u8) -> Self {
        Self {
            year,
            month: Some(month),
            day: None,
        }
    }

    /// Create a full year-month-day date.
    pub fn year_month_day(year: i32, month: u8, day: u8) -> Self {
        Self {
            year,
            month: Some(month),
            day: Some(day),
        }
    }

    /// Parse from DB string format: "YYYY", "YYYY-MM", or "YYYY-MM-DD"
    /// (with optional leading '-' for BCE).
    pub fn from_db_string(s: &str) -> Option<Self> {
        if s.is_empty() {
            return None;
        }
        let (negative, rest) = if let Some(r) = s.strip_prefix('-') {
            (true, r)
        } else {
            (false, s)
        };
        let parts: Vec<&str> = rest.split('-').collect();
        let year_abs: i32 = parts.first()?.parse().ok()?;
        let year = if negative { -year_abs } else { year_abs };
        match parts.len() {
            1 => Some(Self::year_only(year)),
            2 => {
                let month: u8 = parts[1].parse().ok()?;
                if !(1..=12).contains(&month) {
                    return None;
                }
                Some(Self::year_month(year, month))
            }
            3 => {
                let month: u8 = parts[1].parse().ok()?;
                let day: u8 = parts[2].parse().ok()?;
                if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                    return None;
                }
                Some(Self::year_month_day(year, month, day))
            }
            _ => None,
        }
    }

    /// Convert to DB string format.
    pub fn to_db_string(&self) -> String {
        let prefix = if self.year < 0 { "-" } else { "" };
        let abs_year = self.year.unsigned_abs();
        match (self.month, self.day) {
            (None, _) => format!("{prefix}{abs_year}"),
            (Some(m), None) => format!("{prefix}{abs_year}-{m:02}"),
            (Some(m), Some(d)) => format!("{prefix}{abs_year}-{m:02}-{d:02}"),
        }
    }

    /// Wikidata time precision: 9 = year, 10 = month, 11 = day.
    pub fn wikidata_precision(&self) -> u64 {
        match (self.month, self.day) {
            (None, _) => 9,
            (Some(_), None) => 10,
            (Some(_), Some(_)) => 11,
        }
    }

    /// Format as Wikidata time value string (e.g. "+2021-01-01T00:00:00Z").
    pub fn to_wikidata_time(&self) -> String {
        let prefix = if self.year < 0 { "-" } else { "+" };
        let abs_year = self.year.unsigned_abs();
        let month = self.month.unwrap_or(1);
        let day = self.day.unwrap_or(1);
        format!("{prefix}{abs_year}-{month:02}-{day:02}T00:00:00Z")
    }
}

impl fmt::Display for PersonDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_db_string())
    }
}

impl Serialize for PersonDate {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_db_string())
    }
}

impl<'de> Deserialize<'de> for PersonDate {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        PersonDate::from_db_string(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid person date: {s}")))
    }
}
