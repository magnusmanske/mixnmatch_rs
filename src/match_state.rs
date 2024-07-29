#[derive(Debug, Clone)]
pub struct MatchState {
    pub unmatched: bool,
    pub partially_matched: bool,
    pub fully_matched: bool,
    // TODO N/A ?
}

impl MatchState {
    pub fn unmatched() -> Self {
        Self {
            unmatched: true,
            partially_matched: false,
            fully_matched: false,
        }
    }

    pub fn fully_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: false,
            fully_matched: true,
        }
    }

    pub fn not_fully_matched() -> Self {
        Self {
            unmatched: true,
            partially_matched: true,
            fully_matched: false,
        }
    }

    pub fn any_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: true,
            fully_matched: true,
        }
    }

    pub fn get_sql(&self) -> String {
        let mut parts = vec![];
        if self.unmatched {
            parts.push("(`q` IS NULL)")
        }
        if self.partially_matched {
            parts.push("(`q`>0 AND `user`=0)")
        }
        if self.fully_matched {
            parts.push("(`q`>0 AND `user`>0)")
        }
        if parts.is_empty() {
            return "".to_string();
        }
        format!(" AND ({}) ", parts.join(" OR "))
    }
}
