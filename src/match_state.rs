#[derive(Debug, Clone, Copy)]
pub struct MatchState {
    pub unmatched: bool,
    pub partially_matched: bool,
    pub fully_matched: bool,
    // TODO N/A ?
}

impl MatchState {
    pub const fn unmatched() -> Self {
        Self {
            unmatched: true,
            partially_matched: false,
            fully_matched: false,
        }
    }

    pub const fn fully_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: false,
            fully_matched: true,
        }
    }

    pub const fn not_fully_matched() -> Self {
        Self {
            unmatched: true,
            partially_matched: true,
            fully_matched: false,
        }
    }

    pub const fn any_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: true,
            fully_matched: true,
        }
    }

    pub fn get_sql(&self) -> String {
        let mut parts = vec![];
        if self.unmatched {
            parts.push("(`q` IS NULL)");
        }
        if self.partially_matched {
            parts.push("(`q`>0 AND `user`=0)");
        }
        if self.fully_matched {
            parts.push("(`q`>0 AND `user`>0)");
        }
        if parts.is_empty() {
            return "".to_string();
        }
        format!(" AND ({}) ", parts.join(" OR "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_sql() {
        let ms = MatchState {
            unmatched: false,
            fully_matched: false,
            partially_matched: false,
        };
        assert_eq!(ms.get_sql().as_str(), "");
        assert_eq!(
            MatchState::unmatched().get_sql().as_str(),
            " AND ((`q` IS NULL)) "
        );
        assert_eq!(
            MatchState::fully_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`>0)) "
        );
        assert_eq!(
            MatchState::not_fully_matched().get_sql().as_str(),
            " AND ((`q` IS NULL) OR (`q`>0 AND `user`=0)) "
        );
        assert_eq!(
            MatchState::any_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`=0) OR (`q`>0 AND `user`>0)) "
        );
    }
}
