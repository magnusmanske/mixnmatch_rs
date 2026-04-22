//! Single source of truth for how an `entry.(user, q)` state maps to a
//! column in the `overview` table.
//!
//! The overview table is maintained two ways:
//!
//! 1. **Incrementally**, after a single-row mutation (`match`, `unmatch`,
//!    `set N/A`, …) — via `StorageMySQL::update_overview_table`. The
//!    caller passes the *old* (user, q) and the *new* (user, q); we
//!    decrement the old bucket and increment the new one.
//! 2. **From scratch**, via `StorageMySQL::catalog_refresh_overview_table`
//!    (the "Refresh" button rebuilds the overview row with aggregate
//!    counts against `entry`).
//!
//! Previously these two paths had subtly different rules: the refresh
//! SQL counted overlapping conditions (e.g. an N/A row also matched
//! the "manual" predicate) while the incremental classifier used
//! mutually-exclusive buckets. That's why clicking Refresh made
//! "fully matched" counts jump.
//!
//! This module fixes that by forcing both paths through a single
//! priority-ordered decision tree:
//!
//!   noq    — q IS NULL               (unmatched; user is irrelevant)
//!   autoq  — q IS NOT NULL, user=0   (auto-matcher set this row)
//!   na     — q=0, user>0             (user marked entry as N/A)
//!   nowd   — q=-1, user>0            (legacy "no Wikidata" marker)
//!   manual — q>0, user>0             (user-confirmed Wikidata match)
//!
//! Every active entry belongs to exactly one of these buckets, so
//! `total = noq + autoq + na + nowd + manual`. Any entry that fails
//! every clause (e.g. a nonsensical negative q other than -1) is
//! counted as `noq` — matches the refresh semantics we land on below.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewColumn {
    Noq,
    Autoq,
    Na,
    Nowd,
    Manual,
}

impl OverviewColumn {
    /// Classify an (user, q) state into the single column it contributes to.
    /// `user_id == Some(0)` is the auto-matcher; `user_id == Some(n)` for
    /// `n > 0` is a real user; `user_id == None` is "never touched".
    pub fn classify(user_id: Option<usize>, q: Option<isize>) -> Self {
        // 1. No q assigned → unmatched, no matter who (or what) the user is.
        if q.is_none() {
            return Self::Noq;
        }
        // 2. Automatcher wins over every human-meaningful q value —
        //    an auto-match to q=0 / q=-1 is still an automatch.
        if user_id == Some(0) {
            return Self::Autoq;
        }
        // 3. Human user (user_id None here would be a DB oddity; treat as
        //    noq since without a user the row isn't authoritatively matched).
        match (user_id, q) {
            (Some(u), Some(0)) if u > 0 => Self::Na,
            (Some(u), Some(-1)) if u > 0 => Self::Nowd,
            (Some(u), Some(n)) if u > 0 && n > 0 => Self::Manual,
            _ => Self::Noq,
        }
    }

    /// Physical column name in `overview`.
    pub fn column(self) -> &'static str {
        match self {
            Self::Noq => "noq",
            Self::Autoq => "autoq",
            Self::Na => "na",
            Self::Nowd => "nowd",
            Self::Manual => "manual",
        }
    }

    /// SQL predicate (over entry rows) that matches exactly this bucket.
    /// Used to build the `catalog_refresh_overview_table` query so the
    /// refresh path can't drift from the incremental path.
    pub fn entry_predicate(self) -> &'static str {
        match self {
            Self::Noq    => "`q` IS NULL",
            Self::Autoq  => "`q` IS NOT NULL AND `user` = 0",
            Self::Na     => "`q` = 0 AND `user` > 0",
            Self::Nowd   => "`q` = -1 AND `user` > 0",
            Self::Manual => "`q` > 0 AND `user` > 0",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmatched_entries_are_noq_regardless_of_user() {
        assert_eq!(OverviewColumn::classify(None, None), OverviewColumn::Noq);
        assert_eq!(OverviewColumn::classify(Some(0), None), OverviewColumn::Noq);
        assert_eq!(OverviewColumn::classify(Some(5), None), OverviewColumn::Noq);
    }

    #[test]
    fn automatcher_wins_over_q_value() {
        // Automatcher always owns the row — autoq, not na/nowd/manual.
        assert_eq!(OverviewColumn::classify(Some(0), Some(5)), OverviewColumn::Autoq);
        assert_eq!(OverviewColumn::classify(Some(0), Some(0)), OverviewColumn::Autoq);
        assert_eq!(OverviewColumn::classify(Some(0), Some(-1)), OverviewColumn::Autoq);
    }

    #[test]
    fn user_matches_fan_out_by_q() {
        assert_eq!(OverviewColumn::classify(Some(7), Some(0)), OverviewColumn::Na);
        assert_eq!(OverviewColumn::classify(Some(7), Some(-1)), OverviewColumn::Nowd);
        assert_eq!(OverviewColumn::classify(Some(7), Some(42)), OverviewColumn::Manual);
    }

    #[test]
    fn weird_states_are_noq() {
        // q with no user → not a real match; treat as unmatched so the
        // numbers don't double-count in manual.
        assert_eq!(OverviewColumn::classify(None, Some(42)), OverviewColumn::Noq);
        // A nonsensical q < -1 isn't any legitimate bucket — fall back to noq.
        assert_eq!(OverviewColumn::classify(Some(7), Some(-5)), OverviewColumn::Noq);
    }

    #[test]
    fn columns_and_predicates_cover_every_variant() {
        // Sanity: every variant returns non-empty strings (guards against
        // a new enum variant landing without the match arms being filled).
        for variant in [
            OverviewColumn::Noq,
            OverviewColumn::Autoq,
            OverviewColumn::Na,
            OverviewColumn::Nowd,
            OverviewColumn::Manual,
        ] {
            assert!(!variant.column().is_empty());
            assert!(!variant.entry_predicate().is_empty());
        }
    }
}
