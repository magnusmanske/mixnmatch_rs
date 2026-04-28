//! Automatch — strategies that propose Wikidata matches for catalog entries.
//!
//! Reorganised into two topical submodules so each strategy can be
//! read on its own without scrolling past unrelated ones:
//!
//! - [`strategies`] — search/label-based matchers
//!   (`automatch_simple`, `automatch_by_search`, `automatch_creations`,
//!   `automatch_by_sitelink`, `automatch_with_sparql`,
//!   `automatch_from_other_catalogs`, `automatch_people_with_*`,
//!   `purge_automatches`).
//! - [`dates`] — person-date matching and the property-conjunction
//!   "complex" strategy
//!   (`match_person_by_dates`, `match_person_by_single_date`,
//!   `automatch_complex`, plus the `DateMatchField` /
//!   `DateStringLength` enums).
//!
//! What lives here is the shared infrastructure: row types
//! (`AutomatchSearchRow`, `PersonDateMatchRow`, `CandidateDatesRow`,
//! `CandidateDates`, `ResultInOriginalCatalog`,
//! `ResultInOtherCatalog`), the `AutoMatch` struct + Jobbable impl
//! + `new`, and the integration tests.

mod dates;
pub mod matchers;
mod strategies;

pub use dates::{DateMatchField, DateStringLength};
pub use matchers::{MATCHERS, Matcher, run_matcher_for_action};

// Row DTOs live in `crate::storage` (single source of truth — the
// storage trait signatures need them). Re-exported here so existing
// `use crate::automatch::AutomatchSearchRow` paths keep working.
pub use crate::storage::{
    AutomatchSearchRow, CandidateDatesRow, PersonDateMatchRow, ResultInOriginalCatalog,
    ResultInOtherCatalog,
};

use crate::app_state::AppState;
use crate::job::Job;
use crate::job::Jobbable;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    pub(super) static ref RE_YEAR: Regex = Regex::new(r"(\d{3,4})").expect("Regexp error");
}

/// Page size used when the unbatched `automatch_with_sparql` query fails
/// (timeout, dropped connection, etc.) and we fall back to LIMIT/OFFSET
/// pagination. Override per deployment via
/// `task_specific_usize.automatch_sparql_batch_size` in config.
pub(super) const SPARQL_FALLBACK_BATCH_SIZE: usize = 10000;

/// While streaming an unbatched SPARQL response, flush to the per-entry
/// matcher every N rows so we don't hold the whole result in memory.
pub(super) const SPARQL_PROCESS_CHUNK_SIZE: usize = 100000;

#[derive(Debug, Clone)]
pub(super) struct CandidateDates {
    pub entry_id: usize,
    pub born: String,
    pub died: String,
    pub matches: Vec<String>,
}

impl CandidateDates {
    pub(super) fn from_row(r: &CandidateDatesRow) -> Self {
        Self {
            entry_id: r.entry_id,
            born: r.born.clone(),
            died: r.died.clone(),
            matches: r
                .candidates_csv
                .split(',')
                .filter(|q| !q.is_empty())
                .map(|q| format!("Q{q}"))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoMatch {
    pub(super) app: AppState,
    pub(super) job: Option<Job>,
}

impl Jobbable for AutoMatch {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }

    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl AutoMatch {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            job: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{TEST_MUTEX, USER_AUTO, USER_DATE_MATCH, get_test_app};
    use crate::entry::Entry;

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;
    const TEST_ENTRY_ID2: usize = 144000954;

    // TODO finish test
    // #[tokio::test]
    // async fn test_automatch_complex() {
    //     let _test_lock = TEST_MUTEX.lock();
    //     let app = get_test_app();
    //     let mut am = AutoMatch::new(&app);
    //     let result = am.automatch_complex(3663).await.unwrap();
    //     println!("{result:?}");
    // }

    #[tokio::test]
    #[ignore] // Requires database, must run single-threaded
    async fn test_match_person_by_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        Entry::from_id(TEST_ENTRY_ID2, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();

        // Match by date
        let mut am = AutoMatch::new(&app);
        am.match_person_by_dates(TEST_CATALOG_ID).await.unwrap();

        // Check if set
        let entry = Entry::from_id(TEST_ENTRY_ID2, &app).await.unwrap();
        assert!(entry.is_fully_matched());
        assert_eq!(1035, entry.q.unwrap());
    }

    #[tokio::test]
    #[ignore] // Requires database, must run single-threaded
    async fn test_automatch_by_search() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();

        assert!(
            Entry::from_id(TEST_ENTRY_ID, &app)
                .await
                .unwrap()
                .is_unmatched()
        );

        // Run automatch
        let mut am = AutoMatch::new(&app);
        am.automatch_by_search(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(467402));
        assert_eq!(entry.user, Some(USER_AUTO));

        // Clear
        entry.unmatch().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires database, must run single-threaded
    async fn test_automatch_by_sitelink() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();

        let mut am = AutoMatch::new(&app);

        // Run automatch
        am.automatch_by_sitelink(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry2.q, Some(13520818));
        assert_eq!(entry2.user, Some(USER_AUTO));

        // Clear
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires database, must run single-threaded
    async fn test_purge_automatches() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Set a full match
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();
        entry.set_match("Q1", 4).await.unwrap();
        assert!(entry.is_fully_matched());

        // Purge catalog
        let am2 = AutoMatch::new(&app);
        am2.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is still fully matched
        let entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry2.is_fully_matched());

        // Set an automatch
        let mut entry3 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry3.unmatch().await.unwrap();
        entry3.set_match("Q1", 0).await.unwrap();
        assert!(entry3.is_partially_matched());

        // Purge catalog
        let am4 = AutoMatch::new(&app);
        am4.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is now unmatched
        let entry4 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry4.is_unmatched());
    }

    #[tokio::test]
    #[ignore] // Requires database, must run single-threaded
    async fn test_match_person_by_single_date() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();

        let mut am = AutoMatch::new(&app);

        // Set prelim match
        let mut entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry2.set_match("Q13520818", 0).await.unwrap();

        // Run automatch
        am.match_person_by_single_date(
            TEST_CATALOG_ID,
            DateMatchField::Born,
            DateStringLength::Day,
        )
        .await
        .unwrap();

        // Check match
        let mut entry3 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry3.q, Some(13520818));
        assert_eq!(entry3.user, Some(USER_DATE_MATCH));

        // Cleanup
        entry3.unmatch().await.unwrap();
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }

    #[test]
    fn test_extract_sane_year_from_date_valid() {
        assert_eq!(AutoMatch::extract_sane_year_from_date("1990"), Some(1990));
        assert_eq!(AutoMatch::extract_sane_year_from_date("800"), Some(800));
        assert_eq!(
            AutoMatch::extract_sane_year_from_date("1990-05-24"),
            Some(1990)
        );
        assert_eq!(AutoMatch::extract_sane_year_from_date("2000"), Some(2000));
    }

    #[test]
    fn test_extract_sane_year_from_date_invalid() {
        assert_eq!(AutoMatch::extract_sane_year_from_date(""), None);
        assert_eq!(AutoMatch::extract_sane_year_from_date("abc"), None);
        assert_eq!(AutoMatch::extract_sane_year_from_date("12"), None);
    }

    #[test]
    fn test_extract_sane_year_from_date_future_year() {
        // A year far in the future should be rejected
        assert_eq!(AutoMatch::extract_sane_year_from_date("9999"), None);
    }

    #[test]
    fn test_date_match_field_get_field_name() {
        assert_eq!(DateMatchField::Born.get_field_name(), "born");
        assert_eq!(DateMatchField::Died.get_field_name(), "died");
    }

    #[test]
    fn test_date_match_field_get_property() {
        assert_eq!(DateMatchField::Born.get_property(), "P569");
        assert_eq!(DateMatchField::Died.get_property(), "P570");
    }

    #[test]
    fn test_date_precision_as_i32() {
        assert_eq!(DateStringLength::Day.as_i32(), 10);
        assert_eq!(DateStringLength::Year.as_i32(), 4);
    }

    #[test]
    fn test_sort_and_dedup() {
        let mut items = vec![
            "Q3".to_string(),
            "Q1".to_string(),
            "Q2".to_string(),
            "Q1".to_string(),
        ];
        AutoMatch::sort_and_dedup(&mut items);
        assert_eq!(items, vec!["Q1", "Q2", "Q3"]);
    }

    #[test]
    fn test_sort_and_dedup_empty() {
        let mut items: Vec<String> = vec![];
        AutoMatch::sort_and_dedup(&mut items);
        assert!(items.is_empty());
    }

    #[test]
    fn test_candidate_dates_from_row() {
        let row = CandidateDatesRow::new(
            42,
            "1900".to_string(),
            "1980".to_string(),
            "1,2,3".to_string(),
        );
        let cd = CandidateDates::from_row(&row);
        assert_eq!(cd.entry_id, 42);
        assert_eq!(cd.born, "1900");
        assert_eq!(cd.died, "1980");
        assert_eq!(cd.matches, vec!["Q1", "Q2", "Q3"]);
    }

    #[test]
    fn test_candidate_dates_from_row_empty_matches() {
        let row = CandidateDatesRow::new(1, "1900".to_string(), "".to_string(), "".to_string());
        let cd = CandidateDates::from_row(&row);
        assert_eq!(cd.entry_id, 1);
        assert!(cd.matches.is_empty());
    }
}
