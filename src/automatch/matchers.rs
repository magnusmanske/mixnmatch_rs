//! Strategy pattern: each matching algorithm is a unit struct
//! implementing [`Matcher`], collected into a static [`MATCHERS`]
//! registry that the job dispatcher consults.
//!
//! `AutoMatch` (the strategy *context*) still holds the shared
//! state — `app: AppState`, `job: Option<Job>` — and the actual
//! algorithm bodies. Each `Matcher` impl is a thin facade that
//! routes a registered action name to the right method on
//! `AutoMatch`. Adding a new matcher = add one unit struct + one
//! `MATCHERS` entry; nothing in `AutoMatch`'s impl changes unless
//! the new matcher needs new shared logic.
//!
//! The trait deliberately takes `&mut AutoMatch` rather than a
//! generic context — every strategy here uses the same state, so
//! widening it would be ceremony without payoff. If a future
//! matcher needs different state, it can hold its own and bypass
//! this module.

use super::{AutoMatch, DateMatchField, DateStringLength};
use anyhow::Result;
use async_trait::async_trait;

/// Strategy interface for an automatch action. Each impl declares
/// the action name (used by the job dispatcher to look it up) and
/// runs the algorithm against the supplied `AutoMatch` context.
#[async_trait]
pub trait Matcher: Send + Sync {
    /// Job-action string this matcher handles. Must be unique across
    /// the registry; pinned by `matcher_actions_unique` test.
    fn action(&self) -> &'static str;
    /// Execute the strategy. The `AutoMatch` is freshly constructed
    /// per-job and already has `set_current_job` applied.
    async fn run(&self, am: &mut AutoMatch, catalog_id: usize) -> Result<()>;
}

/// Convenience: look up a matcher by action name and run it. Used
/// from the job dispatcher to fold all the per-action lambdas into
/// one trait dispatch.
pub async fn run_matcher_for_action(
    action: &str,
    am: &mut AutoMatch,
    catalog_id: usize,
) -> Option<Result<()>> {
    MATCHERS
        .iter()
        .find(|m| m.action() == action)
        .map(|m| m.run(am, catalog_id))?
        .await
        .into()
}

// ---------------------------------------------------------------------------
// Strategies — unit structs delegating to the existing AutoMatch impl.
// Each one is intentionally tiny; the algorithm lives in
// `automatch/{strategies,dates}.rs`.
// ---------------------------------------------------------------------------

macro_rules! delegate_matcher {
    ($name:ident, $action:literal, $method:ident) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        #[async_trait]
        impl Matcher for $name {
            fn action(&self) -> &'static str {
                $action
            }
            async fn run(&self, am: &mut AutoMatch, catalog_id: usize) -> Result<()> {
                am.$method(catalog_id).await
            }
        }
    };
}

delegate_matcher!(SimpleMatcher,                    "automatch",                          automatch_simple);
delegate_matcher!(BySearchMatcher,                  "automatch_by_search",                automatch_by_search);
delegate_matcher!(BySitelinkMatcher,                "automatch_by_sitelink",              automatch_by_sitelink);
delegate_matcher!(ComplexMatcher,                   "automatch_complex",                  automatch_complex);
delegate_matcher!(CreationsMatcher,                 "automatch_creations",                automatch_creations);
delegate_matcher!(FromOtherCatalogsMatcher,         "automatch_from_other_catalogs",      automatch_from_other_catalogs);
delegate_matcher!(PeopleWithBirthYearMatcher,       "automatch_people_with_birth_year",   automatch_people_with_birth_year);
delegate_matcher!(PeopleWithInitialsMatcher,        "automatch_people_with_initials",     automatch_people_with_initials);
delegate_matcher!(SparqlMatcher,                    "automatch_sparql",                   automatch_with_sparql);
delegate_matcher!(PersonDatesMatcher,               "match_person_dates",                 match_person_by_dates);
delegate_matcher!(PurgeAutomatchesMatcher,          "purge_automatches",                  purge_automatches);

/// Person-date matchers (born / died) take an extra `field`
/// argument so they don't fit the `delegate_matcher!` macro.
#[derive(Debug, Clone, Copy)]
pub struct OnBirthdateMatcher;
#[async_trait]
impl Matcher for OnBirthdateMatcher {
    fn action(&self) -> &'static str {
        "match_on_birthdate"
    }
    async fn run(&self, am: &mut AutoMatch, catalog_id: usize) -> Result<()> {
        am.match_person_by_single_date(catalog_id, DateMatchField::Born, DateStringLength::Day)
            .await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OnDeathdateMatcher;
#[async_trait]
impl Matcher for OnDeathdateMatcher {
    fn action(&self) -> &'static str {
        "match_on_deathdate"
    }
    async fn run(&self, am: &mut AutoMatch, catalog_id: usize) -> Result<()> {
        am.match_person_by_single_date(catalog_id, DateMatchField::Died, DateStringLength::Day)
            .await
    }
}

/// Single source of truth for the available matching strategies.
/// Adding a new strategy: define a unit struct above, impl
/// `Matcher` for it, append it here.
pub static MATCHERS: &[&(dyn Matcher + 'static)] = &[
    &SimpleMatcher,
    &BySearchMatcher,
    &BySitelinkMatcher,
    &ComplexMatcher,
    &CreationsMatcher,
    &FromOtherCatalogsMatcher,
    &PeopleWithBirthYearMatcher,
    &PeopleWithInitialsMatcher,
    &SparqlMatcher,
    &PersonDatesMatcher,
    &PurgeAutomatchesMatcher,
    &OnBirthdateMatcher,
    &OnDeathdateMatcher,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Catches a copy-paste typo where two matchers register the
    /// same action — only the first would ever be invoked.
    #[test]
    fn matcher_actions_unique() {
        let mut seen: HashSet<&'static str> = HashSet::new();
        for m in MATCHERS {
            assert!(
                seen.insert(m.action()),
                "duplicate matcher action: {}",
                m.action()
            );
        }
    }

    /// Pin every action name a job-dispatcher caller might rely on.
    /// If the registry shrinks accidentally, this fails at build
    /// time instead of producing "Unknown action 'X'" at runtime.
    #[test]
    fn matcher_registry_known_actions() {
        let actions: HashSet<&'static str> = MATCHERS.iter().map(|m| m.action()).collect();
        for required in [
            "automatch",
            "automatch_by_search",
            "automatch_by_sitelink",
            "automatch_complex",
            "automatch_creations",
            "automatch_from_other_catalogs",
            "automatch_people_with_birth_year",
            "automatch_people_with_initials",
            "automatch_sparql",
            "match_person_dates",
            "purge_automatches",
            "match_on_birthdate",
            "match_on_deathdate",
        ] {
            assert!(
                actions.contains(required),
                "MATCHERS missing required action: {required}"
            );
        }
    }
}
