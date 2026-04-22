#![allow(clippy::mod_module_files)]

//! HTTP API. Each per-feature handler module is small and self-contained;
//! `router` wires them together and owns the dispatcher logic.

pub mod common;
pub mod cors;

mod admin;
mod catalog;
mod code_fragments;
mod creation_candidates;
mod data;
mod dg;
mod download;
mod entry;
mod import;
mod issues;
mod jobs;
mod large_catalogs;
mod locations;
mod lua;
mod matching;
mod misc;
mod navigation;
mod proxy;
mod quick_compare;
mod rc;
mod router;
mod sparql;
mod sync;
mod upload;
mod widar;

// Re-export business-logic entry points used by `crate::micro_api` while it
// still exists. Once micro_api is removed these can become `pub(crate)`.
pub use code_fragments::{get_for_catalog as code_fragments_get_for_catalog,
    save_from_params as code_fragments_save_from_params};
pub use creation_candidates::run as creation_candidates_run;
pub use large_catalogs::{
    lc_catalogs_data, lc_locations_data, lc_rc_data, lc_report_data, lc_report_list_data,
    lc_set_status_data,
};
pub use lua::run_from_params as lua_run_from_params;
pub use quick_compare::run as quick_compare_run;
pub use sparql::list_from_params as sparql_list_from_params;
pub use sync::get as sync_get;

pub use router::{SharedState, router};

#[cfg(test)]
mod tests {
    use super::router;
    use crate::app_state::get_test_app;

    #[test]
    fn test_router_builds() {
        // Verifies router construction doesn't panic
        let _ = router(get_test_app());
    }
}
