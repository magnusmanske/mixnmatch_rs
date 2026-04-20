#![allow(clippy::mod_module_files)]

//! HTTP API. Each per-feature handler module is small and self-contained;
//! `router` wires them together and owns the dispatcher logic.

pub mod common;

mod admin;
mod catalog;
mod data;
mod delegated;
mod dg;
mod download;
mod entry;
mod import;
mod issues;
mod jobs;
mod large_catalogs;
mod locations;
mod matching;
mod misc;
mod navigation;
mod proxy;
mod rc;
mod router;
mod upload;
mod widar;

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
