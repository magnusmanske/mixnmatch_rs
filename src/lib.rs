#![forbid(unsafe_code)]
#![allow(clippy::collapsible_if)]
#![warn(
    clippy::cognitive_complexity,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_link_with_quotes,
    // clippy::doc_markdown,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::float_equality_without_abs,
    keyword_idents,
    // clippy::missing_const_for_fn,
    missing_copy_implementations,
    missing_debug_implementations,
    // clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::mod_module_files,
    non_ascii_idents,
    noop_method_call,
    // clippy::option_if_let_else,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::semicolon_if_nothing_returned,
    clippy::unseparated_literal_suffix,
    clippy::shadow_unrelated,
    clippy::similar_names,
    clippy::suspicious_operation_groupings,
    // unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    clippy::unused_self,
    clippy::use_debug,
    clippy::used_underscore_binding,
    clippy::useless_let_if_seq,
    // clippy::wildcard_dependencies,
    clippy::wildcard_imports
)]

pub type DbId = usize;
pub type PropertyId = usize;
pub type ItemId = isize;

pub mod announce;
pub mod api;
pub mod app_state;
pub mod auth;
pub mod automatch;
pub mod autoscrape;
pub mod autoscrape_levels;
pub mod autoscrape_regex;
pub mod autoscrape_resolve;
pub mod autoscrape_scraper;
pub mod auxiliary_data;
pub mod auxiliary_matcher;
pub mod bespoke_scrapers;
pub mod catalog;
pub mod catalog_merger;
pub mod cersei;
pub mod claim_dedup;
pub mod cli;
pub mod code_fragment;
pub mod coordinate_matcher;
pub mod coordinates;
pub mod datasource;
pub mod entry;
pub mod entry_query;
pub mod import_catalog;
pub mod issue;
pub mod item_creator;
pub mod job;
pub mod job_progress;
pub mod job_row;
pub mod job_runner;
pub mod job_status;
pub mod large_catalogs;
pub mod maintenance;
pub mod match_state;
pub mod meta_entry;
pub mod metrics;
pub mod microsync;
pub mod mnm_link;
pub mod mysql_misc;
pub mod overview;
pub mod person;
pub mod person_date;
pub mod php_wrapper;
pub mod process;
pub mod prop_todo;
pub mod reference_fixer;
pub mod static_cache;
pub mod storage;
pub mod storage_mysql;
pub mod task_size;
#[cfg(test)]
pub(crate) mod test_support;
pub mod taxon_matcher;
pub mod update_catalog;
pub mod util;
pub mod wd_match_sync;
pub mod wdqs;
pub mod wdrc;
pub mod wikidata;
pub mod wikidata_commands;
pub mod wikidata_item_builder;
pub mod wikidata_writer;
