#![forbid(unsafe_code)]
#![warn(
    clippy::cognitive_complexity,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_link_with_quotes,
    clippy::doc_markdown,
    clippy::empty_line_after_outer_attr,
    // clippy::empty_structs_with_brackets,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::float_equality_without_abs,
    keyword_idents,
    clippy::missing_const_for_fn,
    missing_copy_implementations,
    missing_debug_implementations,
    // clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::mod_module_files,
    non_ascii_idents,
    noop_method_call,
    // clippy::option_if_let_else,
    // clippy::print_stderr,
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

// pub mod api;
pub mod app_state;
pub mod automatch;
pub mod autoscrape;
pub mod autoscrape_levels;
pub mod autoscrape_resolve;
pub mod autoscrape_scraper;
pub mod auxiliary_matcher;
pub mod bespoke_scrapers;
pub mod catalog;
pub mod coordinate_matcher;
pub mod datasource;
pub mod entry;
pub mod extended_entry;
pub mod issue;
pub mod job;
pub mod job_row;
pub mod job_status;
pub mod maintenance;
pub mod match_state;
pub mod microsync;
pub mod mysql_misc;
pub mod person;
pub mod php_wrapper;
pub mod prop_todo;
pub mod storage;
pub mod storage_mysql;
pub mod task_size;
// pub mod storage_wikibase;
pub mod taxon_matcher;
pub mod update_catalog;
pub mod wdrc;
pub mod wikidata;
pub mod wikidata_commands;

/*
ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
cargo test -- --test-threads=1
cargo test  -- --nocapture

git pull && ./build.sh && \rm ~/rustbot.* ; toolforge jobs restart rustbot


git pull && ./build.sh && toolforge jobs delete rustbot ; \rm ~/rustbot.* ; \
toolforge jobs run --image tf-php74 --mem 5Gi --cpu 3 --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot

rm ~/build.err ; \
toolforge jobs run build --command "bash -c 'source ~/.profile && cd ~/mixnmatch_rs && cargo build --release'" --image php7.4 --mem 2G --cpu 3 --wait ; \
cat ~/build.err


# WAS:
toolforge jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot

toolforge jobs delete rustbot2 ; \rm ~/rustbot2.* ; \
toolforge jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh second' rustbot2
*/
