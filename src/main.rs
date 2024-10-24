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
pub mod error;
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
pub mod storage;
pub mod storage_mysql;
pub mod task_size;
// pub mod storage_wikibase;
pub mod taxon_matcher;
pub mod update_catalog;
pub mod wdrc;
pub mod wikidata;
pub mod wikidata_commands;

use anyhow::Result;
use std::env;

#[derive(Debug, Default)]
pub struct PropTodo {
    pub id: usize,
    pub prop_num: u64,
    pub name: String,
    pub default_type: String,
    pub status: String,
    pub note: String,
    pub user_id: u64,
    pub items_using: Option<u64>,
    pub number_of_records: Option<u64>,
}

impl PropTodo {
    pub fn new(prop_num: u64, name: String) -> Self {
        Self {
            prop_num,
            name,
            status: "NO_CATALOG".to_string(),
            ..Default::default()
        }
    }

    pub fn from_row(r: mysql_async::Row) -> Option<Self> {
        Some(Self {
            id: r.get(0)?,
            prop_num: r.get(1)?,
            name: r.get(2)?,
            default_type: r.get(3)?,
            status: r.get(4)?,
            note: r.get(5)?,
            user_id: r.get(6)?,
            items_using: r.get(7)?,
            number_of_records: r.get(8)?,
        })
    }
}

async fn run() -> Result<()> {
    let argv: Vec<String> = env::args_os().map(|s| s.into_string().unwrap()).collect();
    let config_file = argv
        .get(2)
        .map(|s| s.to_owned())
        .unwrap_or("config.json".into());
    let app = app_state::AppState::from_config_file(&config_file)?;
    match argv.get(1).map(|s| s.as_str()) {
        Some("job") => {
            app.run_single_job(
                argv.get(3)
                    .expect("Job ID as third parameter required")
                    .parse::<usize>()
                    .unwrap(),
            )
            .await
        }
        Some("hpjob") => app.run_single_hp_job().await,
        // Some("from_props") => {
        //     let props: Vec<u32> = argv
        //         .get(3)
        //         .expect("Comma-separated props as third parameter")
        //         .split(',')
        //         .filter_map(|s| s.parse::<u32>().ok())
        //         .collect();
        //     let min_entries = argv.get(4).and_then(|s| s.parse::<u16>().ok()).unwrap_or(2);
        //     app.run_from_props(props, min_entries).await
        // }
        Some("test") => {
            // bespoke_scrapers::BespokeScraper6479::new(&app).run().await;
            let maintenance = maintenance::Maintenance::new(&app);
            maintenance.match_by_name_and_full_dates().await
        }
        Some("server") => app.forever_loop().await,
        Some(other) => panic!("Unrecodnized command '{other}'"),
        None => panic!("Command required: server CONFIG_FILE | job CONFIG_FILE JOB_ID"),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> Result<()> {
    match run().await {
        Ok(_) => {}
        Err(e) => println!("CATASTROPHIC FAILURE: {e}"),
    }
    Ok(())
}

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
