pub mod app_state;
pub mod automatch;
pub mod autoscrape;
pub mod auxiliary_matcher;
pub mod catalog;
pub mod coordinate_matcher;
pub mod entry;
pub mod error;
pub mod issue;
pub mod job;
pub mod maintenance;
pub mod microsync;
pub mod mixnmatch;
pub mod php_wrapper;
pub mod taxon_matcher;
pub mod update_catalog;
pub mod wikidata_commands;

use app_state::AppState;
use std::env;

async fn run(app: AppState) -> Result<(), app_state::GenericError> {
    let argv: Vec<String> = env::args_os().map(|s| s.into_string().unwrap()).collect();
    match argv.get(1).map(|s| s.as_str()) {
        Some("job") => {
            app.run_single_job(argv.get(2).unwrap().parse::<usize>().unwrap())
                .await
        }
        Some("hpjob") => app.run_single_hp_job().await,
        Some("test") => {
            let mnm = crate::mixnmatch::MixNMatch::new(app.clone());
            let mut job = crate::job::Job::new(&mnm);
            job.set_next().await?;
            // let job_id = job.get_next_job_id().await;
            // let id_opt = j.set_next().await;
            println!("{job:?}");
            Ok(())
        }
        _ => app.forever_loop().await,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> Result<(), app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;
    // let runtime = app.runtime.clone();
    // runtime.block_on(async move {
    match run(app).await {
        Ok(_) => {}
        Err(e) => println!("CATASTROPHIC FAILURE: {e}"),
    }
    // });
    Ok(())
}

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
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
