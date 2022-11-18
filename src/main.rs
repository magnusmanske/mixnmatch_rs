pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;
pub mod taxon_matcher ;
pub mod update_catalog ;
pub mod auxiliary_matcher ;
pub mod autoscrape ;
pub mod microsync ;
pub mod catalog ;
pub mod entry ;
pub mod job ;
pub mod issue ;
pub mod wikidata_commands;
pub mod php_wrapper;
pub mod maintenance;
use std::env;

const MAX_CONCURRENT_JOBS: usize = 20 ; // Runs fine with >40 in <500MB but might stress the APIs. Use usize::MAX for unlimited

#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;
    let argv: Vec<String> = env::args_os().map(|s|s.into_string().unwrap()).collect();
    match argv.get(1).map(|s|s.as_str()) {
        Some("job") => app.run_single_job(argv.get(2).unwrap().parse::<usize>().unwrap()).await,
        _ => app.forever_loop(MAX_CONCURRENT_JOBS).await
    }
}

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
cargo test  -- --nocapture

git pull && ./build.sh && toolforge-jobs delete rustbot && \rm ~/rustbot.* && \
toolforge-jobs run --image tf-php74 --cpu 2 --mem 1500Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot
*/
