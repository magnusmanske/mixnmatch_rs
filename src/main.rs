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

#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;
    let argv: Vec<String> = env::args_os().map(|s|s.into_string().unwrap()).collect();
    match argv.get(1).map(|s|s.as_str()) {
        Some("job") => app.run_single_job(argv.get(2).unwrap().parse::<usize>().unwrap()).await,
        Some("test") => {
            let mnm = crate::mixnmatch::MixNMatch::new(app.clone());
            let mut j = crate::job::Job::new(&mnm);
            let id_opt = j.set_next().await;
            println!("{:?}",id_opt);
            Ok(())
        }
        Some("second") => app.forever_loop(false).await,
        _ => app.forever_loop(true).await
    }
}

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
cargo test  -- --nocapture

git pull && ./build.sh && toolforge-jobs delete rustbot && \rm ~/rustbot.* && \
toolforge-jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot
toolforge-jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh second' rustbot2
*/
