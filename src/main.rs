pub mod app_state;
pub mod error;
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
use tokio::runtime;

const DEFAULT_THREADS: usize = 4;

async fn run() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;
    let argv: Vec<String> = env::args_os().map(|s|s.into_string().unwrap()).collect();
    match argv.get(1).map(|s|s.as_str()) {
        Some("job") => app.run_single_job(argv.get(2).unwrap().parse::<usize>().unwrap()).await,
        Some("hpjob") => app.run_single_hp_job().await,
        Some("test") => {
            let mnm = crate::mixnmatch::MixNMatch::new(app.clone());
            let mut j = crate::job::Job::new(&mnm);
            let id_opt = j.set_next().await;
            println!("{:?}",id_opt);
            Ok(())
        }
        Some("second") => app.forever_loop(false).await, // Won't do long-running actions, so as to not block in autoscrape etc
        _ => app.forever_loop(true).await
    }
}

fn main() -> Result<(),app_state::GenericError> {
    let threads = match env::var("MNM_THREADS") {
        Ok(s) => s.parse::<usize>().unwrap_or(DEFAULT_THREADS),
        Err(_) => DEFAULT_THREADS,
    };
    println!("Using {threads} threads");
    let threaded_rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(threads)
        .thread_name("mixnmatch")
        .thread_stack_size(64*threads * 1024 * 1024)
        .build()?;

    threaded_rt.block_on(async move {
        match run().await {
            Ok(_) => {},
            Err(e) => println!("CATASTROPHIC FAILURE: {e}"),
        }
    });
    Ok(())
}

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
cargo test  -- --nocapture

git pull && ./build.sh && toolforge-jobs delete rustbot ; \rm ~/rustbot.* ; \
toolforge-jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot

toolforge-jobs delete rustbot2 ; \rm ~/rustbot2.* ; \
toolforge-jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh second' rustbot2
*/
