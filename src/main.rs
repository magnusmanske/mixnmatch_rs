pub mod app_state;
pub mod error;
pub mod mixnmatch;
pub mod automatch ;
pub mod taxon_matcher ;
pub mod update_catalog ;
pub mod auxiliary_matcher ;
pub mod coordinate_matcher ;
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
use app_state::AppState;
use tokio::runtime;

async fn run(app: AppState) -> Result<(),app_state::GenericError> {
    let argv: Vec<String> = env::args_os().map(|s|s.into_string().unwrap()).collect();
    match argv.get(1).map(|s|s.as_str()) {
        Some("job") => app.run_single_job(argv.get(2).unwrap().parse::<usize>().unwrap()).await,
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
        _ => app.forever_loop().await
    }
}

fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;

    let threads = match env::var("MNM_THREADS") {
        Ok(s) => s.parse::<usize>().unwrap_or(app.default_threads),
        Err(_) => app.default_threads,
    };
    println!("Using {threads} threads");

    let threaded_rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(threads)
        .thread_name("mixnmatch")
        .thread_stack_size(app.thread_stack_factor*threads * 1024 * 1024)
        .build()?;

    threaded_rt.block_on(async move {
        match run(app).await {
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

git pull && ./build.sh && \rm ~/rustbot.* ; toolforge jobs restart rustbot


git pull && ./build.sh && toolforge jobs delete rustbot ; \rm ~/rustbot.* ; \
toolforge jobs run --image tf-php74 --mem 5Gi --cpu 3 --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot

rm ~/build.err ; \
toolforge jobs run build --command "bash -c 'source ~/.profile && cd ~/mixnmatch_rs && cargo build --release'" --image python3.11 --mem 2G --cpu 3 ; \
cat ~/build.err


# WAS:
toolforge jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot

toolforge jobs delete rustbot2 ; \rm ~/rustbot2.* ; \
toolforge jobs run --image tf-php74 --mem 1000Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh second' rustbot2
*/
