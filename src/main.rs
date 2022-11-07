pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;
pub mod taxon_matcher ;
pub mod update_catalog ;
pub mod catalog ;
pub mod entry ;
pub mod job ;

use std::env;
use std::sync::{Arc, Mutex};
pub use lazy_static::*;
use std::{thread, time};
use crate::job::*;

const MAX_CONCURRENT_JOBS: usize = 10 ; // Runs fine with >40 in <500MB but might stress the APIs. Use usize::MAX for unlimited

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
cargo test  -- --test-threads=1 --nocapture

git pull && ./build.sh
toolforge-jobs delete rustbot && \rm ~/rustbot.*
toolforge-jobs run --image tf-golang111 --mem 500Mi --continuous --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot
#jsub -mem 500m -cwd -N rustbot ./run.sh
*/

fn hold_on() {
    thread::sleep(time::Duration::from_secs(5));
}

#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json")?;
    let mnm = mixnmatch::MixNMatch::new(app.clone());

    let valid_actions = vec!(
        "automatch_by_search",
        "automatch_from_other_catalogs",
        "taxon_matcher",
        "purge_automatches",
        "match_person_dates",
        "match_on_birthdate",
        "update_from_tabbed_file",
        "automatch_by_sitelink"
    );

    let argv: Vec<String> = env::args_os().map(|s|s.into_string().unwrap()).collect();
    match argv.get(1).map(|s|s.as_str()) {
        Some("job") => {
            let job_id = argv.get(2).unwrap().parse::<usize>().unwrap();
            let mut job = Job::new(&mnm);
            job.set_from_id(job_id).await?;
            match job.set_status(STATUS_RUNNING).await {
                Ok(_) => {
                    println!("Finished successfully");
                }
                Err(e) => {
                    println!("ERROR: {}",e);
                }
            }
            return job.run().await;
        }
        _ => {} // Any other will start the bot
    }

    let concurrent:Arc<Mutex<usize>> = Arc::new(Mutex::new(0));

    // Reset old running&failed jobs
    Job::new(&mnm).reset_running_jobs(&Some(valid_actions.clone())).await?;
    Job::new(&mnm).reset_failed_jobs(&Some(valid_actions.clone())).await?;
    println!("Old {:?} jobs reset, starting bot",&valid_actions);

    loop {
        if *concurrent.lock().unwrap()>=MAX_CONCURRENT_JOBS {
            hold_on();
            continue;
        }
        let mut job = Job::new(&mnm);
        match job.set_next(&Some(valid_actions.clone())).await {
            Ok(true) => {
                let _ = job.set_status(STATUS_RUNNING).await;
                let concurrent = concurrent.clone();
                tokio::spawn(async move {
                    *concurrent.lock().unwrap() += 1;
                    let _ = job.run().await;
                    *concurrent.lock().unwrap() -= 1;
                });
            }
            Ok(false) => {
                hold_on();
            }
            _ => {
                println!("MAIN LOOP: Something went wrong");
                hold_on();
            }
        }
    }
    // app.disconnect().await?; // Never happens
    //Ok(())
}
