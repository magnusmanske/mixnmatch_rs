pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;
pub mod taxon_matcher ;
pub mod update_catalog ;
pub mod entry ;
pub mod job ;

pub use lazy_static::*;
use std::{thread, time};
use crate::job::*;

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
cargo test  -- --test-threads=1 --nocapture

git pull
./build.sh
\rm ~/rustbot.*
toolforge-jobs run --image tf-golang111 --mem 500Mi --command '/data/project/mix-n-match/mixnmatch_rs/run.sh' rustbot
#jsub -mem 500m -cwd -N rustbot ./run.sh
*/

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
        "update_from_tabbed_file"
    );
    Job::new(&mnm).reset_running_jobs(&Some(valid_actions.clone())).await?; // Reset jobs
    println!("Old {:?} jobs reset, starting bot",&valid_actions);

    loop {
        let mut job = Job::new(&mnm);
        match job.set_next(&Some(valid_actions.clone())).await {
            Ok(true) => {
                tokio::spawn(async move { let _ = job.run().await; });
            }
            Ok(false) => {
                thread::sleep(time::Duration::from_secs(5));
            }
            _ => {
                println!("MAIN LOOP: Something went wrong");
                thread::sleep(time::Duration::from_secs(5));
            }
        }
    }
    // app.disconnect().await?; // Never happens
    //Ok(())
}
