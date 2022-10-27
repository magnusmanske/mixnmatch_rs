pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;
pub mod entry ;
pub mod job ;

use std::{thread, time};
use crate::job::*;

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/


#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json").await?;
    let mnm = mixnmatch::MixNMatch::new(app.clone());
    let valid_actions = vec!("automatch_by_search");
    Job::new(&mnm).reset_running_jobs(&Some(valid_actions.clone())).await?; // Reset jobs
    loop {
        let mut job = Job::new(&mnm);
        match job.set_next(&Some(valid_actions.clone())).await {
            Ok(true) => {
                tokio::spawn(async move {
                    job.run().await.unwrap();
                })
                //.await.unwrap() // TESTING
                ;
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
