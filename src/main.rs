extern crate serde_json;

use std::env;
use std::fs::File;
use serde_json::Value;
pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/


#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let mut path = env::current_dir().expect("Can't get CWD");
    path.push("config.json");
    let file = File::open(&path)?;
    let config: Value = serde_json::from_reader(file)?;

    let app = app_state::AppState::new_from_config(&config).await;
    let mnm = mixnmatch::MixNMatch::new(app.clone());
    let am = automatch::AutoMatch::new(mnm.clone());
    am.automatch_by_search(5338).await?;

    //app.wd_pool.disconnect().await?;
    //app.mnm_pool.disconnect().await?;
    Ok(())
}
