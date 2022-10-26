extern crate serde_json;

pub mod app_state;
pub mod mixnmatch;
pub mod automatch ;
pub mod entry ;

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/


#[tokio::main]
async fn main() -> Result<(),app_state::GenericError> {
    let app = app_state::AppState::from_config_file("config.json").await?;
    let mnm = mixnmatch::MixNMatch::new(app.clone());
    let am = automatch::AutoMatch::new(&mnm);
    am.automatch_by_search(5338).await?;

    //app.wd_pool.disconnect().await?;
    //app.mnm_pool.disconnect().await?;
    Ok(())
}
