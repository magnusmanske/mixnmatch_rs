extern crate serde_json;

use std::env;
use std::fs::File;
use serde_json::Value;
use mysql_async::prelude::*;
use mysql_async::from_row;
pub mod app_state;

/*
ssh magnus@tools-login.wmflabs.org -L 3309:wikidatawiki.web.db.svc.eqiad.wmflabs:3306 -N &
ssh magnus@tools-login.wmflabs.org -L 3308:tools-db:3306 -N &
*/

type GenericError = Box<dyn std::error::Error + Send + Sync>;


#[tokio::main]
async fn main() -> Result<(),GenericError> {
    let mut path = env::current_dir().expect("Can't get CWD");
    path.push("config.json");
    let file = File::open(&path).expect(format!("Can not open config file at {:?}", &path).as_str());
    let config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");

    let app = app_state::AppState::new_from_config(&config).await;


    let mut conn = app.wd_pool.get_conn().await?;

    let rows = conn.exec_iter(r"SELECT page_title,page_namespace from page LIMIT 1",())
        .await?
        .map_and_drop(from_row::<(String,i32)>)
        .await?;
    println!("{:?}",&rows);

    drop(conn);


    app.wd_pool.disconnect().await?;
    app.mnm_pool.disconnect().await?;
    Ok(())
}
