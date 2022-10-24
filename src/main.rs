#[macro_use]
extern crate serde_json;

use std::env;
use std::fs::File;
use serde_json::Value;
use mysql_async::prelude::*;
use mysql_async::Pool;

type GenericError = Box<dyn std::error::Error + Send + Sync>;

pub struct AppState {
    wikidata_pool: mysql_async::Pool,
    mnm_pool: mysql_async::Pool
}

impl AppState {
    pub async fn new_from_config(config: &Value) -> Self {
        let ret = Self {
            wikidata_pool: mysql_async::Pool::new(config["db_wikidata"].as_str().unwrap()),
            mnm_pool: mysql_async::Pool::new(config["db_mnm"].as_str().unwrap())
        };
        ret
    }

    pub async fn disconnect(&self) -> Result<(),GenericError> {
        //self.wikidata_pool.disconnect().await?;
        //self.mnm_pool.disconnect().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(),GenericError> {
    let basedir = env::current_dir()
        .expect("Can't get CWD")
        .to_str()
        .expect("Can't convert CWD to_str")
        .to_string();
    let path = basedir.to_owned() + "/config.json";
    let file = File::open(&path).unwrap_or_else(|_| panic!("Can not open config file at {}", &path));
    let config: Value =
        serde_json::from_reader(file).expect("Can not parse JSON from config file");

    let app = AppState::new_from_config(&config).await;
    let conn = app.wikidata_pool.get_conn().await?;

    let result = "SELECT customer_id, amount, account_name FROM payment"
    .with(()).await?;
    println!("{}",&result);

    drop(conn);
    app.disconnect().await?;
    Ok(())
}
