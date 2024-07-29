use crate::mysql_misc::MySQLMisc;
use anyhow::Result;
use itertools::Itertools;
use mysql_async::{from_row, prelude::*};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct WDRC {
    pool: mysql_async::Pool,
}

impl MySQLMisc for WDRC {
    fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }
}

impl WDRC {
    pub fn new(config: &Value) -> Self {
        Self {
            pool: Self::create_pool(config),
        }
    }

    pub async fn get_item_property_ts(
        &self,
        prop2catalog_ids: &HashMap<usize, Vec<usize>>,
        last_ts: &str,
    ) -> Result<Vec<(usize, usize, String)>> {
        let properties = prop2catalog_ids.keys().cloned().collect_vec();
        let props_str = properties.iter().map(|p| format!("{p}")).join(",");
        let sql = format!("SELECT DISTINCT `item`,`property`,`timestamp` FROM `statements` WHERE `property` IN ({props_str}) AND `timestamp`>='{last_ts}'") ;
        let mut conn = self.get_conn().await?;
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize, String)>)
            .await?;
        Ok(results)
    }
}
