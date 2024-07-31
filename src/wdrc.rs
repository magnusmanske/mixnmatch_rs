use crate::{
    app_state::{AppState, USER_AUX_MATCH},
    catalog::Catalog,
    entry::Entry,
    mysql_misc::MySQLMisc,
};
use anyhow::{anyhow, Result};
use futures::future::join_all;
use itertools::Itertools;
use mysql_async::{from_row, prelude::*};
use serde_json::Value;
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

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
        let results = self
            .get_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize, String)>)
            .await?;
        Ok(results)
    }

    async fn get_wrdc_api_responses(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let rand = rand::random::<u32>();
        let url = format!("https://wdrc.toolforge.org/api.php?format=jsonl&{query}&random={rand}");
        let client = wikimisc::wikidata::Wikidata::new().reqwest_client()?;
        let mut text;
        loop {
            text = client.get(&url).send().await?.text().await?;
            if !text.contains("<head><title>429 Too Many Requests</title></head>") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        Ok(text
            .split('\n')
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .collect())
    }

    fn yesterday(&self) -> String {
        let yesterday = chrono::Utc::now() - chrono::Duration::days(1);
        TimeStamp::datetime(&yesterday)
    }

    async fn sync_redirects(&self, app: &AppState) -> Result<()> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_sync_redirects")
            .await?
            .unwrap_or_else(|| self.yesterday());
        let mut new_ts = last_ts.to_owned();
        let mut redirects = HashMap::new();
        for j in self
            .get_wrdc_api_responses(&format!("action=redirects&since={last_ts}"))
            .await?
        {
            let from = j["item"]
                .as_str()
                .map(AppState::item2numeric)
                .and_then(|i| i)
                .unwrap_or(0);
            let to = j["target"]
                .as_str()
                .map(AppState::item2numeric)
                .and_then(|i| i)
                .unwrap_or(0);
            let ts = j["timestamp"]
                .as_str()
                .unwrap_or_else(|| &new_ts)
                .to_string();
            redirects.insert(from, to);
            if new_ts < ts {
                new_ts = ts;
            }
        }
        redirects.retain(|old_q, new_q| *old_q > 0 && *new_q > 0 && *old_q != *new_q); // Paranoia
        app.storage().maintenance_sync_redirects(redirects).await?;
        app.storage()
            .set_kv_value("wdrc_sync_redirects", &new_ts)
            .await?;
        Ok(())
    }

    async fn apply_deletions(&self, app: &AppState) -> Result<()> {
        let (last_ts, mut new_ts) = self.get_deletion_timestamps(app).await?;
        let deletions = self.get_deletions(&last_ts, &mut new_ts).await?;
        if !deletions.is_empty() {
            let catalog_ids = app.storage().maintenance_apply_deletions(deletions).await?;
            for catalog_id in catalog_ids {
                let catalog = Catalog::from_id(catalog_id, app).await?;
                let _ = catalog.refresh_overview_table().await;
            }
        }
        app.storage()
            .set_kv_value("wdrc_apply_deletions", &new_ts)
            .await?;
        Ok(())
    }

    async fn get_deletion_timestamps(&self, app: &AppState) -> Result<(String, String)> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_apply_deletions")
            .await?
            .unwrap_or_else(|| self.yesterday());
        let new_ts = last_ts.to_owned();
        Ok((last_ts, new_ts))
    }

    async fn get_deletions(&self, last_ts: &str, new_ts: &mut String) -> Result<Vec<isize>> {
        let mut deletions = vec![];
        for j in self
            .get_wrdc_api_responses(&format!("action=deletions&since={last_ts}"))
            .await?
        {
            let item = j["item"]
                .as_str()
                .map(AppState::item2numeric)
                .and_then(|i| i)
                .unwrap_or(0);
            let ts = j["timestamp"]
                .as_str()
                .unwrap_or_else(|| &*new_ts)
                .to_string();
            deletions.push(item);
            if *new_ts < ts {
                *new_ts = ts;
            }
        }
        deletions.sort();
        deletions.dedup();
        deletions.retain(|q| *q > 0);
        Ok(deletions)
    }

    async fn get_prop2catalog_ids(&self, app: &AppState) -> Result<HashMap<usize, Vec<usize>>> {
        let mut ret: HashMap<usize, Vec<usize>> = HashMap::new();
        let results = app.storage().maintenance_get_prop2catalog_ids().await?;
        for (catalog_id, property) in results {
            ret.entry(property).or_default().push(catalog_id);
        }
        Ok(ret)
    }

    async fn sync_property_propval2item(
        &self,
        property: usize,
        entity_ids: Vec<String>,
        app: &AppState,
    ) -> Option<HashMap<String, isize>> {
        let api = app.wikidata().get_mw_api().await.ok()?;
        let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
        entities.load_entities(&api, &entity_ids).await.ok()?;
        let mut propval2item: HashMap<String, Vec<isize>> = HashMap::new();
        for q in entity_ids {
            let q_num = match AppState::item2numeric(&q) {
                Some(q_num) => q_num,
                None => continue,
            };
            let i = match entities.get_entity(q) {
                Some(i) => i,
                None => continue,
            };
            let prop_values: Vec<String> = i
                .claims_with_property(format!("P{property}"))
                .iter()
                .map(|statement| statement.main_snak())
                .filter_map(|snak| snak.data_value().to_owned())
                .map(|datavalue| datavalue.value().to_owned())
                .filter_map(|value| match value {
                    wikimisc::wikibase::Value::StringValue(v) => Some(v),
                    _ => None,
                })
                .collect();
            for prop_value in prop_values {
                propval2item.entry(prop_value).or_default().push(q_num);
            }
        }

        Some(
            propval2item
                .iter_mut()
                .map(|(prop_value, items)| {
                    items.sort();
                    items.dedup();
                    (prop_value, items)
                })
                .filter(|(_prop_value, items)| items.len() == 1)
                .map(|(prop_value, items)| (prop_value.to_owned(), *items.first().unwrap()))
                .collect(),
        )
    }

    async fn sync_property(
        &self,
        property: usize,
        results: &[(usize, usize)],
        prop2catalog_ids: &HashMap<usize, Vec<usize>>,
        app: &AppState,
    ) -> Result<()> {
        let entity_ids = results
            .iter()
            .filter(|(_item, prop)| *prop == property)
            .map(|(item, _prop)| format!("Q{item}"))
            .collect_vec();
        let propval2item = match self
            .sync_property_propval2item(property, entity_ids, app)
            .await
        {
            Some(x) => x,
            None => return Ok(()),
        };
        if propval2item.is_empty() {
            return Ok(());
        }
        let dummy = vec![];
        let catalogs = prop2catalog_ids.get(&property).unwrap_or(&dummy);
        let params: Vec<String> = propval2item
            .keys()
            .map(|propval| propval.to_string())
            .collect();

        let results = app
            .storage()
            .maintenance_sync_property(catalogs, &propval2item, params)
            .await?;
        for (id, ext_id, user, _mnm_q) in results {
            let wd_item_q = match propval2item.get(&ext_id) {
                Some(wd_item_q) => wd_item_q,
                None => continue,
            };
            if user.is_none() || user == Some(0) {
                match Entry::from_id(id, app).await {
                    Ok(mut entry) => {
                        if entry.q != Some(*wd_item_q) || !entry.is_fully_matched() {
                            // Only if something is different
                            let _ = entry
                                .set_match(&format!("Q{wd_item_q}"), USER_AUX_MATCH)
                                .await;
                            // println!("P{property}: {} => {}",entry.get_entry_url().unwrap_or("".into()),entry.get_item_url().unwrap_or("".into()));
                        }
                    }
                    Err(_) => continue, // Ignore error
                }
            }
        }
        Ok(())
    }

    pub async fn sync_properties(&self, app: &AppState) -> Result<()> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_sync_properties")
            .await?
            .unwrap_or_else(|| self.yesterday());
        let prop2catalog_ids = self.get_prop2catalog_ids(app).await?;
        let results = app
            .wdrc()
            .get_item_property_ts(&prop2catalog_ids, &last_ts)
            .await?;
        let new_ts = match results.iter().map(|(_item, _property, ts)| ts).max() {
            Some(ts) => ts.to_owned(),
            None => return Ok(()), // No results
        };
        let batch_size = *app
            .task_specific_usize()
            .get("wdrc_sync_properties_batch_size")
            .unwrap_or(&10);
        let all_results = results
            .into_iter()
            .map(|(item, property, _ts)| (item, property))
            .collect_vec();
        for results in all_results.chunks(batch_size) {
            let results = results.to_vec();
            let properties = results
                .iter()
                .map(|(_item, property)| *property)
                .sorted()
                .dedup()
                .collect_vec();
            let futures = properties
                .iter()
                .map(|property| self.sync_property(*property, &results, &prop2catalog_ids, app))
                .collect_vec();
            let results = join_all(futures).await;
            let failed = results.into_iter().filter(|r| r.is_err()).collect_vec();
            if let Some(Err(e)) = failed.first() {
                return Err(anyhow!("{e}"));
            }
        }
        app.storage()
            .set_kv_value("wdrc_sync_properties", &new_ts)
            .await?;
        Ok(())
    }

    pub async fn sync(&self, app: &AppState) -> Result<()> {
        self.sync_redirects(app).await?;
        self.apply_deletions(app).await?;
        self.sync_properties(app).await?;
        Ok(())
    }
}
