use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::mixnmatch::*;
use crate::storage::Storage;
use anyhow::{anyhow, Result};
use futures::future::join_all;
use itertools::Itertools;
use mysql_async::from_row;
use mysql_async::prelude::*;
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;

pub struct Maintenance {
    mnm: MixNMatch,
}

impl Maintenance {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self { mnm: mnm.clone() }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and replaces redirects with their targets.
    pub async fn fix_redirects(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks meta items, such as disambiguation pages.
    pub async fn unlink_meta_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks deleted pages
    pub async fn unlink_deleted_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Fixes redirected items, and unlinks deleted and meta items.
    /// This is more efficient than calling the functions individually, because it uses the same batching run.
    pub async fn fix_matched_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Removes P17 auxiliary values for entryies of type Q5 (human)
    pub async fn remove_p17_for_humans(&self) -> Result<()> {
        let dummy = 0;
        let sql = r#"DELETE FROM auxiliary WHERE aux_p=17 AND EXISTS (SELECT * FROM entry WHERE entry_id=entry.id AND `type`="Q5") AND id>:dummy"#;
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params! {dummy})
            .await?;
        Ok(())
    }

    pub async fn cleanup_mnm_relations(&self) -> Result<()> {
        let sql = "DELETE from mnm_relation WHERE entry_id=0 or target_entry_id=0";
        self.mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, ())
            .await?;
        Ok(())
    }

    // WDRC sync stuff

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

    async fn wdrc_sync_redirects(&self) -> Result<()> {
        let last_ts = self
            .mnm
            .get_storage()
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
                .map(|s| self.mnm.item2numeric(s))
                .and_then(|i| i)
                .unwrap_or(0);
            let to = j["target"]
                .as_str()
                .map(|s| self.mnm.item2numeric(s))
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
        let mut conn = self.mnm.app.get_mnm_conn().await?;
        for (old_q, new_q) in redirects {
            let sql = "UPDATE `entry` SET `q`=:new_q WHERE `q`=:old_q";
            conn.exec_drop(sql, params! {old_q,new_q}).await?;
        }
        drop(conn);
        self.mnm
            .get_storage()
            .set_kv_value("wdrc_sync_redirects", &new_ts)
            .await?;
        Ok(())
    }

    async fn wdrc_apply_deletions(&self) -> Result<()> {
        let last_ts = self
            .mnm
            .get_storage()
            .get_kv_value("wdrc_apply_deletions")
            .await?
            .unwrap_or_else(|| self.yesterday());
        let mut new_ts = last_ts.to_owned();
        let mut deletions = vec![];
        for j in self
            .get_wrdc_api_responses(&format!("action=deletions&since={last_ts}"))
            .await?
        {
            let item = j["item"]
                .as_str()
                .map(|s| self.mnm.item2numeric(s))
                .and_then(|i| i)
                .unwrap_or(0);
            let ts = j["timestamp"]
                .as_str()
                .unwrap_or_else(|| &new_ts)
                .to_string();
            deletions.push(item);
            if new_ts < ts {
                new_ts = ts;
            }
        }
        deletions.sort();
        deletions.dedup();
        deletions.retain(|q| *q > 0);
        if !deletions.is_empty() {
            let mut conn = self.mnm.app.get_mnm_conn().await?;
            let deletions_string = deletions
                .iter()
                .map(|i| format!("{}", *i))
                .collect::<Vec<String>>()
                .join(",");

            let sql =
                format!("SELECT DISTINCT `catalog` FROM `entry` WHERE `q` IN ({deletions_string})");
            let catalog_ids = conn
                .exec_iter(sql, ())
                .await?
                .map_and_drop(from_row::<usize>)
                .await?;

            let sql = format!("UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({deletions_string})");
            conn.exec_drop(sql, ()).await?;
            drop(conn);

            for catalog_id in catalog_ids {
                let catalog = Catalog::from_id(catalog_id, &self.mnm).await?;
                let _ = catalog.refresh_overview_table().await;
            }
        }
        self.mnm
            .get_storage()
            .set_kv_value("wdrc_apply_deletions", &new_ts)
            .await?;
        Ok(())
    }

    async fn wdrc_get_prop2catalog_ids(&self) -> Result<HashMap<usize, Vec<usize>>> {
        let mut ret: HashMap<usize, Vec<usize>> = HashMap::new();
        let sql = r"SELECT `id`,`wd_prop` FROM `catalog` WHERE `wd_prop` IS NOT NULL AND `wd_qual` IS NULL AND `active`=1";
        let results = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize)>)
            .await?;
        for (catalog_id, property) in results {
            ret.entry(property).or_default().push(catalog_id);
        }
        Ok(ret)
    }

    async fn wdrc_sync_property_propval2item(
        &self,
        property: usize,
        entity_ids: Vec<String>,
    ) -> Option<HashMap<String, isize>> {
        let api = self.mnm.get_mw_api().await.ok()?;
        let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
        entities.load_entities(&api, &entity_ids).await.ok()?;
        let mut propval2item: HashMap<String, Vec<isize>> = HashMap::new();
        for q in entity_ids {
            let q_num = match self.mnm.item2numeric(&q) {
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

    async fn wdrc_sync_property(
        &self,
        property: usize,
        results: &[(usize, usize)],
        prop2catalog_ids: &HashMap<usize, Vec<usize>>,
    ) -> Result<()> {
        let entity_ids = results
            .iter()
            .filter(|(_item, prop)| *prop == property)
            .map(|(item, _prop)| format!("Q{item}"))
            .collect_vec();
        let propval2item = match self
            .wdrc_sync_property_propval2item(property, entity_ids)
            .await
        {
            Some(x) => x,
            None => return Ok(()),
        };
        if propval2item.is_empty() {
            return Ok(());
        }
        let catalogs_str = match prop2catalog_ids.get(&property) {
            Some(ids) => ids.iter().map(|id| format!("{id}")).join(","),
            None => return Ok(()),
        };
        let qm_propvals = MixNMatch::sql_placeholders(propval2item.len());
        let params: Vec<String> = propval2item
            .keys()
            .map(|propval| propval.to_string())
            .collect();
        let sql = format!(
            r"SELECT `id`,`ext_id`,`user`,`q` FROM `entry` WHERE `catalog` IN ({catalogs_str}) AND `ext_id` IN ({qm_propvals})"
        );
        let results = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, params)
            .await?
            .map_and_drop(from_row::<(usize, String, Option<usize>, Option<usize>)>)
            .await?;
        for (id, ext_id, user, _mnm_q) in results {
            let wd_item_q = match propval2item.get(&ext_id) {
                Some(wd_item_q) => wd_item_q,
                None => continue,
            };
            if user.is_none() || user == Some(0) {
                match Entry::from_id(id, &self.mnm).await {
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

    pub async fn wdrc_sync_properties(&self) -> Result<()> {
        let last_ts = self
            .mnm
            .get_storage()
            .get_kv_value("wdrc_sync_properties")
            .await?
            .unwrap_or_else(|| self.yesterday());
        let prop2catalog_ids = self.wdrc_get_prop2catalog_ids().await?;
        let properties = prop2catalog_ids.keys().cloned().collect_vec();
        let props_str = properties.iter().map(|p| format!("{p}")).join(",");
        let sql = format!("SELECT DISTINCT `item`,`property`,`timestamp` FROM `statements` WHERE `property` IN ({props_str}) AND `timestamp`>='{last_ts}'") ;
        let results = self
            .mnm
            .app
            .get_wdrc_conn()
            .await? // (item,property)
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize, String)>)
            .await?;
        let new_ts = match results.iter().map(|(_item, _property, ts)| ts).max() {
            Some(ts) => ts.to_owned(),
            None => return Ok(()), // No results
        };
        let batch_size = *self
            .mnm
            .app
            .task_specific_usize
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
                .map(|property| self.wdrc_sync_property(*property, &results, &prop2catalog_ids))
                .collect_vec();
            let results = join_all(futures).await;
            let failed = results.into_iter().filter(|r| r.is_err()).collect_vec();
            if let Some(Err(e)) = failed.first() {
                return Err(anyhow!("{e}"));
            }
        }
        self.mnm
            .get_storage()
            .set_kv_value("wdrc_sync_properties", &new_ts)
            .await?;
        Ok(())
    }

    pub async fn wdrc_sync(&self) -> Result<()> {
        self.wdrc_sync_redirects().await?;
        self.wdrc_apply_deletions().await?;
        self.wdrc_sync_properties().await?;
        Ok(())
    }

    // END WDRC STUFF

    /// Finds redirects in a batch of items, and changes MnM matches to their respective targets.
    async fn fix_redirected_items_batch(&self, unique_qs: &Vec<String>) -> Result<()> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect`
            WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0
            AND `page_title` IN ({})",placeholders);
        let page2rd = self
            .mnm
            .app
            .get_wd_conn()
            .await?
            .exec_iter(sql, unique_qs)
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        for (from, to) in &page2rd {
            if let (Some(from), Some(to)) = (self.mnm.item2numeric(from), self.mnm.item2numeric(to))
            {
                if from > 0 && to > 0 {
                    let sql = "UPDATE `entry` SET `q`=:to WHERE `q`=:from";
                    self.mnm
                        .app
                        .get_mnm_conn()
                        .await?
                        .exec_drop(sql, params! {from,to})
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Finds deleted items in a batch of items, and unlinks MnM matches to them.
    async fn unlink_deleted_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!(
            "SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({})",
            placeholders
        );
        let found_items = self
            .mnm
            .app
            .get_wd_conn()
            .await?
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q| !found_items.contains(q))
            .cloned()
            .collect();
        self.unlink_item_matches(&not_found).await?;
        Ok(())
    }

    /// Finds meta items (disambig etc) in a batch of items, and unlinks MnM matches to them.
    async fn unlink_meta_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT DISTINCT lt_title AS page_title FROM page,pagelinks,linktarget WHERE page_namespace=0 AND page_title IN ({}) AND pl_from=page_id AND lt_title IN ('{}')",&placeholders,&META_ITEMS.join("','"));
        let meta_items = self
            .mnm
            .app
            .get_wd_conn()
            .await?
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        self.unlink_item_matches(&meta_items).await?;
        Ok(())
    }

    /// Unlinks MnM matches to items in a list.
    pub async fn unlink_item_matches(&self, items: &[String]) -> Result<()> {
        let items: Vec<isize> = items
            .iter()
            .filter_map(|q| self.mnm.item2numeric(q))
            .collect();

        if !items.is_empty() {
            let items: Vec<String> = items.iter().map(|q| format!("{}", q)).collect();
            let sql = format!(
                "UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({})",
                items.join(",")
            );
            self.mnm
                .app
                .get_mnm_conn()
                .await?
                .exec_drop(sql, mysql_async::Params::Empty)
                .await?;
        }
        Ok(())
    }

    /// Finds some unmatched (Q5) entries where there is a (unique) full match for that name,
    /// and uses it as an auto-match
    pub async fn maintenance_automatch(&self) -> Result<()> {
        let sql = "SELECT e1.id,e2.q FROM entry e1,entry e2
            WHERE e1.ext_name=e2.ext_name AND e1.id!=e2.id
            AND e1.type='Q5' AND e2.type='Q5'
            AND e1.q IS NULL
            AND e2.type IS NOT NULL AND e2.user>0
            HAVING
            (SELECT count(DISTINCT q) FROM entry e3 WHERE e3.ext_name=e2.ext_name AND e3.type=e2.type AND e3.q IS NOT NULL AND e3.user>0)=1
            LIMIT 500";
        let new_automatches = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, isize)>)
            .await?;
        let sql = "UPDATE `entry` SET `q`=:q,`user`=0,`timestamp`=:timestamp WHERE `id`=:entry_id AND `q` IS NULL" ;
        let mut conn = self.mnm.app.get_mnm_conn().await?;
        for (entry_id, q) in &new_automatches {
            let timestamp = TimeStamp::now();
            conn.exec_drop(sql, params! {entry_id,q,timestamp}).await?;
        }
        drop(conn);
        Ok(())
    }

    /// Retrieves a batch of (unique) Wikidata items, in a given matching state.
    async fn get_items(
        &self,
        catalog_id: usize,
        offset: usize,
        state: &MatchState,
    ) -> Result<Vec<String>> {
        let batch_size = 5000;
        let sql = format!("SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            state.get_sql()
        ) ;
        let ret = self
            .mnm
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        let ret = ret.iter().map(|q| format!("Q{}", q)).collect();
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Entry;

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_unlink_meta_items() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

        // Set a match to a disambiguation item
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.set_match("Q16456", 2).await.unwrap();

        // Remove matches to disambiguation items
        let maintenance = Maintenance::new(&mnm);
        maintenance
            .unlink_meta_items(TEST_CATALOG_ID, &MatchState::any_matched())
            .await
            .unwrap();

        // Check that removal was successful
        assert_eq!(Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().q, None);
    }

    #[tokio::test]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm)
            .await
            .unwrap()
            .set_match("Q100000067", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&mnm);
        ms.fix_redirects(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q, Some(91013264));
    }

    #[tokio::test]
    async fn test_unlink_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm)
            .await
            .unwrap()
            .set_match("Q115205673", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&mnm);
        ms.unlink_deleted_items(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q, None);
    }
}
