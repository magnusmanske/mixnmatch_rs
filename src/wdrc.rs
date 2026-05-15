use crate::{
    app_state::{AppContext, ExternalServicesContext, USER_AUTO, USER_AUX_MATCH, WikidataContext, item2numeric},
    catalog::Catalog,
    entry::{Entry, EntryWriter},
    mysql_misc::MySQLMisc,
};
use anyhow::{Result, anyhow};
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
    pub fn new(config: &Value) -> Result<Self> {
        Ok(Self {
            pool: Self::create_pool(config)?,
        })
    }

    pub async fn get_item_property_ts(
        &self,
        prop2catalog_ids: &HashMap<usize, Vec<usize>>,
        last_ts: &str,
    ) -> Result<Vec<(usize, usize, String)>> {
        let props_str = prop2catalog_ids.keys().join(",");
        let sql = format!(
            "SELECT DISTINCT `item`,`property`,`timestamp` FROM `statements` WHERE `property` IN ({props_str}) AND `timestamp`>='{last_ts}'"
        );
        let results = self
            .get_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize, String)>)
            .await?;
        Ok(results)
    }

    /// Maximum number of WDRC retry attempts on `429 Too Many Requests`.
    /// Previously this loop was unbounded with a fixed 1-second sleep — a
    /// genuine WDRC outage parked the calling job forever. 6 attempts with
    /// 1/2/4/8/16/30 s capped backoff sums to ~61 s of patience, well within
    /// the per-action wall-clock budget defined in `job.rs`.
    const WDRC_MAX_ATTEMPTS: u32 = 6;
    const WDRC_BACKOFF_CAP_SECS: u64 = 30;
    /// Sentinel HTML produced by the WDRC frontend when rate-limiting.
    /// We match on the title string rather than HTTP 429 because the
    /// reverse proxy in front of WDRC returns 200 + this body.
    const WDRC_RATE_LIMIT_MARKER: &str = "<head><title>429 Too Many Requests</title></head>";

    async fn get_wrdc_api_responses(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let rand = rand::random::<u32>();
        let url = format!("https://wdrc.toolforge.org/api.php?format=jsonl&{query}&random={rand}");
        let client = wikimisc::wikidata::Wikidata::new().reqwest_client()?;
        let mut backoff = std::time::Duration::from_secs(1);
        for attempt in 0..Self::WDRC_MAX_ATTEMPTS {
            let text = client.get(&url).send().await?.text().await?;
            if !text.contains(Self::WDRC_RATE_LIMIT_MARKER) {
                return Self::parse_wdrc_jsonl(&text);
            }
            if attempt + 1 == Self::WDRC_MAX_ATTEMPTS {
                return Err(anyhow!(
                    "WDRC kept returning 429 after {} attempts",
                    Self::WDRC_MAX_ATTEMPTS
                ));
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(Self::WDRC_BACKOFF_CAP_SECS));
        }
        unreachable!("loop returned or errored on every branch")
    }

    /// Parse a WDRC JSONL body. Distinguishes "no lines" (legitimate empty
    /// response) from "all lines failed to parse" (upstream sent HTML or
    /// some other non-JSONL surface that didn't trigger the 429 marker).
    /// Without this, a malformed 500 page used to silently advance the
    /// `wdrc_sync_*` KV pointer and skip the outage window.
    fn parse_wdrc_jsonl(text: &str) -> Result<Vec<serde_json::Value>> {
        let lines: Vec<&str> = text.split('\n').filter(|l| !l.trim().is_empty()).collect();
        let total = lines.len();
        let parsed: Vec<serde_json::Value> = lines
            .iter()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .collect();
        if total > 0 && parsed.is_empty() {
            // Truncate to keep the error message bounded and char-boundary
            // safe (see auth/flow.rs::truncate_for_log for the canonical
            // version; inlined here to avoid a cross-module export).
            const MAX: usize = 200;
            let mut end = text.len().min(MAX);
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
            return Err(anyhow!(
                "WDRC returned {total} non-empty lines, none parseable as JSONL: {}{}",
                &text[..end],
                if text.len() > MAX { "…" } else { "" }
            ));
        }
        Ok(parsed)
    }

    fn yesterday() -> String {
        let yesterday = chrono::Utc::now() - chrono::Duration::days(1);
        TimeStamp::datetime(&yesterday)
    }

    async fn sync_redirects(&self, app: &dyn ExternalServicesContext) -> Result<()> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_sync_redirects")
            .await?
            .unwrap_or_else(Self::yesterday);
        let mut new_ts = last_ts.to_owned();
        let mut redirects = HashMap::new();
        for j in self
            .get_wrdc_api_responses(&format!("action=redirects&since={last_ts}"))
            .await?
        {
            let ts = Self::sync_redirects_add_redirect(j, &new_ts, &mut redirects);
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

    async fn apply_deletions(&self, app: &dyn AppContext) -> Result<()> {
        let (last_ts, mut new_ts) = self.get_deletion_timestamps(app).await?;
        let deletions = self.get_deletions(&last_ts, &mut new_ts).await?;
        if !deletions.is_empty() {
            // Convert numeric Q-IDs to "Q{n}" strings for the Wikidata replica page table lookup.
            // This cross-reference ensures we only unmatch items that are confirmed still deleted,
            // preventing false positives from temporary deletions that were subsequently restored.
            // See https://codeberg.org/magnusmanske/mixnmatch/issues/124
            let qs: Vec<String> = deletions.iter().map(|n| format!("Q{n}")).collect();
            let confirmed_deleted = app.wikidata().get_deleted_items(&qs).await?;
            let confirmed_numeric: Vec<isize> = confirmed_deleted
                .iter()
                .filter_map(|q| item2numeric(q))
                .collect();
            if !confirmed_numeric.is_empty() {
                let catalog_ids = app
                    .storage()
                    .maintenance_apply_deletions(confirmed_numeric)
                    .await?;
                for catalog_id in catalog_ids {
                    let catalog = Catalog::from_id(catalog_id, app).await?;
                    let _ = catalog.refresh_overview_table(app).await;
                }
            }
        }
        app.storage()
            .set_kv_value("wdrc_apply_deletions", &new_ts)
            .await?;
        Ok(())
    }

    async fn get_deletion_timestamps(&self, app: &dyn ExternalServicesContext) -> Result<(String, String)> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_apply_deletions")
            .await?
            .unwrap_or_else(Self::yesterday);
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
                .map(item2numeric)
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

    async fn get_prop2catalog_ids(&self, app: &dyn ExternalServicesContext) -> Result<HashMap<usize, Vec<usize>>> {
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
        app_state: &dyn WikidataContext,
    ) -> Option<HashMap<String, isize>> {
        let api = app_state.wikidata().get_mw_api().await.ok()?;
        let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
        entities.load_entities(&api, &entity_ids).await.ok()?;
        let mut propval2item: HashMap<String, Vec<isize>> = HashMap::new();
        for q in entity_ids {
            let q_num = match item2numeric(&q) {
                Some(q_num) => q_num,
                None => continue,
            };
            let i = match entities.get_entity(q) {
                Some(i) => i,
                None => continue,
            };
            let prop_values = Self::sync_property_propval2item_get_prop_values(property, i);
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
        app: &dyn AppContext,
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
        let ext_ids: Vec<String> = propval2item
            .keys()
            .map(|propval| propval.to_string())
            .collect();

        let new_results = app
            .storage()
            .maintenance_sync_property(catalogs, ext_ids)
            .await?;
        for (id, ext_id, user) in new_results {
            if user.is_none() || user == Some(USER_AUTO) {
                let wd_item_q = match propval2item.get(&ext_id) {
                    Some(wd_item_q) => *wd_item_q,
                    None => continue,
                };
                let _ = Self::match_unmatched_entry(id, wd_item_q, app).await;
            }
        }
        Ok(())
    }

    pub async fn sync_properties(&self, app: &dyn AppContext) -> Result<()> {
        let last_ts = app
            .storage()
            .get_kv_value("wdrc_sync_properties")
            .await?
            .unwrap_or_else(Self::yesterday);
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
        for tmp_results in all_results.chunks(batch_size) {
            self.sync_properties_process_results(tmp_results, &prop2catalog_ids, app)
                .await?;
        }
        app.storage()
            .set_kv_value("wdrc_sync_properties", &new_ts)
            .await?;
        Ok(())
    }

    async fn sync_properties_process_results(
        &self,
        results: &[(usize, usize)],
        prop2catalog_ids: &HashMap<usize, Vec<usize>>,
        app: &dyn AppContext,
    ) -> Result<()> {
        let results = results.to_vec();
        let properties = results
            .iter()
            .map(|(_item, property)| *property)
            .sorted()
            .dedup()
            .collect_vec();
        let futures = properties
            .iter()
            .map(|property| self.sync_property(*property, &results, prop2catalog_ids, app))
            .collect_vec();
        let new_results = join_all(futures).await;
        let failed = new_results.into_iter().filter(|r| r.is_err()).collect_vec();
        if let Some(Err(e)) = failed.first() {
            return Err(anyhow!("{e}"));
        }
        Ok(())
    }

    pub async fn sync(&self, app: &dyn AppContext) -> Result<()> {
        self.sync_redirects(app).await?;
        self.apply_deletions(app).await?;
        self.sync_properties(app).await?;
        Ok(())
    }

    async fn match_unmatched_entry(
        entry_id: usize,
        wd_item_q: isize,
        app: &dyn AppContext,
    ) -> Result<()> {
        let mut entry = Entry::from_id(entry_id, app).await?;
        if !entry.is_fully_matched() {
            EntryWriter::new(app, &mut entry)
                .set_match(&format!("Q{wd_item_q}"), USER_AUX_MATCH)
                .await?;
            // println!("P{property}: {} => {}",entry.get_entry_url().unwrap_or("".into()),entry.get_item_url().unwrap_or("".into()));
        }
        Ok(())
    }

    fn sync_redirects_add_redirect(
        j: Value,
        new_ts: &str,
        redirects: &mut HashMap<isize, isize>,
    ) -> String {
        let from = j["item"]
            .as_str()
            .map(item2numeric)
            .and_then(|i| i)
            .unwrap_or(0);
        let to = j["target"]
            .as_str()
            .map(item2numeric)
            .and_then(|i| i)
            .unwrap_or(0);
        let ts = j["timestamp"].as_str().unwrap_or(new_ts).to_string();
        redirects.insert(from, to);
        ts
    }

    fn sync_property_propval2item_get_prop_values(
        property: usize,
        i: wikimisc::wikibase::Entity,
    ) -> Vec<String> {
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
        prop_values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wdrc_jsonl_empty_input_returns_empty_vec() {
        let out = WDRC::parse_wdrc_jsonl("").unwrap();
        assert!(out.is_empty(), "empty input must be Ok([])");
    }

    #[test]
    fn parse_wdrc_jsonl_only_whitespace_returns_empty_vec() {
        let out = WDRC::parse_wdrc_jsonl("\n\n   \n").unwrap();
        assert!(out.is_empty(), "whitespace-only must be Ok([])");
    }

    #[test]
    fn parse_wdrc_jsonl_valid_lines() {
        let body = r#"{"a":1}
{"b":2}
{"c":3}"#;
        let out = WDRC::parse_wdrc_jsonl(body).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["a"], 1);
        assert_eq!(out[2]["c"], 3);
    }

    #[test]
    fn parse_wdrc_jsonl_mixes_valid_and_invalid() {
        // Half-and-half: invalid lines are dropped, valid lines kept.
        // total>0 && parsed not empty → Ok.
        let body = "not json\n{\"a\":1}\nalso not json\n{\"b\":2}\n";
        let out = WDRC::parse_wdrc_jsonl(body).unwrap();
        assert_eq!(out.len(), 2);
    }

    /// Regression for F-16: a 500 HTML page from WDRC that didn't trigger
    /// the 429 marker used to silently return Ok([]) — the same surface as
    /// a legitimate empty response. The wdrc_sync_* KV pointer would
    /// advance past the outage window, skipping real edits.
    #[test]
    fn parse_wdrc_jsonl_all_invalid_returns_err() {
        let body = "<html><body>500 Internal Server Error</body></html>";
        let err = WDRC::parse_wdrc_jsonl(body)
            .expect_err("non-empty body where every line is unparseable must Err");
        let msg = err.to_string();
        assert!(msg.contains("none parseable"), "msg={msg}");
        // Ensure the truncated body appears for operator debugging.
        assert!(msg.contains("html"), "msg={msg}");
    }

    /// Sanity-check the retry bounds. The job-level budget is 1h for
    /// wdrc_sync (per `job.rs` ACTION_TIMEOUTS_SECS), so the WDRC backoff
    /// total must stay well below that.
    #[test]
    fn wdrc_retry_budget_is_bounded() {
        let attempts = WDRC::WDRC_MAX_ATTEMPTS;
        assert!(
            (3..=10).contains(&attempts),
            "WDRC_MAX_ATTEMPTS={attempts} outside sane window [3, 10]"
        );
        let cap = WDRC::WDRC_BACKOFF_CAP_SECS;
        assert!(cap <= 60, "WDRC_BACKOFF_CAP_SECS={cap} too large");
    }
}
