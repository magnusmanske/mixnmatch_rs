use crate::{error::MnMError, mysql_misc::MySQLMisc, wikidata_commands::WikidataCommand};
use anyhow::{anyhow, Result};
use itertools::Itertools;
use mysql_async::{from_row, prelude::*};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    time::Duration,
};
use urlencoding::encode;

pub const WIKIDATA_API_URL: &str = "https://www.wikidata.org/w/api.php";
pub const META_ITEMS: &[&str] = &[
    "Q4167410",  // Wikimedia disambiguation page
    "Q11266439", // Wikimedia template
    "Q4167836",  // Wikimedia category
    "Q13406463", // Wikimedia list article
    "Q22808320", // Wikimedia human name disambiguation page
    "Q17362920", // Wikimedia duplicated page
];

#[derive(Debug, Clone)]
pub struct Wikidata {
    pool: mysql_async::Pool,
    mw_api: Option<mediawiki::api::Api>,
    bot_name: String,
    bot_password: String,
}

impl MySQLMisc for Wikidata {
    fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }
}

impl Wikidata {
    pub fn new(config: &Value, bot_name: String, bot_password: String) -> Self {
        Self {
            pool: Self::create_pool(config),
            mw_api: None,
            bot_name,
            bot_password,
        }
    }

    fn testing() -> bool {
        *crate::app_state::TESTING.lock().unwrap()
    }

    // Database things

    pub async fn automatch_by_sitlinks_get_wd_matches(
        &self,
        params: Vec<String>,
        site: &String,
    ) -> Result<Vec<(usize, String)>> {
        let placeholders = Self::sql_placeholders(params.len());
        let sql = format!(
            "SELECT `ips_item_id`,`ips_site_page`
            FROM `wb_items_per_site`
            WHERE `ips_site_id`='{site}'
            AND `ips_site_page` IN ({placeholders})"
        );
        let mut conn = self.get_conn().await?;
        let wd_matches = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(wd_matches)
    }

    async fn get_meta_items_link_targets(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT lt_id FROM linktarget WHERE lt_namespace=0 AND lt_title IN ('{}')",
            &META_ITEMS.join("','")
        );
        let mut conn = self.get_conn().await?;
        let meta_items_link_target_ids = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<u64>)
            .await?
            .iter()
            .map(|i| format!("{i}"))
            .collect();
        Ok(meta_items_link_target_ids)
    }

    // `items` should be a unique list of Qids
    pub async fn get_meta_items(&self, items: &Vec<String>) -> Result<Vec<String>> {
        let meta_items_link_target_ids = self.get_meta_items_link_targets().await?;
        let placeholders = Self::sql_placeholders(items.len());
        let sql = format!(
            "SELECT DISTINCT page_title AS page_title
            FROM page,pagelinks,linktarget
	        WHERE page_namespace=0
	        AND lt_namespace=0
	        AND page_title IN ({placeholders})
	        AND pl_from=page_id
	        AND pl_target_id IN ({})",
            &meta_items_link_target_ids.join(",")
        );
        let mut conn = self.get_conn().await?;
        let meta_items = conn
            .exec_iter(sql, items)
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(meta_items)
    }

    pub async fn search_with_type(&self, name: &str) -> Result<Vec<String>> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q
        			FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang
           			WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name
	                AND EXISTS (SELECT * FROM page,pagelinks,linktarget WHERE page_title=concat('Q',wbit_item_id) AND page_namespace=0 AND pl_target_id=lt_id AND pl_from=page_id AND lt_namespace=0 AND lt_title=:type_q)
					GROUP BY name,q";
        let mut conn = self.get_conn().await?;
        Ok(conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    pub async fn search_without_type(&self, name: &str) -> Result<Vec<String>> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name GROUP BY name,q";
        let mut conn = self.get_conn().await?;
        Ok(conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    pub async fn get_redirected_items(
        &self,
        unique_qs: &Vec<String>,
    ) -> Result<Vec<(String, String)>> {
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect`
                WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0
                AND `page_title` IN ({})",placeholders);
        let mut conn = self.get_conn().await?;
        let page2rd = conn
            .exec_iter(sql, unique_qs)
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        Ok(page2rd)
    }

    pub async fn get_deleted_items(&self, unique_qs: &[String]) -> Result<Vec<String>> {
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!(
            "SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({})",
            placeholders
        );
        let found_items: HashSet<String> = self
            .get_conn()
            .await?
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?
            .into_iter()
            .collect();
        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q| !found_items.contains(*q))
            .cloned()
            .collect();
        Ok(not_found)
    }

    pub async fn search_db_with_type(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        let items = if type_q.is_empty() {
            self.search_without_type(name).await?
        } else {
            self.search_with_type(name).await?
        };
        Ok(items)
    }

    /// Removes "meta items" (eg disambiguation pages) from an item list.
    /// Items are in format "Qxxx".
    pub async fn remove_meta_items(&self, items: &mut Vec<String>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        items.sort();
        items.dedup();
        let meta_items = self.get_meta_items(items).await?;
        items.retain(|item| !meta_items.iter().any(|q| q == item));
        Ok(())
    }

    // API stuff

    pub fn bot_name(&self) -> &str {
        &self.bot_name
    }

    pub fn bot_password(&self) -> &str {
        &self.bot_password
    }

    pub async fn get_mw_api(
        &self,
    ) -> Result<mediawiki::api::Api, mediawiki::media_wiki_error::MediaWikiError> {
        /*if self.mw_api.lock().unwrap().is_none() {
            let new_api = mediawiki::api::Api::new(WIKIDATA_API_URL).await?;
            *self.mw_api.lock().unwrap() = Some(new_api);
        }
        if let Some(mw_api) = (*self.mw_api.lock().unwrap()).as_ref() {
            return Ok(mw_api.clone());
        }
        panic!("No MediaWiki API created")*/
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
        mediawiki::api::Api::new_from_builder(WIKIDATA_API_URL, builder).await
    }

    async fn api_log_in(&mut self) -> Result<()> {
        if self.mw_api.is_none() {
            self.mw_api = Some(self.get_mw_api().await?);
        }
        let mw_api = match self.mw_api.as_mut() {
            Some(api) => api,
            None => return Err(MnMError::ApiUnreachable.into()),
        };
        if mw_api.user().logged_in() {
            // Already logged in
            return Ok(());
        }
        mw_api
            .login(self.bot_name.to_owned(), self.bot_password.to_owned())
            .await?;
        Ok(())
    }

    /// Performs a Wikidata API search for the query string. Returns item IDs matching the query.
    pub async fn search_api(&self, query: &str) -> Result<Vec<String>> {
        self.search_with_limit(query, None).await
    }

    pub async fn search_with_limit(
        &self,
        query: &str,
        srlimit: Option<usize>,
    ) -> Result<Vec<String>> {
        // TODO via mw_api?
        if query.is_empty() {
            return Ok(vec![]);
        }
        let query = encode(query);
        let srlimit = srlimit.unwrap_or(10);
        let url = format!("{WIKIDATA_API_URL}?action=query&list=search&format=json&srsearch={query}&srlimit={srlimit}");
        let v = wikimisc::wikidata::Wikidata::new()
            .reqwest_client()?
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        let v = v.as_object().ok_or(anyhow!("bad result"))?;
        let v = v.get("query").ok_or(anyhow!("no key 'query'"))?;
        let v = v.as_object().ok_or(anyhow!("not an object"))?;
        let v = v.get("search").ok_or(anyhow!("no key 'search'"))?;
        let v = v.as_array().ok_or(anyhow!("not an array"))?;
        let ret = v
            .iter()
            .filter_map(|result| {
                let result = result.as_object()?;
                let result = result.get("title")?;
                let result = result.as_str()?;
                Some(result.to_string())
            })
            .collect();
        Ok(ret)
    }

    //TODO test
    pub async fn set_wikipage_text(
        &mut self,
        title: &str,
        wikitext: &str,
        summary: &str,
    ) -> Result<()> {
        self.api_log_in().await?;
        if let Some(mw_api) = self.mw_api.as_mut() {
            let mut params: HashMap<String, String> = HashMap::new();
            params.insert("action".to_string(), "edit".to_string());
            params.insert("title".to_string(), title.to_string());
            params.insert("summary".to_string(), summary.to_string());
            params.insert("text".to_string(), wikitext.to_string());
            params.insert("token".to_string(), mw_api.get_edit_token().await?);
            if mw_api.post_query_api_json_mut(&params).await.is_err() {
                println!("set_wikipage_text failed for [[{}]]", &title);
            }
        }
        Ok(())
    }

    //TODO test
    pub async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()> {
        if Self::testing() {
            println!("SKIPPING COMMANDS {:?}", commands);
            return Ok(());
        }
        if commands.is_empty() {
            return Ok(());
        }
        let mut item2commands: HashMap<usize, Vec<WikidataCommand>> = HashMap::new();
        for (key, group) in &commands.into_iter().chunk_by(|command| command.item_id) {
            item2commands.insert(key, group.collect());
        }

        self.api_log_in().await?;
        for (item_id, commands) in &item2commands {
            let mut comments: HashSet<String> = HashSet::new();
            let mut json = json!({});
            for command in commands {
                if let Some(comment) = &command.comment {
                    comments.insert(comment.to_owned());
                }
                command.edit_entity(&mut json);
            }
            let comment: String = comments.iter().join(";");

            if let Some(mw_api) = self.mw_api.as_mut() {
                let mut params: HashMap<String, String> = HashMap::new();
                params.insert("action".to_string(), "wbeditentity".to_string());
                params.insert("id".to_string(), format!("Q{}", item_id));
                params.insert("data".to_string(), json.to_string());
                params.insert("token".to_string(), mw_api.get_edit_token().await?);
                if !comment.is_empty() {
                    params.insert("summary".to_string(), comment);
                }
                if mw_api.post_query_api_json_mut(&params).await.is_err() {
                    println!("wbeditentiry failed for Q{}: {:?}", item_id, commands);
                }
            }
        }

        Ok(())
    }

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn search_with_type_api(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        if type_q.is_empty() {
            return self.search_with_limit(name, None).await;
        }
        let mut query = format!("{} haswbstatement:P31={}", name, type_q);
        if type_q != "Q13442814" {
            // Exclude "scholarly article"
            query = format!("{} -haswbstatement:P31=Q13442814", query);
        }
        let meta_items: Vec<String> = META_ITEMS
            .iter()
            .map(|q| format!(" -haswbstatement:P31={}", q))
            .collect();
        query += &meta_items.join("");
        self.search_with_limit(&query, None).await
    }

    /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        wikimisc::wikidata::Wikidata::new()
            .load_sparql_csv(sparql)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_wd() -> Wikidata {
        let app = crate::app_state::get_test_app();
        let wd = app.wikidata();
        wd.to_owned()
    }

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Wikidata::sql_placeholders(0), "".to_string());
        assert_eq!(Wikidata::sql_placeholders(1), "?".to_string());
        assert_eq!(Wikidata::sql_placeholders(3), "?,?,?".to_string());
    }

    #[tokio::test]
    async fn test_api_log_in() {
        let mut wd = get_test_wd();
        wd.api_log_in().await.unwrap();
        let api = wd.mw_api.as_ref().unwrap();
        assert!(api.user().logged_in());
    }

    #[tokio::test]
    async fn test_wd_search() {
        let wd = get_test_wd();
        assert!(wd.search_api("").await.unwrap().is_empty());
        assert_eq!(
            wd.search_api("Magnus Manske haswbstatement:P31=Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
        assert_eq!(
            wd.search_with_type_api("Magnus Manske", "Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
    }

    #[tokio::test]
    async fn test_remove_meta_items() {
        let wd = get_test_wd();
        let mut items: Vec<String> = ["Q1", "Q3522", "Q2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        wd.remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items, ["Q1", "Q2"]);
    }
}
