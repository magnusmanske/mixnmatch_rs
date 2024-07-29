use crate::{error::MnMError, mysql_misc::MySQLMisc, wikidata_commands::WikidataCommand};
use anyhow::{anyhow, Result};
use itertools::Itertools;
use mysql_async::{from_row, futures::GetConn, prelude::*};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use urlencoding::encode;
// use wikimisc::wikibase::{EntityTrait, ItemEntity};

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

    pub fn get_conn(&self) -> GetConn {
        self.pool.get_conn()
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }

    pub async fn automatch_by_sitlinks_get_wd_matches(
        &self,
        params: Vec<String>,
        site: &String,
    ) -> Result<Vec<(usize, String)>> {
        let placeholders = Self::sql_placeholders(params.len());
        let sql = format!("SELECT `ips_item_id`,`ips_site_page` FROM `wb_items_per_site` WHERE `ips_site_id`='{}' AND `ips_site_page` IN ({})",&site,placeholders);
        let mut conn = self.get_conn().await?;
        let wd_matches = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(wd_matches)
    }

    // `items` should be a unique list of Qids
    pub async fn get_meta_items(&self, items: &Vec<String>) -> Result<Vec<String>> {
        let placeholders = Self::sql_placeholders(items.len());
        let sql = format!(
            "SELECT DISTINCT page_title AS page_title
            FROM page,pagelinks,linktarget
	        WHERE page_namespace=0
	        AND lt_namespace=0
	        AND page_title IN ({placeholders})
	        AND pl_from=page_id
	        AND lt_id=pl_target_id
	        AND lt_title IN ('{}')",
            &META_ITEMS.join("','")
        );
        let mut conn = self.get_conn().await?;
        let meta_items = conn
            .exec_iter(sql, items)
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(meta_items)
    }

    pub async fn search_with_type(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name
                AND EXISTS (SELECT * FROM page,pagelinks,linktarget WHERE page_title=concat('Q',wbit_item_id) AND page_namespace=0 AND pl_target_id=lt_id AND pl_from=page_id AND lt_namespace=0 AND lt_title=:type_q)
                GROUP BY name,q";
        let mut conn = self.get_conn().await?;
        Ok(conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    pub async fn search_without_type(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
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
        let mut conn = self.get_conn().await?;
        let found_items = conn
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q| !found_items.contains(q))
            .cloned()
            .collect();
        Ok(not_found)
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

    fn testing() -> bool {
        *crate::mixnmatch::TESTING.lock().unwrap()
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
        for (key, group) in &commands.into_iter().group_by(|command| command.item_id) {
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
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mixnmatch::get_test_mnm;

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Wikidata::sql_placeholders(0), "".to_string());
        assert_eq!(Wikidata::sql_placeholders(1), "?".to_string());
        assert_eq!(Wikidata::sql_placeholders(3), "?,?,?".to_string());
    }

    #[tokio::test]
    async fn test_api_log_in() {
        let mut mnm = get_test_mnm();
        let wd = mnm.app.wikidata_mut();
        wd.api_log_in().await.unwrap();
        assert!(wd.get_mw_api().await.unwrap().user().logged_in());
    }
}
