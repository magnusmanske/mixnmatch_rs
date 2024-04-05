use crate::app_state::*;
use crate::entry::*;
use crate::error::MnMError;
use crate::wikidata_commands::WikidataCommand;
use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use mysql_async::from_row;
use mysql_async::{prelude::*, Conn};
use regex::Regex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;
use std::sync::Mutex;
use std::time::Duration;
use tempfile::tempfile;
use urlencoding::encode;
use wikibase::EntityTrait;

pub const MNM_SITE_URL: &str = "https://mix-n-match.toolforge.org";

/// Global function for tests.
pub fn get_test_mnm() -> MixNMatch {
    let mut ret =
        MixNMatch::new(AppState::from_config_file("config.json").expect("Cannot create test MnM"));
    ret.testing = true;
    ret
}

lazy_static! {
    pub static ref TEST_MUTEX: Mutex<bool> = Mutex::new(true); // To lock the test entry in the database
}

lazy_static! {
    static ref SANITIZE_PERSON_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"^(Sir|Mme|Dr|Mother|Father)\.{0,1} ").expect("Regex failure"),
        Regex::new(r"\b[A-Z]\. /").expect("Regex failure"),
        Regex::new(r" (\&) ").expect("Regex failure"),
        Regex::new(r"\(.+?\)").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_PERSON_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"\s*\(.*?\)\s*").expect("Regex failure"),
        Regex::new(r"[, ]+(Jr\.{0,1}|Sr\.{0,1}|PhD\.{0,1}|MD|M\.D\.)$").expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"\s*(Ritter|Freiherr)\s+").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_PERSON_NAME_TWO_RE: Regex =
        Regex::new(r"^(\S+) .*?(\S+)$").expect("Regex failure");
    static ref RE_ITEM2NUMERIC: Regex = Regex::new(r"(-{0,1}\d+)").expect("Regex failure");
}

pub const Q_NA: isize = 0;
pub const Q_NOWD: isize = -1;
pub const USER_AUTO: usize = 0;
pub const USER_DATE_MATCH: usize = 3;
pub const USER_AUX_MATCH: usize = 4;
pub const USER_LOCATION_MATCH: usize = 5;
pub const WIKIDATA_API_URL: &str = "https://www.wikidata.org/w/api.php";
pub const META_ITEMS: &[&str] = &[
    "Q4167410",  // Wikimedia disambiguation page
    "Q11266439", // Wikimedia template
    "Q4167836",  // Wikimedia category
    "Q13406463", // Wikimedia list article
    "Q22808320", // Wikimedia human name disambiguation page
    "Q17362920", // Wikimedia duplicated page
];
pub const WIKIDATA_USER_AGENT: &str = "MixNMmatch_RS/1.0";
pub const TABLES_WITH_ENTRY_ID_FIELDS: &[&str] = &[
    "aliases",
    "descriptions",
    "auxiliary",
    "issues",
    "kv_entry",
    "mnm_relation",
    "multi_match",
    "person_dates",
    "location",
    "log",
    "entry_creation",
    "entry2given_name",
    "statement_text",
];

#[derive(Debug, Clone)]
pub struct MatchState {
    pub unmatched: bool,
    pub partially_matched: bool,
    pub fully_matched: bool,
    // TODO N/A ?
}

impl MatchState {
    pub fn unmatched() -> Self {
        Self {
            unmatched: true,
            partially_matched: false,
            fully_matched: false,
        }
    }

    pub fn fully_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: false,
            fully_matched: true,
        }
    }

    pub fn not_fully_matched() -> Self {
        Self {
            unmatched: true,
            partially_matched: true,
            fully_matched: false,
        }
    }

    pub fn any_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: true,
            fully_matched: true,
        }
    }

    pub fn get_sql(&self) -> String {
        let mut parts = vec![];
        if self.unmatched {
            parts.push("(`q` IS NULL)")
        }
        if self.partially_matched {
            parts.push("(`q`>0 AND `user`=0)")
        }
        if self.fully_matched {
            parts.push("(`q`>0 AND `user`>0)")
        }
        if parts.is_empty() {
            return "".to_string();
        }
        format!(" AND ({}) ", parts.join(" OR "))
    }
}

#[derive(Debug, Clone)]
pub struct MixNMatch {
    pub app: AppState,
    mw_api: Option<mediawiki::api::Api>,
    pub testing: bool,
}

impl MixNMatch {
    pub fn new(app: AppState) -> Self {
        Self {
            app,
            mw_api: None,
            testing: false,
        }
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

    /// Computes the column of the overview table that is affected, given a user ID and item ID
    pub fn get_overview_column_name_for_user_and_q(
        &self,
        user_id: &Option<usize>,
        q: &Option<isize>,
    ) -> &str {
        match (user_id, q) {
            (Some(0), _) => "autoq",
            (Some(_), None) => "noq",
            (Some(_), Some(0)) => "na",
            (Some(_), Some(-1)) => "nowd",
            (Some(_), _) => "manual",
            _ => "noq",
        }
    }

    /// Updates the overview table for a catalog, given the old Entry object, and the user ID and new item.
    //TODO test
    pub async fn update_overview_table(
        &self,
        old_entry: &Entry,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()> {
        let mut conn = self.app.get_mnm_conn().await?;
        self.update_overview_table_db(old_entry, user_id, q, &mut conn)
            .await
    }

    pub async fn update_overview_table_db(
        &self,
        old_entry: &Entry,
        user_id: Option<usize>,
        q: Option<isize>,
        conn: &mut Conn,
    ) -> Result<()> {
        let add_column = self.get_overview_column_name_for_user_and_q(&user_id, &q);
        let reduce_column =
            self.get_overview_column_name_for_user_and_q(&old_entry.user, &old_entry.q);
        let catalog_id = old_entry.catalog;
        let sql = format!(
            "UPDATE overview SET {}={}+1,{}={}-1 WHERE catalog=:catalog_id",
            &add_column, &add_column, &reduce_column, &reduce_column
        );
        conn.exec_drop(sql, params! {catalog_id}).await?;
        Ok(())
    }

    /// Adds the item into a queue for reference fixer. Possibly deprecated.
    //TODO test
    pub async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<()> {
        let mut conn = self.app.get_mnm_conn().await?;
        self.queue_reference_fixer_db(q_numeric, &mut conn).await
    }

    pub async fn queue_reference_fixer_db(&self, q_numeric: isize, conn: &mut Conn) -> Result<()> {
        conn.exec_drop(r"INSERT INTO `reference_fixer` (`q`,`done`) VALUES (:q_numeric,0) ON DUPLICATE KEY UPDATE `done`=0",params! {q_numeric}).await?;
        Ok(())
    }

    /// Removes "meta items" (eg disambiguation pages) from an item list.
    /// Items are in format "Qxxx".
    pub async fn remove_meta_items(&self, items: &mut Vec<String>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        items.sort();
        items.dedup();
        let mut sql = "SELECT DISTINCT page_title FROM page,pagelinks WHERE page_namespace=0 AND page_title IN ('".to_string() ;
        sql += &items.join("','");
        sql += "') AND pl_from=page_id AND pl_title IN ('";
        sql += &META_ITEMS.join("','");
        sql += "')";

        let meta_items = self
            .app
            .get_wd_conn()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;

        items.retain(|item| !meta_items.iter().any(|q| q == item));
        Ok(())
    }

    /// Returns the current UTF time as a timestamp, 14 char format
    pub fn get_timestamp() -> String {
        Utc::now().format("%Y%m%d%H%M%S").to_string()
    }

    /// Returns the given UTF time as a timestamp, 14 char format
    pub fn get_timestamp_relative(utc: &DateTime<Utc>) -> String {
        utc.format("%Y%m%d%H%M%S").to_string()
    }

    pub fn parse_timestamp(ts: &str) -> Option<DateTime<Utc>> {
        match NaiveDateTime::parse_from_str(ts, "%Y%m%d%H%M%S")
            .ok()?
            .and_local_timezone(Utc)
        {
            chrono::offset::LocalResult::Single(d) => Some(d),
            _ => None,
        }
    }

    /// Checks if the log already has a removed match for this entry.
    /// If a q_numeric item is given, and a specific one is in the log entry, it will only trigger on this combination.
    //TODO test
    pub async fn avoid_auto_match(
        &self,
        entry_id: usize,
        q_numeric: Option<isize>,
    ) -> Result<bool> {
        let mut conn = self.app.get_mnm_conn().await?;
        self.avoid_auto_match_db(entry_id, q_numeric, &mut conn)
            .await
    }

    pub async fn avoid_auto_match_db(
        &self,
        entry_id: usize,
        q_numeric: Option<isize>,
        conn: &mut Conn,
    ) -> Result<bool> {
        let mut sql = r"SELECT id FROM `log` WHERE `entry_id`=:entry_id".to_string();
        if let Some(q) = q_numeric {
            sql += &format!(" AND (q IS NULL OR q={})", &q)
        }
        let rows = conn
            .exec_iter(sql, params! {entry_id})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(!rows.is_empty())
    }

    /// Converts a string like "Q12345" to the numeric 12334
    pub fn item2numeric(&self, q: &str) -> Option<isize> {
        RE_ITEM2NUMERIC
            .captures_iter(q)
            .next()
            .and_then(|cap| cap[1].parse::<isize>().ok())
    }

    //TODO test
    pub fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num, "?".to_string());
        placeholders.join(",")
    }

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn wd_search_with_type(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        if type_q.is_empty() {
            return self.wd_search(name).await;
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
        self.wd_search(&query).await
    }

    pub async fn wd_search_with_type_db(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        let items = if type_q.is_empty() {
            let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name GROUP BY name,q";
            self.app
                .get_wd_conn()
                .await?
                .exec_iter(sql, params! {name})
                .await?
                .map_and_drop(from_row::<String>)
                .await?
        } else {
            let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name
            AND EXISTS (SELECT * FROM page,pagelinks WHERE page_title=concat('Q',wbit_item_id) AND page_namespace=0 AND pl_from=page_id AND pl_namespace=0 AND pl_title=:type_q)
            GROUP BY name,q";
            self.app
                .get_wd_conn()
                .await?
                .exec_iter(sql, params! {name})
                .await?
                .map_and_drop(from_row::<String>)
                .await?
        };
        Ok(items)
    }

    pub fn reqwest_client_wd() -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .user_agent(WIKIDATA_USER_AGENT)
            .timeout(Duration::from_secs(15))
            .build()?)
    }

    /// Performs a Wikidata API search for the query string. Returns item IDs matching the query.
    pub async fn wd_search(&self, query: &str) -> Result<Vec<String>> {
        // TODO via mw_api?
        if query.is_empty() {
            return Ok(vec![]);
        }
        let query = encode(query);
        let url = format!(
            "{}?action=query&list=search&format=json&srsearch={}",
            WIKIDATA_API_URL, query
        );
        let v = Self::reqwest_client_wd()?
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
    pub fn sanitize_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SANITIZE_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name.trim().to_string()
    }

    //TODO test
    pub fn simplify_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SIMPLIFY_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name = SIMPLIFY_PERSON_NAME_TWO_RE
            .replace_all(&name, "$1 $2")
            .to_string();
        name.trim().to_string()
    }

    pub fn import_file_path(&self) -> String {
        self.app.import_file_path.to_owned()
    }

    //TODO test
    pub async fn get_random_active_catalog_id_with_property(&self) -> Option<usize> {
        let sql = "SELECT id FROM catalog WHERE active=1 AND wd_prop IS NOT NULL and wd_qual IS NULL ORDER by rand() LIMIT 1" ;
        let ids = self
            .app
            .get_mnm_conn()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?;
        ids.first().map(|x| x.to_owned())
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

    /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        let url = format!("https://query.wikidata.org/sparql?query={}", sparql);
        let mut f = tempfile()?;
        let mut res = Self::reqwest_client_wd()?
            .get(url)
            .header(
                reqwest::header::ACCEPT,
                reqwest::header::HeaderValue::from_str("text/csv")?,
            )
            .send()
            .await?;
        while let Some(chunk) = res.chunk().await? {
            f.write_all(chunk.as_ref())?;
        }
        f.seek(std::io::SeekFrom::Start(0))?;
        Ok(csv::ReaderBuilder::new()
            .flexible(true)
            .has_headers(true)
            .delimiter(b',')
            .from_reader(f))

        /* HOWTO use:
        let mut reader = self.mnm.load_sparql_csv(&sparql).await?;
        for result in reader.records() {
            let record = result.unwrap();
        }*/
    }

    //TODO test
    pub async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()> {
        if self.testing {
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

    pub async fn create_new_wikidata_item(&mut self, item: wikibase::ItemEntity) -> Result<String> {
        let comment = "Mix'n'match item creation (V2)".to_string();
        let json = item.to_json();
        self.api_log_in().await?;
        if let Some(mw_api) = self.mw_api.as_mut() {
            let mut params: HashMap<String, String> = HashMap::new();
            params.insert("action".to_string(), "wbeditentity".to_string());
            params.insert("new".to_string(), "item".to_string());
            params.insert("data".to_string(), json.to_string());
            params.insert("token".to_string(), mw_api.get_edit_token().await?);
            if !comment.is_empty() {
                params.insert("summary".to_string(), comment);
            }
            let res = mw_api.post_query_api_json_mut(&params).await?;
            Ok(res["entity"]["id"]
                .as_str()
                .ok_or(anyhow!("Can't get ID of new entity"))?
                .to_string())
        } else {
            Err(anyhow!("Could not get a mutable API object"))
        }
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
            .login(
                self.app.bot_name.to_owned(),
                self.app.bot_password.to_owned(),
            )
            .await?;
        Ok(())
    }

    pub async fn get_kv_value(&self, key: &str) -> Result<Option<String>> {
        let sql = r"SELECT `kv_value` FROM `kv` WHERE `kv_key`=:key";
        Ok(self
            .app
            .get_mnm_conn()
            .await?
            .exec_iter(sql, params! {key})
            .await?
            .map_and_drop(from_row::<String>)
            .await?
            .pop())
    }

    pub async fn set_kv_value(&self, key: &str, value: &str) -> Result<()> {
        let sql = r"INSERT INTO `kv` (`kv_key`,`kv_value`) VALUES (:key,:value) ON DUPLICATE KEY UPDATE `kv_value`=:value";
        self.app
            .get_mnm_conn()
            .await?
            .exec_drop(sql, params! {key,value})
            .await?;
        Ok(())
    }

    pub fn tool_root_dir() -> String {
        std::env::var("TOOL_DATA_DIR").unwrap_or("/data/project/mix-n-match".to_string())
    }

    pub fn is_on_toolforge() -> bool {
        std::path::Path::new("/etc/wmcs-project").exists()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[test]
    fn test_get_sql() {
        let ms = MatchState {
            unmatched: false,
            fully_matched: false,
            partially_matched: false,
        };
        assert_eq!(ms.get_sql().as_str(), "");
        assert_eq!(
            MatchState::unmatched().get_sql().as_str(),
            " AND ((`q` IS NULL)) "
        );
        assert_eq!(
            MatchState::fully_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`>0)) "
        );
        assert_eq!(
            MatchState::not_fully_matched().get_sql().as_str(),
            " AND ((`q` IS NULL) OR (`q`>0 AND `user`=0)) "
        );
        assert_eq!(
            MatchState::any_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`=0) OR (`q`>0 AND `user`>0)) "
        );
    }

    #[tokio::test]
    async fn test_api_log_in() {
        let mut mnm = get_test_mnm();
        mnm.api_log_in().await.unwrap();
        assert!(mnm.mw_api.unwrap().user().logged_in());
    }

    #[tokio::test]
    async fn test_remove_meta_items() {
        let mnm = get_test_mnm();
        let mut items: Vec<String> = ["Q1", "Q3522", "Q2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        mnm.remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items, ["Q1", "Q2"]);
    }

    #[tokio::test]
    async fn test_get_overview_column_name_for_user_and_q() {
        let mnm = get_test_mnm();
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&Some(0), &None),
            "autoq"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&Some(2), &Some(1)),
            "manual"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&Some(2), &Some(0)),
            "na"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&Some(2), &Some(-1)),
            "nowd"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&Some(2), &None),
            "noq"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&None, &None),
            "noq"
        );
        assert_eq!(
            mnm.get_overview_column_name_for_user_and_q(&None, &Some(1)),
            "noq"
        );
    }

    #[tokio::test]
    async fn test_load_sparql_csv() {
        let mnm = get_test_mnm();
        let sparql = "SELECT ?item ?itemLabel WHERE {?item wdt:P31 wd:Q146. SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". } }";
        let mut reader = mnm.load_sparql_csv(sparql).await.unwrap();
        assert!(reader
            .records()
            .filter_map(|r| r.ok())
            .filter_map(|x| x.get(1).map(|x| x.to_string()))
            .any(|s| s == "Dusty the Klepto Kitty"));
    }

    #[tokio::test]
    async fn test_wd_search() {
        let mnm = get_test_mnm();
        assert!(mnm.wd_search("").await.unwrap().is_empty());
        assert_eq!(
            mnm.wd_search("Magnus Manske haswbstatement:P31=Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
        assert_eq!(
            mnm.wd_search_with_type("Magnus Manske", "Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
    }

    #[test]
    fn test_get_timestamp() {
        let ts = MixNMatch::get_timestamp();
        assert_eq!(ts.len(), 14);
        assert_eq!(ts.chars().next(), Some('2'));
    }

    #[test]
    fn test_parse_timestamp() {
        let ts = "20221027123456";
        let dt = MixNMatch::parse_timestamp(ts).unwrap();
        let ts2 = dt.format("%Y%m%d%H%M%S").to_string();
        assert_eq!(ts, ts2)
    }

    #[test]
    fn test_item2numeric() {
        let mnm = get_test_mnm();
        assert_eq!(mnm.item2numeric("foobar"), None);
        assert_eq!(mnm.item2numeric("12345"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345X"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345X6"), Some(12345));
    }

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(MixNMatch::sql_placeholders(0), "".to_string());
        assert_eq!(MixNMatch::sql_placeholders(1), "?".to_string());
        assert_eq!(MixNMatch::sql_placeholders(3), "?,?,?".to_string());
    }

    #[test]
    fn test_sanitize_person_name() {
        assert_eq!(
            MixNMatch::sanitize_person_name("Sir John Doe"),
            "John Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Mme. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Mother Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Father Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
    }

    #[test]
    fn test_simplify_person_name() {
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, Jr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, Sr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, PhD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, MD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, M.D."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Sir Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Baron Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Baronesse Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Graf Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Gräfin Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Prince Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Princess Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Prof. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Rev. Jane Doe"),
            "Jane Doe".to_string()
        );
    }
}

unsafe impl Send for MixNMatch {}
unsafe impl Sync for MixNMatch {}
