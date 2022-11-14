use std::sync::Mutex;
use std::collections::{HashMap, HashSet};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest;
use urlencoding::encode;
use chrono::{DateTime, Utc, NaiveDateTime};
use serde_json::{Value,json};
use mysql_async::prelude::*;
use mysql_async::from_row;
use itertools::Itertools;
use crate::app_state::*;
use crate::entry::*;
use crate::wikidata_commands::WikidataCommand;

pub const MNM_SITE_URL: &'static str = "https://mix-n-match.toolforge.org";

/// Global function for tests.
pub fn get_test_mnm() -> MixNMatch {
    let mut ret = MixNMatch::new(AppState::from_config_file("config.json").unwrap());
    ret.testing = true;
    ret
}

lazy_static! {
    pub static ref TEST_MUTEX: Mutex<bool> = Mutex::new(true); // To lock the test entry in the database
}


lazy_static! {
    static ref SANITIZE_PERSON_NAME_RES : Vec<Regex> = vec![
        Regex::new(r"^(Sir|Mme|Dr|Mother|Father)\.{0,1} ").unwrap(),
        Regex::new(r"\b[A-Z]\. /").unwrap(),
        Regex::new(r" (\&) ").unwrap(),
        Regex::new(r"\(.+?\)").unwrap(),
        Regex::new(r"\s+").unwrap(),
    ];
    static ref SIMPLIFY_PERSON_NAME_RES : Vec<Regex> = vec![
        Regex::new(r"\s*\(.*?\)\s*").unwrap(),
        Regex::new(r"[, ]+(Jr\.{0,1}|Sr\.{0,1}|PhD\.{0,1}|MD|M\.D\.)$").unwrap(),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+").unwrap(),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+").unwrap(),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+").unwrap(),
        Regex::new(r"\s*(Ritter|Freiherr)\s+").unwrap(),
        Regex::new(r"\s+").unwrap(),
    ];
    static ref SIMPLIFY_PERSON_NAME_TWO_RE : Regex = Regex::new(r"^(\S+) .*?(\S+)$").unwrap();
}

pub const Q_NA: isize = 0;
pub const Q_NOWD: isize = -1;
pub const USER_AUTO: usize = 0;
pub const USER_DATE_MATCH: usize = 3;
pub const USER_AUX_MATCH: usize = 4;
pub const WIKIDATA_API_URL: &'static str = "https://www.wikidata.org/w/api.php";
pub const META_ITEMS: &'static [&'static str] = &[
    "Q4167410", // Wikimedia disambiguation page
    "Q11266439", // Wikimedia template
    "Q4167836", // Wikimedia category
    "Q13406463", // Wikimedia list article
    "Q22808320", // Wikimedia human name disambiguation page
    "Q17362920", // Wikimedia duplicated page
    ] ;
pub const WIKIDATA_USER_AGENT: &'static str = "MixNMmatch_RS/1.0";
pub const TABLES_WITH_ENTRY_ID_FIELDS: &'static [&'static str] = &[
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
    "statement_text"
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
        Self { unmatched:true , partially_matched:false , fully_matched:false }
    }

    pub fn fully_matched() -> Self {
        Self { unmatched:false , partially_matched:false , fully_matched:true }
    }

    pub fn not_fully_matched() -> Self {
        Self { unmatched:true , partially_matched:true , fully_matched:false }
    }

    pub fn get_sql(&self) -> String {
        let mut parts = vec![] ;
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
            return "".to_string() ;
        }
        return format!(" AND ({}) ",parts.join(" OR ")) ;
    }

}

#[derive(Debug, Clone)]
pub struct MixNMatch {
    pub app: AppState,
    mw_api: Option<mediawiki::api::Api>,
    pub testing: bool
}

impl MixNMatch {
    pub fn new(app: AppState) -> Self {
        Self {
            app,
            mw_api: None,
            testing: false
        }
    }

    pub async fn get_mw_api(&self) -> Result<mediawiki::api::Api,mediawiki::media_wiki_error::MediaWikiError> {
        /*if self.mw_api.lock().unwrap().is_none() {
            let new_api = mediawiki::api::Api::new(WIKIDATA_API_URL).await?;
            *self.mw_api.lock().unwrap() = Some(new_api);
        }
        if let Some(mw_api) = (*self.mw_api.lock().unwrap()).as_ref() {
            return Ok(mw_api.clone());
        }
        panic!("No MediaWiki API created")*/
        mediawiki::api::Api::new(WIKIDATA_API_URL).await
    }
    
    /// Computes the column of the overview table that is affected, given a user ID and item ID
    pub fn get_overview_column_name_for_user_and_q(&self, user_id: &Option<usize>, q: &Option<isize> ) -> &str {
        match (user_id,q) {
            (Some(0),_) => "autoq",
            (Some(_),None) => "noq",
            (Some(_),Some(0)) => "na",
            (Some(_),Some(-1)) => "nowd",
            (Some(_),_) => "manual",
            _ => "noq"
        }
    }

    /// Updates the overview table for a catalog, given the old Entry object, and the user ID and new item.
    pub async fn update_overview_table(&self, old_entry: &Entry, user_id: Option<usize>, q: Option<isize>) -> Result<(),GenericError> {
        let add_column = self.get_overview_column_name_for_user_and_q(&user_id,&q);
        let reduce_column = self.get_overview_column_name_for_user_and_q(&old_entry.user,&old_entry.q);
        let catalog_id = old_entry.catalog ;
        let sql = format!("UPDATE overview SET {}={}+1,{}={}-1 WHERE catalog=:catalog_id",&add_column,&add_column,&reduce_column,&reduce_column) ;
        self.app.get_mnm_conn().await?.exec_drop(sql,params! {catalog_id}).await?;
        Ok(())
    }

    pub async fn refresh_overview_table(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = r"REPLACE INTO `overview` (catalog,total,noq,autoq,na,manual,nowd,multi_match,types) VALUES (
            :catalog_id,
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id),
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q` IS NULL),
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `user`=0),
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q`=0),
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q` IS NOT NULL AND `user`>0),
            (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q`=-1),
            (SELECT count(*) FROM `multi_match` WHERE `catalog`=:catalog_id),
            (SELECT group_concat(DISTINCT `type` SEPARATOR '|') FROM `entry` WHERE `catalog`=:catalog_id)
            )";
        self.app.get_mnm_conn().await?.exec_drop(sql,params! {catalog_id}).await?;
        Ok(())
    }

    /// Adds the item into a queue for reference fixer. Possibly deprecated.
    pub async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<(),GenericError>{
        self.app.get_mnm_conn().await?
            .exec_drop(r"INSERT INTO `reference_fixer` (`q`,`done`) VALUES (:q_numeric,0) ON DUPLICATE KEY UPDATE `done`=0",params! {q_numeric}).await?;
        Ok(())
    }

    /// Removes "meta items" (eg disambiguation pages) from an item list.
    /// Items are in format "Qxxx".
    pub async fn remove_meta_items(&self, items: &mut Vec<String>) -> Result<(),GenericError> {
        if items.is_empty() {
            return Ok(());
        }
        let mut sql = "SELECT DISTINCT page_title FROM page,pagelinks WHERE page_namespace=0 AND page_title IN ('".to_string() ;
        sql += &items.join("','");
        sql += "') AND pl_from=page_id AND pl_title IN ('" ;
        sql += &META_ITEMS.join("','");
        sql += "')";

        let meta_items = self.app.get_wd_conn().await?
            .exec_iter(sql, ()).await?
            .map_and_drop(from_row::<String>).await?;

        items.retain(|item| !meta_items.iter().any(|q|q==item));
        Ok(())
    }

    /// Returns the current UTF time as a timestamp, 14 char format
    pub fn get_timestamp() -> String {
        let utc: DateTime<Utc> = Utc::now();
        let ts = utc.format("%Y%m%d%H%M%S").to_string();
        return ts ;
    }

    pub fn parse_timestamp(ts: &str) -> Option<DateTime<Utc>> {
        Some(NaiveDateTime::parse_from_str(ts,"%Y%m%d%H%M%S").ok()?.and_local_timezone(Utc).unwrap()) // This unwrap() is OK!
    }

    /// Checks if the log already has a removed match for this entry.
    /// If a q_numeric item is given, and a specific one is in the log entry, it will only trigger on this combination.
    pub async fn avoid_auto_match(&self, entry_id: usize, q_numeric: Option<isize>) -> Result<bool,GenericError> {
        let mut sql = r"SELECT id FROM `log` WHERE `entry_id`=:entry_id".to_string() ;
        match q_numeric {
            Some(q) => { sql += &format!(" AND (q IS NULL OR q={})",&q) }
            None => {}
        }
        let rows = self.app.get_mnm_conn().await?
            .exec_iter(sql,params! {entry_id}).await?
            .map_and_drop(from_row::<usize>).await?;
        Ok(!rows.is_empty())
    }

    /// Converts a string like "Q12345" to the numeric 12334
    pub fn item2numeric(&self, q: &str) -> Option<isize> {
        let re = Regex::new(r"(-{0,1}\d+)").unwrap();
        for cap in re.captures_iter(q) {
            return cap[1].parse::<isize>().ok()
        }
        None
    }

    pub fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num,"?".to_string());
        placeholders.join(",")
    }

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn wd_search_with_type(&self, name: &str, type_q: &str) -> Result<Vec<String>,GenericError> {
        if name.is_empty() {
            return Ok(vec![]) ;
        }
        if type_q.is_empty() {
            return self.wd_search(&name).await ;
        }
        let mut query = format!("{} haswbstatement:P31={}",name,type_q);
        if type_q!="Q13442814" { // Exclude "scholarly article"
            query = format!("{} -haswbstatement:P31=Q13442814",query);
        }
        let meta_items:Vec<String> = META_ITEMS.iter().map(|q|format!(" -haswbstatement:P31={}",q)).collect() ;
        query += &meta_items.join("");
        self.wd_search(&query).await
    }

    /// Performs a Wikidata API search for the query string. Returns item IDs matching the query.
    pub async fn wd_search(&self, query: &str) -> Result<Vec<String>,GenericError> {    
        // TODO via mw_api?
        if query.is_empty() {
            return Ok(vec![]) ;
        }
        let query = encode(&query);
        let url = format!("{}?action=query&list=search&format=json&srsearch={}",WIKIDATA_API_URL,query);
        let client = reqwest::Client::builder()
            .user_agent(WIKIDATA_USER_AGENT)
            .build()?;
        let v = client.get(url)
            //.header(reqwest::header::USER_AGENT,WIKIDATA_USER_AGENT)
            .send()
            .await?
            .json::<Value>()
            .await?;
        let v = v.as_object().ok_or("bad result")?;
        let v = v.get("query").ok_or("no key 'query'")?;
        let v = v.as_object().ok_or("not an object")?;
        let v = v.get("search").ok_or("no key 'search'")?;
        let v = v.as_array().ok_or("not an array")?;
        let ret = v.iter().filter_map(|result|{
            let result = result.as_object()?;
            let result = result.get("title")?;
            let result = result.as_str()?;
            Some(result.to_string())
        }).collect();
        Ok(ret)
    }


    pub fn sanitize_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SANITIZE_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name," ").to_string();
        }
        name.trim().to_string()
    }

    pub fn simplify_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SIMPLIFY_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name," ").to_string();
        }
        name = SIMPLIFY_PERSON_NAME_TWO_RE.replace_all(&name,"$1 $2").to_string();
        name.trim().to_string()
    }

    pub fn import_file_path(&self) -> String {
        self.app.import_file_path.to_owned()
    }

    pub async fn execute_commands(&mut self, commands: Vec<WikidataCommand> ) -> Result<(),GenericError> {
        if self.testing {
            println!("SKIPPING COMMANDS {:?}",commands);
            return Ok(())
        }
        if commands.is_empty() {
            return Ok(())
        }
        let mut item2commands: HashMap<usize,Vec<WikidataCommand>> = HashMap::new();
        for (key, group) in &commands.into_iter().group_by(|command| command.item_id) {
            item2commands.insert(key, group.collect());
        }
        
        self.api_log_in().await?;
        for (item_id,commands) in &item2commands {
            let mut comments: HashSet<String> = HashSet::new();
            let mut json = json!({});
            for command in commands {
                if let Some(comment)=&command.comment {
                    comments.insert(comment.to_owned());
                }
                command.edit_entity(&mut json);
            }
            let comment: String = comments.iter().join(";");
            
            if let Some(mw_api) = self.mw_api.as_mut() {
                let mut params: HashMap<String,String> = HashMap::new();
                params.insert("action".to_string(),"wbeditentity".to_string());
                params.insert("id".to_string(),format!("Q{}",item_id));
                params.insert("data".to_string(),json.to_string());
                params.insert("token".to_string(), mw_api.get_edit_token().await?);
                if !comment.is_empty() {
                    params.insert("summary".to_string(),comment);
                }
                match mw_api.post_query_api_json_mut(&params).await {
                    Ok(_) => {}
                    _ => {
                        println!("wbeditentiry failed for Q{}: {:?}",item_id,commands);
                    }
                }
            }
        }

        Ok(())
    }

    async fn api_log_in(&mut self) -> Result<(),GenericError> {
        if self.mw_api.is_none() {
            self.mw_api = Some(self.get_mw_api().await?);
        }
        let mw_api = self.mw_api.as_mut().unwrap();
        if mw_api.user().logged_in() { // Already logged in
            return Ok(());
        }
        mw_api.login(self.app.bot_name.to_owned(), self.app.bot_password.to_owned()).await?;
        Ok(())
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;

    // TODO test sanitize_person_name
    // TODO test simplify_person_name

    #[tokio::test]
    async fn test_remove_meta_items() {
        let mnm = get_test_mnm();
        let mut items: Vec<String> = ["Q1","Q3522","Q2"].iter().map(|s|s.to_string()).collect() ;
        mnm.remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items,["Q1","Q2"]);
    }

    #[tokio::test]
    async fn test_get_overview_column_name_for_user_and_q() {
        let mnm = get_test_mnm();
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&Some(0),&None),"autoq");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&Some(2),&Some(1)),"manual");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&Some(2),&Some(0)),"na");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&Some(2),&Some(-1)),"nowd");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&Some(2),&None),"noq");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&None,&None),"noq");
        assert_eq!(mnm.get_overview_column_name_for_user_and_q(&None,&Some(1)),"noq");
    }

    #[test]
    fn test_get_timestamp() {
        let ts = MixNMatch::get_timestamp();
        assert_eq!(ts.len(),14);
        assert_eq!(ts.chars().next(),Some('2'));
    }

    #[test]
    fn test_parse_timestamp() {
        let ts = "20221027123456";
        let dt = MixNMatch::parse_timestamp(ts).unwrap();
        let ts2 = dt.format("%Y%m%d%H%M%S").to_string();
        assert_eq!(ts,ts2)
    }
    
}

unsafe impl Send for MixNMatch {}
unsafe impl Sync for MixNMatch {}
