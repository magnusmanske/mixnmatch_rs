use regex::Regex;
use reqwest;
use urlencoding::encode;
use chrono::{DateTime, Utc};
use serde_json::{Value};
use mysql_async::prelude::*;
use mysql_async::{from_row, Row};
use crate::app_state::*;

pub const USER_AUTO: usize = 0;
pub const USER_DATE_MATCH: usize = 3;
pub const USER_AUX_MATCH: usize = 4;
pub const WIKIDATA_API_URL: &'static str = "https://www.wikidata.org/w/api.php";
pub const META_ITEMS: &'static [&'static str] = &["Q4167410","Q11266439","Q4167836","Q13406463","Q22808320"] ;

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
pub struct Entry {
    pub id: usize,
    pub catalog: usize,
    pub ext_id: String,
    pub ext_url: String,
    pub ext_name: String,
    pub ext_desc: String,
    pub q: Option<isize>,
    pub user: Option<usize>,
    pub timetamp: Option<String>,
    pub random: f64,
    pub type_name: Option<String>
}

impl Entry {
    pub fn from_row(row: &Row) -> Self {
        Entry {
            id: row.get(0).unwrap(),
            catalog: row.get(1).unwrap(),
            ext_id: row.get(2).unwrap(),
            ext_url: row.get(3).unwrap(),
            ext_name: row.get(4).unwrap(),
            ext_desc: row.get(5).unwrap(),
            q: row.get(6),
            user: row.get(7),
            timetamp: row.get(7),
            random: row.get(9).unwrap(),
            type_name: row.get(10).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MixNMatch {
    pub app: AppState,
}

impl MixNMatch {
    pub fn new(app: AppState) -> Self {
        Self {
            app,
        }
    }

    /// Sets the match for an entry ID, by calling set_entry_object_match.
    pub async fn set_entry_match(&self, entry_id: usize, q: &str, user_id: usize) -> Result<bool,GenericError> {
        let entry = self.get_entry_by_id(entry_id).await?;
        self.set_entry_object_match(&entry,q,user_id).await
    }

    /// Sets the match for an entry object.
    pub async fn set_entry_object_match(&self, entry: &Entry, q: &str, user_id: usize) -> Result<bool,GenericError> {
        let entry_id = entry.id;
        let q_numeric = self.item2numeric(q).ok_or(format!("'{}' is not a valid item",&q))?;
        let timestamp = Self::get_timestamp();
        let mut sql = format!("UPDATE `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id");
        if user_id==USER_AUTO {
            if self.avoid_auto_match(entry_id,Some(q_numeric)).await? {
                return Ok(false) // Nothing wrong but shouldn't be matched
            }
            sql += &MatchState::not_fully_matched().get_sql() ;
        }
        let mut conn = self.app.get_mnm_conn().await? ;
        conn.exec_drop(sql, params! {q_numeric,user_id,timestamp}).await?;
        if conn.affected_rows()==0 { // Nothing changed
            return Ok(false)
        }
        drop(conn);

        self.update_overview_table(&entry, Some(user_id), Some(q_numeric)).await?;

        let is_full_match = user_id>0 && q_numeric>0 ;
        self.set_match_status(entry_id,"UNKNOWN",is_full_match).await?;

        if user_id!=USER_AUTO {
            self.remove_multi_match(entry_id).await?;
        }

        self.queue_reference_fixer(q_numeric).await?;

        Ok(true)
    }

    pub async fn set_multi_match(&self, entry_id: usize, items: &Vec<String>) -> Result<(),GenericError> {
        let qs_numeric: Vec<String> = items.iter().filter_map(|q|self.item2numeric(q)).map(|q|q.to_string()).collect();
        if qs_numeric.len()<1 || qs_numeric.len()>10 {
            return self.remove_multi_match(entry_id).await;
        }
        let candidates = qs_numeric.join(",");
        let candidate_count = qs_numeric.len();
        let sql = r"REPLACE INTO `multi_match` (entry_id,catalog,candidates,candidate_count) VALUES (:entry_id,(SELECT catalog FROM entry WHERE id=:entry_id),:candidates,:candidates_count)";
        self.app.get_mnm_conn().await?.exec_drop(sql,params! {entry_id,candidates,candidate_count}).await?;
        Ok(())
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
        let catalog = old_entry.catalog ;
        let sql = format!("UPDATE overview SET {}={}+1,{}={}-1 WHERE catalog=:catalog",&add_column,&add_column,&reduce_column,&reduce_column) ;
        self.app.get_mnm_conn().await?.exec_drop(sql,params! {catalog}).await?;
        Ok(())
    }

    /// Returns an Entry object for a given entry ID.
    pub async fn get_entry_by_id(&self, entry_id: usize) -> Result<Entry,GenericError> {
        let rows: Vec<Entry> = self.app.get_mnm_conn().await?
            .exec_iter(r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timetamp,random,type_name FROM `entry` WHERE `id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(|row| Entry::from_row(&row)).await?;
        let ret = rows.get(0).ok_or(format!("No entry #{}",entry_id))? ;
        Ok(ret.clone())
    }

    /// Updates the entry matching status in multiple tables.
    pub async fn set_match_status(&self, entry_id: usize, status: &str, is_matched: bool) -> Result<(),GenericError>{
        let is_matched = if is_matched { 1 } else { 0 } ;
        let timestamp = Self::get_timestamp();
        let mut conn = self.app.get_mnm_conn().await?;
        conn.exec_drop(r"INSERT INTO `wd_matches` (`entry_id`,`status`,`timestamp`,`catalog`) VALUES (:entry_id,:status,:timestamp,(SELECT entry.catalog FROM entry WHERE entry.id=:entry_id)) ON DUPLICATE KEY UPDATE `status`=:status,`timestamp`=:timestamp",params! {entry_id,status,timestamp}).await?;
        conn.exec_drop(r"UPDATE person_dates SET is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE auxiliary SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE statement_text SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        Ok(())
    }

    /// Adds the item into a quere for reference fixer. Possibly deprecated.
    pub async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<(),GenericError>{
        self.app.get_mnm_conn().await?
            .exec_drop(r"INSERT INTO `reference_fixer` (`q`,`done`) VALUES (:q_numeric,0) ON DUPLICATE KEY UPDATE `done`=0",params! {q_numeric}).await?;
        Ok(())
    }

    /// Removes multi-matches for an entry, eg when the entry has been fully matched.
    pub async fn remove_multi_match(&self, entry_id: usize) -> Result<(),GenericError>{
        self.app.get_mnm_conn().await?
            .exec_drop(r"DELETE FROM multi_match WHERE entry_id=:entry_id",params! {entry_id}).await?;
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

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn wd_search_with_type(&self, name: &str, type_q: &str) -> Result<Vec<String>,GenericError> {
        if type_q=="" {
            return self.wd_search(&name).await ;
        }
        let mut query = format!("{} haswbstatement:P31={}",name,type_q);
        if type_q!="Q13442814" { // Exclude "scholarly article"
            query = format!("{} -haswbstatement:P31=Q13442814",query);
        }
        let meta_items:Vec<String> = META_ITEMS.iter().map(|q|format!(" -haswbstatement:P31={}",q)).collect() ;
        query += &meta_items.join("");
        println!("{}",query);
        self.wd_search(&query).await
    }

    /// Performs a Wikidata API search for the query string.
    pub async fn wd_search(&self, query: &str) -> Result<Vec<String>,GenericError> {    
        let query = encode(&query);
        let url = format!("{}?action=query&list=search&format=json&srsearch={}",WIKIDATA_API_URL,query);
        let body = reqwest::get(url).await?.text().await?;
        let v: Value = serde_json::from_str(&body)?;
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


    pub async fn test_db_connection(&self) -> Result<(),GenericError> {
        let limit = 3 ;
        let rows = self.app.get_mnm_conn().await?
            .exec_iter(r"SELECT page_title,page_namespace from page LIMIT :limit",params! {limit}).await?
            .map_and_drop(from_row::<(String,i32)>).await?;
        println!("{:?}",&rows);
        Ok(())
    }

}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc;
    use static_init::dynamic;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[dynamic(drop)]
    static mut MNM_CACHE: Vec<Arc<MixNMatch>> = vec![]; // TODO Option?

    async fn get_mnm() -> Arc<MixNMatch> {
        if MNM_CACHE.read().is_empty() {
            let app = AppState::from_config_file("config.json").await.unwrap();
            let mnm = MixNMatch::new(app.clone());
            let mut lock = MNM_CACHE.write();
            lock.push(Arc::new(mnm));
        }
        MNM_CACHE.read()[0].clone()
    }

    #[tokio::test]
    async fn test_remove_meta_items() {
        let mnm = get_mnm().await;
        let mut items: Vec<String> = ["Q1","Q3522","Q2"].iter().map(|s|s.to_string()).collect() ;
        mnm.remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items,["Q1","Q2"]);
    }

    #[tokio::test]
    async fn test_get_overview_column_name_for_user_and_q() {
        let mnm = get_mnm().await;
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
}