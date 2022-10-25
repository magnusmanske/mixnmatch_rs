use regex::Regex;
use reqwest;
use urlencoding::encode;
use chrono::{DateTime, Utc};
use serde_json::{Value};
use mysql_async::prelude::*;
use mysql_async::{from_row};
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
pub struct MixNMatch {
    pub app: AppState,
}

impl MixNMatch {
    pub fn new(app: AppState) -> Self {
        Self {
            app,
        }
    }

    pub async fn set_entry_match(&self, entry_id: usize, q: &str, user_id: usize) -> Result<bool,GenericError> {
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

        Ok(true)
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
/*
    #[test]
    fn test_filter_meta_items() {
        let mut items: Vec<String> = ["Q1","Q11266439","Q2"].iter().map(|s|s.to_string()).collect() ;
        MixNMatch::filter_meta_items(&mut items);
        assert_eq!(items,["Q1","Q2"]);
    }
*/
    #[test]
    fn test_get_timestamp() {
        let ts = MixNMatch::get_timestamp();
        assert_eq!(ts.len(),14);
        assert_eq!(ts.chars().next(),Some('2'));
    }
}