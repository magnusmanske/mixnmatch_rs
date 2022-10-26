use mysql_async::prelude::*;
use mysql_async::{Row,from_row};
use crate::mixnmatch::*;
use crate::app_state::*;

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
    pub timestamp: Option<String>,
    pub random: f64,
    pub type_name: Option<String>,
    pub mnm: Option<MixNMatch>
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
            q: row.get(6).unwrap(),
            user: row.get(7).unwrap(),
            timestamp: row.get(7).unwrap(),
            random: row.get(9).unwrap(),
            type_name: row.get(10).unwrap(),
            mnm: None
        }
    }

    /// Returns an Entry object for a given entry ID.
    pub async fn from_id(entry_id: usize, mnm: &MixNMatch) -> Result<Entry,GenericError> {
        let mut rows: Vec<Entry> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,random,`type` FROM `entry` WHERE `id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?;
        let mut ret = rows.pop().ok_or(format!("No entry #{}",entry_id))?.to_owned() ;
        ret.mnm = Some(mnm.clone());
        Ok(ret.clone())
    }

    pub fn mnm(&self) -> Result<&MixNMatch,GenericError> {
        let mnm = self.mnm.as_ref().ok_or("Entry: No mnm set")?;
        Ok(mnm)
    }

    /// Sets the match for an entry object.
    pub async fn set_match(&mut self, q: &str, user_id: usize) -> Result<bool,GenericError> {
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let q_numeric = mnm.item2numeric(q).ok_or(format!("'{}' is not a valid item",&q))?;
        let timestamp = MixNMatch::get_timestamp();
        let mut sql = format!("UPDATE `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id");
        if user_id==USER_AUTO {
            if mnm.avoid_auto_match(entry_id,Some(q_numeric)).await? {
                return Ok(false) // Nothing wrong but shouldn't be matched
            }
            sql += &MatchState::not_fully_matched().get_sql() ;
        }
        let preserve = (Some(user_id.clone()),Some(timestamp.clone()),Some(q_numeric.clone()));
        let mut conn = mnm.app.get_mnm_conn().await? ;
        conn.exec_drop(sql, params! {q_numeric,user_id,timestamp}).await?;
        if conn.affected_rows()==0 { // Nothing changed
            return Ok(false)
        }
        drop(conn);

        mnm.update_overview_table(&self, Some(user_id), Some(q_numeric)).await?;

        let is_full_match = user_id>0 && q_numeric>0 ;
        self.set_match_status("UNKNOWN",is_full_match).await?;

        if user_id!=USER_AUTO {
            self.remove_multi_match().await?;
        }

        mnm.queue_reference_fixer(q_numeric).await?;

        self.user = preserve.0;
        self.timestamp = preserve.1;
        self.q = preserve.2;

        Ok(true)
    }

    /// Updates the entry matching status in multiple tables.
    pub async fn set_match_status(&self, status: &str, is_matched: bool) -> Result<(),GenericError>{
        let mnm = self.mnm()?;
        let entry_id = self.id;
        let is_matched = if is_matched { 1 } else { 0 } ;
        let timestamp = MixNMatch::get_timestamp();
        let mut conn = mnm.app.get_mnm_conn().await?;
        conn.exec_drop(r"INSERT INTO `wd_matches` (`entry_id`,`status`,`timestamp`,`catalog`) VALUES (:entry_id,:status,:timestamp,(SELECT entry.catalog FROM entry WHERE entry.id=:entry_id)) ON DUPLICATE KEY UPDATE `status`=:status,`timestamp`=:timestamp",params! {entry_id,status,timestamp}).await?;
        conn.exec_drop(r"UPDATE person_dates SET is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE auxiliary SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE statement_text SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        Ok(())
    }


    /// Retrieves the multi-matches for an entry
    pub async fn get_multi_match(&self) ->  Result<Vec<String>,GenericError> {
        let mnm = self.mnm()?;
        let entry_id = self.id;
        let rows: Vec<String> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT candidates FROM multi_match WHERE entry_id=:entry_id",params! {entry_id}).await?
            .map_and_drop(from_row::<String>).await?;
        if rows.len()!=1 {
            Ok(vec![])
        } else {
            let ret = rows.get(0).ok_or("get_multi_match err1")?.split(",").map(|s|format!("Q{}",s)).collect();
            Ok(ret)
        }
    }

    /// Sets multi-matches for an entry
    pub async fn set_multi_match(&self, items: &Vec<String>) -> Result<(),GenericError> {
        let mnm = self.mnm()?;
        let entry_id = self.id;
        let qs_numeric: Vec<String> = items.iter().filter_map(|q|mnm.item2numeric(q)).map(|q|q.to_string()).collect();
        if qs_numeric.len()<1 || qs_numeric.len()>10 {
            return self.remove_multi_match().await;
        }
        let candidates = qs_numeric.join(",");
        let candidates_count = qs_numeric.len();
        let sql = r"REPLACE INTO `multi_match` (entry_id,catalog,candidates,candidate_count) VALUES (:entry_id,(SELECT catalog FROM entry WHERE id=:entry_id),:candidates,:candidates_count)";
        mnm.app.get_mnm_conn().await?.exec_drop(sql,params! {entry_id,candidates,candidates_count}).await?;
        Ok(())
    }

    /// Removes multi-matches for an entry, eg when the entry has been fully matched.
    pub async fn remove_multi_match(&self) -> Result<(),GenericError>{
        let mnm = self.mnm()?;
        let entry_id = self.id;
        mnm.app.get_mnm_conn().await?
            .exec_drop(r"DELETE FROM multi_match WHERE entry_id=:entry_id",params! {entry_id}).await?;
        Ok(())
    }


}


#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc;
    use static_init::dynamic;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[dynamic(drop)]
    static mut MNM_CACHE: Option<Arc<MixNMatch>> = None;

    async fn get_mnm() -> Arc<MixNMatch> {
        if MNM_CACHE.read().is_none() {
            let app = AppState::from_config_file("config.json").await.unwrap();
            let mnm = MixNMatch::new(app.clone());
            (*MNM_CACHE.write()) = Some(Arc::new(mnm));
        }
        MNM_CACHE.read().as_ref().map(|s| s.clone()).unwrap()
    }

    #[tokio::test]
    async fn test_multimatch() {
        let mnm = get_mnm().await;
        let entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        let items: Vec<String> = ["Q1","Q23456","Q7"].iter().map(|s|s.to_string()).collect();
        entry.set_multi_match(&items).await.unwrap();
        let result = entry.get_multi_match().await.unwrap();
        assert_eq!(result,items);
        entry.remove_multi_match().await.unwrap();
        let result = entry.get_multi_match().await.unwrap();
        let empty: Vec<String> = vec![];
        assert_eq!(result,empty);
    }

}