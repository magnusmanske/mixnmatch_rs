use mysql_async::prelude::*;
use mysql_async::{Row,from_row,Value};
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
            q: Self::value2opt_isize(row.get(6).unwrap()).unwrap(),
            user: Self::value2opt_usize(row.get(7).unwrap()).unwrap(),
            timestamp: Self::value2opt_string(row.get(8).unwrap()).unwrap(),
            random: row.get(9).unwrap(),
            type_name: Self::value2opt_string(row.get(10).unwrap()).unwrap(),
            mnm: None
        }
    }

    /// Helper function for from_row().
    fn value2opt_string(value: mysql_async::Value) -> Result<Option<String>,GenericError> {
        match value {
            Value::Bytes(s) => Ok(Some(std::str::from_utf8(&s)?.to_owned())),
            _ => Ok(None)
        }
    }

    /// Helper function for from_row().
    fn value2opt_isize(value: mysql_async::Value) -> Result<Option<isize>,GenericError> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None)
        }
    }

    /// Helper function for from_row().
    fn value2opt_usize(value: mysql_async::Value) -> Result<Option<usize>,GenericError> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None)
        }
    }

    /// Returns an Entry object for a given entry ID.
    pub async fn from_id(entry_id: usize, mnm: &MixNMatch) -> Result<Entry,GenericError> {
        let sql = r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,random,`type` FROM `entry` WHERE `id`=:entry_id";
        let mut conn = mnm.app.get_mnm_conn().await? ;
        let mut rows: Vec<Entry> = conn
            .exec_iter(sql,params! {entry_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?;
        // `id` is a unique index, so there can be only zero or one row in rows.
        let mut ret = rows.pop().ok_or(format!("No entry #{}",entry_id))?.to_owned() ;
        ret.set_mnm(mnm);
        Ok(ret)
    }

    /// Sets the MixNMatch object. Automatically done when created via from_id().
    pub fn set_mnm(&mut self, mnm: &MixNMatch) {
        self.mnm = Some(mnm.clone());
    }

    /// Returns the MixNMatch object reference.
    pub fn mnm(&self) -> Result<&MixNMatch,GenericError> {
        let mnm = self.mnm.as_ref().ok_or("Entry: No mnm set")?;
        Ok(mnm)
    }

    // Sets a match for the entry, and marks the entry as matched in other tables.
    pub async fn set_match(&mut self, q: &str, user_id: usize) -> Result<bool,GenericError> {
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let q_numeric = mnm.item2numeric(q).ok_or(format!("'{}' is not a valid item",&q))?;
        let timestamp = MixNMatch::get_timestamp();
        let mut sql = format!("UPDATE `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id AND (`q` IS NULL OR `q`!=:q_numeric)");
        if user_id==USER_AUTO {
            if mnm.avoid_auto_match(entry_id,Some(q_numeric)).await? {
                return Ok(false) // Nothing wrong but shouldn't be matched
            }
            sql += &MatchState::not_fully_matched().get_sql() ;
        }
        let preserve = (Some(user_id.clone()),Some(timestamp.clone()),Some(q_numeric.clone()));
        let mut conn = mnm.app.get_mnm_conn().await? ;
        conn.exec_drop(sql, params! {q_numeric,user_id,timestamp,entry_id}).await?;
        let nothing_changed = conn.affected_rows()==0 ;
        drop(conn);
        if nothing_changed {
            return Ok(false)
        }

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

    // Removes the current match from the entry, and marks the entry as unmatched in other tables.
    pub async fn unmatch(&mut self)  -> Result<(),GenericError>{
        let entry_id = self.id;
        let mnm = self.mnm()?;
        mnm.app.get_mnm_conn().await?
        .exec_drop(r"UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `id`=:entry_id",params! {entry_id}).await?;
        self.set_match_status("UNKNOWN",false).await?;
        self.user = None;
        self.timestamp = None;
        self.q = None;
        Ok(())
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
    use static_init::dynamic;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[dynamic(drop)]
    static mut MNM_CACHE: Option<MixNMatch> = None;

    async fn get_mnm() -> MixNMatch {
        if MNM_CACHE.read().is_none() {
            let app = AppState::from_config_file("config.json").await.unwrap();
            let mnm = MixNMatch::new(app.clone());
            (*MNM_CACHE.write()) = Some(mnm);
        }
        MNM_CACHE.read().as_ref().map(|s| s.clone()).unwrap().clone()
    }

    #[tokio::test]
    async fn test_match() {
        let mnm = get_mnm().await;

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().unmatch().await.unwrap();

        // Check if clear
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());

        // Set and check in-memory changes
        entry.set_match("Q1",4).await.unwrap();
        assert_eq!(entry.q,Some(1));
        assert_eq!(entry.user,Some(4));
        assert!(!entry.timestamp.is_none());

        // Check in-database changes
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(1));
        assert_eq!(entry.user,Some(4));
        assert!(!entry.timestamp.is_none());

        // Clear and check in-memory changes
        entry.unmatch().await.unwrap();
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());

        // Check in-database changes
        let entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());

    }

    #[tokio::test]
    async fn test_utf8() {
        println!("0");
        let mnm = get_mnm().await;
        println!("1");
        let entry= Entry::from_id(102826400, &mnm).await.unwrap();
        assert_eq!("이희정",&entry.ext_name);
    }

    #[tokio::test]
    async fn test_multimatch() {
        println!("0");
        let mnm = get_mnm().await;
        println!("1");
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        println!("2");
        entry.unmatch().await.unwrap();
        println!("3");
        let items: Vec<String> = ["Q1","Q23456","Q7"].iter().map(|s|s.to_string()).collect();
        entry.set_multi_match(&items).await.unwrap();
        println!("4");
        let result = entry.get_multi_match().await.unwrap();
        println!("5");
        assert_eq!(result,items);
        entry.remove_multi_match().await.unwrap();
        println!("6");
        let result = entry.get_multi_match().await.unwrap();
        println!("7");
        let empty: Vec<String> = vec![];
        assert_eq!(result,empty);
    }

}