use std::error::Error;
use std::fmt;
use rand::prelude::*;
use std::collections::HashMap;
use mysql_async::prelude::*;
use mysql_async::{Row,from_row,Value};
use wikibase::locale_string::LocaleString;
use crate::mixnmatch::*;
use crate::app_state::*;

pub const ENTRY_NEW_ID: usize = 0;

#[derive(Debug, Clone, PartialEq)]
pub struct CoordinateLocation {
    pub lat: f64,
    pub lon: f64
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct AuxiliaryRow {
    pub row_id: usize,
    pub prop_numeric: usize,
    pub value: String,
    pub in_wikidata: bool,
    pub entry_is_matched: bool
}

impl AuxiliaryRow {
    //TODO test
    pub fn from_row(row: &Row) -> Self {
        Self {
            row_id: row.get(0).unwrap(),
            prop_numeric: row.get(1).unwrap(),
            value: row.get(2).unwrap(),
            in_wikidata: row.get(3).unwrap(),
            entry_is_matched: row.get(4).unwrap(),
        }
    }
}

#[derive(Debug)]
pub enum EntryError {
    TryingToUpdateNewEntry,
    TryingToInsertExistingEntry,
    EntryInsertFailed
}

impl Error for EntryError {}

impl fmt::Display for EntryError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
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
    pub timestamp: Option<String>,
    pub random: f64,
    pub type_name: Option<String>,
    pub mnm: Option<MixNMatch>
}

impl Entry {

    //TODO test
    pub fn new_from_catalog_and_ext_id(catalog_id: usize, ext_id: &str) -> Self {
        Self {
            id: ENTRY_NEW_ID,
            catalog: catalog_id,
            ext_id: ext_id.to_string(),
            ext_url: "".to_string(),
            ext_name: "".to_string(),
            ext_desc: "".to_string(),
            q: None,
            user: None,
            timestamp: None,
            random: rand::thread_rng().gen(),
            type_name: None,
            mnm: None
        }   
    }

    /// Returns an Entry object for a given entry ID.
    //TODO test
    pub async fn from_id(entry_id: usize, mnm: &MixNMatch) -> Result<Self,GenericError> {
        let sql = r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,random,`type` FROM `entry` WHERE `id`=:entry_id";
        let mut rows: Vec<Self> = mnm.app.get_mnm_conn().await?
            .exec_iter(sql,params! {entry_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?;
        // `id` is a unique index, so there can be only zero or one row in rows.
        let mut ret = rows.pop().ok_or(format!("No entry #{}",entry_id))?.to_owned() ;
        ret.set_mnm(mnm);
        Ok(ret)
    }

    /// Returns an Entry object for a given external ID in a catalog.
    //TODO test
    pub async fn from_ext_id(catalog_id: usize, ext_id: &str, mnm: &MixNMatch) -> Result<Entry,GenericError> {
        let sql = r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,random,`type` FROM `entry` WHERE `catalog`=:catalog_id AND `ext_id`=:ext_id";
        let mut conn = mnm.app.get_mnm_conn().await? ;
        let mut rows: Vec<Entry> = conn
            .exec_iter(sql,params! {catalog_id,ext_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?;
        // `catalog`/`ext_id` comprises a unique index, so there can be only zero or one row in rows.
        let mut ret = rows.pop().ok_or(format!("No entry '{}' in catalog #{}",ext_id,catalog_id))?.to_owned() ;
        ret.set_mnm(mnm);
        Ok(ret)
    }
    
    //TODO test
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

    /// Inserts the current entry into the database. id must be ENTRY_NEW_ID.
    //TODO test
    pub async fn insert_as_new(&mut self) -> Result<(),GenericError> {
        if self.id!=ENTRY_NEW_ID {
            return Err(Box::new(EntryError::TryingToInsertExistingEntry));
        }
        let sql = "INSERT IGNORE INTO `entry` (`catalog`,`ext_id`,`ext_url`,`ext_name`,`ext_desc`,`q`,`user`,`timestamp`,`random`,`type`) VALUES (:catalog,:ext_id,:ext_url,:ext_name,:ext_desc,:q,:user,:timestamp,:random,:type_name)";
        let params = params! {
            "catalog" => self.catalog,
            "ext_id" => self.ext_id.to_owned(),
            "ext_url" => self.ext_url.to_owned(),
            "ext_name" => self.ext_name.to_owned(),
            "ext_desc" => self.ext_desc.to_owned(),
            "q" => self.q,
            "user" => self.user,
            "timestamp" => self.timestamp.to_owned(),
            "random" => self.random,
            "type_name" => self.type_name.to_owned(),
        };
        let mut conn = self.mnm()?.app.get_mnm_conn().await?;
        conn.exec_drop(sql, params).await?;
        self.id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(())
    }

    /// Deletes the entry and all of its associated data in the database. Resets the local ID to 0
    //TODO test
    pub async fn delete(&mut self) -> Result<(),GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mut conn = self.mnm()?.app.get_mnm_conn().await?;
        for table in TABLES_WITH_ENTRY_ID_FIELDS {
            let sql = format!("DELETE FROM `{}` WHERE `entry_id`=:entry_id",table);
            conn.exec_drop(sql, params! {entry_id}).await?;
        }
        let sql = "DELETE FROM `entry` WHERE `id`=:entry_id";
        conn.exec_drop(sql, params! {entry_id}).await?;
        // TODO overview table?
        self.id = ENTRY_NEW_ID;
        Ok(())
    }

    /// Helper function for from_row().
    //TODO test
    fn value2opt_string(value: mysql_async::Value) -> Result<Option<String>,GenericError> {
        match value {
            Value::Bytes(s) => Ok(Some(std::str::from_utf8(&s)?.to_owned())),
            _ => Ok(None)
        }
    }

    /// Helper function for from_row().
    //TODO test
    fn value2opt_isize(value: mysql_async::Value) -> Result<Option<isize>,GenericError> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None)
        }
    }

    /// Helper function for from_row().
    //TODO test
    fn value2opt_usize(value: mysql_async::Value) -> Result<Option<usize>,GenericError> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None)
        }
    }

    //TODO test
    pub fn get_entry_url(&self) -> Option<String> {
        if self.id==ENTRY_NEW_ID {
            None
        } else {
            Some(format!("https://mix-n-match.toolforge.org/#/entry/{}",self.id))
        }
    }

    /// Sets the MixNMatch object. Automatically done when created via from_id().
    //TODO test
    pub fn set_mnm(&mut self, mnm: &MixNMatch) {
        self.mnm = Some(mnm.clone());
    }

    /// Returns the MixNMatch object reference.
    //TODO test
    pub fn mnm(&self) -> Result<&MixNMatch,GenericError> {
        let mnm = self.mnm.as_ref().ok_or("Entry: No mnm set")?;
        Ok(mnm)
    }

    /// Updates ext_name locally and in the database
    //TODO test
    pub async fn set_ext_name(&mut self, ext_name: &str) -> Result<(),GenericError> {
        if self.ext_name!=ext_name {
            self.check_valid_id()?;
            let entry_id = self.id;
            self.ext_name = ext_name.to_string();
            let sql = "UPDATE `entry` SET `ext_name`=:ext_name WHERE `id`=:entry_id";
            self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {ext_name,entry_id}).await?;
        }
        Ok(())
    }

    //TODO test
    pub async fn set_auxiliary_in_wikidata(&self, aux_id: usize, in_wikidata: bool) -> Result<(),GenericError> {
        let sql = "UPDATE `auxiliary` SET `in_wikidata`=:in_wikidata WHERE `id`=:aux_id AND `in_wikidata`!=:in_wikidata";
        self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {in_wikidata,aux_id}).await?;
        Ok(())
    }

    /// Updates ext_desc locally and in the database
    //TODO test
    pub async fn set_ext_desc(&mut self, ext_desc: &str) -> Result<(),GenericError> {
        if self.ext_desc!=ext_desc {
            self.check_valid_id()?;
            let entry_id = self.id;
            self.ext_desc = ext_desc.to_string();
            let sql = "UPDATE `entry` SET `ext_desc`=:ext_desc WHERE `id`=:entry_id";
            self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {ext_desc,entry_id}).await?;
        }
        Ok(())
    }

    /// Updates ext_id locally and in the database
    //TODO test
    pub async fn set_ext_id(&mut self, ext_id: &str) -> Result<(),GenericError> {
        if self.ext_id!=ext_id {
            self.check_valid_id()?;
            let entry_id = self.id;
            self.ext_id = ext_id.to_string();
            let sql = "UPDATE `entry` SET `ext_id`=:ext_id WHERE `id`=:entry_id";
            self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {ext_id,entry_id}).await?;
        }
        Ok(())
    }

    /// Updates ext_url locally and in the database
    //TODO test
    pub async fn set_ext_url(&mut self, ext_url: &str) -> Result<(),GenericError> {
        if self.ext_url!=ext_url {
            self.check_valid_id()?;
            let entry_id = self.id;
            self.ext_url = ext_url.to_string();
            let sql = "UPDATE `entry` SET `ext_url`=:ext_url WHERE `id`=:entry_id";
            self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {ext_url,entry_id}).await?;
        }
        Ok(())
    }
    
    /// Updates type_name locally and in the database
    //TODO test
    pub async fn set_type_name(&mut self, type_name: Option<String>) -> Result<(),GenericError> {
        if self.type_name!=type_name {
            self.check_valid_id()?;
            let entry_id = self.id;
            self.type_name = type_name.clone();
            let sql = "UPDATE `entry` SET `type`=:type_name WHERE `id`=:entry_id";
            self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {type_name,entry_id}).await?;
        }
        Ok(())
    }

    /// Update person dates in the database, where necessary
    pub async fn set_person_dates(&self, born: &Option<String>, died: &Option<String>) -> Result<(),GenericError> {
        let (already_born,already_died) = self.get_person_dates().await?;
        if already_born!=*born || already_died!=*died {
            let entry_id = self.id;
            if born.is_none() && died.is_none() {
                let sql = "DELETE FROM `person_dates` WHERE `entry_id`=:entry_id";
                self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id}).await?;
            } else {
                let born = born.to_owned().unwrap_or("".to_string());
                let died = died.to_owned().unwrap_or("".to_string());
                let sql = "REPLACE INTO `person_dates` (`entry_id`,`born`,`died`) VALUES (:entry_id,:born,:died)";
                self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,born,died}).await?;
            }
        }
        Ok(())
    }

    /// Returns the birth and death date of a person as a tuple (born,died)
    /// Born/died are Option<String>
    pub async fn get_person_dates(&self) -> Result<(Option<String>,Option<String>),GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let mut rows: Vec<(String,String)> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT `born`,`died` FROM `person_dates` WHERE `entry_id`=:entry_id LIMIT 1",params! {entry_id}).await?
            .map_and_drop(from_row::<(String,String)>).await?;
        match rows.pop() {
            Some(bd) => {
                let born = if bd.0.is_empty() { None } else { Some(bd.0) } ;
                let died = if bd.1.is_empty() { None } else { Some(bd.1) } ;
                Ok((born,died))
            },
            None => Ok((None,None))
        }
    }

    //TODO test
    pub async fn set_language_description(&self, language: &str, text: Option<String>) -> Result<(),GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        match text {
            Some(text) => {
                let sql = "REPLACE INTO `descriptions` (`entry_id`,`language`,`label`) VALUES (:entry_id,:language,:text)";
                self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,language,text}).await?;
            }
            None => {
                let sql = "DELETE FROM `descriptions` WHERE `entry_id`=:entry_id AND `language`=:language";
                self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,language}).await?;
            }
        }
        Ok(())
    }

    /// Returns a LocaleString Vec of all aliases of the entry
    //TODO test
    pub async fn get_aliases(&self) -> Result<Vec<LocaleString>,GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let rows: Vec<(String,String)> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT `language`,`label` FROM `aliases` WHERE `entry_id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(from_row::<(String,String)>).await?;
        let mut ret: Vec<wikibase::locale_string::LocaleString> = vec![];
        rows.iter().for_each(|(k,v)|{ret.push(LocaleString::new(k,v));});
        Ok(ret)
    }

    //TODO test
    pub async fn add_alias(&self, s: &LocaleString) -> Result<(),GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let language = s.language();
        let label = s.value();
        let sql = "INSERT IGNORE INTO `aliases` (`entry_id`,`language`,`label`) VALUES (:entry_id,:language,:label)";
        self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,language,label}).await?;
        Ok(())
    }

    /// Returns a language:text HashMap of all language descriptions of the entry
    //TODO test
    pub async fn get_language_descriptions(&self) -> Result<HashMap<String,String>,GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let rows: Vec<(String,String)> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT `language`,`label` FROM `descriptions` WHERE `entry_id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(from_row::<(String,String)>).await?;
        let mut map: HashMap<String,String> = HashMap::new();
        rows.iter().for_each(|(k,v)|{map.insert(k.to_string(),v.to_string());});
        Ok(map)
    }

    //TODO test
    pub async fn set_auxiliary(&self, prop_numeric: usize, value: Option<String>) -> Result<(),GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        match value {
            Some(value) => {
                if !value.is_empty() {
                    let sql = "REPLACE INTO `auxiliary` (`entry_id`,`aux_p`,`aux_name`) VALUES (:entry_id,:prop_numeric,:value)";
                    self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,prop_numeric,value}).await?;
                }
            }
            None => {
                let sql = "DELETE FROM `auxiliary` WHERE `entry_id`=:entry_id AND `aux_p`=:prop_numeric";
                self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,prop_numeric}).await?;
            }
        }
        Ok(())
    }
    

    /// Update coordinate location in the database, where necessary
    pub async fn set_coordinate_location(&self, cl: &Option<CoordinateLocation>) -> Result<(),GenericError> {
        let existing_cl = self.get_coordinate_location().await?;
        if existing_cl!=*cl {
            let entry_id = self.id;
            match cl {
                Some(cl) => {
                    let lat = cl.lat;
                    let lon = cl.lon;
                    let sql = "REPLACE INTO `location` (`entry_id`,`lat`,`lon`) VALUES (:entry_id,:lat,:lon)";
                    self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id,lat,lon}).await?;    
                }
                None => {
                    let sql = "DELETE FROM `location` WHERE `entry_id`=:entry_id";
                    self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql, params! {entry_id}).await?;    
                }
            }
        }
        Ok(())
    }

    /// Returns the coordinate locationm or None
    pub async fn get_coordinate_location(&self) -> Result<Option<CoordinateLocation>,GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let mut rows: Vec<(f64,f64)> = mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT `lat`,`lon` FROM `location` WHERE `entry_id`=:entry_id LIMIT 1",params! {entry_id}).await?
            .map_and_drop(from_row::<(f64,f64)>).await?;
        Ok(rows.pop().map(|cl|CoordinateLocation{lat:cl.0,lon:cl.1}))
    }

    /// Returns auxiliary data for the entry
    //TODO test
    pub async fn get_aux(&self) -> Result<Vec<AuxiliaryRow>,GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        Ok(mnm.app.get_mnm_conn().await?
            .exec_iter(r"SELECT `id`,`aux_p`,`aux_name`,`in_wikidata`,`entry_is_matched` FROM `auxiliary` WHERE `entry_id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(|row| AuxiliaryRow::from_row(&row)).await?)
    }

    /// Before q query or an update to the entry in the database, checks if this is a valid entry ID (eg not a new entry)
    //TODO test
    pub fn check_valid_id(&self) -> Result<(),GenericError> {
        match self.id {
            ENTRY_NEW_ID => Err(Box::new(EntryError::TryingToUpdateNewEntry)),
            _ => Ok(())
        }
    }

    /// Sets a match for the entry, and marks the entry as matched in other tables.
    pub async fn set_match(&mut self, q: &str, user_id: usize) -> Result<bool,GenericError> {
        self.check_valid_id()?;
        let entry_id = self.id;
        let mnm = self.mnm()?;
        let q_numeric = mnm.item2numeric(q).ok_or(format!("'{}' is not a valid item",&q))?;
        let timestamp = MixNMatch::get_timestamp();
        let mut sql = format!("UPDATE `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id AND (`q` IS NULL OR `q`!=:q_numeric OR `user`!=:user_id)");
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
    //TODO test
    pub async fn set_match_status(&self, status: &str, is_matched: bool) -> Result<(),GenericError>{
        let mnm = self.mnm()?;
        let entry_id = self.id;
        let is_matched = if is_matched { 1 } else { 0 } ;
        let timestamp = MixNMatch::get_timestamp();
        let mut conn = mnm.app.get_mnm_conn().await?;
        conn.exec_drop(r"INSERT INTO `wd_matches` (`entry_id`,`status`,`timestamp`,`catalog`) VALUES (:entry_id,:status,:timestamp,(SELECT entry.catalog FROM entry WHERE entry.id=:entry_id)) ON DUPLICATE KEY UPDATE `status`=:status,`timestamp`=:timestamp",params! {entry_id,status,timestamp}).await?;
        conn.exec_drop(r"UPDATE `person_dates` SET is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE `auxiliary` SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        conn.exec_drop(r"UPDATE `statement_text` SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",params! {is_matched,entry_id}).await?;
        Ok(())
    }


    /// Retrieves the multi-matches for an entry
    //TODO test
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

    //TODO test
    pub fn is_unmatched(&self) -> bool {
        self.q.is_none()
    }

    //TODO test
    pub fn is_partially_matched(&self) -> bool {
        self.user==Some(0)
    }

    //TODO test
    pub fn is_fully_matched(&self) -> bool {
        match self.user {
            Some(user_id) => user_id>0,
            None => false
        }
    }

}


#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_person_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        let born = Some("1974-05-24".to_string());
        let died = Some("2000-01-01".to_string());
        assert_eq!(entry.get_person_dates().await.unwrap(),(born.to_owned(),died.to_owned()));

        // Remove died
        entry.set_person_dates(&born, &None).await.unwrap();
        assert_eq!(entry.get_person_dates().await.unwrap(),(born.to_owned(),None));

        // Remove born
        entry.set_person_dates(&None, &died).await.unwrap();
        assert_eq!(entry.get_person_dates().await.unwrap(),(None,died.to_owned()));

        // Remove entire row
        entry.set_person_dates(&None, &None).await.unwrap();
        assert_eq!(entry.get_person_dates().await.unwrap(),(None,None));
        
        // Set back to original and check
        entry.set_person_dates(&born, &died).await.unwrap();
        assert_eq!(entry.get_person_dates().await.unwrap(),(born.to_owned(),died.to_owned()));
    }

    #[tokio::test]
    async fn test_coordinate_location() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        let cl = CoordinateLocation { lat: 1.234, lon: -5.678 };
        assert_eq!(entry.get_coordinate_location().await.unwrap(),Some(cl.to_owned()));

        // Switch
        let cl2 = CoordinateLocation{lat:cl.lon,lon:cl.lat} ;
        entry.set_coordinate_location(&Some(cl2.to_owned())).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(),Some(cl2));

        // Remove
        entry.set_coordinate_location(&None).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(),None);
        
        // Set back to original and check
        entry.set_coordinate_location(&Some(cl.to_owned())).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(),Some(cl));
    }
    

    #[tokio::test]
    async fn test_match() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

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
        let mnm = get_test_mnm();
        let entry= Entry::from_id(102826400, &mnm).await.unwrap();
        assert_eq!("이희정",&entry.ext_name);
    }

    #[tokio::test]
    async fn test_multimatch() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.unmatch().await.unwrap();
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