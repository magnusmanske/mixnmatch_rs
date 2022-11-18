use mysql_async::prelude::*;
use mysql_async::from_row;
use crate::app_state::*;
use crate::mixnmatch::*;

pub struct Maintenance {
    mnm: MixNMatch,
}

impl Maintenance {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and replaces redirects with their targets.
    pub async fn fix_redirects(&self, catalog_id: usize, state: &MatchState) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks meta items, such as disambiguation pages.
    pub async fn unlink_meta_items(&self, catalog_id: usize, state: &MatchState) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks deleted pages
    pub async fn unlink_deleted_items(&self, catalog_id: usize, state: &MatchState) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Fixes redirected items, and unlinks deleted and meta items.
    /// This is more efficient than calling the functions individually, because it uses the same batching run.
    pub async fn fix_matched_items(&self, catalog_id: usize, state: &MatchState) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_items(catalog_id, offset, state).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Finds redirects in a batch of items, and changes MnM matches to their respective targets.
    async fn fix_redirected_items_batch(&self,unique_qs: &Vec<String>) -> Result<(),GenericError> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect` 
            WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0 
            AND `page_title` IN ({})",placeholders);
        let page2rd = self.mnm.app.get_wd_conn().await?
            .exec_iter(sql, unique_qs).await?
            .map_and_drop(from_row::<(String,String)>).await?;
        for (from,to) in &page2rd {
            if let (Some(from),Some(to)) = (self.mnm.item2numeric(from),self.mnm.item2numeric(to)) {
                if from>0 && to>0 {
                    let sql = "UPDATE `entry` SET `q`=:to WHERE `q`=:from";
                    self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {from,to}).await?;
                }
            }
        }
        Ok(())
    }

    /// Finds deleted items in a batch of items, and unlinks MnM matches to them.
    async fn unlink_deleted_items_batch(&self,unique_qs: &Vec<String>) -> Result<(),GenericError> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({})",placeholders);
        let found_items = self.mnm.app.get_wd_conn().await?
            .exec_iter(sql, unique_qs.clone()).await?
            .map_and_drop(from_row::<String>).await?;
        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q|!found_items.contains(q))
            .cloned()
            .collect();            
        self.unlink_item_matches(&not_found).await?;
        Ok(())
    }

    /// Finds meta items (disambig etc) in a batch of items, and unlinks MnM matches to them.
    async fn unlink_meta_items_batch(&self,unique_qs: &Vec<String>) -> Result<(),GenericError> {
        let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT DISTINCT page_title FROM page,pagelinks WHERE page_namespace=0 AND page_title IN ({}) AND pl_from=page_id AND pl_title IN ('{}')",&placeholders,&META_ITEMS.join("','"));
        let meta_items = self.mnm.app.get_wd_conn().await?
            .exec_iter(sql, unique_qs.clone()).await?
            .map_and_drop(from_row::<String>).await?;
        self.unlink_item_matches(&meta_items).await?;
        Ok(())
    }

    /// Unlinks MnM matches to items in a list.
    pub async fn unlink_item_matches(&self, items: &Vec<String>) -> Result<(),GenericError> {
        let items: Vec<isize> = items
            .iter()
            .filter_map(|q|self.mnm.item2numeric(q))
            .collect();

        if !items.is_empty() {
            let items: Vec<String> = items.iter().map(|q|format!("{}",q)).collect();                
            let sql = format!("UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({})",items.join(","));
            self.mnm.app.get_mnm_conn().await?.exec_drop(sql,mysql_async::Params::Empty).await?;
        }
        Ok(())
    }

    /// Retrieves a batch of (unique) Wikidata items, in a given matching state.
    async fn get_items(&self, catalog_id: usize, offset: usize, state: &MatchState) -> Result<Vec<String>,GenericError> {
        let batch_size = 5000;
        let sql = format!("SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            state.get_sql()
        ) ;
        let ret = self.mnm.app.get_mnm_conn().await?
            .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
            .map_and_drop(from_row::<usize>).await?;
        let ret = ret.iter().map(|q|format!("Q{}",q)).collect();
        Ok(ret)
    }

}


#[cfg(test)]
mod tests {
    use crate::entry::Entry;
    use super::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_unlink_meta_items() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

        // Set a match to a disambiguation item
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.set_match("Q16456", 2).await.unwrap();

        // Remove matches to disambiguation items
        let maintenance = Maintenance::new(&mnm);
        maintenance.unlink_meta_items(TEST_CATALOG_ID,&MatchState::any_matched()).await.unwrap();

        // Check that removal was successful
        assert_eq!(Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().q,None);
    }

    #[tokio::test]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().set_match("Q100000067", 2).await.unwrap();
        let ms = Maintenance::new(&mnm);
        ms.fix_redirects(TEST_CATALOG_ID,&MatchState::fully_matched()).await.unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(91013264));
    }

    #[tokio::test]
    async fn test_unlink_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().set_match("Q115205673", 2).await.unwrap();
        let ms = Maintenance::new(&mnm);
        ms.unlink_deleted_items(TEST_CATALOG_ID,&MatchState::fully_matched()).await.unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,None);
    }

}