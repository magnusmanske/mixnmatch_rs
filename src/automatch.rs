use mysql_async::prelude::*;
use mysql_async::from_row;
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::entry::*;

#[derive(Debug, Clone)]
pub struct AutoMatch {
    mnm: MixNMatch
}

impl AutoMatch {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone()
        }
    }

    pub async fn automatch_by_search(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,
            IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases` 
            FROM `entry` WHERE `catalog`=:catalog_id {} 
            ORDER BY `id` LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut offset = 0 ;
        let batch_size = 5000 ;
        loop {
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;

            for result in &results {
                let entry_id = result.0 ;
                let label = &result.1 ;
                let type_q = &result.2 ;
                let aliases: Vec<&str> = result.3.split("|").collect();
                let mut items = match self.mnm.wd_search_with_type(label,type_q).await {
                    Ok(items) => items,
                    _ => continue // Ignore error
                } ;
                for alias in &aliases {
                    let mut tmp = match self.mnm.wd_search_with_type(alias,type_q).await {
                        Ok(tmp) => tmp,
                        _ => continue // Ignore error
                    };
                    items.append(&mut tmp);
                }
                items.sort();
                items.dedup();
                if self.mnm.remove_meta_items(&mut items).await.is_err() {
                    continue ; // Ignore error
                }
                if items.is_empty() {
                    continue ;
                }
                let mut entry= match Entry::from_id(entry_id, &self.mnm).await {
                    Ok(entry) => entry,
                    _ => continue // Ignore error
                };
                if entry.set_match(&items[0],USER_AUTO).await.is_err() {
                    continue // Ignore error
                }
                if items.len()>1 { // Multi-match
                    let _ = entry.set_multi_match(&items).await.is_err(); // Ignore error
                }
            }

            if results.len()<batch_size {
                break;
            }
            offset += results.len()
        }

        Ok(())
    }

    pub async fn automatch_from_other_catalogs(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = "SELECT ext_name,`type`,q FROM entry 
            WHERE ext_name IN (SELECT DISTINCT ext_name FROM entry WHERE catalog=:catalog_id AND q IS NULL)
            AND q IS NOT NULL AND q > 0 AND user IS NOT NULL AND user>0
            AND catalog IN (SELECT id from catalog WHERE active=1)
            GROUP BY ext_name,type HAVING count(DISTINCT q)=1
            LIMIT :batch_size OFFSET :offset";
        let mut conn = self.mnm.app.get_mnm_conn().await?;
        let mut offset = 0 ;
        let batch_size = 5000 ;
        loop {
            let results = conn
                .exec_iter(sql.clone(),params! {catalog_id,batch_size,offset}).await?
                .map_and_drop(from_row::<(String,String,Option<usize>)>).await?;
            for result in &results {
                let ext_name = &result.0;
                let type_name = &result.1;
                let q_numeric = result.2.unwrap() as isize; // Safe unwrap
                let sql = "SELECT DISTINCT `id` FROM `entry` WHERE `catalog`=:catalog_id AND `ext_name`=:ext_name AND `type`=:type_name AND `q` IS NULL";
                let entry_ids = conn
                    .exec_iter(sql.clone(),params! {catalog_id,ext_name,type_name}).await?
                    .map_and_drop(from_row::<usize>).await?;
                if entry_ids.len()==1{
                    let q=format!("Q{}",q_numeric);
                    match Entry::from_id(entry_ids[0], &self.mnm).await?.set_match(&q,USER_AUTO).await {
                        Ok(_) => {}
                        _ => {} // Ignore error
                    }
                }
            }
            if results.len()<batch_size {
                break;
            }
            offset += results.len()
        }
        Ok(())
    }

}


#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_automatch_by_search() {
        let mnm = get_test_mnm();

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().unmatch().await.unwrap();

        // Run automatch
        let am = AutoMatch::new(&mnm);
        am.automatch_by_search(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(13520818));
        assert_eq!(entry.user,Some(0));
        
        // Clear
        entry.unmatch().await.unwrap();
    }

}