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
    pub fn new(mnm: MixNMatch) -> Self {
        Self {
            mnm: mnm
        }
    }

    pub async fn automatch_by_search(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,(SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id) AS aliases FROM `entry` WHERE `catalog`=:catalog_id {} ORDER BY `id` LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut offset = 0 ;
        let batch_size = 5 ; // TODO
        loop {
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;

            for result in &results {
                let entry_id = result.0 ;
                let label = &result.1 ;
                let type_q = &result.2 ;
                let aliases: Vec<&str> = result.3.split("|").collect();
                let mut items = self.mnm.wd_search_with_type(label,type_q).await?;
                for alias in &aliases {
                    let mut tmp = self.mnm.wd_search_with_type(alias,type_q).await?;
                    items.append(&mut tmp);
                }
                items.sort();
                items.dedup();
                self.mnm.remove_meta_items(&mut items).await?;
                if items.is_empty() {
                    continue ;
                }
                let mut entry= Entry::from_id(entry_id, &self.mnm).await.unwrap();
                if items.len()==1 { // Single match
                    entry.set_match(&items[0],USER_AUTO).await?;
                } else { // Multi-match
                    entry.set_multi_match(&items).await?;
                }
                //println!("#{}: {} / {:?}",&entry_id,&label,&items);
            }


            if results.len()<batch_size {
                break;
            }
            offset += results.len()
        }

        Ok(())
    }

}