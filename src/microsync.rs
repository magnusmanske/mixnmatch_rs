use mysql_async::prelude::*;
use mysql_async::from_row;
use std::error::Error;
use std::fmt;
use crate::app_state::*;
use crate::catalog::*;
use crate::mixnmatch::*;
//use crate::entry::*;
use crate::job::*;

const BLACKLISTED_CATALOGS: &'static [usize] = &[
    506
];


#[derive(Debug)]
pub enum MicrosyncError {
    UnsuitableCatalogProperty
}

impl Error for MicrosyncError {}

impl fmt::Display for MicrosyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}


pub struct Microsync {
    mnm: MixNMatch,
    job: Option<Job>
}

impl Jobbable for Microsync {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}


impl Microsync {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
            job: None
        }
    }

    pub async fn check_catalog(&self, catalog_id: usize) -> Result<(),GenericError> {
        if BLACKLISTED_CATALOGS.contains(&catalog_id) {
            return Ok(()) // TODO error?
        }
        let catalog = Catalog::from_id(catalog_id,&self.mnm).await?;
        let property = match (catalog.wd_prop,catalog.wd_qual) {
            (Some(prop),None) => prop,
            _ => return Err(Box::new(MicrosyncError::UnsuitableCatalogProperty))
        };
        self.fix_redirects(catalog_id).await?;
        self.fix_deleted_items(catalog_id).await?;

        let multiple_extid_in_wikidata = self.get_multiple_extid_in_wikidata(property).await?;
        let multiple_q_in_mnm = self.get_multiple_q_in_mnm(catalog_id).await?;
        // TODO item_differs, extid_not_in_mnm
        // TODO write wikitext to page
        Ok(())
    }

    async fn fix_redirects(&self, catalog_id: usize) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_fully_matched_items(catalog_id, offset).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
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
                        let sql = "UPDATE `entry` SET `q`=:to WHERE `catalog`=:catalog_id AND `q`=:from";
                        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {from,to,catalog_id}).await?;
                    }
                }
            }
        }
    }

    async fn fix_deleted_items(&self, catalog_id: usize) -> Result<(),GenericError> {
        todo!();
    }

    async fn get_multiple_extid_in_wikidata(&self, property: usize) -> Result<(),GenericError> {
        todo!();
    }

    async fn get_multiple_q_in_mnm(&self, catalog_id: usize) -> Result<(),GenericError> {
        todo!();
    }

    async fn get_fully_matched_items(&self, catalog_id: usize, offset: usize) -> Result<Vec<String>,GenericError> {
        let batch_size = 5000;
        let sql = format!("SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            MatchState::fully_matched().get_sql()
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

    use super::*;
    use crate::entry::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;

    // TODO test sanitize_person_name
    // TODO test simplify_person_name

    #[tokio::test]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().set_match("Q100000067", 2).await.unwrap();
        let ms = Microsync::new(&mnm);
        ms.fix_redirects(TEST_CATALOG_ID).await.unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(91013264));

    }
}