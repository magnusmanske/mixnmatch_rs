use std::error::Error;
use std::fmt;
use crate::app_state::*;
use crate::catalog::*;
use crate::mixnmatch::*;
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
        Ok(())
    }

    async fn fix_redirects(&self, catalog_id: usize) -> Result<(),GenericError> {
        todo!();
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

}