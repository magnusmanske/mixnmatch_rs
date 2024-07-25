use std::collections::HashMap;

use crate::entry::AuxiliaryRow;
use crate::mixnmatch::*;
use crate::storage::Storage;
use anyhow::{anyhow, Result};
use wikimisc::wikibase::Reference;
use wikimisc::wikibase::Snak;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub id: usize,
    pub name: Option<String>,
    pub url: Option<String>,
    pub desc: String,
    pub type_name: String,
    pub wd_prop: Option<usize>,
    pub wd_qual: Option<usize>,
    pub search_wp: String,
    pub active: bool,
    pub owner: usize,
    pub note: String,
    pub source_item: Option<usize>,
    pub has_person_date: String,
    pub taxon_run: bool,
    pub mnm: Option<MixNMatch>,
}

impl Catalog {
    /// Returns a Catalog object for a given entry ID.
    pub async fn from_id(catalog_id: usize, mnm: &MixNMatch) -> Result<Self> {
        let mut ret = mnm.get_storage().get_catalog_from_id(catalog_id).await?;
        ret.set_mnm(mnm);
        Ok(ret)
    }

    /// Returns a HashMap of key-value pairs for the catalog.
    pub async fn get_key_value_pairs(&self) -> Result<HashMap<String, String>> {
        self.mnm()?
            .get_storage()
            .get_catalog_key_value_pairs(self.id)
            .await
    }

    /// Sets the MixNMatch object. Automatically done when created via from_id().
    //TODO test
    pub fn set_mnm(&mut self, mnm: &MixNMatch) {
        self.mnm = Some(mnm.clone());
    }

    fn mnm(&self) -> Result<&MixNMatch> {
        match &self.mnm {
            Some(mnm) => Ok(mnm),
            None => Err(anyhow!("Catalog {}: MnM not set", self.id)),
        }
    }

    //TODO test
    pub async fn refresh_overview_table(&self) -> Result<()> {
        self.mnm()?
            .get_storage()
            .catalog_refresh_overview_table(self.id)
            .await
    }

    pub async fn references(&self, entry: &crate::entry::Entry) -> Vec<Reference> {
        let mut snaks = vec![];
        if let Some(source_item) = self.source_item {
            let value = format!("Q{source_item}");
            let snak = Snak::new_item("P248", &value);
            snaks.push(snak);
        }
        if self.wd_prop.is_some() && self.wd_qual.is_none() {
            let prop = self.wd_prop.unwrap(); // Safe
            let prop = format!("P{prop}");
            let value = AuxiliaryRow::fix_external_id(&prop, &entry.ext_id);
            let snak = Snak::new_external_id(&prop, &value);
            snaks.push(snak);
        } else if !entry.ext_url.is_empty() {
            let snak = Snak::new_string("P854", &entry.ext_url);
            snaks.push(snak);
        }
        if let Some(ts) = entry.get_creation_time().await {
            if let Some(date) = ts.split(' ').next() {
                let time = format!("+{date}T00:00:00Z");
                let snak = Snak::new_time("P813", &time, 11);
                snaks.push(snak);
            }
        }
        if snaks.is_empty() {
            return vec![];
        }
        let reference = Reference::new(snaks);
        vec![reference]
    }

    // TODO test
    pub async fn set_taxon_run(
        &mut self,
        storage: &impl Storage,
        new_taxon_run: bool,
    ) -> Result<()> {
        if self.taxon_run != new_taxon_run {
            storage
                .set_catalog_taxon_run(self.id, new_taxon_run)
                .await?;
            self.taxon_run = new_taxon_run;
        }
        Ok(())
    }

    pub async fn number_of_entries(&self) -> Result<usize> {
        let ret = self
            .mnm()?
            .get_storage()
            .number_of_entries_in_catalog(self.id)
            .await?;
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_catalog_from_id() {
        let mnm = get_test_mnm();
        let catalog = Catalog::from_id(TEST_CATALOG_ID, &mnm).await.unwrap();
        assert_eq!(catalog.name.unwrap(), "TEST CATALOG");
    }
}
