use crate::app_state::AppState;
use crate::entry::AuxiliaryRow;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use wikimisc::wikibase::Reference;
use wikimisc::wikibase::Snak;

pub const BLANK_CATALOG_ID: usize = 0;

#[derive(Debug, Clone, Default)]
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
    pub app: Option<AppState>,
}

impl Catalog {
    /// Returns a Catalog object for a given ID.
    pub async fn from_id(catalog_id: usize, app: &AppState) -> Result<Self> {
        let mut ret = app.storage().get_catalog_from_id(catalog_id).await?;
        ret.set_mnm(app);
        Ok(ret)
    }

    /// Returns a Catalog object for a given name.
    pub async fn from_name(name: &str, app: &AppState) -> Result<Self> {
        let mut ret = app.storage().get_catalog_from_name(name).await?;
        ret.set_mnm(app);
        Ok(ret)
    }

    pub fn new(app: &AppState) -> Self {
        Self {
            id: BLANK_CATALOG_ID,
            app: Some(app.clone()),
            ..Default::default()
        }
    }

    pub async fn create_catalog(&mut self) -> Result<()> {
        self.id = self.app()?.storage().create_catalog(self).await?;
        Ok(())
    }

    /// Returns a `HashMap` of key-value pairs for the catalog.
    pub async fn get_key_value_pairs(&self) -> Result<HashMap<String, String>> {
        self.app()?
            .storage()
            .get_catalog_key_value_pairs(self.id)
            .await
    }

    /// Sets the `MixNMatch` object. Automatically done when created via `from_id()`.
    //TODO test
    pub fn set_mnm(&mut self, app: &AppState) {
        self.app = Some(app.clone());
    }

    fn app(&self) -> Result<&AppState> {
        self.app
            .as_ref()
            .map_or_else(|| Err(anyhow!("Catalog {}: app not set", self.id)), Ok)
    }

    //TODO test
    pub async fn refresh_overview_table(&self) -> Result<()> {
        self.app()?
            .storage()
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
        match (self.wd_prop, self.wd_qual) {
            (Some(prop), None) => {
                let prop = format!("P{prop}");
                let value = AuxiliaryRow::fix_external_id(&prop, &entry.ext_id);
                let snak = Snak::new_external_id(&prop, &value);
                snaks.push(snak);
            }
            _ => {
                if !entry.ext_url.is_empty() {
                    let snak = Snak::new_string("P854", &entry.ext_url);
                    snaks.push(snak);
                }
            }
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
    pub async fn set_taxon_run(&mut self, new_taxon_run: bool) -> Result<()> {
        if self.taxon_run != new_taxon_run {
            self.app()?
                .storage()
                .set_catalog_taxon_run(self.id, new_taxon_run)
                .await?;
            self.taxon_run = new_taxon_run;
        }
        Ok(())
    }

    /// Changes the `has_person_date` field of a catalog, in both struct and database.
    ///
    /// # Returns
    ///
    /// * `Result<bool>` - A result indicating whether the `has_person_date` field was changed to "yes".
    pub async fn check_and_set_person_date(&mut self) -> Result<bool> {
        let has_new_dates = if self.has_person_date != "yes"
            && self
                .app()?
                .storage()
                .do_catalog_entries_have_person_date(self.id)
                .await?
        {
            self.set_has_person_date("yes").await?;
            true
        } else {
            false
        };
        Ok(has_new_dates)
    }

    pub async fn set_has_person_date(&mut self, new_has_person_date: &str) -> Result<()> {
        self.app()?
            .storage()
            .set_has_person_date(self.id, new_has_person_date)
            .await?;
        self.has_person_date = new_has_person_date.to_string();
        Ok(())
    }

    pub async fn number_of_entries(&self) -> Result<usize> {
        let ret = self
            .app()?
            .storage()
            .number_of_entries_in_catalog(self.id)
            .await?;
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    const TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_catalog_from_id() {
        let app = get_test_app();
        let catalog = Catalog::from_id(TEST_CATALOG_ID, &app).await.unwrap();
        assert_eq!(catalog.name.unwrap(), "TEST CATALOG");
    }
}
