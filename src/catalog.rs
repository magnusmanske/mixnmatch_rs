use crate::app_state::AppState;
use crate::entry::AuxiliaryRow;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use wikimisc::wikibase::Reference;
use wikimisc::wikibase::Snak;

pub type CatalogId = Option<usize>;

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    id: CatalogId,
    name: Option<String>,
    url: Option<String>,
    desc: String,
    type_name: String,
    wd_prop: Option<usize>,
    wd_qual: Option<usize>,
    search_wp: String,
    active: bool,
    owner: usize,
    note: String,
    source_item: Option<usize>,
    has_person_date: String,
    taxon_run: bool,
    app: Option<AppState>,
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
            app: Some(app.clone()),
            ..Default::default()
        }
    }

    pub fn from_mysql_row(row: &mysql_async::Row) -> Option<Self> {
        Some(Self {
            id: row.get(0)?,
            name: row.get(1)?,
            url: row.get(2)?,
            desc: row.get(3)?,
            type_name: row.get(4)?,
            wd_prop: row.get(5)?,
            wd_qual: row.get(6)?,
            search_wp: row.get(7)?,
            active: row.get(8)?,
            owner: row.get(9)?,
            note: row.get(10)?,
            source_item: row.get(11)?,
            has_person_date: row.get(12)?,
            taxon_run: row.get(13)?,
            app: None,
        })
    }

    pub async fn create_catalog(&mut self) -> Result<()> {
        self.id = Some(self.app()?.storage().create_catalog(self).await?);
        Ok(())
    }

    pub fn id(&self) -> CatalogId {
        self.id
    }

    pub fn get_valid_id(&self) -> Result<usize> {
        match self.id {
            Some(id) => Ok(id),
            None => Err(anyhow!("No catalog ID set")),
        }
    }

    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub fn set_name(&mut self, name: Option<String>) {
        self.name = name;
    }

    pub fn url(&self) -> Option<&String> {
        self.url.as_ref()
    }

    pub fn set_url(&mut self, url: Option<String>) {
        self.url = url;
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn wd_prop(&self) -> Option<usize> {
        self.wd_prop
    }

    pub fn set_wd_prop(&mut self, wd_prop: Option<usize>) {
        self.wd_prop = wd_prop;
    }

    pub fn wd_qual(&self) -> Option<usize> {
        self.wd_qual
    }

    pub fn search_wp(&self) -> &str {
        &self.search_wp
    }

    pub fn set_search_wp(&mut self, search_wp: &str) {
        self.search_wp = search_wp.to_string();
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    pub fn set_owner(&mut self, owner: usize) {
        self.owner = owner;
    }

    pub fn owner(&self) -> usize {
        self.owner
    }

    pub fn set_note(&mut self, note: &str) {
        self.note = note.to_string();
    }

    pub fn note(&self) -> &str {
        &self.note
    }

    pub fn source_item(&self) -> Option<usize> {
        self.source_item
    }

    pub fn has_person_date(&self) -> &str {
        &self.has_person_date
    }

    pub fn taxon_run(&self) -> bool {
        self.taxon_run
    }

    pub async fn delete(&mut self) -> Result<()> {
        self.app()?
            .storage()
            .delete_catalog(self.get_valid_id()?)
            .await?;
        self.id = None;
        Ok(())
    }

    /// Returns a `HashMap` of key-value pairs for the catalog.
    pub async fn get_key_value_pairs(&self) -> Result<HashMap<String, String>> {
        self.app()?
            .storage()
            .get_catalog_key_value_pairs(self.get_valid_id()?)
            .await
    }

    /// Sets the `MixNMatch` object. Automatically done when created via `from_id()`.
    //TODO test
    pub fn set_mnm(&mut self, app: &AppState) {
        self.app = Some(app.clone());
    }

    fn app(&self) -> Result<&AppState> {
        self.app.as_ref().map_or_else(
            || Err(anyhow!("Catalog {}: app not set", self.get_valid_id()?)),
            Ok,
        )
    }

    //TODO test
    pub async fn refresh_overview_table(&self) -> Result<()> {
        self.app()?
            .storage()
            .catalog_refresh_overview_table(self.get_valid_id()?)
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
                .set_catalog_taxon_run(self.get_valid_id()?, new_taxon_run)
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
                .do_catalog_entries_have_person_date(self.get_valid_id()?)
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
            .set_has_person_date(self.get_valid_id()?, new_has_person_date)
            .await?;
        self.has_person_date = new_has_person_date.to_string();
        Ok(())
    }

    pub async fn number_of_entries(&self) -> Result<usize> {
        let ret = self
            .app()?
            .storage()
            .number_of_entries_in_catalog(self.get_valid_id()?)
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
