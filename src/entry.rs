use crate::app_state::{AppState, USER_AUTO};
use crate::auxiliary_data::AuxiliaryRow;
use crate::catalog::Catalog;
use crate::coordinates::CoordinateLocation;
use crate::person::Person;
use crate::person_date::PersonDate;
use crate::{DbId, ItemId, PropertyId};
use anyhow::{Result, anyhow};
use mysql_async::Value;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use wikimisc::timestamp::TimeStamp;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::locale_string::LocaleString;
use wikimisc::wikibase::{EntityTrait, ItemEntity, Reference, Snak, Statement};

pub const WESTERN_LANGUAGES: &[&str] = &["en", "de", "fr", "es", "nl", "it", "pt"];

#[derive(Debug, Clone, Copy)]
pub enum EntryError {
    TryingToUpdateNewEntry,
    TryingToInsertExistingEntry,
    EntryInsertFailed,
}

impl Error for EntryError {}

impl fmt::Display for EntryError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EntryError::TryingToUpdateNewEntry => write!(f, "EntryError::TryingToUpdateNewEntry"),
            EntryError::TryingToInsertExistingEntry => {
                write!(f, "EntryError::TryingToInsertExistingEntry")
            }
            EntryError::EntryInsertFailed => write!(f, "EntryError::EntryInsertFailed"),
        }
    }
}

pub type EntryId = Option<DbId>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Entry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: EntryId,
    pub catalog: DbId,
    pub ext_id: String,
    #[serde(default)]
    pub ext_url: String,
    pub ext_name: String,
    #[serde(default)]
    pub ext_desc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub q: Option<ItemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<DbId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub random: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip)]
    pub app: Option<AppState>,
}

impl Entry {
    /// Returns an Entry object for a given entry ID.
    //TODO test
    pub async fn from_id(entry_id: DbId, app: &AppState) -> Result<Self> {
        let mut ret = app.storage().entry_from_id(entry_id).await?;
        ret.set_app(app);
        Ok(ret)
    }

    pub fn new_from_catalog_and_ext_id(catalog_id: DbId, ext_id: &str) -> Self {
        Self {
            catalog: catalog_id,
            ext_id: ext_id.to_string(),
            random: rand::rng().random(),
            ..Default::default()
        }
    }

    /// Returns an Entry object for a given external ID in a catalog.
    //TODO test
    pub async fn from_ext_id(catalog_id: DbId, ext_id: &str, app: &AppState) -> Result<Entry> {
        let mut ret = app.storage().entry_from_ext_id(catalog_id, ext_id).await?;
        ret.set_app(app);
        Ok(ret)
    }

    pub async fn multiple_from_ids(
        entry_ids: &[DbId],
        app: &AppState,
    ) -> Result<HashMap<DbId, Self>> {
        let mut ret = app.storage().multiple_from_ids(entry_ids).await?;
        ret.iter_mut().for_each(|(_id, entry)| {
            entry.set_app(app);
        });
        Ok(ret)
    }

    /// Inserts the current entry into the database. id must be None.
    //TODO test
    pub async fn insert_as_new(&mut self) -> Result<EntryId> {
        if self.id.is_some() {
            return Err(EntryError::TryingToInsertExistingEntry.into());
        }
        self.id = self.app()?.storage().entry_insert_as_new(self).await?;
        Ok(self.id)
    }

    /// Deletes the entry and all of its associated data in the database. Resets the local ID to 0
    //TODO test
    pub async fn delete(&mut self) -> Result<()> {
        self.app()?
            .storage()
            .entry_delete(self.get_valid_id()?)
            .await?;
        // TODO overview table?
        self.id = None;
        Ok(())
    }

    /// Helper function for `from_row()`.
    //TODO test
    pub fn value2opt_string(value: mysql_async::Value) -> Result<Option<String>> {
        match value {
            Value::Bytes(s) => Ok(Some(std::str::from_utf8(&s)?.to_owned())),
            _ => Ok(None),
        }
    }

    /// Helper function for `from_row()`.
    //TODO test
    pub fn value2opt_isize(value: mysql_async::Value) -> Result<Option<isize>> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None),
        }
    }

    /// Helper function for `from_row()`.
    //TODO test
    pub fn value2opt_usize(value: mysql_async::Value) -> Result<Option<usize>> {
        match value {
            Value::Int(i) => Ok(Some(i.try_into()?)),
            _ => Ok(None),
        }
    }

    pub fn get_entry_url(&self) -> Option<String> {
        Some(format!(
            "https://mix-n-match.toolforge.org/#/entry/{}",
            self.id?
        ))
    }

    pub fn get_item_url(&self) -> Option<String> {
        self.q
            .map(|q| format!("https://www.wikidata.org/wiki/Q{q}"))
    }

    /// Sets the `AppState` object. Automatically done when created via `from_id()`.
    pub fn set_app(&mut self, app: &AppState) {
        self.app = Some(app.clone());
    }

    /// Returns the `MixNMatch` object reference.
    pub fn app(&self) -> Result<&AppState> {
        let app = self.app.as_ref().ok_or(anyhow!("Entry: No app set"))?;
        Ok(app)
    }

    pub async fn get_creation_time(&self) -> Option<String> {
        let entry_id = self.get_valid_id().ok()?;
        self.app()
            .ok()?
            .storage()
            .entry_get_creation_time(entry_id)
            .await
    }

    pub fn description(&self) -> &str {
        &self.ext_desc
    }

    /// Updates `ext_name` locally and in the database
    //TODO test
    pub async fn set_ext_name(&mut self, ext_name: &str) -> Result<()> {
        if self.ext_name != ext_name {
            self.get_valid_id()?;
            self.ext_name = ext_name.to_string();
            self.app()?
                .storage()
                .entry_set_ext_name(ext_name, self.get_valid_id()?)
                .await?;
        }
        Ok(())
    }

    //TODO test
    pub async fn set_auxiliary_in_wikidata(&self, aux_id: DbId, in_wikidata: bool) -> Result<()> {
        self.app()?
            .storage()
            .entry_set_auxiliary_in_wikidata(in_wikidata, aux_id)
            .await
    }

    pub async fn add_mnm_relation(
        &self,
        prop_numeric: PropertyId,
        target_entry_id: DbId,
    ) -> Result<()> {
        self.app()?
            .storage()
            .add_mnm_relation(self.get_valid_id()?, prop_numeric, target_entry_id)
            .await
    }

    /// Updates `ext_desc` locally and in the database
    //TODO test
    pub async fn set_ext_desc(&mut self, ext_desc: &str) -> Result<()> {
        if self.ext_desc != ext_desc {
            self.get_valid_id()?;
            self.ext_desc = ext_desc.to_string();
            self.app()?
                .storage()
                .entry_set_ext_desc(ext_desc, self.get_valid_id()?)
                .await?;
        }
        Ok(())
    }

    pub async fn add_to_item(&self, item: &mut ItemEntity) -> Result<()> {
        let catalog = Catalog::from_id(self.catalog, self.app()?).await?;
        let references = catalog.references(self).await;
        let language = catalog.search_wp().to_string();
        self.add_to_item_own_id(&catalog, &references, item);
        self.add_to_item_type(&references, item);
        self.add_to_item_name_and_aliases(&language, item).await?;
        self.add_to_item_descriptions(language, item).await?;
        self.add_to_item_coordinates(&references, item).await?;
        self.add_to_item_person_dates(&references, item).await?;
        self.add_to_item_auxiliary(references, item).await?;
        Ok(())
    }

    async fn add_to_item_auxiliary(
        &self,
        references: Vec<Reference>,
        item: &mut ItemEntity,
    ) -> Result<()> {
        let auxiliary = self.get_aux().await?;
        if !auxiliary.is_empty() {
            let api = self.app()?.wikidata().get_mw_api().await?;
            let ec = EntityContainer::new();
            let props2load: Vec<String> = auxiliary.iter().map(|a| a.prop_as_string()).collect();
            let _ = ec.load_entities(&api, &props2load).await; // Try to pre-load all properties in one query
            for aux in auxiliary {
                if let Ok(prop) = ec.load_entity(&api, aux.prop_as_string()).await {
                    if let Some(claim) = aux.get_claim_for_aux(prop, &references) {
                        Self::add_claim_or_references(item, claim);
                    }
                }
            }
        }
        Ok(())
    }

    async fn add_to_item_person_dates(
        &self,
        references: &[Reference],
        item: &mut ItemEntity,
    ) -> Result<()> {
        let (born, died) = self.get_person_dates().await?;
        if let Some(pd) = born {
            let snak = Snak::new_time("P569", &pd.to_wikidata_time(), pd.wikidata_precision());
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
        if let Some(pd) = died {
            let snak = Snak::new_time("P570", &pd.to_wikidata_time(), pd.wikidata_precision());
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
        Ok(())
    }

    async fn add_to_item_coordinates(
        &self,
        references: &[Reference],
        item: &mut ItemEntity,
    ) -> Result<()> {
        if let Some(coord) = self.get_coordinate_location().await? {
            let snak = Snak::new_coordinate("P625", coord.lat(), coord.lon());
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
        Ok(())
    }

    async fn add_to_item_descriptions(
        &self,
        language: String,
        item: &mut ItemEntity,
    ) -> Result<()> {
        let mut descriptions = self.get_language_descriptions().await?;
        if !self.ext_desc.is_empty() {
            descriptions.insert(language.to_owned(), self.ext_desc.to_owned());
        }
        for (lang, desc) in descriptions {
            if item.description_in_locale(&lang).is_none() {
                let desc = LocaleString::new(&lang, &desc);
                item.descriptions_mut().push(desc);
            }
        }
        Ok(())
    }

    async fn add_to_item_name_and_aliases(
        &self,
        language: &str,
        item: &mut ItemEntity,
    ) -> Result<()> {
        let mut aliases = self.get_aliases().await?;
        let name = &self.ext_name;
        let name = Person::sanitize_name(name);
        let locale_string = LocaleString::new(language, &name);
        let names = if self.type_name == Some("Q5".into()) && WESTERN_LANGUAGES.contains(&language)
        {
            vec![LocaleString::new("mul", &name)]
        } else {
            vec![locale_string.to_owned()]
        };
        for ls in names {
            if item.label_in_locale(ls.language()).is_none() {
                item.labels_mut().push(ls);
            } else {
                aliases.push(ls);
            }
        }

        // Aliases
        for alias in aliases {
            if !item.labels().contains(&alias) && !item.aliases().contains(&alias) {
                item.aliases_mut().push(alias);
            }
        }

        Ok(())
    }

    fn add_to_item_type(&self, references: &[Reference], item: &mut ItemEntity) {
        // Type
        if let Some(tn) = &self.type_name {
            if !tn.is_empty() {
                let snak = Snak::new_item("P31", tn);
                let claim = Statement::new_normal(snak, vec![], references.to_owned());
                Self::add_claim_or_references(item, claim);
            }
        }
    }

    fn add_to_item_own_id(
        &self,
        catalog: &Catalog,
        references: &[Reference],
        item: &mut ItemEntity,
    ) {
        if let (Some(prop), None) = (catalog.wd_prop(), catalog.wd_qual()) {
            let snak = Snak::new_external_id(&format!("P{prop}"), &self.ext_id);
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
    }

    fn add_claim_or_references(item: &mut ItemEntity, mut claim: Statement) {
        // Remove self-referencing references
        if claim
            .references()
            .iter()
            .flat_map(|r| r.snaks())
            .any(|snak| snak == claim.main_snak())
        {
            claim.set_references(vec![]);
        }

        // Check if the claim already exists in the item
        for existing_claim in item.claims_mut() {
            if existing_claim.main_snak() == claim.main_snak() {
                // Claim exists, just add references
                let mut references = existing_claim.references().to_owned();
                for reference in claim.references() {
                    if !references.contains(reference) {
                        references.push(reference.to_owned());
                    }
                }
                existing_claim.set_references(references);
                return;
            }
        }

        // Claim doesn't exist, add it
        item.add_claim(claim);
    }

    /// Updates `ext_id` locally and in the database
    //TODO test
    pub async fn set_ext_id(&mut self, ext_id: &str) -> Result<()> {
        if self.ext_id != ext_id {
            self.get_valid_id()?;
            self.ext_id = ext_id.to_string();
            self.app()?
                .storage()
                .entry_set_ext_id(ext_id, self.get_valid_id()?)
                .await?;
        }
        Ok(())
    }

    /// Updates `ext_url` locally and in the database
    //TODO test
    pub async fn set_ext_url(&mut self, ext_url: &str) -> Result<()> {
        if self.ext_url != ext_url {
            self.ext_url = ext_url.to_string();
            self.app()?
                .storage()
                .entry_set_ext_url(ext_url, self.get_valid_id()?)
                .await?;
        }
        Ok(())
    }

    /// Updates `type_name` locally and in the database
    //TODO test
    pub async fn set_type_name(&mut self, type_name: Option<String>) -> Result<()> {
        if self.type_name != type_name {
            self.type_name.clone_from(&type_name);
            self.app()?
                .storage()
                .entry_set_type_name(type_name, self.get_valid_id()?)
                .await?;
        }
        Ok(())
    }

    /// Update person dates in the database, where necessary
    pub async fn set_person_dates(
        &self,
        born: &Option<PersonDate>,
        died: &Option<PersonDate>,
    ) -> Result<()> {
        let (already_born, already_died) = self.get_person_dates().await?;
        if already_born != *born || already_died != *died {
            let entry_id = self.id.ok_or(anyhow!("Entry ID not found"))?;
            if born.is_none() && died.is_none() {
                self.app()?
                    .storage()
                    .entry_delete_person_dates(entry_id)
                    .await?;
            } else {
                let born = born.map(|d| d.to_db_string()).unwrap_or_default();
                let died = died.map(|d| d.to_db_string()).unwrap_or_default();
                self.app()?
                    .storage()
                    .entry_set_person_dates(entry_id, born, died)
                    .await?;
            }
        }
        Ok(())
    }

    /// Returns the birth and death date of a person as a tuple (born,died)
    pub async fn get_person_dates(&self) -> Result<(Option<PersonDate>, Option<PersonDate>)> {
        let entry_id = self.get_valid_id()?;
        let (born_str, died_str) = self.app()?.storage().entry_get_person_dates(entry_id).await?;
        let born = born_str.as_deref().and_then(PersonDate::from_db_string);
        let died = died_str.as_deref().and_then(PersonDate::from_db_string);
        Ok((born, died))
    }

    //TODO test
    pub async fn set_language_description(
        &self,
        language: &str,
        text: Option<String>,
    ) -> Result<()> {
        let entry_id = self.get_valid_id()?;
        match text {
            Some(text) => {
                self.app()?
                    .storage()
                    .entry_set_language_description(entry_id, language, text)
                    .await?;
            }
            None => {
                self.app()?
                    .storage()
                    .entry_remove_language_description(entry_id, language)
                    .await?;
            }
        }
        Ok(())
    }

    /// Returns a `LocaleString` Vec of all aliases of the entry
    //TODO test
    pub async fn get_aliases(&self) -> Result<Vec<LocaleString>> {
        self.app()?
            .storage()
            .entry_get_aliases(self.get_valid_id()?)
            .await
    }

    pub async fn add_alias(&self, s: &LocaleString) -> Result<()> {
        let language = s.language();
        let label = s.value();
        self.app()?
            .storage()
            .entry_add_alias(self.get_valid_id()?, language, label)
            .await?;
        Ok(())
    }

    /// Returns a language:text `HashMap` of all language descriptions of the entry
    //TODO test
    pub async fn get_language_descriptions(&self) -> Result<HashMap<String, String>> {
        self.app()?
            .storage()
            .entry_get_language_descriptions(self.get_valid_id()?)
            .await
    }

    //TODO test
    pub async fn set_auxiliary(
        &self,
        prop_numeric: PropertyId,
        value: Option<String>,
    ) -> Result<()> {
        let entry_id = self.get_valid_id()?;
        match value {
            Some(value) => {
                if !value.is_empty() {
                    self.app()?
                        .storage()
                        .entry_set_auxiliary(entry_id, prop_numeric, value)
                        .await?;
                }
            }
            None => {
                self.app()?
                    .storage()
                    .entry_remove_auxiliary(entry_id, prop_numeric)
                    .await?;
            }
        }
        Ok(())
    }

    /// Update coordinate location in the database, where necessary
    pub async fn set_coordinate_location(&self, cl: &Option<CoordinateLocation>) -> Result<()> {
        let existing_cl = self.get_coordinate_location().await?;
        if existing_cl != *cl {
            let entry_id = self.get_valid_id()?;
            match cl {
                Some(cl) => {
                    self.app()?
                        .storage()
                        .entry_set_coordinate_location(entry_id, cl.lat(), cl.lon(), cl.precision())
                        .await?;
                }
                None => {
                    self.app()?
                        .storage()
                        .entry_remove_coordinate_location(entry_id)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Returns the coordinate locationm or None
    pub async fn get_coordinate_location(&self) -> Result<Option<CoordinateLocation>> {
        self.app()?
            .storage()
            .entry_get_coordinate_location(self.get_valid_id()?)
            .await
    }

    /// Returns auxiliary data for the entry
    //TODO test
    pub async fn get_aux(&self) -> Result<Vec<AuxiliaryRow>> {
        self.app()?
            .storage()
            .entry_get_aux(self.get_valid_id()?)
            .await
    }

    /// Before q query or an update to the entry in the database, checks if this is a valid entry ID (eg not a new entry)
    pub fn get_valid_id(&self) -> Result<DbId> {
        match self.id {
            Some(id) => Ok(id),
            None => Err(anyhow!("No entry ID set")),
        }
    }

    /// Sets a match for the entry, and marks the entry as matched in other tables.
    pub async fn set_match(&mut self, q: &str, user_id: DbId) -> Result<bool> {
        self.get_valid_id()?;
        let q_numeric = AppState::item2numeric(q).ok_or(anyhow!("'{}' is not a valid item", &q))?;

        let timestamp = TimeStamp::now();
        if self
            .app()?
            .storage()
            .entry_set_match(self, user_id, q_numeric, &timestamp)
            .await?
        {
            self.user = Some(user_id);
            self.timestamp = Some(timestamp);
            self.q = Some(q_numeric);
        }

        Ok(true)
    }

    // Removes the current match from the entry, and marks the entry as unmatched in other tables.
    pub async fn unmatch(&mut self) -> Result<()> {
        self.app()?
            .storage()
            .entry_unmatch(self.get_valid_id()?)
            .await?;
        self.user = None;
        self.timestamp = None;
        self.q = None;
        Ok(())
    }

    /// Updates the entry matching status in multiple tables.
    //TODO test
    pub async fn set_match_status(&self, status: &str, is_matched: bool) -> Result<()> {
        let is_matched = if is_matched { 1 } else { 0 };
        self.app()?
            .storage()
            .entry_set_match_status(self.get_valid_id()?, status, is_matched)
            .await
    }

    /// Retrieves the multi-matches for an entry
    //TODO test
    pub async fn get_multi_match(&self) -> Result<Vec<String>> {
        let rows: Vec<String> = self
            .app()?
            .storage()
            .entry_get_multi_matches(self.get_valid_id()?)
            .await?;
        if rows.len() != 1 {
            Ok(vec![])
        } else {
            let ret = rows
                .first()
                .ok_or(anyhow!("get_multi_match err1"))?
                .split(',')
                .map(|q| format!("Q{q}"))
                .collect();
            Ok(ret)
        }
    }

    /// Sets auto-match and multi-match for an entry
    pub async fn set_auto_and_multi_match(&mut self, items: &[String]) -> Result<()> {
        let mut qs_numeric: Vec<ItemId> = items
            .iter()
            .filter_map(|q| AppState::item2numeric(q))
            .collect();
        if qs_numeric.is_empty() {
            return Ok(());
        }
        qs_numeric.sort();
        qs_numeric.dedup();
        if self.q == Some(qs_numeric[0]) {
            return Ok(()); // Automatch exists, skipping multimatch
        }
        self.set_match(&format!("Q{}", qs_numeric[0]), USER_AUTO)
            .await?;
        if qs_numeric.len() > 1 {
            self.set_multi_match(items).await?;
        }
        Ok(())
    }

    /// Sets multi-matches for an entry
    pub async fn set_multi_match(&self, items: &[String]) -> Result<()> {
        let entry_id = self.get_valid_id()?;
        let app = self.app()?;
        let qs_numeric: Vec<String> = items
            .iter()
            .filter_map(|q| AppState::item2numeric(q))
            .map(|q| q.to_string())
            .collect();
        if qs_numeric.is_empty() || qs_numeric.len() > 10 {
            return self.remove_multi_match().await;
        }
        let candidates = qs_numeric.join(",");
        let candidates_count = qs_numeric.len();

        app.storage()
            .entry_set_multi_match(entry_id, candidates, candidates_count)
            .await?;
        Ok(())
    }

    /// Removes multi-matches for an entry, eg when the entry has been fully matched.
    pub async fn remove_multi_match(&self) -> Result<()> {
        self.app()?
            .storage()
            .entry_remove_multi_match(self.get_valid_id()?)
            .await
    }

    /// Checks if the entry is unmatched
    pub const fn is_unmatched(&self) -> bool {
        self.q.is_none()
    }

    /// Checks if the entry is partially matched (q > 0 and user == 0)
    pub fn is_partially_matched(&self) -> bool {
        self.q.is_some_and(|q| q > 0) && self.user == Some(0)
    }

    /// Checks if the entry is fully matched
    pub const fn is_fully_matched(&self) -> bool {
        match self.user {
            Some(user_id) => user_id > 0,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{TEST_MUTEX, get_test_app};

    const _TEST_CATALOG_ID: DbId = 5526;
    const TEST_ENTRY_ID: DbId = 143962196;

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_person_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        let born = Some(PersonDate::year_month_day(1974, 5, 24));
        let died = Some(PersonDate::year_month_day(2000, 1, 1));
        assert_eq!(
            entry.get_person_dates().await.unwrap(),
            (born, died)
        );

        // Remove died
        entry.set_person_dates(&born, &None).await.unwrap();
        assert_eq!(
            entry.get_person_dates().await.unwrap(),
            (born, None)
        );

        // Remove born
        entry.set_person_dates(&None, &died).await.unwrap();
        assert_eq!(
            entry.get_person_dates().await.unwrap(),
            (None, died)
        );

        // Remove entire row
        entry.set_person_dates(&None, &None).await.unwrap();
        assert_eq!(entry.get_person_dates().await.unwrap(), (None, None));

        // Set back to original and check
        entry.set_person_dates(&born, &died).await.unwrap();
        assert_eq!(
            entry.get_person_dates().await.unwrap(),
            (born, died)
        );
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_coordinate_location() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();

        // Save whatever is currently in the DB so we can restore it at the end
        let original = entry.get_coordinate_location().await.unwrap();

        let cl = CoordinateLocation::new(1.234, -5.678);

        // Set a known value
        entry.set_coordinate_location(&Some(cl)).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(), Some(cl));

        // Switch lat/lon
        let cl2 = CoordinateLocation::new(cl.lon(), cl.lat());
        entry.set_coordinate_location(&Some(cl2)).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(), Some(cl2));

        // Remove
        entry.set_coordinate_location(&None).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(), None);

        // Restore original value
        entry.set_coordinate_location(&original).await.unwrap();
        assert_eq!(entry.get_coordinate_location().await.unwrap(), original);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_match() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();

        // Check if clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());

        // Set and check in-memory changes
        entry.set_match("Q1", 4).await.unwrap();
        assert_eq!(entry.q, Some(1));
        assert_eq!(entry.user, Some(4));
        assert!(entry.timestamp.is_some());

        // Check in-database changes
        let mut entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry2.q, Some(1));
        assert_eq!(entry2.user, Some(4));
        assert!(entry2.timestamp.is_some());

        // Clear and check in-memory changes
        entry2.unmatch().await.unwrap();
        assert!(entry2.q.is_none());
        assert!(entry2.user.is_none());
        assert!(entry2.timestamp.is_none());

        // Check in-database changes
        let entry3 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry3.q.is_none());
        assert!(entry3.user.is_none());
        assert!(entry3.timestamp.is_none());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_utf8() {
        let app = get_test_app();
        let entry = Entry::from_id(102826400, &app).await.unwrap();
        assert_eq!("이희정", &entry.ext_name);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_multimatch() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.unmatch().await.unwrap();
        let items: Vec<String> = ["Q1", "Q23456", "Q7"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        entry.set_multi_match(&items).await.unwrap();
        let result1 = entry.get_multi_match().await.unwrap();
        assert_eq!(result1, items);
        entry.remove_multi_match().await.unwrap();
        let result2 = entry.get_multi_match().await.unwrap();
        let empty: Vec<String> = vec![];
        assert_eq!(result2, empty);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_get_item_url() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // !!
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();

        // !!!!!
        entry.set_match("Q12345", 4).await.unwrap();

        assert_eq!(
            entry.get_item_url(),
            Some("https://www.wikidata.org/wiki/Q12345".to_string())
        );

        // !!!!!
        entry.unmatch().await.unwrap();

        assert_eq!(entry.get_item_url(), None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_get_entry_url() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(
            entry.get_entry_url(),
            Some(format!(
                "https://mix-n-match.toolforge.org/#/entry/{TEST_ENTRY_ID}"
            ))
        );
        let entry2 = Entry::new_from_catalog_and_ext_id(1, "234");
        assert_eq!(entry2.get_entry_url(), None);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_is_unmatched() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.set_match("Q12345", 4).await.unwrap();
        assert!(!entry.is_unmatched());
        entry.unmatch().await.unwrap();
        assert!(entry.is_unmatched());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_is_partially_matched() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.set_match("Q12345", 0).await.unwrap();
        assert!(entry.is_partially_matched());
        entry.unmatch().await.unwrap();
        assert!(!entry.is_partially_matched());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn is_fully_matched() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.set_match("Q12345", 4).await.unwrap();
        assert!(entry.is_fully_matched());
        entry.unmatch().await.unwrap();
        assert!(!entry.is_fully_matched());
    }

    #[test]
    fn test_is_partially_matched_unit() {
        // Partially matched: q > 0 and user == 0
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "test");
        entry.q = Some(42);
        entry.user = Some(0);
        assert!(entry.is_partially_matched());

        // Not partially matched: q is None, user == 0
        let mut entry2 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry2.q = None;
        entry2.user = Some(0);
        assert!(!entry2.is_partially_matched());

        // Not partially matched: q > 0, user > 0 (fully matched)
        let mut entry3 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry3.q = Some(42);
        entry3.user = Some(5);
        assert!(!entry3.is_partially_matched());

        // Not partially matched: q is None, user is None (unmatched)
        let entry4 = Entry::new_from_catalog_and_ext_id(1, "test");
        assert!(!entry4.is_partially_matched());

        // Not partially matched: q <= 0, user == 0
        let mut entry5 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry5.q = Some(0);
        entry5.user = Some(0);
        assert!(!entry5.is_partially_matched());

        let mut entry6 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry6.q = Some(-1);
        entry6.user = Some(0);
        assert!(!entry6.is_partially_matched());
    }

    #[test]
    fn test_is_fully_matched_unit() {
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "test");
        entry.q = Some(42);
        entry.user = Some(5);
        assert!(entry.is_fully_matched());

        let mut entry2 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry2.q = Some(42);
        entry2.user = Some(0);
        assert!(!entry2.is_fully_matched());

        let entry3 = Entry::new_from_catalog_and_ext_id(1, "test");
        assert!(!entry3.is_fully_matched());
    }

    #[test]
    fn test_is_unmatched_unit() {
        let entry = Entry::new_from_catalog_and_ext_id(1, "test");
        assert!(entry.is_unmatched());

        let mut entry2 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry2.q = Some(42);
        assert!(!entry2.is_unmatched());
    }

    #[test]
    fn test_entry_error_display() {
        assert_eq!(
            format!("{}", EntryError::TryingToUpdateNewEntry),
            "EntryError::TryingToUpdateNewEntry"
        );
        assert_eq!(
            format!("{}", EntryError::TryingToInsertExistingEntry),
            "EntryError::TryingToInsertExistingEntry"
        );
        assert_eq!(
            format!("{}", EntryError::EntryInsertFailed),
            "EntryError::EntryInsertFailed"
        );
    }

    #[test]
    fn test_value2opt_string() {
        // Bytes variant should return Some(String)
        let val_bytes = mysql_async::Value::Bytes(b"hello".to_vec());
        assert_eq!(
            Entry::value2opt_string(val_bytes).unwrap(),
            Some("hello".to_string())
        );

        // Non-Bytes variant should return None
        let val_null = mysql_async::Value::NULL;
        assert_eq!(Entry::value2opt_string(val_null).unwrap(), None);

        // Int variant should return None
        let val_int = mysql_async::Value::Int(42);
        assert_eq!(Entry::value2opt_string(val_int).unwrap(), None);
    }

    #[test]
    fn test_value2opt_isize() {
        let val_pos = mysql_async::Value::Int(42);
        assert_eq!(Entry::value2opt_isize(val_pos).unwrap(), Some(42_isize));

        let val_neg = mysql_async::Value::Int(-5);
        assert_eq!(Entry::value2opt_isize(val_neg).unwrap(), Some(-5_isize));

        let val_null = mysql_async::Value::NULL;
        assert_eq!(Entry::value2opt_isize(val_null).unwrap(), None);

        let val_bytes = mysql_async::Value::Bytes(b"hello".to_vec());
        assert_eq!(Entry::value2opt_isize(val_bytes).unwrap(), None);
    }

    #[test]
    fn test_value2opt_usize() {
        let val_int = mysql_async::Value::Int(42);
        assert_eq!(Entry::value2opt_usize(val_int).unwrap(), Some(42_usize));

        let val_null = mysql_async::Value::NULL;
        assert_eq!(Entry::value2opt_usize(val_null).unwrap(), None);

        let val_bytes = mysql_async::Value::Bytes(b"hello".to_vec());
        assert_eq!(Entry::value2opt_usize(val_bytes).unwrap(), None);
    }

    #[test]
    fn test_get_entry_url_unit() {
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "test");
        // No id set => None
        assert_eq!(entry.get_entry_url(), None);

        entry.id = Some(12345);
        assert_eq!(
            entry.get_entry_url(),
            Some("https://mix-n-match.toolforge.org/#/entry/12345".to_string())
        );
    }

    #[test]
    fn test_get_item_url_unit() {
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "test");
        // No q set => None
        assert_eq!(entry.get_item_url(), None);

        entry.q = Some(42);
        assert_eq!(
            entry.get_item_url(),
            Some("https://www.wikidata.org/wiki/Q42".to_string())
        );
    }

    #[test]
    fn test_fix_external_id() {
        // P213 (ISNI) should strip spaces
        assert_eq!(
            AuxiliaryRow::fix_external_id("P213", "0000 0001 2345 6789"),
            "0000000123456789"
        );
        // Other properties should pass through
        assert_eq!(
            AuxiliaryRow::fix_external_id("P214", "some value"),
            "some value"
        );
    }

    #[test]
    fn test_coordinate_location_accessors() {
        let cl = CoordinateLocation::new(1.5, -2.5);
        assert!((cl.lat() - 1.5).abs() < f64::EPSILON);
        assert!((cl.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_get_valid_id_unit() {
        let entry = Entry::new_from_catalog_and_ext_id(1, "test");
        assert!(entry.get_valid_id().is_err());

        let mut entry2 = Entry::new_from_catalog_and_ext_id(1, "test");
        entry2.id = Some(99);
        assert_eq!(entry2.get_valid_id().unwrap(), 99);
    }

    #[test]
    fn test_description() {
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "test");
        entry.ext_desc = "A test description".to_string();
        assert_eq!(entry.description(), "A test description");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_check_valid_id() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry.get_valid_id().is_ok());
        let entry2 = Entry::new_from_catalog_and_ext_id(1, "234");
        assert!(entry2.get_valid_id().is_err());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_add_alias() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        let s = LocaleString::new("en", "test");
        entry.add_alias(&s).await.unwrap();
        assert!(entry.get_aliases().await.unwrap().contains(&s));
    }

    #[test]
    fn test_new_from_catalog_and_ext_id_defaults() {
        let entry = Entry::new_from_catalog_and_ext_id(42, "ext123");
        assert_eq!(entry.catalog, 42);
        assert_eq!(entry.ext_id, "ext123");
        assert!(entry.id.is_none());
        assert!(entry.ext_url.is_empty());
        assert!(entry.ext_name.is_empty());
        assert!(entry.ext_desc.is_empty());
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());
        assert!(entry.type_name.is_none());
        assert!(entry.app.is_none());
        // random should be in [0, 1)
        assert!(entry.random >= 0.0 && entry.random < 1.0);
    }

    #[test]
    fn test_entry_default() {
        let entry = Entry::default();
        assert!(entry.id.is_none());
        assert_eq!(entry.catalog, 0);
        assert!(entry.ext_id.is_empty());
        assert!(entry.ext_url.is_empty());
        assert!(entry.ext_name.is_empty());
        assert!(entry.ext_desc.is_empty());
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());
        assert!(entry.type_name.is_none());
    }

    #[test]
    fn test_entry_match_state_transitions() {
        let mut entry = Entry::new_from_catalog_and_ext_id(1, "x");
        // Starts unmatched
        assert!(entry.is_unmatched());
        assert!(!entry.is_partially_matched());
        assert!(!entry.is_fully_matched());

        // Set to partially matched (q > 0, user == 0)
        entry.q = Some(100);
        entry.user = Some(0);
        assert!(!entry.is_unmatched());
        assert!(entry.is_partially_matched());
        assert!(!entry.is_fully_matched());

        // Set to fully matched (q > 0, user > 0)
        entry.user = Some(5);
        assert!(!entry.is_unmatched());
        assert!(!entry.is_partially_matched());
        assert!(entry.is_fully_matched());
    }
}
