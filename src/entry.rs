use crate::app_state::{AppState, USER_AUTO};
use crate::auxiliary_data::AuxiliaryRow;
use crate::catalog::Catalog;
use crate::coordinates::CoordinateLocation;
use crate::person::Person;
use crate::person_date::PersonDate;
use crate::util::wikidata_props as wp;
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

/// Normalise an `entry.type` value to the storage contract: either an
/// empty string or the form `Q\d+`. Legacy PHP-era imports sometimes
/// wrote the literal label `"person"` here — translate that to `"Q5"`
/// at write time so downstream readers don't have to carry a compat
/// check. Anything that doesn't match either form becomes `""` — the
/// column is NOT NULL, so we always return a string.
pub fn normalize_entry_type(type_name: Option<&str>) -> String {
    let s = type_name.unwrap_or("").trim();
    if s.is_empty() {
        return String::new();
    }
    if s.eq_ignore_ascii_case("person") {
        return "Q5".to_string();
    }
    let bytes = s.as_bytes();
    // Accept Q\d+ or q\d+; canonicalise the leading letter to upper-case
    // so the stored value is always `Q…`.
    if matches!(bytes.first(), Some(&b'Q' | &b'q'))
        && bytes.len() > 1
        && bytes[1..].iter().all(u8::is_ascii_digit)
    {
        let mut out = String::with_capacity(s.len());
        out.push('Q');
        out.push_str(&s[1..]);
        return out;
    }
    String::new()
}

/// Default precision (1 arcsecond ≈ 31 m) used when we need to emit a
/// `P625` globe-coordinate claim but the source didn't record a precision.
/// `wbeditentity` rejects coordinates with a `null`/missing precision, so
/// we must always hand it a concrete number. 1/3600 is Wikidata's own
/// default when picking a point from the map UI, so it matches the fidelity
/// of hand-entered coordinates at city/landmark level.
const DEFAULT_COORDINATE_PRECISION: f64 = 1.0 / 3600.0;

/// Build a `P625` snak. `Snak::new_coordinate` from wikibase sets precision
/// to `None`, which `wbeditentity` rejects — so construct the snak
/// ourselves and fill in a concrete precision (the stored value if we have
/// one, otherwise the arcsecond default).
fn build_p625_snak(lat: f64, lon: f64, precision: Option<f64>) -> Snak {
    use wikimisc::wikibase::{
        Coordinate, DataValue, DataValueType, SnakDataType, SnakType,
    };
    Snak::new(
        SnakDataType::GlobeCoordinate,
        wp::P_COORDINATES,
        SnakType::Value,
        Some(DataValue::new(
            DataValueType::GlobeCoordinate,
            wikimisc::wikibase::Value::Coordinate(Coordinate::new(
                None,
                "http://www.wikidata.org/entity/Q2".to_string(),
                lat,
                lon,
                Some(precision.unwrap_or(DEFAULT_COORDINATE_PRECISION)),
            )),
        )),
    )
}

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
    // `type` on the wire — matches the PHP API contract and every
    // frontend read site (`entry.type`, `entry_details.js:104`,
    // `entry_list_item.js`, etc.). The Rust field keeps the `type_name`
    // name because `type` is a reserved word. `alias = "type_name"`
    // accepts older import files that were written against the Rust
    // port's earlier (ad-hoc) field name.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "type",
        alias = "type_name"
    )]
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
        let storage = self.app()?.storage().clone();
        self.id = storage.entry_insert_as_new(self).await?;
        // Bump the cached overview counters so they don't drift below
        // reality when the autoscrape pipeline matches one of these
        // freshly-inserted rows. `entry_insert_as_new` uses
        // `INSERT IGNORE`, so id=Some(0) means the row already
        // existed — skip the bump in that case.
        if matches!(self.id, Some(id) if id > 0) {
            let _ = storage
                .overview_apply_insert(self.catalog, self.user, self.q)
                .await;
        }
        Ok(self.id)
    }

    /// Deletes the entry and all of its associated data in the database. Resets the local ID to 0
    //TODO test
    pub async fn delete(&mut self) -> Result<()> {
        // Snapshot the bucket coordinates *before* the DELETE — once the
        // row is gone we can't classify which overview column to debit.
        let catalog = self.catalog;
        let user = self.user;
        let q = self.q;
        let storage = self.app()?.storage().clone();
        storage.entry_delete(self.get_valid_id()?).await?;
        let _ = storage.overview_apply_delete(catalog, user, q).await;
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
        // `use_description_for_new` gates whether the catalog's ext_desc is
        // copied into a newly-created item's description. Default is on;
        // catalog admins can opt out via kv_catalog.
        let use_desc = catalog
            .get_key_value_pairs()
            .await
            .unwrap_or_default()
            .get("use_description_for_new")
            .map(|v| v != "0")
            .unwrap_or(true);
        self.add_to_item_own_id(&catalog, &references, item);
        self.add_to_item_type(&references, item);
        self.add_to_item_name_and_aliases(&language, item).await?;
        self.add_to_item_descriptions(language, use_desc, item).await?;
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
            let snak = Snak::new_time(wp::P_DATE_OF_BIRTH, &pd.to_wikidata_time(), pd.wikidata_precision());
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
        if let Some(pd) = died {
            let snak = Snak::new_time(wp::P_DATE_OF_DEATH, &pd.to_wikidata_time(), pd.wikidata_precision());
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
            let snak = build_p625_snak(coord.lat(), coord.lon(), coord.precision());
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Self::add_claim_or_references(item, claim);
        }
        Ok(())
    }

    async fn add_to_item_descriptions(
        &self,
        language: String,
        use_ext_desc: bool,
        item: &mut ItemEntity,
    ) -> Result<()> {
        let mut descriptions = self.get_language_descriptions().await?;
        if use_ext_desc && !self.ext_desc.is_empty() {
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
                let snak = Snak::new_item(wp::P_INSTANCE_OF, tn);
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
        // Normalise the incoming claim in isolation: strip self-referencing
        // snaks per-snak (not per-block, so a 3-snak reference block keeps
        // its two legitimate snaks if one of them happens to echo the main
        // snak), dedupe snaks inside each reference block, drop empty
        // blocks, and dedupe the reference blocks themselves so equivalent
        // blocks with snaks in different orders collapse into one.
        Self::normalise_claim(&mut claim);

        // Merge into a structurally-equivalent existing claim (same main
        // snak AND same multiset of qualifier snaks). Comparing on main_snak
        // alone was lossy: qualifier-bearing claims were merged with ones
        // that didn't share those qualifiers, silently dropping them.
        for existing in item.claims_mut() {
            if Self::claim_core_equivalent(existing, &claim) {
                let mut refs = existing.references().to_vec();
                for r in claim.references() {
                    if !refs
                        .iter()
                        .any(|existing_ref| Self::reference_equivalent(existing_ref, r))
                    {
                        refs.push(r.clone());
                    }
                }
                existing.set_references(refs);
                return;
            }
        }

        item.add_claim(claim);
    }

    /// Clean up a claim before it enters the item: dedupe qualifiers and
    /// references without changing the claim's meaning.
    fn normalise_claim(claim: &mut Statement) {
        // Qualifiers: drop exact duplicates, preserving order.
        let mut qs: Vec<Snak> = Vec::with_capacity(claim.qualifiers().len());
        for q in claim.qualifiers() {
            if !qs.contains(q) {
                qs.push(q.clone());
            }
        }
        claim.set_qualifier_snaks(qs);

        // References: per block, drop snaks equal to the main snak (those
        // are circular and add no provenance value) and drop duplicate
        // snaks. Drop blocks that end up empty. Then dedupe whole blocks.
        let main = claim.main_snak().clone();
        let mut new_refs: Vec<Reference> = Vec::new();
        for r in claim.references() {
            let mut snaks: Vec<Snak> = Vec::with_capacity(r.snaks().len());
            for s in r.snaks() {
                if *s == main {
                    continue;
                }
                if snaks.contains(s) {
                    continue;
                }
                snaks.push(s.clone());
            }
            if snaks.is_empty() {
                continue;
            }
            let candidate = Reference::new(snaks);
            if !new_refs
                .iter()
                .any(|existing| Self::reference_equivalent(existing, &candidate))
            {
                new_refs.push(candidate);
            }
        }
        claim.set_references(new_refs);
    }

    /// Two reference blocks are equivalent if they carry the same snaks in
    /// any order. `Reference`'s derived `PartialEq` is order-sensitive, so
    /// callers that merge references across entries need this instead.
    /// Both inputs are expected to be dedup'd already (via `normalise_claim`).
    fn reference_equivalent(a: &Reference, b: &Reference) -> bool {
        let sa = a.snaks();
        let sb = b.snaks();
        sa.len() == sb.len() && sa.iter().all(|s| sb.contains(s))
    }

    /// Two claims are equivalent enough to merge (i.e. same main snak and
    /// same qualifier set, order-insensitive). Qualifier-bearing variants
    /// are kept separate from bare claims so we don't lose qualifiers.
    /// Ignores rank/type/id — those don't affect the claim's identity for
    /// the new-item-creation use case.
    fn claim_core_equivalent(a: &Statement, b: &Statement) -> bool {
        if a.main_snak() != b.main_snak() {
            return false;
        }
        let aq = a.qualifiers();
        let bq = b.qualifiers();
        aq.len() == bq.len() && aq.iter().all(|s| bq.contains(s))
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
        // Snapshot old (user, q) so the overview shift runs against the
        // pre-unmatch bucket. Without this the overview drifts: the
        // catalog's `manual`/`na`/`nowd`/`autoq` counter stays elevated
        // until the next full Refresh.
        let old_entry = self.clone();
        let entry_id = self.get_valid_id()?;
        self.app()?.storage().entry_unmatch(entry_id).await?;
        self.user = None;
        self.timestamp = None;
        self.q = None;
        self.app()?
            .storage()
            .update_overview_table(&old_entry, None, None)
            .await?;
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

    // ----- normalize_entry_type -----

    #[test]
    fn normalize_entry_type_empty_cases() {
        assert_eq!(normalize_entry_type(None), "");
        assert_eq!(normalize_entry_type(Some("")), "");
        assert_eq!(normalize_entry_type(Some("   ")), "");
    }

    #[test]
    fn normalize_entry_type_legacy_person_maps_to_q5() {
        assert_eq!(normalize_entry_type(Some("person")), "Q5");
        // Case-insensitive for the legacy label too.
        assert_eq!(normalize_entry_type(Some("Person")), "Q5");
        assert_eq!(normalize_entry_type(Some("PERSON")), "Q5");
    }

    #[test]
    fn normalize_entry_type_accepts_qids() {
        assert_eq!(normalize_entry_type(Some("Q5")), "Q5");
        assert_eq!(normalize_entry_type(Some("Q16521")), "Q16521");
        assert_eq!(normalize_entry_type(Some("Q1")), "Q1");
    }

    #[test]
    fn normalize_entry_type_uppercases_lowercase_q_prefix() {
        // Lowercase `q` is a common typo / case drift from external
        // sources; canonicalise to `Q…` on write so the stored value
        // is consistent and downstream equality checks don't need
        // case folding.
        assert_eq!(normalize_entry_type(Some("q5")), "Q5");
        assert_eq!(normalize_entry_type(Some("q16521")), "Q16521");
        assert_eq!(normalize_entry_type(Some("q1")), "Q1");
    }

    #[test]
    fn normalize_entry_type_trims_surrounding_whitespace() {
        assert_eq!(normalize_entry_type(Some("  Q5 ")), "Q5");
        assert_eq!(normalize_entry_type(Some("\tq5\n")), "Q5");
    }

    #[test]
    fn normalize_entry_type_rejects_garbage() {
        // Lone Q/q, digits with non-numeric suffix, and unrelated
        // labels all resolve to "" — the column is NOT NULL so we
        // never propagate a bad value.
        assert_eq!(normalize_entry_type(Some("Q")), "");
        assert_eq!(normalize_entry_type(Some("q")), "");
        assert_eq!(normalize_entry_type(Some("Q5a")), "");
        assert_eq!(normalize_entry_type(Some("q5a")), "");
        assert_eq!(normalize_entry_type(Some("human")), "");
        assert_eq!(normalize_entry_type(Some("not_person")), "");
    }

    // ----- Serde field-name contract for `entry.type` -----

    #[test]
    fn entry_serializes_type_field_as_type() {
        // Frontend reads `entry.type` (matches PHP API). The struct field
        // is `type_name` because `type` is a reserved word in Rust; serde
        // renames it on the wire.
        let e = Entry {
            type_name: Some("Q5".into()),
            ..Default::default()
        };
        let v: serde_json::Value = serde_json::to_value(&e).unwrap();
        assert_eq!(v["type"], serde_json::json!("Q5"));
        assert!(v.get("type_name").is_none(), "must not also emit type_name");
    }

    #[test]
    fn entry_deserializes_either_type_or_type_name() {
        // Canonical key.
        let canonical: Entry =
            serde_json::from_str(r#"{"catalog":1,"ext_id":"x","ext_name":"n","type":"Q5"}"#).unwrap();
        assert_eq!(canonical.type_name.as_deref(), Some("Q5"));
        // Legacy alias for older import files produced before the rename.
        let legacy: Entry =
            serde_json::from_str(r#"{"catalog":1,"ext_id":"x","ext_name":"n","type_name":"Q5"}"#).unwrap();
        assert_eq!(legacy.type_name.as_deref(), Some("Q5"));
    }

    // ----- P625 snak precision -----

    fn snak_precision(snak: &Snak) -> Option<f64> {
        let dv = snak.data_value().as_ref()?;
        match dv.value() {
            wikimisc::wikibase::Value::Coordinate(c) => *c.precision(),
            _ => None,
        }
    }

    #[test]
    fn build_p625_snak_applies_stored_precision() {
        let snak = build_p625_snak(51.5, -0.1, Some(0.001));
        assert_eq!(snak_precision(&snak), Some(0.001));
    }

    #[test]
    fn build_p625_snak_falls_back_to_arcsecond_default() {
        // Missing precision is what was triggering "Missing required field
        // 'precision'" in wbeditentity — make sure we always emit a number.
        let snak = build_p625_snak(51.5, -0.1, None);
        let prec = snak_precision(&snak).expect("precision must be set");
        assert!((prec - 1.0 / 3600.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_p625_snak_serializes_precision_as_number() {
        let snak = build_p625_snak(51.5, -0.1, None);
        let json = serde_json::to_value(&snak).unwrap();
        let precision = json
            .pointer("/datavalue/value/precision")
            .expect("precision key must be present in JSON");
        assert!(precision.is_number(), "precision must serialize as a number, got {precision:?}");
    }

    // ----- Claim/reference dedup (no DB, no network) -----

    fn stmt_item(prop: &str, q: &str) -> Statement {
        Statement::new_normal(Snak::new_item(prop, q), vec![], vec![])
    }

    fn stmt_string(prop: &str, v: &str) -> Statement {
        Statement::new_normal(Snak::new_string(prop, v), vec![], vec![])
    }

    fn with_refs(mut s: Statement, refs: Vec<Reference>) -> Statement {
        s.set_references(refs);
        s
    }

    fn with_quals(mut s: Statement, quals: Vec<Snak>) -> Statement {
        s.set_qualifier_snaks(quals);
        s
    }

    fn r_url(val: &str) -> Reference {
        Reference::new(vec![Snak::new_string("P854", val)])
    }

    #[test]
    fn dedup_merges_same_main_snak_and_unions_references() {
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(
            &mut item,
            with_refs(stmt_item("P31", "Q5"), vec![r_url("https://a")]),
        );
        Entry::add_claim_or_references(
            &mut item,
            with_refs(stmt_item("P31", "Q5"), vec![r_url("https://b")]),
        );
        Entry::add_claim_or_references(
            &mut item,
            with_refs(stmt_item("P31", "Q5"), vec![r_url("https://a")]),
        );
        assert_eq!(item.claims().len(), 1, "same claim should merge into one");
        let refs = item.claims()[0].references();
        assert_eq!(refs.len(), 2, "duplicate reference blocks should collapse");
        let urls: Vec<&str> = refs
            .iter()
            .flat_map(|r| r.snaks())
            .filter_map(|snak| match snak.data_value() {
                Some(dv) => match dv.value() {
                    wikimisc::wikibase::Value::StringValue(v) => Some(v.as_str()),
                    _ => None,
                },
                None => None,
            })
            .collect();
        assert!(urls.contains(&"https://a"));
        assert!(urls.contains(&"https://b"));
    }

    #[test]
    fn dedup_reference_equivalence_is_order_insensitive() {
        let block_a_then_b = Reference::new(vec![
            Snak::new_string("P248", "Q1"),
            Snak::new_string("P854", "https://foo"),
        ]);
        let block_b_then_a = Reference::new(vec![
            Snak::new_string("P854", "https://foo"),
            Snak::new_string("P248", "Q1"),
        ]);
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(
            &mut item,
            with_refs(stmt_item("P31", "Q5"), vec![block_a_then_b]),
        );
        Entry::add_claim_or_references(
            &mut item,
            with_refs(stmt_item("P31", "Q5"), vec![block_b_then_a]),
        );
        assert_eq!(item.claims()[0].references().len(), 1);
    }

    #[test]
    fn dedup_self_referencing_ref_snak_is_dropped_per_snak_not_per_block() {
        // A reference block that happens to carry the main snak as one of
        // its snaks should keep its other snaks, not be emptied entirely.
        let main = Snak::new_string("P214", "12345");
        let stated_in = Snak::new_item("P248", "Q54919");
        let block = Reference::new(vec![main.clone(), stated_in.clone()]);

        let claim = Statement::new_normal(main, vec![], vec![block]);
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, claim);
        let refs = item.claims()[0].references();
        assert_eq!(refs.len(), 1, "the block should survive, shrunk");
        assert_eq!(refs[0].snaks().len(), 1);
        assert_eq!(refs[0].snaks()[0], stated_in);
    }

    #[test]
    fn dedup_claims_with_different_qualifiers_stay_separate() {
        // Same main snak, different qualifier sets => two distinct claims.
        // Merging them would silently drop whichever qualifier set arrived
        // second — the whole point of qualifiers is that they change the
        // claim's meaning.
        let bare = stmt_item("P106", "Q482980");
        let with_q = with_quals(
            stmt_item("P106", "Q482980"),
            vec![Snak::new_item("P1686", "Q12345")],
        );
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, bare);
        Entry::add_claim_or_references(&mut item, with_q);
        assert_eq!(item.claims().len(), 2);
    }

    #[test]
    fn dedup_claims_with_same_qualifiers_in_different_order_merge() {
        let q1 = Snak::new_item("P580", "Q1");
        let q2 = Snak::new_item("P582", "Q2");
        let a = with_quals(stmt_item("P39", "Q11696"), vec![q1.clone(), q2.clone()]);
        let b = with_quals(stmt_item("P39", "Q11696"), vec![q2, q1]);
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, a);
        Entry::add_claim_or_references(&mut item, b);
        assert_eq!(item.claims().len(), 1);
    }

    #[test]
    fn dedup_removes_duplicate_snaks_within_one_reference_block() {
        let dup = Reference::new(vec![
            Snak::new_string("P854", "https://example"),
            Snak::new_string("P854", "https://example"),
        ]);
        let claim = Statement::new_normal(Snak::new_item("P31", "Q5"), vec![], vec![dup]);
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, claim);
        assert_eq!(item.claims()[0].references()[0].snaks().len(), 1);
    }

    #[test]
    fn dedup_qualifiers_are_deduplicated_on_insert() {
        let q = Snak::new_item("P580", "Q1");
        let claim = with_quals(stmt_string("P214", "123"), vec![q.clone(), q.clone(), q]);
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, claim);
        assert_eq!(item.claims()[0].qualifiers().len(), 1);
    }

    #[test]
    fn dedup_does_not_conflate_different_main_snaks() {
        let mut item = ItemEntity::new_empty();
        Entry::add_claim_or_references(&mut item, stmt_item("P31", "Q5"));
        Entry::add_claim_or_references(&mut item, stmt_item("P31", "Q16521"));
        assert_eq!(item.claims().len(), 2);
    }
}
