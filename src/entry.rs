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
    use wikimisc::wikibase::{Coordinate, DataValue, DataValueType, SnakDataType, SnakType};
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
}

/// Repository — encapsulates the static "load Entry from storage"
/// methods that previously lived as `Entry::from_id`,
/// `Entry::from_ext_id`, and `Entry::multiple_from_ids`. New code
/// should prefer this over the static methods on Entry; the static
/// methods are kept as thin facades so existing call sites
/// (~25 files) don't need to change in lockstep.
///
/// Why split: `Entry` currently mixes domain model + repository +
/// Wikidata writer + self-DI'd `AppState`, an Active Record
/// anti-pattern flagged in the design-pattern audit (#4.4). Lifting
/// loads into a dedicated `EntryRepo` is the first piece of the
/// three-way split (model / repo / writer) and the only piece
/// tractable in one commit.
///
/// The repo holds a borrowed `&AppState`, so it's cheap to
/// construct per call (`EntryRepo(&app).find(id).await`) without
/// any cloning. Future work can swap the inner type for `&dyn
/// AppContext` to break the AppState concrete dependency.
#[derive(Debug)]
pub struct EntryRepo<'a>(pub &'a AppState);

impl<'a> EntryRepo<'a> {
    pub fn new(app: &'a AppState) -> Self {
        Self(app)
    }

    /// Load an entry by primary key.
    pub async fn find(&self, entry_id: DbId) -> Result<Entry> {
        self.0.storage().entry_from_id(entry_id).await
    }

    /// Load multiple entries by primary key in one round-trip.
    /// Missing ids are silently absent from the returned map.
    pub async fn find_many(&self, entry_ids: &[DbId]) -> Result<HashMap<DbId, Entry>> {
        self.0.storage().multiple_from_ids(entry_ids).await
    }

    /// Load an entry by `(catalog_id, ext_id)`. The pair is unique
    /// per the storage contract.
    pub async fn find_by_ext_id(&self, catalog_id: DbId, ext_id: &str) -> Result<Entry> {
        self.0.storage().entry_from_ext_id(catalog_id, ext_id).await
    }
}

/// Writer — encapsulates the entry-mutation API (`set_match`,
/// `unmatch`, `set_auxiliary`, …) with the application context held
/// **explicitly** alongside the entry.
///
/// This is the second piece of the [Active-Record → Domain Model +
/// Repository + Writer] split flagged in the architecture audit
/// (#4.4 → §3.2 runtime cycle).
///
/// **For new code, use:**
///
/// ```ignore
/// let mut entry = EntryRepo::new(&app).find(id).await?;
/// EntryWriter::new(&app, &mut entry).set_match("Q42", USER).await?;
/// ```
#[derive(Debug)]
pub struct EntryWriter<'a> {
    ctx: &'a AppState,
    entry: &'a mut Entry,
}

impl<'a> EntryWriter<'a> {
    pub fn new(ctx: &'a AppState, entry: &'a mut Entry) -> Self {
        Self { ctx, entry }
    }

    /// The wrapped entry as an immutable view — useful for callers
    /// that want to inspect data after a mutation.
    pub fn as_entry(&self) -> &Entry {
        self.entry
    }

    /// The application context used for all storage / Wikidata
    /// round-trips made through this writer.
    pub fn ctx(&self) -> &AppState {
        self.ctx
    }

    /// Set a match for the wrapped entry and update related rows.
    /// **This is the canonical implementation.** `Entry::set_match` now
    /// forwards here — when this PR's sister PRs migrate the remaining
    /// writers (`unmatch`, `set_auxiliary`, …) into this same shape,
    /// the `Entry::app: Option<AppState>` field can be dropped.
    pub async fn set_match(&mut self, q: &str, user_id: DbId) -> Result<bool> {
        self.entry.get_valid_id()?;
        let q_numeric = AppState::item2numeric(q)
            .ok_or_else(|| anyhow!("'{}' is not a valid item", &q))?;
        let timestamp = TimeStamp::now();
        if self
            .ctx
            .storage()
            .entry_set_match(self.entry, user_id, q_numeric, &timestamp)
            .await?
        {
            self.entry.user = Some(user_id);
            self.entry.timestamp = Some(timestamp);
            self.entry.q = Some(q_numeric);
        }
        Ok(true)
    }

    /// Remove the wrapped entry's match and decrement the catalog
    /// overview counters. **Canonical implementation.** `Entry::unmatch`
    /// forwards here.
    pub async fn unmatch(&mut self) -> Result<()> {
        // Snapshot the pre-unmatch (user, q) so the overview shift runs
        // against the bucket the row was in *before* we cleared the
        // fields. Without this, the overview drifts: counters stay
        // elevated until the next full Refresh.
        let old_entry = self.entry.clone();
        let entry_id = self.entry.get_valid_id()?;
        self.ctx.storage().entry_unmatch(entry_id).await?;
        self.entry.user = None;
        self.entry.timestamp = None;
        self.entry.q = None;
        self.ctx
            .storage()
            .update_overview_table(&old_entry, None, None)
            .await?;
        Ok(())
    }

    /// Set or clear the auxiliary value for `(entry, prop_numeric)`.
    /// **Canonical implementation.** `Entry::set_auxiliary` forwards to
    /// the shared helper [`write_auxiliary`].
    pub async fn set_auxiliary(
        &mut self,
        prop_numeric: PropertyId,
        value: Option<String>,
    ) -> Result<()> {
        write_auxiliary(self.ctx, self.entry.get_valid_id()?, prop_numeric, value).await
    }

    /// Update the entry's person dates (P569 / P570) where they differ
    /// from the stored pair. **Canonical implementation.**
    /// `Entry::set_person_dates` forwards to the shared helper
    /// [`write_person_dates`].
    pub async fn set_person_dates(
        &mut self,
        born: &Option<PersonDate>,
        died: &Option<PersonDate>,
    ) -> Result<()> {
        write_person_dates(self.ctx, self.entry.get_valid_id()?, born, died).await
    }

    /// Update the entry's coordinate location (P625) where it differs
    /// from the stored value. **Canonical implementation.**
    /// `Entry::set_coordinate_location` forwards to the shared helper
    /// [`write_coordinate_location`].
    pub async fn set_coordinate_location(&mut self, cl: &Option<CoordinateLocation>) -> Result<()> {
        write_coordinate_location(self.ctx, self.entry.get_valid_id()?, cl).await
    }

    /// Update `ext_name` locally and in the database. **Canonical
    /// implementation.** `Entry::set_ext_name` forwards here.
    pub async fn set_ext_name(&mut self, ext_name: &str) -> Result<()> {
        if self.entry.ext_name == ext_name {
            return Ok(());
        }
        let entry_id = self.entry.get_valid_id()?;
        self.entry.ext_name = ext_name.to_string();
        self.ctx
            .storage()
            .entry_set_ext_name(ext_name, entry_id)
            .await
    }

    /// Update `ext_desc` locally and in the database. **Canonical
    /// implementation.** `Entry::set_ext_desc` forwards here.
    pub async fn set_ext_desc(&mut self, ext_desc: &str) -> Result<()> {
        if self.entry.ext_desc == ext_desc {
            return Ok(());
        }
        let entry_id = self.entry.get_valid_id()?;
        self.entry.ext_desc = ext_desc.to_string();
        self.ctx
            .storage()
            .entry_set_ext_desc(ext_desc, entry_id)
            .await
    }

    /// Update `ext_id` locally and in the database. **Canonical
    /// implementation.** `Entry::set_ext_id` forwards here.
    pub async fn set_ext_id(&mut self, ext_id: &str) -> Result<()> {
        if self.entry.ext_id == ext_id {
            return Ok(());
        }
        let entry_id = self.entry.get_valid_id()?;
        self.entry.ext_id = ext_id.to_string();
        self.ctx
            .storage()
            .entry_set_ext_id(ext_id, entry_id)
            .await
    }

    /// Update `ext_url` locally and in the database. **Canonical
    /// implementation.** `Entry::set_ext_url` forwards here.
    pub async fn set_ext_url(&mut self, ext_url: &str) -> Result<()> {
        if self.entry.ext_url == ext_url {
            return Ok(());
        }
        let entry_id = self.entry.get_valid_id()?;
        self.entry.ext_url = ext_url.to_string();
        self.ctx
            .storage()
            .entry_set_ext_url(ext_url, entry_id)
            .await
    }

    /// Update `type_name` locally and in the database. **Canonical
    /// implementation.** `Entry::set_type_name` forwards here.
    pub async fn set_type_name(&mut self, type_name: Option<String>) -> Result<()> {
        if self.entry.type_name == type_name {
            return Ok(());
        }
        let entry_id = self.entry.get_valid_id()?;
        self.entry.type_name.clone_from(&type_name);
        self.ctx
            .storage()
            .entry_set_type_name(type_name, entry_id)
            .await
    }

    /// Add a localised alias for the entry. **Canonical implementation.**
    /// `Entry::add_alias` forwards to the shared helper [`write_alias`].
    pub async fn add_alias(&self, s: &LocaleString) -> Result<()> {
        write_alias(self.ctx, self.entry.get_valid_id()?, s).await
    }

    /// Update the entry's `is_matched` flag in `entry`/`person_dates`.
    /// Forwards to [`write_match_status`].
    pub async fn set_match_status(&self, status: &str, is_matched: bool) -> Result<()> {
        write_match_status(self.ctx, self.entry.get_valid_id()?, status, is_matched).await
    }

    /// Set or clear a language description. Forwards to
    /// [`write_language_description`].
    pub async fn set_language_description(
        &self,
        language: &str,
        text: Option<String>,
    ) -> Result<()> {
        write_language_description(self.ctx, self.entry.get_valid_id()?, language, text).await
    }

    /// Replace the entry's multi-match list. Forwards to
    /// [`write_multi_match`].
    pub async fn set_multi_match(&self, items: &[String]) -> Result<()> {
        write_multi_match(self.ctx, self.entry.get_valid_id()?, items).await
    }

    /// Remove the entry's multi-match row entirely.
    pub async fn remove_multi_match(&self) -> Result<()> {
        // `write_multi_match` with an empty input takes the remove branch.
        write_multi_match(self.ctx, self.entry.get_valid_id()?, &[]).await
    }

    /// Set the auto-match (q[0]) and, if more candidates remain,
    /// also write the multi-match list. **Canonical implementation.**
    /// `Entry::set_auto_and_multi_match` forwards here.
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
        if self.entry.q == Some(qs_numeric[0]) {
            return Ok(()); // Automatch already there; skip multimatch.
        }
        self.set_match(&format!("Q{}", qs_numeric[0]), USER_AUTO).await?;
        if qs_numeric.len() > 1 {
            self.set_multi_match(items).await?;
        }
        Ok(())
    }

    /// Insert the wrapped entry into the database. `id` must be `None`
    /// before the call. **Canonical implementation.**
    /// `Entry::insert_as_new` forwards here.
    pub async fn insert_as_new(&mut self) -> Result<EntryId> {
        if self.entry.id.is_some() {
            return Err(EntryError::TryingToInsertExistingEntry.into());
        }
        let storage = self.ctx.storage().clone();
        self.entry.id = storage.entry_insert_as_new(self.entry).await?;
        // Bump the cached overview counters so they don't drift below
        // reality when the autoscrape pipeline matches one of these
        // freshly-inserted rows. `entry_insert_as_new` uses
        // `INSERT IGNORE`, so id=Some(0) means the row already
        // existed — skip the bump in that case.
        if matches!(self.entry.id, Some(id) if id > 0) {
            let _ = storage
                .overview_apply_insert(self.entry.catalog, self.entry.user, self.entry.q)
                .await;
        }
        Ok(self.entry.id)
    }

    /// Delete the wrapped entry and its associated rows; resets `id`
    /// to `None` on success. **Canonical implementation.**
    /// `Entry::delete` forwards here.
    pub async fn delete(&mut self) -> Result<()> {
        // Snapshot the bucket coordinates *before* the DELETE — once
        // the row is gone we can't classify which overview column to
        // debit.
        let catalog = self.entry.catalog;
        let user = self.entry.user;
        let q = self.entry.q;
        let storage = self.ctx.storage().clone();
        storage.entry_delete(self.entry.get_valid_id()?).await?;
        let _ = storage.overview_apply_delete(catalog, user, q).await;
        self.entry.id = None;
        Ok(())
    }

    // ── Reader methods ───────────────────────────────────────────────────
    // Thin delegators to the private `read_*` free helpers. Callers that
    // already hold an `EntryWriter` can use these instead of the `Entry`
    // facade methods (which rely on `Entry::app` being set).

    pub async fn get_aux(&self) -> Result<Vec<AuxiliaryRow>> {
        read_aux(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_aliases(&self) -> Result<Vec<LocaleString>> {
        read_aliases(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_person_dates(&self) -> Result<(Option<PersonDate>, Option<PersonDate>)> {
        read_person_dates(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_language_descriptions(&self) -> Result<HashMap<String, String>> {
        read_language_descriptions(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_coordinate_location(&self) -> Result<Option<CoordinateLocation>> {
        read_coordinate_location(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_multi_match(&self) -> Result<Vec<String>> {
        read_multi_match(self.ctx, self.entry.get_valid_id()?).await
    }

    pub async fn get_creation_time(&self) -> Option<String> {
        read_creation_time(self.ctx, self.entry.get_valid_id().ok()?).await
    }

    pub async fn set_auxiliary_in_wikidata(&self, aux_id: DbId, in_wikidata: bool) -> Result<()> {
        write_auxiliary_in_wikidata(self.ctx, aux_id, in_wikidata).await
    }

    pub async fn add_mnm_relation(
        &self,
        prop_numeric: PropertyId,
        target_entry_id: DbId,
    ) -> Result<()> {
        write_mnm_relation(self.ctx, self.entry.get_valid_id()?, prop_numeric, target_entry_id)
            .await
    }

    pub async fn add_to_item(&self, item: &mut ItemEntity) -> Result<()> {
        build_item_from_entry(self.ctx, self.entry, item).await
    }
}

// ── Private writer helpers ───────────────────────────────────────────
//
// Each `write_*` function holds the canonical body for one writer that
// used to take `&self` on `Entry`. Both `EntryWriter::method` and the
// `Entry::method` facade route through the helper, so the body lives
// in exactly one place. When the `Entry::method` facades are deleted
// (the audit's end goal), these can either move into `impl EntryWriter`
// directly or stay as private helpers — either works.

async fn write_auxiliary(
    ctx: &AppState,
    entry_id: DbId,
    prop_numeric: PropertyId,
    value: Option<String>,
) -> Result<()> {
    match value {
        Some(value) => {
            // Empty strings are ignored — the storage layer's INSERT
            // would coerce them to NULL anyway, but the comparison is
            // cheap and self-documenting.
            if !value.is_empty() {
                ctx.storage()
                    .entry_set_auxiliary(entry_id, prop_numeric, value)
                    .await?;
            }
        }
        None => {
            ctx.storage()
                .entry_remove_auxiliary(entry_id, prop_numeric)
                .await?;
        }
    }
    Ok(())
}

async fn write_person_dates(
    ctx: &AppState,
    entry_id: DbId,
    born: &Option<PersonDate>,
    died: &Option<PersonDate>,
) -> Result<()> {
    // Read the currently stored pair and compare; only write if it
    // actually changed. Avoids overview-table churn on no-op imports.
    let (already_born, already_died) = read_person_dates(ctx, entry_id).await?;
    if already_born == *born && already_died == *died {
        return Ok(());
    }
    if born.is_none() && died.is_none() {
        ctx.storage().entry_delete_person_dates(entry_id).await?;
    } else {
        let born = born.map(|d| d.to_db_string()).unwrap_or_default();
        let died = died.map(|d| d.to_db_string()).unwrap_or_default();
        ctx.storage()
            .entry_set_person_dates(entry_id, born, died)
            .await?;
    }
    Ok(())
}

// ── Private reader helpers ───────────────────────────────────────────
//
// Symmetric with the `write_*` helpers below. Entry's `get_*` methods
// forward here, so reading entry-related rows doesn't depend on
// `Entry::app: Option<AppState>` being set. New code that has a
// `&AppState` and a `DbId` can call these directly.

// ── Wikidata `ItemEntity` builder (driven by `Entry::add_to_item`) ──
//
// The chain that constructs a fresh Wikidata item from an `Entry`'s
// data. Each step takes `(ctx, entry, item, …)` explicitly so the
// builder doesn't depend on `Entry::app: Option<AppState>` being set;
// the Wikidata API and the catalog table are accessed via `ctx`. The
// per-section reads (`read_aux`, `read_person_dates`, …) are the same
// helpers Entry's getters now use.

async fn build_item_from_entry(
    ctx: &AppState,
    entry: &Entry,
    item: &mut ItemEntity,
) -> Result<()> {
    let catalog = Catalog::from_id(entry.catalog, ctx).await?;
    let references = catalog.references(ctx, entry).await;
    let language = catalog.search_wp().to_string();
    // `use_description_for_new` gates whether the catalog's ext_desc is
    // copied into a newly-created item's description. Default is on;
    // catalog admins can opt out via kv_catalog.
    let use_desc = catalog
        .get_key_value_pairs(ctx)
        .await
        .unwrap_or_default()
        .get("use_description_for_new")
        .map(|v| v != "0")
        .unwrap_or(true);
    add_own_id_to_item(entry, &catalog, &references, item);
    add_type_to_item(entry, &references, item);
    add_name_and_aliases_to_item(ctx, entry, &language, item).await?;
    add_descriptions_to_item(ctx, entry, language, use_desc, item).await?;
    add_coordinates_to_item(ctx, entry, &references, item).await?;
    add_person_dates_to_item(ctx, entry, &references, item).await?;
    add_auxiliary_to_item(ctx, entry, references, item).await?;
    Ok(())
}

async fn add_auxiliary_to_item(
    ctx: &AppState,
    entry: &Entry,
    references: Vec<Reference>,
    item: &mut ItemEntity,
) -> Result<()> {
    let auxiliary = read_aux(ctx, entry.get_valid_id()?).await?;
    if auxiliary.is_empty() {
        return Ok(());
    }
    let api = ctx.wikidata().get_mw_api().await?;
    let ec = EntityContainer::new();
    let props2load: Vec<String> = auxiliary.iter().map(|a| a.prop_as_string()).collect();
    // Pre-load all property entities in one batch; per-entity load below is
    // a no-op for ones the batch already cached.
    let _ = ec.load_entities(&api, &props2load).await;
    for aux in auxiliary {
        if let Ok(prop) = ec.load_entity(&api, aux.prop_as_string()).await {
            if let Some(claim) = aux.get_claim_for_aux(prop, &references) {
                Entry::add_claim_or_references(item, claim);
            }
        }
    }
    Ok(())
}

async fn add_person_dates_to_item(
    ctx: &AppState,
    entry: &Entry,
    references: &[Reference],
    item: &mut ItemEntity,
) -> Result<()> {
    let (born, died) = read_person_dates(ctx, entry.get_valid_id()?).await?;
    if let Some(pd) = born {
        let snak = Snak::new_time(
            wp::P_DATE_OF_BIRTH,
            &pd.to_wikidata_time(),
            pd.wikidata_precision(),
        );
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        Entry::add_claim_or_references(item, claim);
    }
    if let Some(pd) = died {
        let snak = Snak::new_time(
            wp::P_DATE_OF_DEATH,
            &pd.to_wikidata_time(),
            pd.wikidata_precision(),
        );
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        Entry::add_claim_or_references(item, claim);
    }
    Ok(())
}

async fn add_coordinates_to_item(
    ctx: &AppState,
    entry: &Entry,
    references: &[Reference],
    item: &mut ItemEntity,
) -> Result<()> {
    if let Some(coord) = read_coordinate_location(ctx, entry.get_valid_id()?).await? {
        let snak = build_p625_snak(coord.lat(), coord.lon(), coord.precision());
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        Entry::add_claim_or_references(item, claim);
    }
    Ok(())
}

async fn add_descriptions_to_item(
    ctx: &AppState,
    entry: &Entry,
    language: String,
    use_ext_desc: bool,
    item: &mut ItemEntity,
) -> Result<()> {
    let mut descriptions = read_language_descriptions(ctx, entry.get_valid_id()?).await?;
    if use_ext_desc && !entry.ext_desc.is_empty() {
        descriptions.insert(language.to_owned(), entry.ext_desc.to_owned());
    }
    for (lang, desc) in descriptions {
        if item.description_in_locale(&lang).is_none() {
            let desc = LocaleString::new(&lang, &desc);
            item.descriptions_mut().push(desc);
        }
    }
    Ok(())
}

async fn add_name_and_aliases_to_item(
    ctx: &AppState,
    entry: &Entry,
    language: &str,
    item: &mut ItemEntity,
) -> Result<()> {
    let mut aliases = read_aliases(ctx, entry.get_valid_id()?).await?;
    let name = Person::sanitize_name(&entry.ext_name);
    let locale_string = LocaleString::new(language, &name);
    // Q5 + a Western-script language → write the label as `mul` so it
    // covers every Latin-script language at once. Mirrors PHP behaviour.
    let names = if entry.type_name == Some("Q5".into()) && WESTERN_LANGUAGES.contains(&language) {
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
    for alias in aliases {
        if !item.labels().contains(&alias) && !item.aliases().contains(&alias) {
            item.aliases_mut().push(alias);
        }
    }
    Ok(())
}

fn add_type_to_item(entry: &Entry, references: &[Reference], item: &mut ItemEntity) {
    if let Some(tn) = &entry.type_name {
        if !tn.is_empty() {
            let snak = Snak::new_item(wp::P_INSTANCE_OF, tn);
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            Entry::add_claim_or_references(item, claim);
        }
    }
}

fn add_own_id_to_item(
    entry: &Entry,
    catalog: &Catalog,
    references: &[Reference],
    item: &mut ItemEntity,
) {
    if let (Some(prop), None) = (catalog.wd_prop(), catalog.wd_qual()) {
        let snak = Snak::new_external_id(&format!("P{prop}"), &entry.ext_id);
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        Entry::add_claim_or_references(item, claim);
    }
}

async fn read_creation_time(ctx: &AppState, entry_id: DbId) -> Option<String> {
    ctx.storage().entry_get_creation_time(entry_id).await
}

async fn read_person_dates(
    ctx: &AppState,
    entry_id: DbId,
) -> Result<(Option<PersonDate>, Option<PersonDate>)> {
    let (born_str, died_str) = ctx.storage().entry_get_person_dates(entry_id).await?;
    let born = born_str.as_deref().and_then(PersonDate::from_db_string);
    let died = died_str.as_deref().and_then(PersonDate::from_db_string);
    Ok((born, died))
}

async fn read_aliases(ctx: &AppState, entry_id: DbId) -> Result<Vec<LocaleString>> {
    ctx.storage().entry_get_aliases(entry_id).await
}

async fn read_language_descriptions(
    ctx: &AppState,
    entry_id: DbId,
) -> Result<HashMap<String, String>> {
    ctx.storage().entry_get_language_descriptions(entry_id).await
}

async fn read_coordinate_location(
    ctx: &AppState,
    entry_id: DbId,
) -> Result<Option<CoordinateLocation>> {
    ctx.storage().entry_get_coordinate_location(entry_id).await
}

async fn read_aux(ctx: &AppState, entry_id: DbId) -> Result<Vec<AuxiliaryRow>> {
    ctx.storage().entry_get_aux(entry_id).await
}

/// Returns the entry's multi-match list as `Q…` strings, or an empty
/// vec if there's no row (or the row is malformed). Mirrors the PHP
/// behaviour: `entry_get_multi_matches` returns 0 or 1 rows; any other
/// shape is silently flattened to empty.
async fn read_multi_match(ctx: &AppState, entry_id: DbId) -> Result<Vec<String>> {
    let rows: Vec<String> = ctx.storage().entry_get_multi_matches(entry_id).await?;
    if rows.len() != 1 {
        return Ok(vec![]);
    }
    let ret = rows
        .first()
        .ok_or_else(|| anyhow!("get_multi_match err1"))?
        .split(',')
        .map(|q| format!("Q{q}"))
        .collect();
    Ok(ret)
}

async fn write_alias(ctx: &AppState, entry_id: DbId, s: &LocaleString) -> Result<()> {
    ctx.storage()
        .entry_add_alias(entry_id, s.language(), s.value())
        .await?;
    Ok(())
}

async fn write_auxiliary_in_wikidata(
    ctx: &AppState,
    aux_id: DbId,
    in_wikidata: bool,
) -> Result<()> {
    ctx.storage()
        .entry_set_auxiliary_in_wikidata(in_wikidata, aux_id)
        .await
}

async fn write_mnm_relation(
    ctx: &AppState,
    entry_id: DbId,
    prop_numeric: PropertyId,
    target_entry_id: DbId,
) -> Result<()> {
    ctx.storage()
        .add_mnm_relation(entry_id, prop_numeric, target_entry_id)
        .await
}

async fn write_match_status(
    ctx: &AppState,
    entry_id: DbId,
    status: &str,
    is_matched: bool,
) -> Result<()> {
    let is_matched = if is_matched { 1 } else { 0 };
    ctx.storage()
        .entry_set_match_status(entry_id, status, is_matched)
        .await
}

async fn write_language_description(
    ctx: &AppState,
    entry_id: DbId,
    language: &str,
    text: Option<String>,
) -> Result<()> {
    match text {
        Some(text) => {
            ctx.storage()
                .entry_set_language_description(entry_id, language, text)
                .await?;
        }
        None => {
            ctx.storage()
                .entry_remove_language_description(entry_id, language)
                .await?;
        }
    }
    Ok(())
}

/// Set or clear the multi-match list for an entry. Empty input or
/// more than 10 candidates → remove the multi-match row entirely.
/// Both `set_multi_match` and `remove_multi_match` route through
/// here — the latter calls it with an empty list, which trips the
/// remove branch.
async fn write_multi_match(ctx: &AppState, entry_id: DbId, items: &[String]) -> Result<()> {
    let qs_numeric: Vec<String> = items
        .iter()
        .filter_map(|q| AppState::item2numeric(q))
        .map(|q| q.to_string())
        .collect();
    if qs_numeric.is_empty() || qs_numeric.len() > 10 {
        ctx.storage().entry_remove_multi_match(entry_id).await?;
        return Ok(());
    }
    let candidates = qs_numeric.join(",");
    let candidates_count = qs_numeric.len();
    ctx.storage()
        .entry_set_multi_match(entry_id, candidates, candidates_count)
        .await?;
    Ok(())
}

async fn write_coordinate_location(
    ctx: &AppState,
    entry_id: DbId,
    cl: &Option<CoordinateLocation>,
) -> Result<()> {
    let existing_cl = read_coordinate_location(ctx, entry_id).await?;
    if existing_cl == *cl {
        return Ok(());
    }
    match cl {
        Some(cl) => {
            ctx.storage()
                .entry_set_coordinate_location(entry_id, cl.lat(), cl.lon(), cl.precision())
                .await?;
        }
        None => {
            ctx.storage()
                .entry_remove_coordinate_location(entry_id)
                .await?;
        }
    }
    Ok(())
}

impl Entry {
    /// Returns an Entry object for a given entry ID.
    ///
    /// **Prefer [`EntryRepo::find`]** for new code; this static
    /// facade exists so the existing call sites in `automatch/`,
    /// `maintenance/`, `auxiliary_matcher/`, etc. don't all need to
    /// change in lockstep with the repo extraction.
    //TODO test
    pub async fn from_id(entry_id: DbId, app: &AppState) -> Result<Self> {
        EntryRepo::new(app).find(entry_id).await
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
    ///
    /// **Prefer [`EntryRepo::find_by_ext_id`]** for new code.
    //TODO test
    pub async fn from_ext_id(catalog_id: DbId, ext_id: &str, app: &AppState) -> Result<Entry> {
        EntryRepo::new(app).find_by_ext_id(catalog_id, ext_id).await
    }

    /// **Prefer [`EntryRepo::find_many`]** for new code.
    pub async fn multiple_from_ids(
        entry_ids: &[DbId],
        app: &AppState,
    ) -> Result<HashMap<DbId, Self>> {
        EntryRepo::new(app).find_many(entry_ids).await
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

    pub fn description(&self) -> &str {
        &self.ext_desc
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

    /// Before q query or an update to the entry in the database, checks if this is a valid entry ID (eg not a new entry)
    pub fn get_valid_id(&self) -> Result<DbId> {
        match self.id {
            Some(id) => Ok(id),
            None => Err(anyhow!("No entry ID set")),
        }
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

    /// Verifies the new EntryRepo API loads the same row that
    /// `Entry::from_id` does — the static method should be a thin
    /// facade over the repo, so any divergence is a regression.
    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn entry_repo_find_matches_legacy_static() {
        let app = get_test_app();
        let via_repo = EntryRepo::new(&app).find(TEST_ENTRY_ID).await.unwrap();
        let via_static = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(via_repo.id, via_static.id);
        assert_eq!(via_repo.catalog, via_static.catalog);
        assert_eq!(via_repo.ext_id, via_static.ext_id);
        assert_eq!(via_repo.ext_name, via_static.ext_name);
        assert!(via_repo.id.is_some(), "repo should return a valid entry");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn entry_repo_find_many_returns_map_keyed_by_id() {
        let app = get_test_app();
        let entries = EntryRepo::new(&app)
            .find_many(&[TEST_ENTRY_ID])
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&TEST_ENTRY_ID));
        assert!(
            entries.values().all(|e| e.id.is_some()),
            "repo should return entries with valid ids"
        );
    }

    /// Pin the EntryWriter contract: the writer wraps an Entry +
    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn entry_writer_ctx_accessor() {
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        let writer = EntryWriter::new(&app, &mut entry);
        // The writer's ctx accessor returns the borrowed context unchanged.
        let _: &AppState = writer.ctx();
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_person_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        let born = Some(PersonDate::year_month_day(1974, 5, 24));
        let died = Some(PersonDate::year_month_day(2000, 1, 1));
        assert_eq!(EntryWriter::new(&app, &mut entry).get_person_dates().await.unwrap(), (born, died));

        // Remove died
        EntryWriter::new(&app, &mut entry).set_person_dates(&born, &None).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_person_dates().await.unwrap(), (born, None));

        // Remove born
        EntryWriter::new(&app, &mut entry).set_person_dates(&None, &died).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_person_dates().await.unwrap(), (None, died));

        // Remove entire row
        EntryWriter::new(&app, &mut entry).set_person_dates(&None, &None).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_person_dates().await.unwrap(), (None, None));

        // Set back to original and check
        EntryWriter::new(&app, &mut entry).set_person_dates(&born, &died).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_person_dates().await.unwrap(), (born, died));
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_coordinate_location() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();

        // Save whatever is currently in the DB so we can restore it at the end
        let original = EntryWriter::new(&app, &mut entry).get_coordinate_location().await.unwrap();

        let cl = CoordinateLocation::new(1.234, -5.678);

        // Set a known value
        EntryWriter::new(&app, &mut entry).set_coordinate_location(&Some(cl)).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_coordinate_location().await.unwrap(), Some(cl));

        // Switch lat/lon
        let cl2 = CoordinateLocation::new(cl.lon(), cl.lat());
        EntryWriter::new(&app, &mut entry).set_coordinate_location(&Some(cl2)).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_coordinate_location().await.unwrap(), Some(cl2));

        // Remove
        EntryWriter::new(&app, &mut entry).set_coordinate_location(&None).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_coordinate_location().await.unwrap(), None);

        // Restore original value
        EntryWriter::new(&app, &mut entry).set_coordinate_location(&original).await.unwrap();
        assert_eq!(EntryWriter::new(&app, &mut entry).get_coordinate_location().await.unwrap(), original);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_match() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Clear
        {
            let mut e = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
            EntryWriter::new(&app, &mut e).unmatch().await.unwrap();
        }

        // Check if clear
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert!(entry.q.is_none());
        assert!(entry.user.is_none());
        assert!(entry.timestamp.is_none());

        // Set and check in-memory changes
        EntryWriter::new(&app, &mut entry).set_match("Q1", 4).await.unwrap();
        assert_eq!(entry.q, Some(1));
        assert_eq!(entry.user, Some(4));
        assert!(entry.timestamp.is_some());

        // Check in-database changes
        let mut entry2 = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry2.q, Some(1));
        assert_eq!(entry2.user, Some(4));
        assert!(entry2.timestamp.is_some());

        // Clear and check in-memory changes
        EntryWriter::new(&app, &mut entry2).unmatch().await.unwrap();
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
        EntryWriter::new(&app, &mut entry).unmatch().await.unwrap();
        let items: Vec<String> = ["Q1", "Q23456", "Q7"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        EntryWriter::new(&app, &mut entry).set_multi_match(&items).await.unwrap();
        let result1 = EntryWriter::new(&app, &mut entry).get_multi_match().await.unwrap();
        assert_eq!(result1, items);
        EntryWriter::new(&app, &mut entry).remove_multi_match().await.unwrap();
        let result2 = EntryWriter::new(&app, &mut entry).get_multi_match().await.unwrap();
        let empty: Vec<String> = vec![];
        assert_eq!(result2, empty);
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_get_item_url() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry).set_match("Q12345", 4).await.unwrap();

        assert_eq!(
            entry.get_item_url(),
            Some("https://www.wikidata.org/wiki/Q12345".to_string())
        );

        EntryWriter::new(&app, &mut entry).unmatch().await.unwrap();

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
        EntryWriter::new(&app, &mut entry).set_match("Q12345", 4).await.unwrap();
        assert!(!entry.is_unmatched());
        EntryWriter::new(&app, &mut entry).unmatch().await.unwrap();
        assert!(entry.is_unmatched());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_is_partially_matched() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry).set_match("Q12345", 0).await.unwrap();
        assert!(entry.is_partially_matched());
        EntryWriter::new(&app, &mut entry).unmatch().await.unwrap();
        assert!(!entry.is_partially_matched());
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn is_fully_matched() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry).set_match("Q12345", 4).await.unwrap();
        assert!(entry.is_fully_matched());
        EntryWriter::new(&app, &mut entry).unmatch().await.unwrap();
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
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        let s = LocaleString::new("en", "test");
        EntryWriter::new(&app, &mut entry).add_alias(&s).await.unwrap();
        assert!(EntryWriter::new(&app, &mut entry).get_aliases().await.unwrap().contains(&s));
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
            serde_json::from_str(r#"{"catalog":1,"ext_id":"x","ext_name":"n","type":"Q5"}"#)
                .unwrap();
        assert_eq!(canonical.type_name.as_deref(), Some("Q5"));
        // Legacy alias for older import files produced before the rename.
        let legacy: Entry =
            serde_json::from_str(r#"{"catalog":1,"ext_id":"x","ext_name":"n","type_name":"Q5"}"#)
                .unwrap();
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
        assert!(
            precision.is_number(),
            "precision must serialize as a number, got {precision:?}"
        );
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
