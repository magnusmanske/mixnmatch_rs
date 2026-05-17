use crate::app_state::{AppContext, ExternalServicesContext, USER_AUX_MATCH};
use crate::auxiliary_data::AuxiliaryRow;
use crate::coordinates::CoordinateLocation;
use crate::datasource::DataSource;
use crate::entry::{Entry, EntryWriter};
use crate::mnm_link::MnmLink;
use crate::person_date::PersonDate;
use crate::update_catalog::UpdateCatalogError;
use crate::{DbId, ItemId, PropertyId};
use anyhow::{Result, anyhow};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;
use wikimisc::wikibase::LocaleString;

// ── CSV-import label parsing regexes ────────────────────────────────────────
//
// These pin the column-label conventions used by the CSV / pattern
// importer:
//   - "type" cells must look like `Q\d+`
//   - "born" / "died" cells use `YYYY` / `YYYY-MM` / `YYYY-MM-DD`
//   - column labels of the form `P<digits>` carry auxiliary data
//   - column labels of the form `A<lang>` carry an alias
//   - column labels of the form `D<lang>` carry a language description
static RE_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(Q\d+)$").expect("Regexp construction"));
static RE_DATE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").expect("Regexp construction")
});
static RE_PROPERTY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^P(\d+)$").expect("Regexp construction"));
static RE_ALIAS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^A([a-z]+)$").expect("Regexp construction"));
static RE_DESCRIPTION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^D([a-z]+)$").expect("Regexp construction"));

// ── Serializable sub-structures (only for tables without existing structs) ──

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MetaPersonDates {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub born: Option<PersonDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub died: Option<PersonDate>,
}

impl MetaPersonDates {
    /// Builder helper for the common case "wrap if any date is set,
    /// otherwise None." Avoids the verbose `if x.is_some() || y.is_some()`
    /// boilerplate at every scraper construction site.
    pub fn new_or_none(born: Option<PersonDate>, died: Option<PersonDate>) -> Option<Self> {
        if born.is_some() || died.is_some() {
            Some(Self { born, died })
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaMnmRelation {
    pub property: PropertyId,
    pub target: MnmLink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaIssue {
    pub id: Option<DbId>,
    pub issue_type: String,
    pub json: serde_json::Value,
    pub status: String,
    pub user_id: Option<DbId>,
    pub resolved_ts: Option<String>,
    pub catalog_id: DbId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaKvEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaLogEntry {
    pub id: Option<DbId>,
    pub action: String,
    pub user: Option<DbId>,
    pub timestamp: Option<String>,
    pub q: Option<ItemId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaStatementText {
    pub id: Option<DbId>,
    pub property: PropertyId,
    pub text: String,
    pub in_wikidata: bool,
    pub entry_is_matched: bool,
    pub q: Option<ItemId>,
}

// ── MetaEntry ──────────────────────────────────────────────────────────────

/// A fully-resolved snapshot of an entry and all its associated data, suitable
/// for JSON serialization / deserialization.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaEntry {
    pub entry: Entry,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auxiliary: Vec<AuxiliaryRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinate: Option<CoordinateLocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub person_dates: Option<MetaPersonDates>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mnm_relations: Vec<MetaMnmRelation>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub descriptions: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<LocaleString>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<MetaIssue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kv_entries: Vec<MetaKvEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log_entries: Vec<MetaLogEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub multi_match: Vec<DbId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statement_text: Vec<MetaStatementText>,
}

impl MetaEntry {
    // ── Builder API ──────────────────────────────────────────────────
    //
    // These methods exist so callers that produce records in code
    // (CSV row parsing, scrapers) don't need to know about the
    // wire-format wrapping (`person_dates`) or invent their own
    // dedupe logic (`auxiliary` is a Vec; `add_aux` dedupes by content
    // so the storage layer's REPLACE INTO doesn't get redundant
    // round-trips).

    /// Construct a new MetaEntry seeded with just `(catalog, ext_id)`
    /// and empty defaults everywhere else.
    pub fn new_for_catalog_ext_id(catalog: DbId, ext_id: &str) -> Self {
        let mut me = Self::default();
        me.entry.catalog = catalog;
        me.entry.ext_id = ext_id.to_string();
        me
    }

    /// Push an auxiliary value, deduping on `(prop_numeric, value)`.
    /// Storage layer uses `REPLACE INTO`, so an explicit dedup here just
    /// trims redundant round-trips — duplicate `(prop, value)` pairs
    /// from the same CSV row or scraper pass produce one DB write.
    pub fn add_aux(&mut self, prop_numeric: PropertyId, value: impl Into<String>) {
        let value = value.into();
        let exists = self
            .auxiliary
            .iter()
            .any(|a| a.prop_numeric() == prop_numeric && a.value() == value);
        if !exists {
            self.auxiliary.push(AuxiliaryRow::new(prop_numeric, value));
        }
    }

    /// Set the `born` date, lazily allocating the `person_dates` wrapper.
    /// Preserves any previously-set `died`.
    pub fn set_born(&mut self, date: PersonDate) {
        self.person_dates.get_or_insert_with(Default::default).born = Some(date);
    }

    /// Set the `died` date, lazily allocating the `person_dates` wrapper.
    /// Preserves any previously-set `born`.
    pub fn set_died(&mut self, date: PersonDate) {
        self.person_dates.get_or_insert_with(Default::default).died = Some(date);
    }

    /// Push an alias. The `aliases` table is `INSERT IGNORE`, so the
    /// builder is intentionally lossless on duplicates here — callers
    /// that care can deduplicate beforehand.
    pub fn add_alias(&mut self, language: &str, label: &str) {
        self.aliases.push(LocaleString::new(language, label));
    }

    /// Insert or overwrite a description for a given language.
    pub fn set_description(&mut self, language: &str, text: &str) {
        self.descriptions.insert(language.to_string(), text.to_string());
    }

    /// `born` date, flattening the `person_dates` wrapper. Convenience
    /// accessor for tests/callers that don't want to navigate the
    /// `Option<MetaPersonDates>` two-step.
    pub fn born(&self) -> Option<PersonDate> {
        self.person_dates.and_then(|pd| pd.born)
    }

    /// `died` date, flattening the `person_dates` wrapper.
    pub fn died(&self) -> Option<PersonDate> {
        self.person_dates.and_then(|pd| pd.died)
    }

    /// Construct a MetaEntry from a CSV row using the column / pattern
    /// mapping declared by the catalog's `DataSource`. Each cell is
    /// dispatched through [`MetaEntry::process_cell`] which decodes the
    /// label conventions (`name`, `desc`, `url`, `q`, `type`, `born`,
    /// `died`, `P<num>`, `A<lang>`, `D<lang>`). Missing `type_name` and
    /// `ext_url` are filled from the datasource defaults.
    pub fn from_csv_row(
        row: &csv::StringRecord,
        datasource: &mut DataSource,
    ) -> Result<Self> {
        let ext_id = row
            .get(datasource.ext_id_column)
            .ok_or(anyhow!("No external ID for entry"))?;
        let mut me = Self {
            entry: Entry::new_from_catalog_and_ext_id(datasource.catalog_id, ext_id),
            ..Default::default()
        };

        for (label, col_num) in &datasource.colmap {
            if let Some(cell) = row.get(*col_num) {
                me.process_cell(label, cell)?;
            }
        }
        for pattern in &datasource.patterns {
            if let Some(cell) = row.get(pattern.column_number) {
                if let Some(new_cell) = Self::regex_capture(&pattern.pattern, cell) {
                    me.process_cell(&pattern.use_column_label, &new_cell)?;
                }
            }
        }

        if me.entry.type_name.is_none() {
            me.entry.type_name.clone_from(&datasource.default_type);
        }
        if me.entry.ext_url.is_empty() {
            if let Some(pattern) = &datasource.url_pattern {
                me.entry.ext_url = pattern.replace("$1", &me.entry.ext_id);
            }
        }

        Ok(me)
    }

    /// Dispatch a `(label, cell)` pair from a CSV row. Pattern-prefixed
    /// labels (`A<lang>`, `D<lang>`, `P<num>`) are handled first; bare
    /// labels (`name`, `desc`, `url`, `q`, `type`, `born`, `died`,
    /// `id`) fall through to the explicit `match`. Unknown labels
    /// return `UpdateCatalogError::UnknownColumnLabel`.
    fn process_cell(&mut self, label: &str, cell: &str) -> Result<()> {
        if self.parse_alias(label, cell)
            || self.parse_description(label, cell)
            || self.parse_property(label, cell)?
        {
            return Ok(());
        }
        match label {
            "id" => { /* the ext_id was already set by from_csv_row */ }
            "name" => self.entry.ext_name = cell.to_string(),
            "desc" => self.entry.ext_desc = cell.to_string(),
            "url" => self.entry.ext_url = cell.to_string(),
            "q" | "autoq" => {
                self.entry.q =
                    cell.replace('Q', "").parse::<isize>().ok().filter(|&i| i > 0);
                if self.entry.q.is_some() {
                    // Matches the scraper-style import convention: a `q`
                    // on a CSV row is attributed to the auxiliary-data
                    // matcher (user 4). `set_match` stamps the timestamp
                    // when the row eventually hits storage, so we don't
                    // set it here.
                    self.entry.user = Some(USER_AUX_MATCH);
                }
            }
            "type" => self.entry.type_name = Self::parse_type(cell),
            "born" => {
                if let Some(d) = Self::parse_date(cell) {
                    self.set_born(d);
                }
            }
            "died" => {
                if let Some(d) = Self::parse_date(cell) {
                    self.set_died(d);
                }
            }
            other => {
                return Err(UpdateCatalogError::UnknownColumnLabel(format!(
                    "Don't understand label '{other}'"
                ))
                .into());
            }
        }
        Ok(())
    }

    /// Decode a `type` cell. Empty / non-Q values yield `None`.
    pub fn parse_type(type_name: &str) -> Option<String> {
        Self::regex_capture(&RE_TYPE, type_name)
    }

    /// Decode a `born` / `died` cell. Accepts `YYYY`, `YYYY-MM`,
    /// `YYYY-MM-DD`; anything else yields `None`.
    pub fn parse_date(date: &str) -> Option<PersonDate> {
        let captured = Self::regex_capture(&RE_DATE, date)?;
        PersonDate::from_db_string(&captured)
    }

    /// If `label` is `A<lang>`, push an alias and return true. Otherwise
    /// the label is not an alias and `false` is returned so the dispatcher
    /// can try the next handler.
    fn parse_alias(&mut self, label: &str, cell: &str) -> bool {
        match Self::regex_capture(&RE_ALIAS, label) {
            Some(language) => {
                self.aliases.push(LocaleString::new(language, cell.to_string()));
                true
            }
            None => false,
        }
    }

    /// If `label` is `D<lang>`, insert a description and return true.
    fn parse_description(&mut self, label: &str, cell: &str) -> bool {
        match Self::regex_capture(&RE_DESCRIPTION, label) {
            Some(language) => {
                self.descriptions.insert(language, cell.to_string());
                true
            }
            None => false,
        }
    }

    /// If `label` is `P<num>`, route the value (or `|`-separated values)
    /// into auxiliary or — for P625 — into the coordinate field.
    fn parse_property(&mut self, label: &str, cell: &str) -> Result<bool> {
        let property_num = match Self::regex_capture(&RE_PROPERTY, label) {
            Some(s) => s.parse::<usize>()?,
            None => return Ok(false),
        };

        // P625 is treated specially: coordinates have their own
        // dedicated table and the cell shape (`lat/lon` or
        // `POINT(lat lon)`) doesn't fit the auxiliary row format.
        // TODO: extend to other location properties; possibly also
        // P569/P570 dates.
        if property_num == 625 {
            if let Some(coord) = CoordinateLocation::parse(cell) {
                self.coordinate = Some(coord);
            }
        } else {
            for part in cell.split('|') {
                let part = part.trim();
                if !part.is_empty() {
                    self.add_aux(property_num, part);
                }
            }
        }

        Ok(true)
    }

    /// Extract capture group 1 of `regexp` against `text` as an owned
    /// String. Returns `None` when the regex doesn't match.
    fn regex_capture(regexp: &Regex, text: &str) -> Option<String> {
        regexp.captures(text)?.get(1).map(|m| m.as_str().to_string())
    }

    // ── Repository API (load from storage) ───────────────────────────

    /// Load a complete MetaEntry from storage for a given entry ID.
    pub async fn from_entry_id(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Self> {
        let entry = Entry::from_id(entry_id, app).await?;
        Self::from_entry(&entry, app).await
    }

    /// Load a complete MetaEntry from an already-loaded Entry.
    pub async fn from_entry(entry: &Entry, app: &dyn ExternalServicesContext) -> Result<Self> {
        let entry_id = entry.id.ok_or_else(|| anyhow!("Entry has no id"))?;
        let storage = app.storage();

        // Run independent queries concurrently
        let (aux_result, coord_result, person_dates_result, aliases_result, descriptions_result) = tokio::join!(
            storage.entry_get_aux(entry_id),
            storage.entry_get_coordinate_location(entry_id),
            storage.entry_get_person_dates(entry_id),
            storage.entry_get_aliases(entry_id),
            storage.entry_get_language_descriptions(entry_id),
        );

        let auxiliary = aux_result?;
        let coordinate = coord_result?;

        let (born_str, died_str) = person_dates_result?;
        let born = born_str.as_deref().and_then(PersonDate::from_db_string);
        let died = died_str.as_deref().and_then(PersonDate::from_db_string);
        let person_dates = if born.is_some() || died.is_some() {
            Some(MetaPersonDates { born, died })
        } else {
            None
        };

        let aliases = aliases_result?;

        let descriptions = descriptions_result?;

        // Fetch additional data via direct SQL through storage
        let (mnm_relations, issues, kv_entries, log_entries, multi_match_result, statement_text) = tokio::join!(
            Self::load_mnm_relations(entry_id, app),
            Self::load_issues(entry_id, app),
            Self::load_kv_entries(entry_id, app),
            Self::load_log_entries(entry_id, app),
            storage.entry_get_multi_matches(entry_id),
            Self::load_statement_text(entry_id, app),
        );

        // Parse multi_match candidates string ("1,23456,7") into Vec<DbId>
        let multi_match: Vec<DbId> = multi_match_result?
            .first()
            .map(|s| {
                s.split(',')
                    .filter_map(|q| q.parse::<DbId>().ok())
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            entry: entry.clone(),
            auxiliary,
            coordinate,
            person_dates,
            mnm_relations: mnm_relations?,
            descriptions,
            aliases,
            issues: issues?,
            kv_entries: kv_entries?,
            log_entries: log_entries?,
            multi_match,
            statement_text: statement_text?,
        })
    }

    async fn load_mnm_relations(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Vec<MetaMnmRelation>> {
        app.storage().meta_entry_get_mnm_relations(entry_id).await
    }

    async fn load_issues(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Vec<MetaIssue>> {
        app.storage().meta_entry_get_issues(entry_id).await
    }

    async fn load_kv_entries(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Vec<MetaKvEntry>> {
        app.storage().meta_entry_get_kv_entries(entry_id).await
    }

    async fn load_log_entries(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Vec<MetaLogEntry>> {
        app.storage().meta_entry_get_log_entries(entry_id).await
    }

    async fn load_statement_text(entry_id: DbId, app: &dyn ExternalServicesContext) -> Result<Vec<MetaStatementText>> {
        app.storage().meta_entry_get_statement_text(entry_id).await
    }

    /// Serialize to JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }

    /// Serialize to pretty JSON string.
    pub fn to_json_pretty(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| anyhow!(e))
    }

    /// Deserialize from a JSON string.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| anyhow!(e))
    }

    /// Create a new entry (and all associated data) from this MetaEntry.
    /// Returns the new entry ID.
    pub async fn create_in_storage(&self, app: &dyn AppContext) -> Result<DbId> {
        let mut entry = Entry::new_from_catalog_and_ext_id(self.entry.catalog, &self.entry.ext_id);
        // clone_from reuses the destination's existing String/Option<String>
        // allocation when the lengths line up — cheaper than a fresh clone
        // for the new-Entry hot path during catalog import.
        entry.ext_url.clone_from(&self.entry.ext_url);
        entry.ext_name.clone_from(&self.entry.ext_name);
        entry.ext_desc.clone_from(&self.entry.ext_desc);
        entry.type_name.clone_from(&self.entry.type_name);
        entry.random = self.entry.random;

        let new_id = EntryWriter::new(app, &mut entry).insert_as_new().await?;
        let entry_id = new_id.ok_or_else(|| anyhow!("Failed to insert new entry"))?;

        self.write_associated_data(&mut entry, app).await?;

        // Set match if present (after associated data so log/issues are in place).
        // set_match always stamps `TimeStamp::now()` and ignores any inbound
        // timestamp — that is the canonical defense against import-time
        // timestamp spoofing.
        if let Some(q) = self.entry.q {
            if q > 0 {
                let user = self.entry.user.unwrap_or(0);
                EntryWriter::new(app, &mut entry)
                    .set_match(&format!("Q{q}"), user)
                    .await?;
            }
        }

        Ok(entry_id)
    }

    /// Update an existing entry with data from this MetaEntry.
    /// The entry must already exist in storage.
    pub async fn update_in_storage(&self, app: &dyn AppContext) -> Result<()> {
        let entry_id = self
            .entry
            .id
            .ok_or_else(|| anyhow!("MetaEntry has no entry id for update"))?;

        let mut entry = Entry::from_id(entry_id, app).await?;

        // Update core entry fields through EntryWriter so the no-op guard
        // (skip when unchanged) and field-level write helpers stay
        // canonical. Sequential rather than the previous tokio::join!:
        // each is a single small UPDATE, and the guard makes most calls
        // no-ops when re-importing the same data.
        {
            let mut ew = EntryWriter::new(app, &mut entry);
            ew.set_ext_name(&self.entry.ext_name).await?;
            ew.set_ext_desc(&self.entry.ext_desc).await?;
            ew.set_ext_id(&self.entry.ext_id).await?;
            ew.set_ext_url(&self.entry.ext_url).await?;
            ew.set_type_name(self.entry.type_name.clone()).await?;
        }

        // Clear existing associated data; these are bulk DELETEs without
        // an EntryWriter equivalent, so they stay on the storage layer.
        let storage = app.storage();
        let (r11, r12, r13, r14, r15, r16, r17) = tokio::join!(
            storage.meta_entry_delete_auxiliary(entry_id),
            storage.entry_remove_coordinate_location(entry_id),
            storage.entry_delete_person_dates(entry_id),
            storage.meta_entry_delete_aliases(entry_id),
            storage.meta_entry_delete_descriptions(entry_id),
            storage.meta_entry_delete_mnm_relations(entry_id),
            storage.meta_entry_delete_kv_entries(entry_id),
        );
        r11?;
        r12?;
        r13?;
        r14?;
        r15?;
        r16?;
        r17?;

        self.write_associated_data(&mut entry, app).await?;

        if let Some(q) = self.entry.q {
            if q > 0 {
                let user = self.entry.user.unwrap_or(0);
                EntryWriter::new(app, &mut entry)
                    .set_match(&format!("Q{q}"), user)
                    .await?;
            }
        }

        Ok(())
    }

    /// Update an existing entry using **merge** semantics — the scraper
    /// contract. Empty scalar fields (`ext_name`, `ext_desc`, `ext_url`)
    /// and a `None` `type_name` on the incoming MetaEntry mean "leave
    /// the stored value alone"; matches are only assigned when the
    /// entry is currently unmatched; aliases / auxiliary / descriptions
    /// are add-only (the underlying storage primitives are
    /// `REPLACE INTO` / `INSERT IGNORE` so no pre-fetch is needed).
    ///
    /// The full-replace contract is in [`update_in_storage`]; the JSON
    /// import path uses that one because the user authoritatively
    /// provides the new state.
    ///
    /// `entry` is the previously-loaded record being updated; its
    /// fields are mutated in place to reflect the writes.
    pub async fn update_merge_in_storage(
        &self,
        entry: &mut Entry,
        app: &dyn AppContext,
    ) -> Result<()> {
        // Scalar fields: skip-empty merge.
        {
            let mut ew = EntryWriter::new(app, entry);
            if !self.entry.ext_name.is_empty() {
                ew.set_ext_name(&self.entry.ext_name).await?;
            }
            if !self.entry.ext_desc.is_empty() {
                ew.set_ext_desc(&self.entry.ext_desc).await?;
            }
            if self.entry.type_name.is_some() {
                ew.set_type_name(self.entry.type_name.clone()).await?;
            }
            if !self.entry.ext_url.is_empty() {
                ew.set_ext_url(&self.entry.ext_url).await?;
            }
            // Match is only assigned when the entry is currently
            // unmatched — never override an existing match through
            // the merge path. Uses USER_AUX_MATCH (4) for the
            // scraper-style import convention.
            if ew.as_entry().q.is_none() {
                if let Some(q) = self.entry.q {
                    ew.set_match(&format!("Q{q}"), crate::app_state::USER_AUX_MATCH)
                        .await?;
                }
            }
        }

        // Associated data: add-only (REPLACE/INSERT IGNORE at storage).
        self.write_associated_data(entry, app).await?;
        Ok(())
    }

    /// Write all associated data for an entry through EntryWriter so every
    /// import path (JSON, CSV, scrapers) shares the same overview-counter
    /// bumps, log entries, and per-field guards. `entry` must already
    /// have an id (i.e. either freshly inserted or loaded from storage).
    ///
    /// `pub(crate)` because [`update_merge_in_storage`] calls it without
    /// the preceding bulk-delete step that [`update_in_storage`] runs
    /// for the JSON full-replace path.
    pub(crate) async fn write_associated_data(
        &self,
        entry: &mut Entry,
        app: &dyn AppContext,
    ) -> Result<()> {
        // Auxiliary
        for aux in &self.auxiliary {
            EntryWriter::new(app, entry)
                .set_auxiliary(aux.prop_numeric(), Some(aux.value().to_string()))
                .await?;
        }

        // Coordinate
        if self.coordinate.is_some() {
            EntryWriter::new(app, entry)
                .set_coordinate_location(&self.coordinate)
                .await?;
        }

        // Person dates
        if let Some(pd) = &self.person_dates {
            if pd.born.is_some() || pd.died.is_some() {
                EntryWriter::new(app, entry)
                    .set_person_dates(&pd.born, &pd.died)
                    .await?;
            }
        }

        // Descriptions
        for (lang, text) in &self.descriptions {
            EntryWriter::new(app, entry)
                .set_language_description(lang, Some(text.clone()))
                .await?;
        }

        // Aliases
        for alias in &self.aliases {
            EntryWriter::new(app, entry).add_alias(alias).await?;
        }

        // MnM relations (only EntryId targets can be written)
        for rel in &self.mnm_relations {
            if let Some(target_id) = rel.target.resolve_entry_id(app).await? {
                EntryWriter::new(app, entry)
                    .add_mnm_relation(rel.property, target_id)
                    .await?;
            }
        }

        // KV entries — no EntryWriter helper exists for this table; the
        // direct storage call is the only path.
        for kv in &self.kv_entries {
            app.storage()
                .meta_entry_set_kv_entry(
                    entry.get_valid_id()?,
                    &kv.key,
                    &kv.value,
                )
                .await?;
        }

        // NOTE: Issues, log entries, and multi-match are read-only in MetaEntry.
        // Issues are managed via the Issue API. Log entries are historical records
        // created as side effects of other operations. Multi-match is computed by
        // the matching system. None of these should be written from a MetaEntry.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mnm_link_serde_entry_id() {
        let link = MnmLink::EntryId(42);
        let json = serde_json::to_string(&link).unwrap();
        let back: MnmLink = serde_json::from_str(&json).unwrap();
        assert_eq!(link, back);
    }

    #[test]
    fn test_mnm_link_serde_catalog_ext_id() {
        let link = MnmLink::CatalogExtId {
            catalog: 5,
            ext_id: "abc123".to_string(),
        };
        let json = serde_json::to_string(&link).unwrap();
        let back: MnmLink = serde_json::from_str(&json).unwrap();
        assert_eq!(link, back);
    }

    #[test]
    fn test_mnm_link_serde_qid() {
        let link = MnmLink::WikidataQid(42);
        let json = serde_json::to_string(&link).unwrap();
        let back: MnmLink = serde_json::from_str(&json).unwrap();
        assert_eq!(link, back);
        assert_eq!(link.qid_string(), Some("Q42".to_string()));
    }

    #[test]
    fn test_meta_entry_roundtrip() {
        let me = MetaEntry {
            entry: Entry {
                id: Some(1),
                catalog: 100,
                ext_id: "ext1".to_string(),
                ext_url: "http://example.com".to_string(),
                ext_name: "Test Entry".to_string(),
                ext_desc: "A test".to_string(),
                q: Some(42),
                user: Some(1),
                timestamp: Some("20240101120000".to_string()),
                random: 0.5,
                type_name: Some("Q5".to_string()),
            },
            auxiliary: vec![AuxiliaryRow::new(214, "12345".to_string())],
            coordinate: Some(CoordinateLocation::new_with_precision(51.5, -0.1, None)),
            person_dates: Some(MetaPersonDates {
                born: Some(PersonDate::year_month_day(1950, 1, 1)),
                died: None,
            }),
            mnm_relations: vec![MetaMnmRelation {
                property: 170,
                target: MnmLink::EntryId(99),
            }],
            descriptions: {
                let mut m = HashMap::new();
                m.insert("en".to_string(), "English description".to_string());
                m
            },
            aliases: vec![LocaleString::new("en", "Alias One")],
            issues: vec![],
            kv_entries: vec![MetaKvEntry {
                key: "source".to_string(),
                value: "test".to_string(),
            }],
            log_entries: vec![MetaLogEntry {
                id: Some(1),
                action: "add_q".to_string(),
                user: Some(1),
                timestamp: Some("20240101120000".to_string()),
                q: Some(42),
            }],
            multi_match: vec![42, 99],
            statement_text: vec![MetaStatementText {
                id: Some(1),
                property: 31,
                text: "Q5".to_string(),
                in_wikidata: true,
                entry_is_matched: true,
                q: Some(42),
            }],
        };

        let json = me.to_json().unwrap();
        let back = MetaEntry::from_json(&json).unwrap();
        // `id`, `timestamp`, and `random` are skip_deserializing on Entry
        // — the wire format carries them out (for export / debug) but
        // refuses to read them back in. That's the canonical defense
        // against an import file claiming a server-side PK or stamping
        // a forged timestamp.
        assert_eq!(back.entry.id, None);
        assert_eq!(back.entry.timestamp, None);
        assert!(back.entry.random.abs() < f64::EPSILON);
        // Other fields round-trip normally.
        assert_eq!(back.entry.catalog, 100);
        assert_eq!(back.entry.q, Some(42));
        assert_eq!(back.entry.user, Some(1));
        assert_eq!(back.auxiliary.len(), 1);
        assert_eq!(back.auxiliary[0].prop_numeric(), 214);
        let within_tolerance = (back.coordinate.unwrap().lat() - 51.5).abs() < 0.0001;
        assert!(within_tolerance, "latitude is not within tolerance");
        assert_eq!(back.mnm_relations.len(), 1);
        assert_eq!(back.descriptions.get("en").unwrap(), "English description");
        assert_eq!(back.aliases.len(), 1);
        assert_eq!(back.kv_entries.len(), 1);
        assert_eq!(back.log_entries.len(), 1);
        assert_eq!(back.multi_match, vec![42, 99]);
        assert_eq!(back.statement_text.len(), 1);
        assert_eq!(back.statement_text[0].property, 31);
        assert_eq!(back.statement_text[0].text, "Q5");
    }

    #[test]
    fn test_mnm_link_from_q() {
        let link = MnmLink::from_q("Q42");
        assert_eq!(link, Some(MnmLink::WikidataQid(42)));
        assert!(MnmLink::from_q("invalid").is_none());
    }

    #[test]
    fn test_meta_entry_pretty_json() {
        let me = MetaEntry {
            entry: Entry {
                id: None,
                catalog: 1,
                ext_id: "x".to_string(),
                ext_url: String::new(),
                ext_name: "Name".to_string(),
                ext_desc: String::new(),
                q: None,
                user: None,
                timestamp: None,
                random: 0.0,
                type_name: None,
            },
            auxiliary: vec![],
            coordinate: None,
            person_dates: None,
            mnm_relations: vec![],
            descriptions: HashMap::new(),
            aliases: vec![],
            issues: vec![],
            kv_entries: vec![],
            log_entries: vec![],
            multi_match: vec![],
            statement_text: vec![],
        };
        let pretty = me.to_json_pretty().unwrap();
        assert!(pretty.contains('\n'));
        let back = MetaEntry::from_json(&pretty).unwrap();
        assert_eq!(back.entry.ext_name, "Name");
    }

    #[test]
    fn test_person_date_from_db_string() {
        // Year only
        let d1 = PersonDate::from_db_string("1950").unwrap();
        assert_eq!(d1, PersonDate::year_only(1950));

        // Year-month
        let d2 = PersonDate::from_db_string("1950-03").unwrap();
        assert_eq!(d2, PersonDate::year_month(1950, 3));

        // Year-month-day
        let d3 = PersonDate::from_db_string("1950-03-15").unwrap();
        assert_eq!(d3, PersonDate::year_month_day(1950, 3, 15));

        // BCE
        let d4 = PersonDate::from_db_string("-500").unwrap();
        assert_eq!(d4, PersonDate::year_only(-500));

        let d5 = PersonDate::from_db_string("-500-06-15").unwrap();
        assert_eq!(d5, PersonDate::year_month_day(-500, 6, 15));

        // Empty / invalid
        assert!(PersonDate::from_db_string("").is_none());
        assert!(PersonDate::from_db_string("abc").is_none());
        assert!(PersonDate::from_db_string("1950-13").is_none()); // invalid month
        assert!(PersonDate::from_db_string("1950-01-32").is_none()); // invalid day
    }

    #[test]
    fn test_person_date_to_db_string() {
        assert_eq!(PersonDate::year_only(2021).to_db_string(), "2021");
        assert_eq!(PersonDate::year_month(2021, 1).to_db_string(), "2021-01");
        assert_eq!(
            PersonDate::year_month_day(2021, 1, 5).to_db_string(),
            "2021-01-05"
        );
        assert_eq!(PersonDate::year_only(-500).to_db_string(), "-500");
        assert_eq!(
            PersonDate::year_month_day(-500, 6, 15).to_db_string(),
            "-500-06-15"
        );
    }

    #[test]
    fn test_person_date_roundtrip_db() {
        let dates = [
            "1950",
            "1950-03",
            "1950-03-15",
            "-500",
            "-500-06",
            "-500-06-15",
        ];
        for s in dates {
            let d = PersonDate::from_db_string(s).unwrap();
            assert_eq!(d.to_db_string(), s);
        }
    }

    #[test]
    fn test_person_date_wikidata() {
        let d1 = PersonDate::year_only(2021);
        assert_eq!(d1.to_wikidata_time(), "+2021-01-01T00:00:00Z");
        assert_eq!(d1.wikidata_precision(), 9);

        let d2 = PersonDate::year_month(2021, 6);
        assert_eq!(d2.to_wikidata_time(), "+2021-06-01T00:00:00Z");
        assert_eq!(d2.wikidata_precision(), 10);

        let d3 = PersonDate::year_month_day(2021, 6, 15);
        assert_eq!(d3.to_wikidata_time(), "+2021-06-15T00:00:00Z");
        assert_eq!(d3.wikidata_precision(), 11);

        let d4 = PersonDate::year_only(-500);
        assert_eq!(d4.to_wikidata_time(), "-500-01-01T00:00:00Z");
        assert_eq!(d4.wikidata_precision(), 9);
    }

    #[test]
    fn test_person_date_serde() {
        let d = PersonDate::year_month_day(1950, 3, 15);
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, "\"1950-03-15\"");
        let back: PersonDate = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);

        // BCE
        let d2 = PersonDate::year_only(-500);
        let json2 = serde_json::to_string(&d2).unwrap();
        assert_eq!(json2, "\"-500\"");
        let back2: PersonDate = serde_json::from_str(&json2).unwrap();
        assert_eq!(d2, back2);
    }

    #[test]
    fn test_meta_person_dates_serde() {
        let pd = MetaPersonDates {
            born: Some(PersonDate::year_month_day(1950, 1, 1)),
            died: None,
        };
        let json = serde_json::to_string(&pd).unwrap();
        let back: MetaPersonDates = serde_json::from_str(&json).unwrap();
        assert_eq!(back.born.unwrap(), PersonDate::year_month_day(1950, 1, 1));
        assert!(back.died.is_none());
    }

    // ── Builder API ──────────────────────────────────────────────────
    //
    // Builder methods exist so callers (CSV row parsing, scrapers)
    // don't need to know about the wire-format wrapping
    // (`person_dates`) or invent their own dedupe logic (`auxiliary`
    // is a Vec; `add_aux` dedupes by content).

    #[test]
    fn default_meta_entry_is_empty() {
        let me = MetaEntry::default();
        assert_eq!(me.entry.catalog, 0);
        assert!(me.entry.ext_id.is_empty());
        assert!(me.auxiliary.is_empty());
        assert!(me.coordinate.is_none());
        assert!(me.person_dates.is_none());
        assert!(me.aliases.is_empty());
        assert!(me.descriptions.is_empty());
        assert!(me.mnm_relations.is_empty());
        assert!(me.kv_entries.is_empty());
    }

    #[test]
    fn new_for_catalog_ext_id_seeds_entry_only() {
        let me = MetaEntry::new_for_catalog_ext_id(42, "abc-123");
        assert_eq!(me.entry.catalog, 42);
        assert_eq!(me.entry.ext_id, "abc-123");
        // Everything else is default.
        assert!(me.auxiliary.is_empty());
        assert!(me.person_dates.is_none());
        assert!(me.coordinate.is_none());
    }

    #[test]
    fn add_aux_dedupes_by_content() {
        let mut me = MetaEntry::default();
        me.add_aux(214, "12345");
        me.add_aux(214, "12345");
        assert_eq!(me.auxiliary.len(), 1);
        assert_eq!(me.auxiliary[0].prop_numeric(), 214);
        assert_eq!(me.auxiliary[0].value(), "12345");
    }

    #[test]
    fn add_aux_keeps_distinct_values_on_same_prop() {
        let mut me = MetaEntry::default();
        me.add_aux(31, "Q5");
        me.add_aux(31, "Q515");
        assert_eq!(me.auxiliary.len(), 2);
    }

    #[test]
    fn add_aux_keeps_same_value_on_different_props() {
        let mut me = MetaEntry::default();
        me.add_aux(214, "12345");
        me.add_aux(213, "12345");
        assert_eq!(me.auxiliary.len(), 2);
    }

    #[test]
    fn set_born_on_empty_creates_wrapper() {
        let mut me = MetaEntry::default();
        me.set_born(PersonDate::year_only(1950));
        let pd = me.person_dates.as_ref().expect("person_dates set");
        assert_eq!(pd.born, Some(PersonDate::year_only(1950)));
        assert!(pd.died.is_none());
    }

    #[test]
    fn set_died_on_empty_creates_wrapper() {
        let mut me = MetaEntry::default();
        me.set_died(PersonDate::year_only(2020));
        let pd = me.person_dates.as_ref().expect("person_dates set");
        assert!(pd.born.is_none());
        assert_eq!(pd.died, Some(PersonDate::year_only(2020)));
    }

    #[test]
    fn set_born_preserves_existing_died() {
        let mut me = MetaEntry::default();
        me.set_died(PersonDate::year_only(2020));
        me.set_born(PersonDate::year_only(1950));
        let pd = me.person_dates.as_ref().unwrap();
        assert_eq!(pd.born, Some(PersonDate::year_only(1950)));
        assert_eq!(pd.died, Some(PersonDate::year_only(2020)));
    }

    #[test]
    fn set_born_overwrites_previous_born() {
        let mut me = MetaEntry::default();
        me.set_born(PersonDate::year_only(1950));
        me.set_born(PersonDate::year_only(1951));
        assert_eq!(
            me.person_dates.as_ref().unwrap().born,
            Some(PersonDate::year_only(1951))
        );
    }

    #[test]
    fn add_alias_stores_locale_string() {
        let mut me = MetaEntry::default();
        me.add_alias("en", "John");
        assert_eq!(me.aliases.len(), 1);
        assert_eq!(me.aliases[0], LocaleString::new("en", "John"));
    }

    #[test]
    fn add_alias_accepts_duplicates_in_vec() {
        // No dedupe — aliases table is INSERT IGNORE so the DB layer
        // is authoritative. The builder is intentionally lossless on
        // the in-memory side so a caller can spot duplicates if it
        // matters.
        let mut me = MetaEntry::default();
        me.add_alias("en", "John");
        me.add_alias("en", "John");
        assert_eq!(me.aliases.len(), 2);
    }

    #[test]
    fn set_description_inserts_and_overwrites_by_language() {
        let mut me = MetaEntry::default();
        me.set_description("en", "first");
        assert_eq!(me.descriptions.get("en"), Some(&"first".to_string()));
        me.set_description("en", "second");
        assert_eq!(me.descriptions.get("en"), Some(&"second".to_string()));
        assert_eq!(me.descriptions.len(), 1);
    }

    #[test]
    fn set_description_keeps_independent_languages() {
        let mut me = MetaEntry::default();
        me.set_description("en", "painter");
        me.set_description("de", "Maler");
        assert_eq!(me.descriptions.len(), 2);
    }

    #[test]
    fn born_died_accessors_flatten_wrapper() {
        let mut me = MetaEntry::default();
        assert!(me.born().is_none());
        assert!(me.died().is_none());
        me.set_born(PersonDate::year_only(1950));
        me.set_died(PersonDate::year_only(2020));
        assert_eq!(me.born(), Some(PersonDate::year_only(1950)));
        assert_eq!(me.died(), Some(PersonDate::year_only(2020)));
    }

    #[test]
    fn born_died_accessors_when_only_one_set() {
        let mut me = MetaEntry::default();
        me.set_born(PersonDate::year_only(1950));
        assert_eq!(me.born(), Some(PersonDate::year_only(1950)));
        assert!(me.died().is_none());
    }

    // ── CSV row parsing ──────────────────────────────────────────────
    //
    // These tests cover the CSV column-label conventions: bare labels
    // (`name`, `desc`, `url`, `q`, `type`, `born`, `died`), prefixed
    // labels (`A<lang>` alias, `D<lang>` description, `P<num>`
    // auxiliary / coordinate), and the regex constraints on `type` /
    // `born` / `died` cells.

    #[test]
    fn parse_type_accepts_q_only() {
        assert_eq!(MetaEntry::parse_type("Q5"), Some("Q5".to_string()));
        assert_eq!(MetaEntry::parse_type("Q12345"), Some("Q12345".to_string()));
    }

    #[test]
    fn parse_type_rejects_non_q() {
        assert_eq!(MetaEntry::parse_type(""), None);
        assert_eq!(MetaEntry::parse_type("12345"), None);
        assert_eq!(MetaEntry::parse_type("foobar"), None);
        assert_eq!(MetaEntry::parse_type("P123"), None);
    }

    #[test]
    fn parse_date_accepts_ymd_ym_y() {
        assert_eq!(
            MetaEntry::parse_date("2022-11-03"),
            Some(PersonDate::year_month_day(2022, 11, 3))
        );
        assert_eq!(
            MetaEntry::parse_date("2022-11"),
            Some(PersonDate::year_month(2022, 11))
        );
        assert_eq!(MetaEntry::parse_date("2022"), Some(PersonDate::year_only(2022)));
        assert_eq!(MetaEntry::parse_date("800"), Some(PersonDate::year_only(800)));
    }

    #[test]
    fn parse_date_rejects_bad_input() {
        assert_eq!(MetaEntry::parse_date(""), None);
        // Two-digit year falls below the 3+ digit requirement.
        assert_eq!(MetaEntry::parse_date("22"), None);
        assert_eq!(MetaEntry::parse_date("foobar"), None);
    }

    #[test]
    fn parse_alias_handles_a_prefix() {
        let mut me = MetaEntry::default();
        assert!(me.parse_alias("Aen", "John"));
        assert_eq!(me.aliases.len(), 1);
        assert_eq!(me.aliases[0], LocaleString::new("en", "John"));
        assert!(me.parse_alias("Ade", "Johann"));
        assert_eq!(me.aliases.len(), 2);
    }

    #[test]
    fn parse_alias_ignores_non_a_labels() {
        let mut me = MetaEntry::default();
        assert!(!me.parse_alias("name", "John"));
        assert!(!me.parse_alias("P123", "value"));
        assert!(me.aliases.is_empty());
    }

    #[test]
    fn parse_description_handles_d_prefix() {
        let mut me = MetaEntry::default();
        assert!(me.parse_description("Den", "A painter"));
        assert_eq!(me.descriptions.get("en"), Some(&"A painter".to_string()));
        assert!(me.parse_description("Dfr", "Un peintre"));
        assert_eq!(me.descriptions.get("fr"), Some(&"Un peintre".to_string()));
    }

    #[test]
    fn parse_description_ignores_non_d_labels() {
        let mut me = MetaEntry::default();
        assert!(!me.parse_description("name", "John"));
        assert!(!me.parse_description("P123", "value"));
        assert!(me.descriptions.is_empty());
    }

    #[test]
    fn parse_property_routes_p_number_to_aux() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P214", "12345").unwrap());
        assert!(me.auxiliary.contains(&AuxiliaryRow::new(214, "12345".to_string())));
    }

    #[test]
    fn parse_property_ignores_non_property_labels() {
        let mut me = MetaEntry::default();
        assert!(!me.parse_property("name", "value").unwrap());
        assert!(!me.parse_property("Q5", "value").unwrap());
        assert!(me.auxiliary.is_empty());
    }

    #[test]
    fn parse_property_p625_routes_to_coordinate_not_aux() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P625", "1.5/-2.5").unwrap());
        // P625 must NOT also land in aux — it has its own table.
        assert!(me.auxiliary.is_empty());
        let loc = me.coordinate.unwrap();
        assert!((loc.lat() - 1.5).abs() < f64::EPSILON);
        assert!((loc.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_property_p625_accepts_point_format() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P625", "POINT(1.5 -2.5)").unwrap());
        let loc = me.coordinate.unwrap();
        assert!((loc.lat() - 1.5).abs() < f64::EPSILON);
        assert!((loc.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_property_splits_pipe_separated_values() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P31", "Q5|Q515").unwrap());
        assert_eq!(me.auxiliary.len(), 2);
        assert!(me.auxiliary.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
        assert!(me.auxiliary.contains(&AuxiliaryRow::new(31, "Q515".to_string())));
    }

    #[test]
    fn parse_property_trims_whitespace_around_pipe_values() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P31", "Q5 | Q515 | Q123").unwrap());
        assert_eq!(me.auxiliary.len(), 3);
    }

    #[test]
    fn parse_property_skips_empty_pipe_segments() {
        let mut me = MetaEntry::default();
        assert!(me.parse_property("P31", "Q5||Q515|").unwrap());
        assert_eq!(me.auxiliary.len(), 2);
    }

    #[test]
    fn parse_property_dedupes_repeated_values_via_add_aux() {
        let mut me = MetaEntry::default();
        // `add_aux` (used by parse_property) dedupes by (prop, value),
        // so a pipe-separated cell with a duplicate yields one row.
        assert!(me.parse_property("P31", "Q5|Q5").unwrap());
        assert_eq!(me.auxiliary.len(), 1);
        assert!(me.auxiliary.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
    }

    #[test]
    fn process_cell_sets_bare_labels() {
        let mut me = MetaEntry::default();
        me.process_cell("name", "John Doe").unwrap();
        assert_eq!(me.entry.ext_name, "John Doe");
        me.process_cell("desc", "A painter").unwrap();
        assert_eq!(me.entry.ext_desc, "A painter");
        me.process_cell("url", "http://example.com").unwrap();
        assert_eq!(me.entry.ext_url, "http://example.com");
        me.process_cell("type", "Q5").unwrap();
        assert_eq!(me.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn process_cell_id_is_silently_ignored() {
        // ext_id is set by from_csv_row, not by process_cell; the "id"
        // column label is accepted but produces no field write.
        let mut me = MetaEntry::default();
        me.process_cell("id", "12345").unwrap();
        assert!(me.entry.ext_id.is_empty());
    }

    #[test]
    fn process_cell_born_died_route_through_person_dates_wrapper() {
        let mut me = MetaEntry::default();
        me.process_cell("born", "1900").unwrap();
        assert_eq!(me.born(), Some(PersonDate::year_only(1900)));
        me.process_cell("died", "2000-01-15").unwrap();
        assert_eq!(me.died(), Some(PersonDate::year_month_day(2000, 1, 15)));
    }

    #[test]
    fn process_cell_q_sets_user_aux_match() {
        // A `q` on a CSV row is attributed to USER_AUX_MATCH so
        // create_in_storage's set_match later records the right user.
        let mut me = MetaEntry::default();
        me.process_cell("q", "Q42").unwrap();
        assert_eq!(me.entry.q, Some(42));
        assert_eq!(me.entry.user, Some(crate::app_state::USER_AUX_MATCH));
    }

    #[test]
    fn process_cell_q_zero_is_unmatched() {
        let mut me = MetaEntry::default();
        me.process_cell("q", "Q0").unwrap();
        assert!(me.entry.q.is_none());
        assert!(me.entry.user.is_none());
    }

    #[test]
    fn process_cell_q_negative_is_unmatched() {
        let mut me = MetaEntry::default();
        me.process_cell("q", "Q-1").unwrap();
        assert!(me.entry.q.is_none());
    }

    #[test]
    fn process_cell_unknown_label_errors() {
        let mut me = MetaEntry::default();
        assert!(me.process_cell("foobar", "value").is_err());
    }

    #[test]
    fn process_cell_dispatches_to_alias_description_property_handlers() {
        let mut me = MetaEntry::default();
        me.process_cell("Aen", "Johnny").unwrap();
        assert_eq!(me.aliases.len(), 1);
        me.process_cell("Den", "A scientist").unwrap();
        assert_eq!(me.descriptions.get("en"), Some(&"A scientist".to_string()));
        me.process_cell("P31", "Q5|Q515").unwrap();
        assert_eq!(me.auxiliary.len(), 2);
    }

    #[test]
    fn regex_capture_returns_group_1() {
        let re = Regex::new(r"^Q(\d+)$").unwrap();
        assert_eq!(MetaEntry::regex_capture(&re, "Q123"), Some("123".to_string()));
        assert_eq!(MetaEntry::regex_capture(&re, "P123"), None);
        assert_eq!(MetaEntry::regex_capture(&re, ""), None);
    }

    // ── update_merge_in_storage: DB integration ──────────────────────
    //
    // Pins the scraper-style merge contract end-to-end. Headline rule:
    // empty incoming scalar fields MUST NOT clobber the stored values.

    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn update_merge_skips_empty_ext_name() {
        let app = crate::test_support::test_app().await;
        let (_catalog_id, entry_id) =
            crate::test_support::seed_entry_with_name("Original Name").await.unwrap();

        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();

        // Incoming MetaEntry has empty ext_name — must NOT overwrite.
        let me = MetaEntry::default();
        me.update_merge_in_storage(&mut entry, &app).await.unwrap();

        let reloaded = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(
            reloaded.ext_name, "Original Name",
            "empty incoming ext_name must not clobber stored value"
        );
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn update_merge_overwrites_nonempty_ext_name() {
        let app = crate::test_support::test_app().await;
        let (_catalog_id, entry_id) =
            crate::test_support::seed_entry_with_name("Original Name").await.unwrap();

        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();

        let mut me = MetaEntry::default();
        me.entry.ext_name = "New Name".to_string();
        me.update_merge_in_storage(&mut entry, &app).await.unwrap();

        let reloaded = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(reloaded.ext_name, "New Name");
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn update_merge_skips_match_when_entry_already_matched() {
        // If the existing entry already has a `q`, an incoming `q` is
        // ignored — the merge path must never reassign existing matches.
        let app = crate::test_support::test_app().await;
        let (_catalog_id, entry_id) =
            crate::test_support::seed_entry_with_name("Already matched").await.unwrap();

        // Seed an existing match (user 4 = USER_AUX_MATCH).
        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        EntryWriter::new(&app, &mut entry)
            .set_match("Q42", crate::app_state::USER_AUX_MATCH)
            .await
            .unwrap();

        // Incoming claims a different q. Should be ignored.
        let mut me = MetaEntry::default();
        me.entry.q = Some(999);
        me.update_merge_in_storage(&mut entry, &app).await.unwrap();

        let reloaded = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(
            reloaded.q,
            Some(42),
            "existing match must be preserved when incoming has a different q"
        );
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn update_merge_sets_match_when_entry_unmatched() {
        let app = crate::test_support::test_app().await;
        let (_catalog_id, entry_id) =
            crate::test_support::seed_entry_with_name("Unmatched").await.unwrap();

        let mut entry = Entry::from_id(entry_id, &app).await.unwrap();
        assert!(entry.q.is_none(), "seed entry must start unmatched");

        let mut me = MetaEntry::default();
        me.entry.q = Some(42);
        me.update_merge_in_storage(&mut entry, &app).await.unwrap();

        let reloaded = Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(reloaded.q, Some(42));
        assert_eq!(reloaded.user, Some(crate::app_state::USER_AUX_MATCH));
    }
}
