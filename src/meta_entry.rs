use crate::app_state::AppState;
use crate::entry::{AuxiliaryRow, CoordinateLocation, Entry};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

// ── PersonDate: a date with variable precision ─────────────────────────────

/// A date with variable precision: year-only, year-month, or year-month-day.
/// Negative years represent BCE dates. Serializes to/from the DB string format
/// ("YYYY", "YYYY-MM", "YYYY-MM-DD").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersonDate {
    pub year: i32,
    pub month: Option<u8>,
    pub day: Option<u8>,
}

impl PersonDate {
    /// Create a year-only date.
    pub fn year_only(year: i32) -> Self {
        Self {
            year,
            month: None,
            day: None,
        }
    }

    /// Create a year-month date.
    pub fn year_month(year: i32, month: u8) -> Self {
        Self {
            year,
            month: Some(month),
            day: None,
        }
    }

    /// Create a full year-month-day date.
    pub fn year_month_day(year: i32, month: u8, day: u8) -> Self {
        Self {
            year,
            month: Some(month),
            day: Some(day),
        }
    }

    /// Parse from DB string format: "YYYY", "YYYY-MM", or "YYYY-MM-DD"
    /// (with optional leading '-' for BCE).
    pub fn from_db_string(s: &str) -> Option<Self> {
        if s.is_empty() {
            return None;
        }
        let (negative, rest) = if let Some(r) = s.strip_prefix('-') {
            (true, r)
        } else {
            (false, s)
        };
        let parts: Vec<&str> = rest.split('-').collect();
        let year_abs: i32 = parts.first()?.parse().ok()?;
        let year = if negative { -year_abs } else { year_abs };
        match parts.len() {
            1 => Some(Self::year_only(year)),
            2 => {
                let month: u8 = parts[1].parse().ok()?;
                if !(1..=12).contains(&month) {
                    return None;
                }
                Some(Self::year_month(year, month))
            }
            3 => {
                let month: u8 = parts[1].parse().ok()?;
                let day: u8 = parts[2].parse().ok()?;
                if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                    return None;
                }
                Some(Self::year_month_day(year, month, day))
            }
            _ => None,
        }
    }

    /// Convert to DB string format.
    pub fn to_db_string(&self) -> String {
        let prefix = if self.year < 0 { "-" } else { "" };
        let abs_year = self.year.unsigned_abs();
        match (self.month, self.day) {
            (None, _) => format!("{prefix}{abs_year}"),
            (Some(m), None) => format!("{prefix}{abs_year}-{m:02}"),
            (Some(m), Some(d)) => format!("{prefix}{abs_year}-{m:02}-{d:02}"),
        }
    }

    /// Wikidata time precision: 9 = year, 10 = month, 11 = day.
    pub fn wikidata_precision(&self) -> u64 {
        match (self.month, self.day) {
            (None, _) => 9,
            (Some(_), None) => 10,
            (Some(_), Some(_)) => 11,
        }
    }

    /// Format as Wikidata time value string (e.g. "+2021-01-01T00:00:00Z").
    pub fn to_wikidata_time(&self) -> String {
        let prefix = if self.year < 0 { "-" } else { "+" };
        let abs_year = self.year.unsigned_abs();
        let month = self.month.unwrap_or(1);
        let day = self.day.unwrap_or(1);
        format!("{prefix}{abs_year}-{month:02}-{day:02}T00:00:00Z")
    }
}

impl fmt::Display for PersonDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_db_string())
    }
}

impl Serialize for PersonDate {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_db_string())
    }
}

impl<'de> Deserialize<'de> for PersonDate {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        PersonDate::from_db_string(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid person date: {s}")))
    }
}

// ── MnmLink: a flexible link target ────────────────────────────────────────

/// A link target that can refer to another entry by ID, by catalog+ext_id, or
/// by Wikidata QID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MnmLink {
    /// Link to another MnM entry by its numeric ID.
    EntryId(usize),
    /// Link to an entry identified by catalog ID and external ID.
    CatalogExtId { catalog: usize, ext_id: String },
    /// Link to a Wikidata item by its numeric ID (e.g. 42 for "Q42").
    WikidataQid(isize),
}

impl MnmLink {
    /// Build from a Q-number string like "Q42".
    pub fn from_q(q: &str) -> Option<Self> {
        AppState::item2numeric(q).map(Self::WikidataQid)
    }

    /// Return the "Q…" string for a WikidataQid variant.
    pub fn qid_string(&self) -> Option<String> {
        match self {
            Self::WikidataQid(q) => Some(format!("Q{q}")),
            _ => None,
        }
    }

    /// Try to resolve this link to an entry ID using the given AppState.
    pub async fn resolve_entry_id(&self, app: &AppState) -> Result<Option<usize>> {
        match self {
            Self::EntryId(id) => Ok(Some(*id)),
            Self::CatalogExtId { catalog, ext_id } => {
                match app.storage().entry_from_ext_id(*catalog, ext_id).await {
                    Ok(entry) => Ok(entry.id),
                    Err(_) => Ok(None),
                }
            }
            Self::WikidataQid(_) => Ok(None), // No direct entry resolution
        }
    }
}

// ── Serializable sub-structures (only for tables without existing structs) ──

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MetaPersonDates {
    pub born: Option<PersonDate>,
    pub died: Option<PersonDate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaMnmRelation {
    pub property: usize,
    pub target: MnmLink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaIssue {
    pub id: Option<usize>,
    pub issue_type: String,
    pub json: serde_json::Value,
    pub status: String,
    pub user_id: Option<usize>,
    pub resolved_ts: Option<String>,
    pub catalog_id: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaKvEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaLogEntry {
    pub id: Option<usize>,
    pub action: String,
    pub user: Option<usize>,
    pub timestamp: Option<String>,
    pub q: Option<isize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaAlias {
    pub language: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaStatementText {
    pub id: Option<usize>,
    pub property: usize,
    pub text: String,
    pub in_wikidata: bool,
    pub entry_is_matched: bool,
    pub q: Option<usize>,
}

// ── MetaEntry ──────────────────────────────────────────────────────────────

/// A fully-resolved snapshot of an entry and all its associated data, suitable
/// for JSON serialization / deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaEntry {
    pub entry: Entry,
    pub auxiliary: Vec<AuxiliaryRow>,
    pub coordinate: Option<CoordinateLocation>,
    pub person_dates: Option<MetaPersonDates>,
    pub mnm_relations: Vec<MetaMnmRelation>,
    pub descriptions: HashMap<String, String>,
    pub aliases: Vec<MetaAlias>,
    pub issues: Vec<MetaIssue>,
    pub kv_entries: Vec<MetaKvEntry>,
    pub log_entries: Vec<MetaLogEntry>,
    pub multi_match: Vec<usize>,
    pub statement_text: Vec<MetaStatementText>,
}

impl MetaEntry {
    /// Load a complete MetaEntry from storage for a given entry ID.
    pub async fn from_entry_id(entry_id: usize, app: &AppState) -> Result<Self> {
        let entry = Entry::from_id(entry_id, app).await?;
        Self::from_entry(&entry, app).await
    }

    /// Load a complete MetaEntry from an already-loaded Entry.
    pub async fn from_entry(entry: &Entry, app: &AppState) -> Result<Self> {
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

        let aliases: Vec<MetaAlias> = aliases_result?
            .into_iter()
            .map(|ls| MetaAlias {
                language: ls.language().to_string(),
                value: ls.value().to_string(),
            })
            .collect();

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

        // Parse multi_match candidates string ("1,23456,7") into Vec<usize>
        let multi_match: Vec<usize> = multi_match_result?
            .first()
            .map(|s| {
                s.split(',')
                    .filter_map(|q| q.parse::<usize>().ok())
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

    async fn load_mnm_relations(entry_id: usize, app: &AppState) -> Result<Vec<MetaMnmRelation>> {
        app.storage().meta_entry_get_mnm_relations(entry_id).await
    }

    async fn load_issues(entry_id: usize, app: &AppState) -> Result<Vec<MetaIssue>> {
        app.storage().meta_entry_get_issues(entry_id).await
    }

    async fn load_kv_entries(entry_id: usize, app: &AppState) -> Result<Vec<MetaKvEntry>> {
        app.storage().meta_entry_get_kv_entries(entry_id).await
    }

    async fn load_log_entries(entry_id: usize, app: &AppState) -> Result<Vec<MetaLogEntry>> {
        app.storage().meta_entry_get_log_entries(entry_id).await
    }

    async fn load_statement_text(
        entry_id: usize,
        app: &AppState,
    ) -> Result<Vec<MetaStatementText>> {
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
    pub async fn create_in_storage(&self, app: &AppState) -> Result<usize> {
        let mut entry = Entry::new_from_catalog_and_ext_id(self.entry.catalog, &self.entry.ext_id);
        entry.set_app(app);
        entry.ext_url = self.entry.ext_url.clone();
        entry.ext_name = self.entry.ext_name.clone();
        entry.ext_desc = self.entry.ext_desc.clone();
        entry.type_name = self.entry.type_name.clone();
        entry.random = self.entry.random;

        let new_id = entry.insert_as_new().await?;
        let entry_id = new_id.ok_or_else(|| anyhow!("Failed to insert new entry"))?;

        self.write_associated_data(entry_id, app).await?;

        // Set match if present (after associated data so log/issues are in place)
        if let Some(q) = self.entry.q {
            if q > 0 {
                let user = self.entry.user.unwrap_or(0);
                entry.id = Some(entry_id);
                entry.set_match(&format!("Q{q}"), user).await?;
            }
        }

        Ok(entry_id)
    }

    /// Update an existing entry with data from this MetaEntry.
    /// The entry must already exist in storage.
    pub async fn update_in_storage(&self, app: &AppState) -> Result<()> {
        let entry_id = self
            .entry
            .id
            .ok_or_else(|| anyhow!("MetaEntry has no entry id for update"))?;

        let storage = app.storage();

        // Update core entry fields
        let (r1, r2, r3, r4, r5) = tokio::join!(
            storage.entry_set_ext_name(&self.entry.ext_name, entry_id),
            storage.entry_set_ext_desc(&self.entry.ext_desc, entry_id),
            storage.entry_set_ext_id(&self.entry.ext_id, entry_id),
            storage.entry_set_ext_url(&self.entry.ext_url, entry_id),
            storage.entry_set_type_name(self.entry.type_name.clone(), entry_id),
        );
        r1?;
        r2?;
        r3?;
        r4?;
        r5?;

        // Clear existing associated data and re-write
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

        self.write_associated_data(entry_id, app).await?;

        // Update match if needed
        if let Some(q) = self.entry.q {
            if q > 0 {
                let user = self.entry.user.unwrap_or(0);
                let mut entry = Entry::from_id(entry_id, app).await?;
                entry.set_match(&format!("Q{q}"), user).await?;
            }
        }

        Ok(())
    }

    /// Write all associated data for an entry (used by both create and update).
    async fn write_associated_data(&self, entry_id: usize, app: &AppState) -> Result<()> {
        let storage = app.storage();

        // Auxiliary
        for aux in &self.auxiliary {
            storage
                .entry_set_auxiliary(entry_id, aux.prop_numeric(), aux.value().to_string())
                .await?;
        }

        // Coordinate
        if let Some(coord) = &self.coordinate {
            storage
                .entry_set_coordinate_location(
                    entry_id,
                    coord.lat(),
                    coord.lon(),
                    coord.precision(),
                )
                .await?;
        }

        // Person dates
        if let Some(pd) = &self.person_dates {
            let born = pd.born.map(|d| d.to_db_string()).unwrap_or_default();
            let died = pd.died.map(|d| d.to_db_string()).unwrap_or_default();
            if !born.is_empty() || !died.is_empty() {
                storage.entry_set_person_dates(entry_id, born, died).await?;
            }
        }

        // Descriptions
        for (lang, text) in &self.descriptions {
            storage
                .entry_set_language_description(entry_id, lang, text.clone())
                .await?;
        }

        // Aliases
        for alias in &self.aliases {
            storage
                .entry_add_alias(entry_id, &alias.language, &alias.value)
                .await?;
        }

        // MnM relations (only EntryId targets can be written)
        for rel in &self.mnm_relations {
            if let Some(target_id) = rel.target.resolve_entry_id(app).await? {
                storage
                    .add_mnm_relation(entry_id, rel.property, target_id)
                    .await?;
            }
        }

        // KV entries
        for kv in &self.kv_entries {
            storage
                .meta_entry_set_kv_entry(entry_id, &kv.key, &kv.value)
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
                app: None,
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
            aliases: vec![MetaAlias {
                language: "en".to_string(),
                value: "Alias One".to_string(),
            }],
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
        assert_eq!(back.entry.id, Some(1));
        assert_eq!(back.entry.catalog, 100);
        assert!(back.entry.app.is_none()); // app is skipped
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
                app: None,
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
}
