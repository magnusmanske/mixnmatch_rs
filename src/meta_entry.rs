use crate::app_state::AppState;
use crate::entry::{AuxiliaryRow, CoordinateLocation, Entry};
use crate::issue::{Issue, IssueType};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaPersonDates {
    pub born: Option<String>,
    pub died: Option<String>,
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

        let (born, died) = person_dates_result?;
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
        r1?; r2?; r3?; r4?; r5?;

        // Clear existing associated data and re-write
        let (r1, r2, r3, r4, r5, r6, r7) = tokio::join!(
            storage.meta_entry_delete_auxiliary(entry_id),
            storage.entry_remove_coordinate_location(entry_id),
            storage.entry_delete_person_dates(entry_id),
            storage.meta_entry_delete_aliases(entry_id),
            storage.meta_entry_delete_descriptions(entry_id),
            storage.meta_entry_delete_mnm_relations(entry_id),
            storage.meta_entry_delete_kv_entries(entry_id),
        );
        r1?; r2?; r3?; r4?; r5?; r6?; r7?;

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
                .entry_set_coordinate_location(entry_id, coord.lat(), coord.lon(), coord.precision())
                .await?;
        }

        // Person dates
        if let Some(pd) = &self.person_dates {
            let born = pd.born.clone().unwrap_or_default();
            let died = pd.died.clone().unwrap_or_default();
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

        // Issues
        for issue in &self.issues {
            let issue_type = IssueType::new(&issue.issue_type)?;
            let issue_obj = Issue::new(entry_id, issue_type, issue.json.clone(), app).await?;
            issue_obj.insert().await?;
        }

        // Multi-match
        if !self.multi_match.is_empty() {
            let candidates: String = self
                .multi_match
                .iter()
                .map(|q| q.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let count = self.multi_match.len();
            storage
                .entry_set_multi_match(entry_id, candidates, count)
                .await?;
        }

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
                born: Some("1950-01-01".to_string()),
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
        assert_eq!(back.coordinate.unwrap().lat(), 51.5);
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
}
