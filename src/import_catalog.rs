use crate::app_state::AppState;
use crate::entry::Entry;
use crate::meta_entry::MetaEntry;
use crate::DbId;
use anyhow::{Result, anyhow};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

/// Import mode controls whether entries absent from the file are deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportMode {
    /// Add new entries and replace (update) existing ones. Never delete.
    AddReplace,
    /// Add new entries, replace existing ones, and delete catalog entries that
    /// are not present in the import file.  Fully-matched entries are never
    /// deleted.
    AddReplaceDelete,
}

impl std::fmt::Display for ImportMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddReplace => write!(f, "add_replace"),
            Self::AddReplaceDelete => write!(f, "add_replace_delete"),
        }
    }
}

impl std::str::FromStr for ImportMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "add_replace" => Ok(Self::AddReplace),
            "add_replace_delete" => Ok(Self::AddReplaceDelete),
            other => Err(anyhow!("Unknown import mode '{other}'. Use 'add_replace' or 'add_replace_delete'.")),
        }
    }
}

/// Result summary returned after an import run.
#[derive(Debug, Default, Serialize)]
pub struct ImportResult {
    pub created: usize,
    pub updated: usize,
    pub skipped_fully_matched: usize,
    pub deleted: usize,
    pub errors: Vec<String>,
}

/// Import or update a catalog from a local file (CLI path).
///
/// The JSON file may be either:
/// - A JSON array of MetaEntry objects: `[{...}, {...}]`
/// - A JSON-Lines file with one MetaEntry per line
///
/// This is the CLI entry point; user-id validation is skipped.
pub async fn import_from_file(
    app: &AppState,
    catalog_id: DbId,
    path: &Path,
    mode: ImportMode,
) -> Result<ImportResult> {
    let content = tokio::fs::read_to_string(path).await?;
    let entries = parse_meta_entries(&content)?;
    import_meta_entries(app, catalog_id, entries, mode, None).await
}

/// Import from a previously-uploaded file referenced by the `import_file`
/// table UUID.  The file's `type` column must be `json` or `jsonl`.
///
/// `importing_user_id` is the `import_file.user` value; entry user fields
/// are validated against it.
pub async fn import_from_import_file(
    app: &AppState,
    catalog_id: DbId,
    uuid: &str,
    mode: ImportMode,
) -> Result<ImportResult> {
    // Look up the import_file row
    let (file_type, user_id) = app
        .storage()
        .get_import_file_info(uuid)
        .await?
        .ok_or_else(|| anyhow!("No import_file row found for uuid '{uuid}'"))?;

    let file_type_lower = file_type.to_lowercase();
    if file_type_lower != "json" && file_type_lower != "jsonl" {
        return Err(anyhow!(
            "import_file type '{}' is not supported for MetaEntry import (expected 'json' or 'jsonl')",
            file_type
        ));
    }

    let path = format!("{}/{uuid}", app.import_file_path());
    let content = tokio::fs::read_to_string(&path).await?;
    let entries = parse_meta_entries(&content)?;
    import_meta_entries(app, catalog_id, entries, mode, Some(user_id)).await
}

/// Import from an already-parsed list of MetaEntry objects.
///
/// When `importing_user_id` is `Some(uid)`, every entry's `user` field is
/// validated: it must be `None`, `Some(0)` (auto-match), or `Some(uid)`.
/// When `None` (CLI mode), the check is skipped.
pub async fn import_meta_entries(
    app: &AppState,
    catalog_id: DbId,
    entries: Vec<MetaEntry>,
    mode: ImportMode,
    importing_user_id: Option<DbId>,
) -> Result<ImportResult> {
    let mut result = ImportResult::default();
    let mut seen_ext_ids: HashSet<String> = HashSet::new();

    for meta in &entries {
        // Ensure every entry targets the correct catalog
        if meta.entry.catalog != catalog_id {
            result.errors.push(format!(
                "ext_id '{}': catalog {} in file does not match target catalog {catalog_id}, skipping",
                meta.entry.ext_id, meta.entry.catalog
            ));
            continue;
        }

        // Validate user field when importing via API/import_file
        if let Some(allowed_uid) = importing_user_id {
            if let Some(user) = meta.entry.user {
                if user != 0 && user != allowed_uid {
                    result.errors.push(format!(
                        "ext_id '{}': entry user {} does not match importing user {allowed_uid}, skipping",
                        meta.entry.ext_id, user
                    ));
                    continue;
                }
            }
        }

        seen_ext_ids.insert(meta.entry.ext_id.clone());

        // Sanitize: strip primary keys, timestamp, and set random
        let mut sanitized = meta.clone();
        sanitized.entry.id = None;
        sanitized.entry.timestamp = None;
        sanitized.entry.random = 0.0; // will be set properly per path below
        for aux in &mut sanitized.auxiliary {
            aux.clear_row_id();
        }

        match import_single_entry(app, catalog_id, &sanitized).await {
            Ok(EntryAction::Created) => result.created += 1,
            Ok(EntryAction::Updated) => result.updated += 1,
            Ok(EntryAction::SkippedFullyMatched) => result.skipped_fully_matched += 1,
            Err(e) => {
                result.errors.push(format!(
                    "ext_id '{}': {e}",
                    meta.entry.ext_id
                ));
            }
        }
    }

    // Delete phase (only in AddReplaceDelete mode)
    if mode == ImportMode::AddReplaceDelete {
        match delete_absent_entries(app, catalog_id, &seen_ext_ids).await {
            Ok(count) => result.deleted = count,
            Err(e) => result.errors.push(format!("delete phase: {e}")),
        }
    }

    Ok(result)
}

enum EntryAction {
    Created,
    Updated,
    SkippedFullyMatched,
}

async fn import_single_entry(
    app: &AppState,
    catalog_id: DbId,
    meta: &MetaEntry,
) -> Result<EntryAction> {
    let ext_id = &meta.entry.ext_id;

    match Entry::from_ext_id(catalog_id, ext_id, app).await {
        Ok(existing) => {
            // Entry already exists — update it
            if existing.is_fully_matched() {
                // Protect the Wikidata match: update data but keep q/user/timestamp
                let mut updated_meta = meta.clone();
                updated_meta.entry.id = existing.id;
                updated_meta.entry.q = existing.q;
                updated_meta.entry.user = existing.user;
                updated_meta.entry.timestamp.clone_from(&existing.timestamp);
                updated_meta.update_in_storage(app).await?;
                Ok(EntryAction::SkippedFullyMatched)
            } else {
                let mut updated_meta = meta.clone();
                updated_meta.entry.id = existing.id;
                updated_meta.update_in_storage(app).await?;
                Ok(EntryAction::Updated)
            }
        }
        Err(_) => {
            // Entry does not exist — create it
            let mut new_meta = meta.clone();
            new_meta.entry.catalog = catalog_id;
            new_meta.entry.id = None;
            new_meta.entry.random = rand::rng().random();
            new_meta.create_in_storage(app).await?;
            Ok(EntryAction::Created)
        }
    }
}

/// Delete entries in the catalog whose ext_id is NOT in `keep_ext_ids`.
/// Fully-matched entries are never deleted.
/// Returns the number of deleted entries.
async fn delete_absent_entries(
    app: &AppState,
    catalog_id: DbId,
    keep_ext_ids: &HashSet<String>,
) -> Result<usize> {
    let batch_size: usize = 5000;
    let mut offset: usize = 0;
    let mut deleted: usize = 0;

    loop {
        let batch = app
            .storage()
            .get_entry_batch(catalog_id, batch_size, offset)
            .await?;
        let is_last = batch.len() < batch_size;

        for entry in &batch {
            if keep_ext_ids.contains(&entry.ext_id) {
                continue;
            }
            if entry.is_fully_matched() {
                continue;
            }
            let entry_id = match entry.id {
                Some(id) => id,
                None => continue,
            };
            app.storage().entry_delete(entry_id).await?;
            deleted += 1;
        }

        if is_last {
            break;
        }
        offset += batch_size;
    }

    Ok(deleted)
}

/// Parse a string that is either a JSON array of MetaEntry or JSON-Lines.
fn parse_meta_entries(content: &str) -> Result<Vec<MetaEntry>> {
    let trimmed = content.trim();

    // Try JSON array first
    if trimmed.starts_with('[') {
        let entries: Vec<MetaEntry> = serde_json::from_str(trimmed)
            .map_err(|e| anyhow!("Failed to parse JSON array: {e}"))?;
        return Ok(entries);
    }

    // Fall back to JSON-Lines (one MetaEntry per line)
    let mut entries = Vec::new();
    for (i, line) in trimmed.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: MetaEntry = serde_json::from_str(line)
            .map_err(|e| anyhow!("Failed to parse line {}: {e}", i + 1))?;
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_mode_from_str() {
        assert_eq!(
            "add_replace".parse::<ImportMode>().unwrap(),
            ImportMode::AddReplace
        );
        assert_eq!(
            "add_replace_delete".parse::<ImportMode>().unwrap(),
            ImportMode::AddReplaceDelete
        );
        assert!("bad".parse::<ImportMode>().is_err());
    }

    #[test]
    fn test_import_mode_display() {
        assert_eq!(ImportMode::AddReplace.to_string(), "add_replace");
        assert_eq!(ImportMode::AddReplaceDelete.to_string(), "add_replace_delete");
    }

    #[test]
    fn test_parse_json_array() {
        let json = r#"[{"entry":{"catalog":100,"ext_id":"ext1","ext_name":"Test"}}]"#;
        let parsed = parse_meta_entries(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].entry.ext_id, "ext1");
        assert!(parsed[0].entry.id.is_none());
    }

    #[test]
    fn test_parse_jsonl() {
        let line = r#"{"entry":{"catalog":100,"ext_id":"ext1","ext_name":"Test"}}"#;
        let content = format!("{line}\n{line}\n");
        let parsed = parse_meta_entries(&content).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_parse_empty() {
        let parsed = parse_meta_entries("").unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_parse_minimal_entry() {
        // Only the truly required fields
        let json = r#"[{"entry":{"catalog":42,"ext_id":"abc","ext_name":"Hello"}}]"#;
        let parsed = parse_meta_entries(json).unwrap();
        assert_eq!(parsed.len(), 1);
        let me = &parsed[0];
        assert_eq!(me.entry.catalog, 42);
        assert_eq!(me.entry.ext_id, "abc");
        assert_eq!(me.entry.ext_name, "Hello");
        assert!(me.entry.id.is_none());
        assert!(me.entry.q.is_none());
        assert!(me.entry.user.is_none());
        assert!(me.entry.timestamp.is_none());
        assert!(me.entry.type_name.is_none());
        assert!(me.auxiliary.is_empty());
        assert!(me.coordinate.is_none());
        assert!(me.person_dates.is_none());
        assert!(me.descriptions.is_empty());
        assert!(me.aliases.is_empty());
    }
}
