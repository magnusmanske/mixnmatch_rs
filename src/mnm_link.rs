use crate::{DbId, ItemId, app_state::AppState};
use anyhow::Result;
use serde::{Deserialize, Serialize};

// ── MnmLink: a flexible link target ────────────────────────────────────────

/// A link target that can refer to another entry by ID, by catalog+ext_id, or
/// by Wikidata QID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum MnmLink {
    /// Link to another MnM entry by its numeric ID.
    EntryId(DbId),
    /// Link to an entry identified by catalog ID and external ID.
    CatalogExtId { catalog: DbId, ext_id: String },
    /// Link to a Wikidata item by its numeric ID (e.g. 42 for "Q42").
    WikidataQid(ItemId),
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
    pub async fn resolve_entry_id(&self, app: &AppState) -> Result<Option<DbId>> {
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
