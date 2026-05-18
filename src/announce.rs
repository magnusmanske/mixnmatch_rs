//! Catalog "first-fill" announcement hook.
//!
//! Fires exactly once per catalog the first time a successful (non-yielded)
//! job completes against it with `entry` rows present. Existing catalogs
//! never trigger: only catalogs created through the storage layer after this
//! module landed get the `kv_catalog["announce_first_fill"] = "pending"`
//! marker that the CAS in `try_consume_first_fill_pending` consumes.
//!
//! [`announce_first_fill`] itself is a stub — wire up the Mastodon (or other)
//! notification here when ready. Currently logs at info-level.

use crate::app_state::ExternalServicesContext;
use crate::catalog::Catalog;
use anyhow::Result;
use log::info;

pub const KV_KEY_ANNOUNCE_FIRST_FILL: &str = "announce_first_fill";
pub const KV_VALUE_PENDING: &str = "pending";

pub async fn announce_first_fill(
    app: &dyn ExternalServicesContext,
    catalog_id: usize,
    entry_count: usize,
) -> Result<()> {
    let name = Catalog::from_id(catalog_id, app)
        .await
        .ok()
        .and_then(|c| c.name().cloned())
        .unwrap_or_else(|| format!("catalog {catalog_id}"));
    info!(
        target: "announce",
        "First fill of catalog {catalog_id} ({name}) with {entry_count} entries",
    );
    Ok(())
}
