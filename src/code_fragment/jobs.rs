//! Async job runners for the three Lua code-fragment kinds.
//!
//! These wrap the synchronous Lua VM (defined in `mod.rs`) with the
//! per-catalog batching, storage round-trips, and HTTP fetches that
//! turn a one-shot Lua script into a "process every entry in this
//! catalog" job. Each `run_*_job` is invoked from
//! `Job::run_this_job` via `JOB_HANDLER_REGISTRY`.

use super::{
    DescFromHtmlResult, RE_WHITESPACE, entry_to_lua_entry, get_new_description, run_aux_from_desc,
    run_desc_from_html, run_person_date, validate_born_died,
};
use super::{LuaCommand, LuaEntry};
use crate::app_state::AppState;
use crate::entry::Entry;
use crate::person_date::PersonDate;
use anyhow::{Result, anyhow};
use log::warn;

const ENTRY_BATCH_SIZE: usize = 5000;

/// Outcome of a Lua-backed job runner. The dispatcher uses this to decide
/// whether to fall back to the PHP implementation: only `NoLuaCode` may
/// trigger a fallback. A genuine Lua execution failure is returned as
/// `Err(_)` and must propagate — falling back to PHP would mask the bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LuaJobOutcome {
    /// Lua code existed and the job ran to completion (entry-level errors
    /// were logged and skipped, not propagated).
    Done,
    /// No Lua code fragment is registered for this catalog (row absent or
    /// `lua` column is null/empty). Caller may fall back to PHP.
    NoLuaCode,
}

/// Treat null and empty-after-trim Lua bodies the same way: as "no code
/// registered". Some legacy rows have `lua=''` rather than a missing row.
fn lua_code_present(code: &Option<String>) -> bool {
    code.as_deref().is_some_and(|c| !c.trim().is_empty())
}

/// Run the `update_person_dates` job for a catalog using Lua.
///
/// Returns:
/// - `Ok(LuaJobOutcome::Done)` if the Lua code existed and ran to completion.
/// - `Ok(LuaJobOutcome::NoLuaCode)` if no Lua code is registered — caller may fall back to PHP.
/// - `Err(_)` if Lua existed but failed at the storage / job-orchestration level.
pub async fn run_person_dates_job(catalog_id: usize, app: &AppState) -> Result<LuaJobOutcome> {
    let lua_code_opt = app
        .storage()
        .get_code_fragment_lua("PERSON_DATE", catalog_id)
        .await?;
    if !lua_code_present(&lua_code_opt) {
        return Ok(LuaJobOutcome::NoLuaCode);
    }
    let lua_code = lua_code_opt.unwrap_or_default();

    // Clear existing person dates for this catalog
    app.storage()
        .clear_person_dates_for_catalog(catalog_id)
        .await?;

    let mut offset = 0;
    let mut any_dates_set = false;
    loop {
        let entries = app
            .storage()
            .get_entry_batch(catalog_id, ENTRY_BATCH_SIZE, offset)
            .await?;
        if entries.is_empty() {
            break;
        }
        let batch_len = entries.len();

        for entry in &entries {
            let lua_entry = entry_to_lua_entry(entry);
            if lua_entry.type_name.as_deref() != Some("Q5") {
                continue;
            }
            if lua_entry.ext_desc.is_empty() {
                continue;
            }

            let result = match run_person_date(&lua_code, &lua_entry) {
                Ok(r) => r,
                Err(e) => {
                    warn!("Lua error for PERSON_DATE entry {}: {e}", lua_entry.id);
                    continue;
                }
            };

            if let Some((born, died)) = validate_born_died(&result.born, &result.died) {
                let entry_id = lua_entry.id;
                app.storage()
                    .entry_set_person_dates(entry_id, born, died)
                    .await?;
                any_dates_set = true;
            }
        }

        offset += batch_len;
    }

    if any_dates_set {
        app.storage().set_has_person_date(catalog_id, "yes").await?;
    }

    app.storage()
        .touch_code_fragment("PERSON_DATE", catalog_id)
        .await?;

    Ok(LuaJobOutcome::Done)
}

/// Run the `generate_aux_from_description` job for a catalog using Lua.
///
/// See [`run_person_dates_job`] for return-value semantics.
pub async fn run_aux_from_desc_job(catalog_id: usize, app: &AppState) -> Result<LuaJobOutcome> {
    let lua_code_opt = app
        .storage()
        .get_code_fragment_lua("AUX_FROM_DESC", catalog_id)
        .await?;
    if !lua_code_present(&lua_code_opt) {
        return Ok(LuaJobOutcome::NoLuaCode);
    }
    let lua_code = lua_code_opt.unwrap_or_default();

    let mut offset = 0;
    loop {
        let entries = app
            .storage()
            .get_entry_batch(catalog_id, ENTRY_BATCH_SIZE, offset)
            .await?;
        if entries.is_empty() {
            break;
        }
        let batch_len = entries.len();

        for entry in &entries {
            let lua_entry = entry_to_lua_entry(entry);
            let result = match run_aux_from_desc(&lua_code, &lua_entry) {
                Ok(r) => r,
                Err(e) => {
                    warn!("Lua error for AUX_FROM_DESC entry {}: {e}", lua_entry.id);
                    continue;
                }
            };

            let mut entry_clone = entry.clone();
            entry_clone.set_app(app);
            for cmd in &result.commands {
                if let Err(e) = apply_command(cmd, &mut entry_clone).await {
                    warn!("Error applying command for entry {}: {e}", lua_entry.id);
                }
            }
        }

        offset += batch_len;
    }

    app.storage()
        .touch_code_fragment("AUX_FROM_DESC", catalog_id)
        .await?;

    Ok(LuaJobOutcome::Done)
}

/// Run the `update_descriptions_from_url` job for a catalog using Lua.
/// Fetches HTML from each entry's `ext_url`, runs DESC_FROM_HTML Lua code,
/// applies results.
///
/// See [`run_person_dates_job`] for return-value semantics.
pub async fn run_desc_from_html_job(catalog_id: usize, app: &AppState) -> Result<LuaJobOutcome> {
    let lua_code_opt = app
        .storage()
        .get_code_fragment_lua("DESC_FROM_HTML", catalog_id)
        .await?;
    if !lua_code_present(&lua_code_opt) {
        return Ok(LuaJobOutcome::NoLuaCode);
    }
    let lua_code = lua_code_opt.unwrap_or_default();
    let client = app.http_client().clone();

    let mut offset = 0;
    loop {
        let entries = app
            .storage()
            .get_entry_batch(catalog_id, ENTRY_BATCH_SIZE, offset)
            .await?;
        if entries.is_empty() {
            break;
        }
        let batch_len = entries.len();
        for entry in &entries {
            process_desc_from_html_entry(app, &client, &lua_code, entry).await;
        }
        offset += batch_len;
    }

    finalize_desc_from_html(app, catalog_id).await?;
    Ok(LuaJobOutcome::Done)
}

async fn fetch_html(client: &reqwest::Client, url: &str) -> Option<String> {
    let resp = client.get(url).send().await.ok()?;
    let text = resp.text().await.ok()?;
    Some(RE_WHITESPACE.replace_all(&text, " ").to_string())
}

async fn process_desc_from_html_entry(
    app: &AppState,
    client: &reqwest::Client,
    lua_code: &str,
    entry: &Entry,
) {
    let lua_entry = entry_to_lua_entry(entry);
    if lua_entry.ext_url.is_empty() {
        return;
    }
    let Some(html) = fetch_html(client, &lua_entry.ext_url).await else {
        return;
    };
    let result = match run_desc_from_html(lua_code, &lua_entry, &html) {
        Ok(r) => r,
        Err(e) => {
            warn!("Lua error for DESC_FROM_HTML entry {}: {e}", lua_entry.id);
            return;
        }
    };

    let mut entry_clone = entry.clone();
    entry_clone.set_app(app);
    apply_desc_from_html_result(&mut entry_clone, &result).await;
}

async fn apply_desc_from_html_result(entry: &mut Entry, result: &DescFromHtmlResult) {
    apply_person_dates_from_result(entry, &result.born, &result.died).await;
    apply_location_from_result(entry, result.location).await;
    apply_aux_from_result(entry, &result.aux).await;
    apply_change_name(entry, result.change_name.as_ref()).await;
    apply_change_type(entry, result.change_type.as_ref()).await;
    apply_descriptions_from_result(entry, &result.descriptions).await;
    for cmd in &result.commands {
        let _ = apply_command(cmd, entry).await;
    }
}

async fn apply_person_dates_from_result(entry: &mut Entry, born: &str, died: &str) {
    if born.is_empty() && died.is_empty() {
        return;
    }
    if let Some((born, died)) = validate_born_died(born, died) {
        let born_pd = PersonDate::from_db_string(&born);
        let died_pd = PersonDate::from_db_string(&died);
        let _ = entry.set_person_dates(&born_pd, &died_pd).await;
    }
}

async fn apply_location_from_result(entry: &mut Entry, location: Option<(f64, f64)>) {
    if let Some((lat, lon)) = location {
        let cl = crate::coordinates::CoordinateLocation::new(lat, lon);
        let _ = entry.set_coordinate_location(&Some(cl)).await;
    }
}

async fn apply_aux_from_result(entry: &mut Entry, aux: &[(String, String)]) {
    for (prop_str, value) in aux {
        let prop_str = prop_str.trim_start_matches('P');
        if let Ok(prop_numeric) = prop_str.parse::<usize>() {
            let _ = entry.set_auxiliary(prop_numeric, Some(value.clone())).await;
        }
    }
}

async fn apply_change_name(entry: &mut Entry, change: Option<&(String, String)>) {
    if let Some((from, to)) = change {
        if from != to {
            let _ = entry.set_ext_name(to).await;
        }
    }
}

async fn apply_change_type(entry: &mut Entry, change: Option<&(String, String)>) {
    if let Some((_from, to)) = change {
        let _ = entry.set_type_name(Some(to.clone())).await;
    }
}

async fn apply_descriptions_from_result(entry: &mut Entry, descriptions: &[String]) {
    if descriptions.is_empty() {
        return;
    }
    let new_desc = get_new_description(&entry.ext_desc, descriptions);
    if new_desc != entry.ext_desc {
        let _ = entry.set_ext_desc(&new_desc).await;
    }
}

async fn finalize_desc_from_html(app: &AppState, catalog_id: usize) -> Result<()> {
    app.storage()
        .touch_code_fragment("DESC_FROM_HTML", catalog_id)
        .await?;
    let _ = app
        .storage()
        .queue_job(catalog_id, "update_person_dates", None)
        .await;
    let _ = app
        .storage()
        .queue_job(catalog_id, "generate_aux_from_description", None)
        .await;
    Ok(())
}

/// Apply a [`LuaCommand`] to an entry in the database.
pub(super) async fn apply_command(cmd: &LuaCommand, entry: &mut Entry) -> Result<()> {
    match cmd {
        LuaCommand::SetAux {
            property, value, ..
        } => {
            let prop_str = property.trim_start_matches('P');
            let prop_numeric: usize = prop_str
                .parse()
                .map_err(|_| anyhow!("Invalid property '{property}'"))?;
            entry.set_auxiliary(prop_numeric, Some(value.clone())).await
        }
        LuaCommand::SetMatch { q, .. } => {
            entry.set_match(q, 0).await?;
            Ok(())
        }
        LuaCommand::SetLocation { lat, lon, .. } => {
            let cl = crate::coordinates::CoordinateLocation::new(*lat, *lon);
            entry.set_coordinate_location(&Some(cl)).await
        }
        LuaCommand::SetPersonDates { born, died, .. } => {
            let born_pd = PersonDate::from_db_string(born);
            let died_pd = PersonDate::from_db_string(died);
            entry.set_person_dates(&born_pd, &died_pd).await
        }
        LuaCommand::SetDescription { value, .. } => entry.set_ext_desc(value).await,
        LuaCommand::SetEntryName { value, .. } => entry.set_ext_name(value).await,
        LuaCommand::SetEntryType { value, .. } => entry.set_type_name(Some(value.clone())).await,
        LuaCommand::AddAlias {
            label, language, ..
        } => {
            let ls = wikimisc::wikibase::locale_string::LocaleString::new(language, label);
            entry.add_alias(&ls).await
        }
        LuaCommand::AddLocationText { .. } => {
            // Location text is not yet implemented in the Rust storage layer
            Ok(())
        }
    }
}

// Re-import LuaEntry into a type so the rustc warning about
// unused imports doesn't fire when `LuaEntry` ends up only used
// by paths that go through `entry_to_lua_entry`. (No-op at runtime.)
#[allow(dead_code)]
fn _force_lua_entry_use(_e: &LuaEntry) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lua_code_present_treats_none_as_absent() {
        assert!(!lua_code_present(&None));
    }

    #[test]
    fn lua_code_present_treats_empty_string_as_absent() {
        assert!(!lua_code_present(&Some(String::new())));
    }

    #[test]
    fn lua_code_present_treats_whitespace_only_as_absent() {
        assert!(!lua_code_present(&Some("   \n\t ".to_string())));
    }

    #[test]
    fn lua_code_present_treats_real_code_as_present() {
        assert!(lua_code_present(&Some(
            "function f() return 1 end".to_string()
        )));
    }
}
