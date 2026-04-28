//! Run a code-fragment Lua function (PERSON_DATE / AUX_FROM_DESC /
//! DESC_FROM_HTML) for a given entry. The Lua engine itself lives in
//! `crate::code_fragment`; this file is the API-shaped wrapper that loads the
//! entry + Lua source from the DB and shapes the result for JSON consumers.

use crate::api::code_fragments;
use crate::api::common::{self, ApiError, Params, json_resp};
use crate::app_state::AppState;
use crate::code_fragment::{self, LuaEntry, entry_to_lua_entry};
use axum::response::Response;
use serde_json::{Value, json};
use tower_sessions::Session;

/// Lua functions we know how to dispatch. Anything else is rejected at the
/// API boundary so we don't load arbitrary code-fragment names from params.
const VALID_FUNCTIONS: &[&str] = &["PERSON_DATE", "AUX_FROM_DESC", "DESC_FROM_HTML"];

/// Run a Lua code fragment and return the JSON-shaped result (no envelope).
///
/// `function`: which canonical function to run.
/// `entry_id`: the entry whose context the code runs against.
/// `html`: optional HTML body (only consumed by DESC_FROM_HTML).
pub async fn run(
    app: &AppState,
    function: &str,
    entry_id: usize,
    html: &str,
) -> Result<Value, ApiError> {
    if !VALID_FUNCTIONS.contains(&function) {
        return Err(ApiError(format!(
            "unsupported function: {function}. Must be one of: {}",
            VALID_FUNCTIONS.join(", ")
        )));
    }

    let entry = crate::entry::Entry::from_id(entry_id, app)
        .await
        .map_err(|e| ApiError(format!("entry {entry_id} not found: {e}")))?;

    let lua_code = app
        .storage()
        .get_code_fragment_lua(function, entry.catalog)
        .await
        .map_err(|e| ApiError(format!("database error: {e}")))?
        .ok_or_else(|| {
            ApiError(format!(
                "no Lua code fragment for function={function} catalog={}",
                entry.catalog
            ))
        })?;

    let lua_entry = entry_to_lua_entry(&entry);

    match function {
        "PERSON_DATE" => run_person_date(&lua_code, &lua_entry),
        "AUX_FROM_DESC" => run_aux_from_desc(&lua_code, &lua_entry),
        "DESC_FROM_HTML" => run_desc_from_html(&lua_code, &lua_entry, html),
        _ => unreachable!("guarded above"),
    }
}

/// Convenience wrapper: parse the standard `function`/`entry_id`/`html` param
/// triplet out of a `Params` map, then dispatch to `run`.
pub async fn run_from_params(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let function = params
        .get("function")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError("missing required parameter: function".into()))?
        .to_string();
    let entry_id_str = params
        .get("entry_id")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError("missing required parameter: entry_id".into()))?;
    let entry_id: usize = entry_id_str
        .parse()
        .map_err(|_| ApiError("parameter 'entry_id' must be a positive integer".into()))?;
    let html = params.get("html").cloned().unwrap_or_default();
    run(app, &function, entry_id, &html).await
}

pub fn run_person_date(lua_code: &str, entry: &LuaEntry) -> Result<Value, ApiError> {
    let result = code_fragment::run_person_date(lua_code, entry)
        .map_err(|e| ApiError(format!("Lua execution error: {e}")))?;
    Ok(json!({
        "born": result.born,
        "died": result.died,
    }))
}

pub fn run_aux_from_desc(lua_code: &str, entry: &LuaEntry) -> Result<Value, ApiError> {
    let result = code_fragment::run_aux_from_desc(lua_code, entry)
        .map_err(|e| ApiError(format!("Lua execution error: {e}")))?;
    let commands: Vec<Value> = result.commands.iter().map(|c| c.to_json()).collect();
    Ok(json!({ "commands": commands }))
}

pub fn run_desc_from_html(
    lua_code: &str,
    entry: &LuaEntry,
    html: &str,
) -> Result<Value, ApiError> {
    let result = code_fragment::run_desc_from_html(lua_code, entry, html)
        .map_err(|e| ApiError(format!("Lua execution error: {e}")))?;
    let commands: Vec<Value> = result.commands.iter().map(|c| c.to_json()).collect();
    Ok(json!({
        "descriptions": result.descriptions,
        "born": result.born,
        "died": result.died,
        "change_type": result.change_type.map(|(a, b)| json!([a, b])),
        "change_name": result.change_name.map(|(a, b)| json!([a, b])),
        "location": result.location.map(|(lat, lon)| json!({"lat": lat, "lon": lon})),
        "aux": result.aux.iter().map(|(p, v)| json!({"property": p, "value": v})).collect::<Vec<_>>(),
        "location_texts": result.location_texts.iter().map(|(p, v)| json!({"property": p, "value": v})).collect::<Vec<_>>(),
        "commands": commands,
    }))
}

/// `?query=test_code_fragment` — gated behind OAuth + the same allow-list
/// `code_fragments` uses. Pulls the function name out of a JSON `fragment`
/// param (rather than a top-level `function`) to match the existing PHP
/// frontend contract; otherwise just delegates to `run`.
pub async fn query_test_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = common::require_user_id(app, session, params).await?;
    if !code_fragments::user_is_allowed(uid) {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    let entry_id = common::get_param_int(params, "entry_id", 0) as usize;
    if entry_id == 0 {
        return Err(ApiError("No entry_id".into()));
    }
    let fragment_str = common::get_param(params, "fragment", "{}");
    let fragment: Value =
        serde_json::from_str(&fragment_str).map_err(|_| ApiError("Bad fragment".into()))?;
    let function = fragment
        .get("function")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if function.is_empty() {
        return Err(ApiError(format!("Bad fragment function '{function}'")));
    }
    let html = params.get("html").cloned().unwrap_or_default();
    let data = run(app, function, entry_id, &html).await?;
    Ok(json_resp(json!({
        "status": "OK",
        "data": data,
        "tested_via": "lua",
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_entry() -> LuaEntry {
        LuaEntry {
            id: 1,
            catalog: 1,
            ext_id: "x".into(),
            ext_url: String::new(),
            ext_name: "Test".into(),
            ext_desc: "1920-2000".into(),
            q: None,
            user: None,
            type_name: Some("Q5".into()),
        }
    }

    #[test]
    fn person_date_extracts_dates() {
        let lua = r#"
local b, d = string.match(o.ext_desc, "(%d%d%d%d)%-(%d%d%d%d)")
if b then born = b; died = d end
"#;
        let v = run_person_date(lua, &dummy_entry()).unwrap();
        assert_eq!(v["born"], "1920");
        assert_eq!(v["died"], "2000");
    }

    #[test]
    fn aux_from_desc_emits_commands() {
        let mut entry = dummy_entry();
        entry.ext_desc = "VIAF: 12345".into();
        let lua = r#"
local m = string.match(o.ext_desc, "VIAF: (%d+)")
if m then setAux(o.id, 214, m) end
"#;
        let v = run_aux_from_desc(lua, &entry).unwrap();
        assert_eq!(v["commands"][0]["type"], "set_aux");
        assert_eq!(v["commands"][0]["value"], "12345");
    }

    #[test]
    fn desc_from_html_returns_descriptions() {
        let lua = r#"
local m = string.match(html, "<h1>(.-)</h1>")
if m then d[#d+1] = m end
"#;
        let v = run_desc_from_html(lua, &dummy_entry(), "<html><h1>Great person</h1></html>").unwrap();
        assert_eq!(v["descriptions"][0], "Great person");
    }

    #[test]
    fn invalid_lua_surfaces_error() {
        let v = run_person_date("invalid lua {{{{", &dummy_entry());
        assert!(v.is_err());
    }
}
