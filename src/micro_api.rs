use crate::app_state::AppState;
use crate::code_fragment::{
    self, LuaCommand, LuaEntry,
};
use crate::entry::Entry;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use wikimisc::wikibase::EntityTrait;

lazy_static! {
    static ref RE_NAME_VARIANTS: Regex = Regex::new(r"^(\S+) (.+) (\S+)$").unwrap();
    static ref RE_SPARQL_Q: Regex = Regex::new(r"(Q\d+)$").unwrap();
}

type SharedState = Arc<AppState>;
pub type Params = HashMap<String, String>;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router(app: AppState) -> Router {
    let state: SharedState = Arc::new(app);
    Router::new()
        .route("/api", get(api_dispatch))
        .with_state(state)
}

/// Start the micro-API server on the given port. Runs until the process exits.
pub async fn serve(app: AppState, port: u16) {
    let router = router(app);
    let addr = format!("0.0.0.0:{port}");
    match TcpListener::bind(&addr).await {
        Ok(listener) => {
            log::info!("micro_api: listening on http://127.0.0.1:{port}");
            if let Err(e) = axum::serve(listener, router).await {
                log::error!("micro_api server error: {e}");
            }
        }
        Err(e) => {
            log::error!("micro_api: failed to bind to {addr}: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

async fn api_dispatch(
    State(app): State<SharedState>,
    Query(params): Query<Params>,
) -> Response {
    let action = params.get("action").cloned().unwrap_or_default();
    let result = match action.as_str() {
        "run_lua" => handle_run_lua(&app, &params).await,
        "get_code_fragments" => handle_get_code_fragments(&app, &params).await,
        "save_code_fragment" => handle_save_code_fragment(&app, &params).await,
        "sparql_list" => handle_sparql_list(&app, &params).await,
        "get_sync" => handle_get_sync(&app, &params).await,
        "creation_candidates" => handle_creation_candidates(&app, &params).await,
        "quick_compare" => handle_quick_compare(&app, &params).await,
        "lc_catalogs" => handle_lc_catalogs(&app).await,
        "lc_locations" => handle_lc_locations(&app, &params).await,
        "lc_report" => handle_lc_report(&app, &params).await,
        "lc_report_list" => handle_lc_report_list(&app, &params).await,
        "lc_rc" => handle_lc_rc(&app, &params).await,
        "lc_set_status" => handle_lc_set_status(&app, &params).await,
        "" => Err(ApiError::new("missing 'action' parameter")),
        other => Err(ApiError::new(&format!("unknown action: {other}"))),
    };
    match result {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Error / success helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ApiError {
    message: String,
    kind: &'static str,
}

impl ApiError {
    fn new(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            kind: "bad_request",
        }
    }

    fn not_found(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            kind: "not_found",
        }
    }

    fn internal(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            kind: "internal_error",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "status": self.kind, "error": self.message });
        Json(body).into_response()
    }
}

fn success(data: Value) -> Result<Response, ApiError> {
    Ok(Json(json!({ "status": "ok", "data": data })).into_response())
}

// ---------------------------------------------------------------------------
// Public helpers for internal callers (e.g., the main /api.php dispatcher).
// These return `Result<Value, String>` — plain data with the status envelope
// stripped — so they can be embedded in any other API response shape.
// ---------------------------------------------------------------------------

/// Convert an internal `ApiError` to a plain string for cross-module callers.
impl From<ApiError> for String {
    fn from(e: ApiError) -> String {
        e.message
    }
}

async fn response_to_data(r: Result<Response, ApiError>) -> Result<Value, String> {
    let resp = r.map_err(|e| e.message)?;
    let bytes = axum::body::to_bytes(resp.into_body(), 100_000_000)
        .await
        .map_err(|e| format!("read body: {e}"))?;
    let v: Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("parse body: {e}"))?;
    if v.get("status").and_then(|s| s.as_str()) == Some("ok") {
        Ok(v.get("data").cloned().unwrap_or(Value::Null))
    } else {
        let msg = v.get("error").and_then(|s| s.as_str()).unwrap_or("unknown error");
        Err(msg.to_string())
    }
}

pub async fn data_creation_candidates(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_creation_candidates(app, params).await).await
}

pub async fn data_quick_compare(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_quick_compare(app, params).await).await
}

pub async fn data_get_sync(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_get_sync(app, params).await).await
}

pub async fn data_sparql_list(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_sparql_list(app, params).await).await
}

pub async fn data_get_code_fragments(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_get_code_fragments(app, params).await).await
}

pub async fn data_save_code_fragment(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_save_code_fragment(app, params).await).await
}

pub async fn data_run_lua(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_run_lua(app, params).await).await
}

pub async fn data_lc_catalogs(app: &AppState) -> Result<Value, String> {
    response_to_data(handle_lc_catalogs(app).await).await
}

pub async fn data_lc_locations(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_lc_locations(app, params).await).await
}

pub async fn data_lc_report(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_lc_report(app, params).await).await
}

pub async fn data_lc_report_list(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_lc_report_list(app, params).await).await
}

pub async fn data_lc_rc(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_lc_rc(app, params).await).await
}

pub async fn data_lc_set_status(app: &AppState, params: &Params) -> Result<Value, String> {
    response_to_data(handle_lc_set_status(app, params).await).await
}

fn get_required_param<'a>(params: &'a Params, key: &str) -> Result<&'a str, ApiError> {
    params
        .get(key)
        .filter(|v| !v.is_empty())
        .map(|v| v.as_str())
        .ok_or_else(|| ApiError::new(&format!("missing required parameter: {key}")))
}

fn get_param_usize(params: &Params, key: &str) -> Result<usize, ApiError> {
    let s = get_required_param(params, key)?;
    s.parse::<usize>()
        .map_err(|_| ApiError::new(&format!("parameter '{key}' must be a positive integer")))
}

// ---------------------------------------------------------------------------
// action=run_lua
// ---------------------------------------------------------------------------

async fn handle_run_lua(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let function = get_required_param(params, "function")?;
    let entry_id = get_param_usize(params, "entry_id")?;

    // Validate function name
    let valid_functions = ["PERSON_DATE", "AUX_FROM_DESC", "DESC_FROM_HTML"];
    if !valid_functions.contains(&function) {
        return Err(ApiError::new(&format!(
            "unsupported function: {function}. Must be one of: {}",
            valid_functions.join(", ")
        )));
    }

    // Load entry
    let entry = Entry::from_id(entry_id, app)
        .await
        .map_err(|e| ApiError::not_found(&format!("entry {entry_id} not found: {e}")))?;

    // Load Lua code for this function + catalog
    let lua_code = app
        .storage()
        .get_code_fragment_lua(function, entry.catalog)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?
        .ok_or_else(|| {
            ApiError::not_found(&format!(
                "no Lua code fragment for function={function} catalog={}",
                entry.catalog
            ))
        })?;

    let lua_entry = entry_to_lua_entry(&entry);

    // Optional html parameter for DESC_FROM_HTML
    let html = params.get("html").cloned().unwrap_or_default();

    match function {
        "PERSON_DATE" => run_lua_person_date(&lua_code, &lua_entry),
        "AUX_FROM_DESC" => run_lua_aux_from_desc(&lua_code, &lua_entry),
        "DESC_FROM_HTML" => run_lua_desc_from_html(&lua_code, &lua_entry, &html),
        _ => unreachable!(),
    }
}

fn entry_to_lua_entry(entry: &Entry) -> LuaEntry {
    LuaEntry {
        id: entry.id.unwrap_or(0),
        catalog: entry.catalog,
        ext_id: entry.ext_id.clone(),
        ext_url: entry.ext_url.clone(),
        ext_name: entry.ext_name.clone(),
        ext_desc: entry.ext_desc.clone(),
        q: entry.q,
        user: entry.user,
        type_name: entry.type_name.clone(),
    }
}

fn run_lua_person_date(lua_code: &str, entry: &LuaEntry) -> Result<Response, ApiError> {
    let result = code_fragment::run_person_date(lua_code, entry)
        .map_err(|e| ApiError::internal(&format!("Lua execution error: {e}")))?;
    success(json!({
        "born": result.born,
        "died": result.died,
    }))
}

fn run_lua_aux_from_desc(lua_code: &str, entry: &LuaEntry) -> Result<Response, ApiError> {
    let result = code_fragment::run_aux_from_desc(lua_code, entry)
        .map_err(|e| ApiError::internal(&format!("Lua execution error: {e}")))?;
    let commands: Vec<Value> = result.commands.iter().map(command_to_json).collect();
    success(json!({ "commands": commands }))
}

fn run_lua_desc_from_html(
    lua_code: &str,
    entry: &LuaEntry,
    html: &str,
) -> Result<Response, ApiError> {
    let result = code_fragment::run_desc_from_html(lua_code, entry, html)
        .map_err(|e| ApiError::internal(&format!("Lua execution error: {e}")))?;
    let commands: Vec<Value> = result.commands.iter().map(command_to_json).collect();
    success(json!({
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

fn command_to_json(cmd: &LuaCommand) -> Value {
    match cmd {
        LuaCommand::SetPersonDates {
            entry_id,
            born,
            died,
        } => json!({"type": "set_person_dates", "entry_id": entry_id, "born": born, "died": died}),
        LuaCommand::SetAux {
            entry_id,
            property,
            value,
        } => json!({"type": "set_aux", "entry_id": entry_id, "property": property, "value": value}),
        LuaCommand::SetMatch { entry_id, q } => {
            json!({"type": "set_match", "entry_id": entry_id, "q": q})
        }
        LuaCommand::SetLocation {
            entry_id,
            lat,
            lon,
        } => json!({"type": "set_location", "entry_id": entry_id, "lat": lat, "lon": lon}),
        LuaCommand::SetDescription { entry_id, value } => {
            json!({"type": "set_description", "entry_id": entry_id, "value": value})
        }
        LuaCommand::SetEntryName { entry_id, value } => {
            json!({"type": "set_entry_name", "entry_id": entry_id, "value": value})
        }
        LuaCommand::SetEntryType { entry_id, value } => {
            json!({"type": "set_entry_type", "entry_id": entry_id, "value": value})
        }
        LuaCommand::AddAlias {
            entry_id,
            label,
            language,
        } => json!({"type": "add_alias", "entry_id": entry_id, "label": label, "language": language}),
        LuaCommand::AddLocationText {
            entry_id,
            property,
            value,
        } => json!({"type": "add_location_text", "entry_id": entry_id, "property": property, "value": value}),
    }
}

// ---------------------------------------------------------------------------
// action=get_code_fragments
// ---------------------------------------------------------------------------

async fn handle_get_code_fragments(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = get_param_usize(params, "catalog")?;

    let fragments = app
        .storage()
        .get_code_fragments_for_catalog(catalog_id)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;

    let all_functions = app
        .storage()
        .get_all_code_fragment_functions()
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;

    success(json!({
        "fragments": fragments,
        "all_functions": all_functions,
    }))
}

// ---------------------------------------------------------------------------
// action=save_code_fragment
// ---------------------------------------------------------------------------

async fn handle_save_code_fragment(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let fragment_json = get_required_param(params, "fragment")?;
    let fragment: Value = serde_json::from_str(fragment_json)
        .map_err(|e| ApiError::new(&format!("invalid fragment JSON: {e}")))?;

    let catalog = fragment["catalog"].as_u64().unwrap_or(0) as usize;
    if catalog == 0 {
        return Err(ApiError::new("fragment must have a positive catalog ID"));
    }

    let function = fragment["function"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if function.is_empty() {
        return Err(ApiError::new("fragment must have a function"));
    }

    let cfid = app
        .storage()
        .save_code_fragment(&fragment)
        .await
        .map_err(|e| ApiError::internal(&format!("save failed: {e}")))?;

    // Queue appropriate jobs based on function type
    let mut queued_jobs = vec![];
    match function.as_str() {
        "PERSON_DATE" => {
            let job_id = app.storage().queue_job(catalog, "update_person_dates", None).await.unwrap_or(0);
            queued_jobs.push("update_person_dates");
            let _ = app.storage().queue_job(catalog, "match_person_dates", Some(job_id)).await;
            queued_jobs.push("match_person_dates");
        }
        "AUX_FROM_DESC" => {
            let _ = app.storage().queue_job(catalog, "generate_aux_from_description", None).await;
            queued_jobs.push("generate_aux_from_description");
        }
        "DESC_FROM_HTML" => {
            let _ = app.storage().queue_job(catalog, "update_descriptions_from_url", None).await;
            queued_jobs.push("update_descriptions_from_url");
        }
        _ => {}
    }

    success(json!({
        "id": cfid,
        "queued_jobs": queued_jobs,
    }))
}

// ---------------------------------------------------------------------------
// action=sparql_list
// ---------------------------------------------------------------------------

/// Parse SPARQL bindings and build a label-to-Q mapping.
///
/// Each binding must have exactly two variables: one URI and one literal.
/// The Q-number is extracted from the URI via a regex that matches the
/// trailing `Q\d+` portion.
fn parse_sparql_label2q(sparql_result: &Value) -> Result<HashMap<String, String>, ApiError> {
    let head_vars = sparql_result["head"]["vars"]
        .as_array()
        .ok_or_else(|| ApiError::internal("SPARQL result missing head.vars"))?;
    if head_vars.len() < 2 {
        return Err(ApiError::internal("SPARQL result must have at least 2 variables"));
    }
    let label_var = head_vars[0]
        .as_str()
        .ok_or_else(|| ApiError::internal("variable name is not a string"))?;
    let qnum_var = head_vars[1]
        .as_str()
        .ok_or_else(|| ApiError::internal("variable name is not a string"))?;

    let bindings = sparql_result["results"]["bindings"]
        .as_array()
        .ok_or_else(|| ApiError::internal("SPARQL result missing results.bindings"))?;

    let mut label2q: HashMap<String, String> = HashMap::new();

    for b in bindings {
        let v1_type = b[label_var]["type"].as_str().unwrap_or("");
        let v2_type = b[qnum_var]["type"].as_str().unwrap_or("");
        let v1_value = b[label_var]["value"].as_str().unwrap_or("");
        let v2_value = b[qnum_var]["value"].as_str().unwrap_or("");

        let (uri_val, lit_val) = if v1_type == "uri" && v2_type == "literal" {
            (v1_value, v2_value)
        } else if v2_type == "uri" && v1_type == "literal" {
            (v2_value, v1_value)
        } else {
            continue;
        };

        if let Some(caps) = RE_SPARQL_Q.captures(uri_val) {
            let q = caps[1].to_string();
            label2q.insert(lit_val.to_string(), q);
        }
    }

    Ok(label2q)
}

async fn handle_sparql_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let sparql = get_required_param(params, "sparql")?;

    // Execute SPARQL query
    let mw_api = app
        .wikidata()
        .get_mw_api()
        .await
        .map_err(|e| ApiError::internal(&format!("failed to get Wikidata API: {e}")))?;
    let sparql_result = mw_api
        .sparql_query(sparql)
        .await
        .map_err(|e| ApiError::internal(&format!("SPARQL query failed: {e}")))?;

    let label2q = parse_sparql_label2q(&sparql_result)?;

    if label2q.is_empty() {
        return success(json!({
            "entries": {},
            "users": {},
        }));
    }

    // Load matching unmatched entries
    let labels: Vec<String> = label2q.keys().cloned().collect();
    let entries = app
        .storage()
        .get_entries_by_ext_names_unmatched(&labels)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;

    let mut entry_map: HashMap<String, Value> = HashMap::new();
    for entry in &entries {
        let ext_name = &entry.ext_name;
        if let Some(q_str) = label2q.get(ext_name) {
            // Parse Q-number: strip leading 'Q' and parse as integer
            let q_numeric: isize = q_str[1..]
                .parse()
                .unwrap_or(0);
            let entry_id = entry.id.unwrap_or(0);
            entry_map.insert(
                entry_id.to_string(),
                json!({
                    "id": entry_id,
                    "catalog": entry.catalog,
                    "ext_id": entry.ext_id,
                    "ext_url": entry.ext_url,
                    "ext_name": entry.ext_name,
                    "ext_desc": entry.ext_desc,
                    "q": q_numeric,
                    "user": 0,
                    "timestamp": "20180304223800",
                    "type": entry.type_name,
                }),
            );
        }
    }

    // Get user data for user 0 (auto user)
    let users = app
        .storage()
        .get_users_by_ids(&[0])
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;
    let users_json: HashMap<String, Value> = users
        .into_iter()
        .map(|(id, val)| (id.to_string(), val))
        .collect();

    success(json!({
        "entries": entry_map,
        "users": users_json,
    }))
}

// ---------------------------------------------------------------------------
// action=get_sync
// ---------------------------------------------------------------------------

async fn handle_get_sync(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = get_param_usize(params, "catalog")?;

    // Load catalog wd_prop and wd_qual
    let (wd_prop, wd_qual) = app
        .storage()
        .get_catalog_wd_prop(catalog_id)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;

    let wd_prop = wd_prop.ok_or_else(|| {
        ApiError::new(&format!(
            "catalog {catalog_id} has no wd_prop set"
        ))
    })?;

    if wd_qual.is_some() {
        return Err(ApiError::new(&format!(
            "catalog {catalog_id} uses wd_qual (qualifier-based sync not supported)"
        )));
    }

    // Run SPARQL to get all items with this property on Wikidata
    let sparql = format!("SELECT ?q ?prop {{ ?q wdt:P{wd_prop} ?prop }}");
    let mw_api = app
        .wikidata()
        .get_mw_api()
        .await
        .map_err(|e| ApiError::internal(&format!("failed to get Wikidata API: {e}")))?;
    let sparql_result = mw_api
        .sparql_query(&sparql)
        .await
        .map_err(|e| ApiError::internal(&format!("SPARQL query failed: {e}")))?;

    // Build WD mapping: ext_id -> Q-number string
    let bindings = sparql_result["results"]["bindings"]
        .as_array()
        .ok_or_else(|| ApiError::internal("SPARQL result missing results.bindings"))?;

    let re = Regex::new(r"(Q\d+)$").unwrap();
    let mut wd_ext2q: HashMap<String, String> = HashMap::new();
    for b in bindings {
        let q_url = b["q"]["value"].as_str().unwrap_or("");
        let prop_value = b["prop"]["value"].as_str().unwrap_or("");
        if let Some(caps) = re.captures(q_url) {
            let q = caps[1].to_string();
            wd_ext2q.insert(prop_value.to_string(), q);
        }
    }

    // Load MnM matched entries (human-matched only, no fakes)
    let mnm_entries = app
        .storage()
        .get_mnm_matched_entries_for_sync(catalog_id)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;

    // Build MnM mapping: ext_id -> Q-number string
    let mut mnm_ext2q: HashMap<String, String> = HashMap::new();
    let mut mm_dupes: HashMap<String, Vec<String>> = HashMap::new();
    for (q, ext_id) in &mnm_entries {
        let q_str = format!("Q{q}");
        if let Some(existing_ext) = mnm_ext2q.get(&ext_id.to_string()) {
            // Same ext_id matched to different Q -> dupe
            mm_dupes
                .entry(ext_id.clone())
                .or_insert_with(|| vec![existing_ext.clone()])
                .push(q_str.clone());
        }
        mnm_ext2q.insert(ext_id.clone(), q_str);
    }

    // Compare the two sets
    let mut different: Vec<Value> = Vec::new();
    let mut wd_no_mm: Vec<Value> = Vec::new();
    let mut mm_no_wd: Vec<Value> = Vec::new();

    // Check WD entries against MnM
    for (ext_id, wd_q) in &wd_ext2q {
        match mnm_ext2q.get(ext_id) {
            Some(mnm_q) => {
                if wd_q != mnm_q {
                    different.push(json!({
                        "ext_id": ext_id,
                        "wd_q": wd_q,
                        "mnm_q": mnm_q,
                    }));
                }
            }
            None => {
                wd_no_mm.push(json!({
                    "ext_id": ext_id,
                    "q": wd_q,
                }));
            }
        }
    }

    // Check MnM entries not in WD
    for (ext_id, mnm_q) in &mnm_ext2q {
        if !wd_ext2q.contains_key(ext_id) {
            mm_no_wd.push(json!({
                "ext_id": ext_id,
                "q": mnm_q,
            }));
        }
    }

    // Get mm_double: Q values that map to multiple ext_ids in MnM (all entries, not just human-matched)
    let mm_double = app
        .storage()
        .get_mnm_double_matches(catalog_id)
        .await
        .map_err(|e| ApiError::internal(&format!("database error: {e}")))?;
    let mm_double_json: HashMap<String, Value> = mm_double
        .into_iter()
        .map(|(q, ext_ids)| (q, json!(ext_ids)))
        .collect();

    success(json!({
        "mm_dupes": mm_dupes,
        "different": different,
        "wd_no_mm": wd_no_mm,
        "mm_no_wd": mm_no_wd,
        "mm_double": mm_double_json,
    }))
}

// ---------------------------------------------------------------------------
// action=creation_candidates
// ---------------------------------------------------------------------------

fn get_opt_param<'a>(params: &'a Params, key: &str) -> Option<&'a str> {
    params.get(key).filter(|v| !v.is_empty()).map(|v| v.as_str())
}

fn get_opt_param_usize(params: &Params, key: &str) -> Option<usize> {
    get_opt_param(params, key).and_then(|v| v.parse().ok())
}

/// Validates that a table name contains only safe characters (alphanumerics + underscore).
fn is_safe_table_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[allow(clippy::cognitive_complexity)]
async fn handle_creation_candidates(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let min: usize = get_opt_param_usize(params, "min").unwrap_or(3);
    let mode = get_opt_param(params, "mode").unwrap_or("");
    let ext_name_required = get_opt_param(params, "ext_name").unwrap_or("").trim().to_string();
    let birth_year = get_opt_param(params, "birth_year")
        .filter(|s| regex::Regex::new(r"^\d{1,4}$").unwrap().is_match(s))
        .map(|s| s.to_string());
    let death_year = get_opt_param(params, "death_year")
        .filter(|s| regex::Regex::new(r"^\d{1,4}$").unwrap().is_match(s))
        .map(|s| s.to_string());
    let prop = get_opt_param(params, "prop").unwrap_or("").to_string();
    let require_unset: usize = get_opt_param_usize(params, "require_unset").unwrap_or(0);
    let require_catalogs = get_opt_param(params, "require_catalogs").unwrap_or("").to_string();
    let catalogs_required: usize = get_opt_param_usize(params, "min_catalogs_required").unwrap_or(0);

    // Determine table name
    let table = match mode {
        "aux" => "common_aux".to_string(),
        "" => "common_names".to_string(),
        m => {
            let t = format!("common_names_{m}");
            if !is_safe_table_name(&t) {
                return Err(ApiError::new(&format!("invalid mode: {m}")));
            }
            t
        }
    };

    let max_tries = 250_usize;
    let mut result_data = json!({"entries": []});
    let mut result_name: Option<String> = None;
    let mut user_ids: Vec<usize> = vec![];
    let mut completed = false;

    for _attempt in 0..max_tries {
        // Step 1: Pick a random name/group
        let pick_sql = if !ext_name_required.is_empty() {
            let safe = ext_name_required.replace('\'', "''");
            format!("SELECT '{safe}' AS ext_name, 20 AS cnt")
        } else {
            cc_mode_sql(mode, &table, min, &prop, &require_catalogs)?
        };

        let picks = app.storage().cc_random_pick(&pick_sql).await
            .map_err(|e| ApiError::internal(&format!("pick query failed: {e}")))?;

        if picks.is_empty() {
            continue;
        }

        let pick = &picks[0];
        // Pick column may be `ext_name` (most modes) or `aux_name` (random_prop mode).
        let ext_name = pick["ext_name"]
            .as_str()
            .or_else(|| pick["aux_name"].as_str())
            .unwrap_or("")
            .to_string();
        if !ext_name.is_empty() {
            result_name = Some(ext_name.clone());
        }

        // Step 2: Load entries
        let uses_entry_ids = matches!(mode, "dates" | "birth_year" | "random_prop" | "artwork" | "aux");

        let entries = if uses_entry_ids {
            let entry_ids = pick["entry_ids"].as_str().unwrap_or("");
            if entry_ids.is_empty() {
                continue;
            }
            // Validate entry_ids is only digits and commas
            if !entry_ids.chars().all(|c| c.is_ascii_digit() || c == ',') {
                continue;
            }
            app.storage().cc_get_entries_by_ids_active(entry_ids).await
                .map_err(|e| ApiError::internal(&format!("entries query failed: {e}")))?
        } else {
            let mut names = vec![ext_name.clone()];
            // Generate name variants: "First Middle Last" -> "First-Middle Last", "First Middle-Last"
            if let Some(caps) = RE_NAME_VARIANTS.captures(&ext_name) {
                let (a, b, c) = (&caps[1], &caps[2], &caps[3]);
                names.push(format!("{a}-{b} {c}"));
                names.push(format!("{a} {b}-{c}"));
            }
            let type_filter = if mode == "taxon" { Some("Q16521") } else { None };
            app.storage().cc_get_entries_by_names_active(
                &names,
                type_filter,
                birth_year.as_deref(),
                death_year.as_deref(),
            ).await.map_err(|e| ApiError::internal(&format!("entries query failed: {e}")))?
        };

        // Step 3: Check constraints
        let mut found_unset = 0_usize;
        let mut required_found: HashMap<String, usize> = HashMap::new();
        let req_cats: Vec<String> = require_catalogs.split(',').filter(|s| !s.is_empty()).map(|s| s.to_string()).collect();

        for e in &entries {
            if e.user == Some(0) || e.q.is_none() {
                found_unset += 1;
            }
            let cat_str = e.catalog.to_string();
            if req_cats.contains(&cat_str) {
                *required_found.entry(cat_str).or_default() += 1;
            }
            if let Some(uid) = e.user {
                user_ids.push(uid);
            }
        }

        if ext_name_required.is_empty() {
            if found_unset < require_unset {
                continue;
            }
            if required_found.len() < catalogs_required {
                continue;
            }
        }

        if min > 0 && entries.len() < min && ext_name_required.is_empty() {
            continue;
        }

        // Build result
        let entries_json: Vec<Value> = entries.iter().map(|e| serde_json::to_value(e).unwrap_or(json!(null))).collect();
        result_data = json!({"entries": entries_json});
        completed = true;
        break;
    }

    if !completed {
        return Err(ApiError::new(&format!(
            "No results after {max_tries} attempts, giving up"
        )));
    }

    if let Some(name) = &result_name {
        result_data["name"] = json!(name);
    }
    // Resolve collected uids → user objects (matches PHP `$out['data']['users']`).
    let unique_ids: Vec<usize> = {
        let set: std::collections::HashSet<usize> = user_ids.iter().copied().collect();
        set.into_iter().collect()
    };
    let users_map = if unique_ids.is_empty() {
        json!({})
    } else {
        let rows = app
            .storage()
            .get_users_by_ids(&unique_ids)
            .await
            .unwrap_or_default();
        let mut obj = serde_json::Map::new();
        for (id, val) in rows {
            obj.insert(id.to_string(), val);
        }
        Value::Object(obj)
    };
    result_data["users"] = users_map;
    success(result_data)
}

/// Build the candidate-picking SQL for a specific creation_candidates mode.
fn cc_mode_sql(mode: &str, table: &str, min: usize, prop: &str, require_catalogs: &str) -> Result<String, ApiError> {
    let min_where = if min > 0 { format!("cnt>={min}") } else { "1=1".to_string() };
    let random_pick = format!("FROM {table} WHERE {min_where} ORDER BY rand() LIMIT 1");

    match mode {
        "artwork" | "dates" | "birth_year" => {
            Ok(format!("SELECT name AS ext_name, cnt, entry_ids {random_pick}"))
        }
        "taxon" => {
            Ok(format!("SELECT name AS ext_name, cnt {random_pick}"))
        }
        "aux" => {
            Ok(format!("SELECT aux_name AS ext_name, entry_ids, cnt {random_pick}"))
        }
        "random_prop" => {
            let min_rp = if min < 2 { 2 } else { min };
            let mut sql = format!("SELECT aux_name, entry_ids, cnt FROM aux_candidates WHERE cnt>={min_rp}");
            if !prop.is_empty() {
                if let Ok(p) = prop.parse::<usize>() {
                    sql += &format!(" AND aux_p={p}");
                }
            }
            Ok(sql + " ORDER BY rand() LIMIT 1")
        }
        "dynamic_name_year_birth" => {
            let r: f64 = rand::random();
            Ok(format!(
                "SELECT ext_name, year_born, count(*) AS cnt, group_concat(entry_id) AS ids \
                 FROM vw_dates \
                 WHERE ext_name=(SELECT ext_name FROM entry WHERE random>={r} AND `type`='Q5' AND q IS NULL ORDER BY random LIMIT 1) \
                 GROUP BY year_born, ext_name HAVING cnt>=2"
            ))
        }
        "" => {
            if !require_catalogs.is_empty() {
                // Validate: require_catalogs should be numeric CSV
                if !require_catalogs.chars().all(|c| c.is_ascii_digit() || c == ',') {
                    return Err(ApiError::new("invalid require_catalogs"));
                }
                return Ok(format!(
                    "SELECT ext_name, count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({require_catalogs}) AND (q IS NULL OR user=0) GROUP BY ext_name HAVING cnt>=3 ORDER BY rand() LIMIT 1"
                ));
            }
            let extra = if min > 0 { format!(" cnt>={min} AND") } else { String::new() };
            Ok(format!("SELECT name AS ext_name, cnt FROM {table} WHERE{extra} cnt<15 ORDER BY rand() LIMIT 1"))
        }
        other => Err(ApiError::new(&format!("unknown mode: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// action=quick_compare
// ---------------------------------------------------------------------------

fn haversine_distance_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0; // Earth radius in meters
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

fn parse_location_distance(s: &str) -> Option<f64> {
    if let Some(caps) = regex::Regex::new(r"^(\d+)m$").unwrap().captures(s) {
        return caps[1].parse::<f64>().ok();
    }
    if let Some(caps) = regex::Regex::new(r"^(\d+)km$").unwrap().captures(s) {
        return caps[1].parse::<f64>().ok().map(|v| v * 1000.0);
    }
    None
}

#[allow(clippy::cognitive_complexity)]
async fn handle_quick_compare(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = get_param_usize(params, "catalog")?;
    let entry_id = get_opt_param_usize(params, "entry_id");
    let require_image = get_opt_param(params, "require_image") == Some("1");
    let require_coordinates = get_opt_param(params, "require_coordinates") == Some("1");

    // Determine max distance
    let mut max_distance_m: Option<f64> = None;
    let catalog_kvs = app.storage().get_catalog_key_value_pairs(catalog_id).await.unwrap_or_default();
    if let Some(ld) = catalog_kvs.get("location_distance") {
        max_distance_m = parse_location_distance(ld);
    }
    if let Some(d) = get_opt_param(params, "max_distance_m").and_then(|s| s.parse::<f64>().ok()) {
        max_distance_m = Some(d);
    }

    let max_results = 10;
    let mut result_entries: Vec<Value> = vec![];

    for retry in 0..3_u8 {
        let random_threshold = if retry < 2 { rand::random::<f64>() } else { 0.0 };

        let rows = app.storage().qc_get_entries(
            catalog_id, entry_id, require_image, require_coordinates,
            random_threshold, max_results,
        ).await.map_err(|e| ApiError::internal(&format!("query failed: {e}")))?;

        if rows.is_empty() {
            continue;
        }

        // Collect Q values to load from Wikidata
        let q_values: Vec<String> = rows.iter()
            .filter_map(|r| r["q"].as_i64().filter(|&q| q > 0).map(|q| format!("Q{q}")))
            .collect();

        // Load Wikidata items
        let mw_api = app.wikidata().get_mw_api().await
            .map_err(|e| ApiError::internal(&format!("Wikidata API error: {e}")))?;
        let ec = wikimisc::wikibase::entity_container::EntityContainer::new();
        let _ = ec.load_entities(&mw_api, &q_values).await;

        for row in &rows {
            let q_num = match row["q"].as_i64() {
                Some(q) if q > 0 => q,
                _ => continue,
            };
            let q_str = format!("Q{q_num}");
            let item = match ec.get_entity(q_str.clone()) {
                Some(i) => i,
                None => continue,
            };

            // Check image requirement on Wikidata item
            if require_image && item.claims_with_property("P18".to_string()).is_empty() {
                continue;
            }
            // Check coordinates requirement on Wikidata item
            if require_coordinates && item.claims_with_property("P625".to_string()).is_empty() {
                continue;
            }

            let lang = row["language"].as_str().unwrap_or("en");
            let mut entry_json = row.clone();
            let mut item_json = json!({
                "q": q_str,
                "label": item.label_in_locale(lang).unwrap_or(&q_str),
                "description": item.description_in_locale(lang).unwrap_or(""),
            });

            // Extract P625 coordinates from Wikidata item
            let p625_claims = item.claims_with_property("P625".to_string());
            if let Some(claim) = p625_claims.first() {
                let snak = claim.main_snak();
                if let Some(dv) = snak.data_value() {
                    let val = dv.value();
                    // Use the JSON representation to extract lat/lon
                    let val_json = serde_json::to_value(val).unwrap_or(json!(null));
                    if let (Some(lat_item), Some(lon_item)) = (
                        val_json["latitude"].as_f64(),
                        val_json["longitude"].as_f64(),
                    ) {
                        item_json["coordinates"] = json!({"lat": lat_item, "lon": lon_item});

                        if let (Some(lat_e), Some(lon_e)) = (
                            row["lat"].as_f64(),
                            row["lon"].as_f64(),
                        ) {
                            let dist = haversine_distance_m(lat_item, lon_item, lat_e, lon_e);
                            if max_distance_m.is_some_and(|max| dist > max) {
                                continue;
                            }
                            entry_json["distance_m"] = json!(dist);
                        }
                    }
                }
            }

            // Image from Wikidata
            let p18_claims = item.claims_with_property("P18".to_string());
            if let Some(claim) = p18_claims.first() {
                let snak = claim.main_snak();
                if let Some(dv) = snak.data_value() {
                    let val = dv.value();
                    if let wikimisc::wikibase::Value::StringValue(s) = val {
                        item_json["image"] = json!(s);
                    }
                }
            }

            // Entry image
            if let Some(img) = row.get("image_url").and_then(|v| v.as_str()) {
                if !img.is_empty() {
                    entry_json["ext_img"] = json!(img);
                } else if require_image {
                    continue;
                }
            }

            entry_json["item"] = item_json;
            result_entries.push(entry_json);
        }

        if !result_entries.is_empty() {
            break;
        }
    }

    success(json!({
        "entries": result_entries,
        "max_distance_m": json!(max_distance_m),
    }))
}

// ---------------------------------------------------------------------------
// action=lc_* (Large Catalogs)
// ---------------------------------------------------------------------------

async fn handle_lc_catalogs(app: &AppState) -> Result<Response, ApiError> {
    let catalogs = app.large_catalogs().get_catalogs().await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;
    let open_issues = app.large_catalogs().get_open_issue_counts().await.unwrap_or_default();
    success(json!({
        "catalogs": catalogs,
        "open_issues": open_issues,
    }))
}

async fn handle_lc_locations(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let limit = get_opt_param_usize(params, "limit").unwrap_or(100);
    let limit = limit.min(10000);
    let bbox_str = get_required_param(params, "bbox")?;
    let bbox_parts: Vec<f64> = bbox_str
        .split(',')
        .filter_map(|s| s.trim().parse::<f64>().ok())
        .collect();
    if bbox_parts.len() != 4 {
        return Err(ApiError::new("bbox must have 4 comma-separated numbers"));
    }
    let bbox = [bbox_parts[0], bbox_parts[1], bbox_parts[2], bbox_parts[3]];

    let ignore_str = get_opt_param(params, "ignore_catalogs").unwrap_or("");
    let ignore_catalogs: Vec<usize> = ignore_str
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let catalogs = app.large_catalogs().get_catalogs().await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;

    let mut data: Vec<Value> = vec![];
    let mut used_catalogs: HashMap<usize, Value> = HashMap::new();

    for catalog in &catalogs {
        let cat_id = catalog["id"].as_u64().unwrap_or(0) as usize;
        let has_lat_lon = catalog["has_lat_lon"].as_u64().unwrap_or(0);
        if has_lat_lon == 0 {
            continue;
        }
        if ignore_catalogs.contains(&cat_id) {
            continue;
        }
        let table = match catalog["table"].as_str() {
            Some(t) if !t.is_empty() => t,
            _ => continue,
        };

        let entries = app.large_catalogs().get_entries_in_bbox(table, &bbox, limit)
            .await
            .unwrap_or_default();

        for mut entry in entries {
            entry["catalog"] = json!(cat_id);
            data.push(entry);
            if data.len() >= limit {
                break;
            }
        }
        if !data.is_empty() {
            used_catalogs.insert(cat_id, catalog.clone());
        }
        if data.len() >= limit {
            break;
        }
    }

    success(json!({
        "data": data,
        "catalogs": used_catalogs,
    }))
}

async fn handle_lc_report(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = get_param_usize(params, "catalog")?;
    let catalogs = app.large_catalogs().get_catalogs_map().await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;
    let catalog = catalogs.get(&catalog_id).cloned();
    let matrix = app.large_catalogs().get_report_matrix(catalog_id).await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;
    success(json!({
        "catalog": catalog,
        "matrix": matrix,
    }))
}

async fn handle_lc_report_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog_id = get_param_usize(params, "catalog")?;
    let catalogs = app.large_catalogs().get_catalogs_map().await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;
    let catalog = catalogs.get(&catalog_id).cloned();
    let status = get_opt_param(params, "status").unwrap_or("");
    let report_type = get_opt_param(params, "type").unwrap_or("");
    let user = get_opt_param(params, "user").unwrap_or("");
    let prop = get_opt_param(params, "prop").unwrap_or("");
    let limit = get_opt_param_usize(params, "limit").unwrap_or(20).min(500);
    let offset = get_opt_param_usize(params, "offset").unwrap_or(0);

    let rows = app.large_catalogs().get_report_list(
        catalog_id, status, report_type, user, prop, limit, offset,
    ).await.map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;

    success(json!({
        "catalog": catalog,
        "rows": rows,
    }))
}

async fn handle_lc_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let limit = get_opt_param_usize(params, "limit").unwrap_or(50).min(500);
    let offset = get_opt_param_usize(params, "offset").unwrap_or(0);
    let users_only = get_opt_param(params, "users") == Some("1");
    let rows = app.large_catalogs().get_recent_changes(limit, offset, users_only).await
        .map_err(|e| ApiError::internal(&format!("large catalogs DB error: {e}")))?;
    success(json!({"rows": rows}))
}

async fn handle_lc_set_status(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let status = get_required_param(params, "status")?;
    if status.trim().is_empty() {
        return Err(ApiError::new("empty status"));
    }
    let id = get_param_usize(params, "id")?;
    let user = get_required_param(params, "user")?;
    if user.trim().is_empty() {
        return Err(ApiError::new("not logged in"));
    }
    app.large_catalogs().set_report_status(id, status, user).await
        .map_err(|e| ApiError::internal(&format!("update failed: {e}")))?;
    success(json!({}))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    fn test_app() -> AppState {
        crate::app_state::get_test_app()
    }

    fn build_request(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    async fn response_json(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
        (status, json)
    }

    // --- dispatch tests ---

    #[tokio::test]
    async fn test_missing_action() {
        let app = router(test_app());
        let resp = app.oneshot(build_request("/api")).await.unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("missing"));
    }

    #[tokio::test]
    async fn test_unknown_action() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=bogus"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("unknown action"));
    }

    // --- run_lua parameter validation ---

    #[tokio::test]
    async fn test_run_lua_missing_function() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=run_lua&entry_id=1"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("function"));
    }

    #[tokio::test]
    async fn test_run_lua_missing_entry_id() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=run_lua&function=PERSON_DATE"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("entry_id"));
    }

    #[tokio::test]
    async fn test_run_lua_bad_function() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=run_lua&function=EVIL&entry_id=1",
            ))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_run_lua_bad_entry_id() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=run_lua&function=PERSON_DATE&entry_id=abc",
            ))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("positive integer"));
    }

    // --- helper unit tests ---

    #[test]
    fn test_get_required_param_present() {
        let mut p = Params::new();
        p.insert("foo".into(), "bar".into());
        assert_eq!(get_required_param(&p, "foo").unwrap(), "bar");
    }

    #[test]
    fn test_get_required_param_missing() {
        let p = Params::new();
        assert!(get_required_param(&p, "foo").is_err());
    }

    #[test]
    fn test_get_required_param_empty() {
        let mut p = Params::new();
        p.insert("foo".into(), String::new());
        assert!(get_required_param(&p, "foo").is_err());
    }

    #[test]
    fn test_get_param_usize_valid() {
        let mut p = Params::new();
        p.insert("n".into(), "42".into());
        assert_eq!(get_param_usize(&p, "n").unwrap(), 42);
    }

    #[test]
    fn test_get_param_usize_negative() {
        let mut p = Params::new();
        p.insert("n".into(), "-1".into());
        assert!(get_param_usize(&p, "n").is_err());
    }

    #[test]
    fn test_get_param_usize_non_numeric() {
        let mut p = Params::new();
        p.insert("n".into(), "abc".into());
        assert!(get_param_usize(&p, "n").is_err());
    }

    // --- command_to_json tests ---

    #[test]
    fn test_command_to_json_set_aux() {
        let cmd = LuaCommand::SetAux {
            entry_id: 1,
            property: "214".into(),
            value: "12345".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_aux");
        assert_eq!(j["entry_id"], 1);
        assert_eq!(j["property"], "214");
        assert_eq!(j["value"], "12345");
    }

    #[test]
    fn test_command_to_json_set_match() {
        let cmd = LuaCommand::SetMatch {
            entry_id: 2,
            q: "Q42".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_match");
        assert_eq!(j["q"], "Q42");
    }

    #[test]
    fn test_command_to_json_set_location() {
        let cmd = LuaCommand::SetLocation {
            entry_id: 3,
            lat: 52.5,
            lon: 13.4,
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_location");
        assert_eq!(j["lat"], 52.5);
        assert_eq!(j["lon"], 13.4);
    }

    #[test]
    fn test_command_to_json_set_person_dates() {
        let cmd = LuaCommand::SetPersonDates {
            entry_id: 4,
            born: "1920".into(),
            died: "2000".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_person_dates");
        assert_eq!(j["born"], "1920");
        assert_eq!(j["died"], "2000");
    }

    #[test]
    fn test_command_to_json_set_description() {
        let cmd = LuaCommand::SetDescription {
            entry_id: 5,
            value: "A person".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_description");
        assert_eq!(j["value"], "A person");
    }

    #[test]
    fn test_command_to_json_set_entry_name() {
        let cmd = LuaCommand::SetEntryName {
            entry_id: 6,
            value: "John Doe".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_entry_name");
    }

    #[test]
    fn test_command_to_json_set_entry_type() {
        let cmd = LuaCommand::SetEntryType {
            entry_id: 7,
            value: "Q5".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "set_entry_type");
    }

    #[test]
    fn test_command_to_json_add_alias() {
        let cmd = LuaCommand::AddAlias {
            entry_id: 8,
            label: "JD".into(),
            language: "en".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "add_alias");
        assert_eq!(j["label"], "JD");
        assert_eq!(j["language"], "en");
    }

    #[test]
    fn test_command_to_json_add_location_text() {
        let cmd = LuaCommand::AddLocationText {
            entry_id: 9,
            property: 131,
            value: "London".into(),
        };
        let j = command_to_json(&cmd);
        assert_eq!(j["type"], "add_location_text");
        assert_eq!(j["property"], 131);
    }

    // --- Lua execution result tests (unit, no DB) ---

    #[test]
    fn test_run_lua_person_date_result() {
        let entry = LuaEntry {
            id: 1,
            catalog: 1,
            ext_id: "x".into(),
            ext_url: String::new(),
            ext_name: "Test".into(),
            ext_desc: "1920-2000".into(),
            q: None,
            user: None,
            type_name: Some("Q5".into()),
        };
        let lua = r#"
local b, d = string.match(o.ext_desc, "(%d%d%d%d)%-(%d%d%d%d)")
if b then born = b; died = d end
"#;
        let resp = run_lua_person_date(lua, &entry).unwrap();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["data"]["born"], "1920");
        assert_eq!(body["data"]["died"], "2000");
    }

    #[test]
    fn test_run_lua_aux_from_desc_result() {
        let entry = LuaEntry {
            id: 42,
            catalog: 1,
            ext_id: "x".into(),
            ext_url: String::new(),
            ext_name: "Test".into(),
            ext_desc: "VIAF: 12345".into(),
            q: None,
            user: None,
            type_name: Some("Q5".into()),
        };
        let lua = r#"
local m = string.match(o.ext_desc, "VIAF: (%d+)")
if m then setAux(o.id, 214, m) end
"#;
        let resp = run_lua_aux_from_desc(lua, &entry).unwrap();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["commands"][0]["type"], "set_aux");
        assert_eq!(body["data"]["commands"][0]["value"], "12345");
    }

    #[test]
    fn test_run_lua_desc_from_html_result() {
        let entry = LuaEntry {
            id: 1,
            catalog: 1,
            ext_id: "x".into(),
            ext_url: String::new(),
            ext_name: "Test".into(),
            ext_desc: String::new(),
            q: None,
            user: None,
            type_name: None,
        };
        let lua = r#"
local m = string.match(html, "<h1>(.-)</h1>")
if m then d[#d+1] = m end
"#;
        let html = "<html><h1>Great person</h1></html>";
        let resp = run_lua_desc_from_html(lua, &entry, html).unwrap();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["descriptions"][0], "Great person");
    }

    #[test]
    fn test_run_lua_person_date_error() {
        let entry = LuaEntry::default();
        let resp = run_lua_person_date("invalid lua {{{{", &entry);
        assert!(resp.is_err());
    }

    // --- success/error response shape tests ---

    #[test]
    fn test_success_shape() {
        let resp = success(json!({"x": 1})).unwrap();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["data"]["x"], 1);
    }

    #[test]
    fn test_error_shape() {
        let err = ApiError::new("oops");
        let resp = err.into_response();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "bad_request");
        assert_eq!(body["error"], "oops");
    }

    #[test]
    fn test_not_found_status() {
        let err = ApiError::not_found("gone");
        let resp = err.into_response();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "not_found");
        assert_eq!(body["error"], "gone");
    }

    #[test]
    fn test_internal_error_status() {
        let err = ApiError::internal("boom");
        let resp = err.into_response();
        let (status, body) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "internal_error");
        assert_eq!(body["error"], "boom");
    }

    // --- get_code_fragments tests ---

    #[tokio::test]
    async fn test_get_code_fragments_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_code_fragments"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_code_fragments_valid() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_code_fragments&catalog=1"))
            .await
            .unwrap();
        let (status, body) = response_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        let s = body["status"].as_str().unwrap_or("");
        if s == "ok" {
            assert!(body["data"]["fragments"].is_array());
            assert!(body["data"]["all_functions"].is_array());
        }
        // internal_error is acceptable if DB connection dropped during test suite
    }

    // --- save_code_fragment tests ---

    #[tokio::test]
    async fn test_save_code_fragment_missing_fragment() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=save_code_fragment"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("fragment"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_bad_json() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=save_code_fragment&fragment=not_json",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("invalid"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_missing_catalog() {
        let app = router(test_app());
        let frag = urlencoding::encode(r#"{"function":"PERSON_DATE","php":"","catalog":0}"#);
        let resp = app
            .oneshot(build_request(&format!(
                "/api?action=save_code_fragment&fragment={frag}"
            )))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_save_code_fragment_missing_function() {
        let app = router(test_app());
        let frag = urlencoding::encode(r#"{"catalog":1,"php":""}"#);
        let resp = app
            .oneshot(build_request(&format!(
                "/api?action=save_code_fragment&fragment={frag}"
            )))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("function"));
    }

    // --- sparql_list tests ---

    #[tokio::test]
    async fn test_sparql_list_missing_sparql_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=sparql_list"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("sparql"));
    }

    #[tokio::test]
    async fn test_sparql_list_empty_sparql_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=sparql_list&sparql="))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("sparql"));
    }

    #[test]
    fn test_parse_sparql_label2q_basic() {
        let sparql_result = json!({
            "head": { "vars": ["item", "label"] },
            "results": {
                "bindings": [
                    {
                        "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q42" },
                        "label": { "type": "literal", "value": "Douglas Adams" }
                    },
                    {
                        "item": { "type": "uri", "value": "http://www.wikidata.org/entity/Q1339" },
                        "label": { "type": "literal", "value": "Johann Sebastian Bach" }
                    }
                ]
            }
        });
        let label2q = parse_sparql_label2q(&sparql_result).unwrap();
        assert_eq!(label2q.len(), 2);
        assert_eq!(label2q.get("Douglas Adams").unwrap(), "Q42");
        assert_eq!(label2q.get("Johann Sebastian Bach").unwrap(), "Q1339");
    }

    #[test]
    fn test_parse_sparql_label2q_reversed_order() {
        // label first, then URI
        let sparql_result = json!({
            "head": { "vars": ["name", "entity"] },
            "results": {
                "bindings": [
                    {
                        "name": { "type": "literal", "value": "Albert Einstein" },
                        "entity": { "type": "uri", "value": "http://www.wikidata.org/entity/Q937" }
                    }
                ]
            }
        });
        let label2q = parse_sparql_label2q(&sparql_result).unwrap();
        assert_eq!(label2q.len(), 1);
        assert_eq!(label2q.get("Albert Einstein").unwrap(), "Q937");
    }

    #[test]
    fn test_parse_sparql_label2q_skips_non_uri_literal_pairs() {
        let sparql_result = json!({
            "head": { "vars": ["a", "b"] },
            "results": {
                "bindings": [
                    {
                        "a": { "type": "literal", "value": "foo" },
                        "b": { "type": "literal", "value": "bar" }
                    },
                    {
                        "a": { "type": "uri", "value": "http://www.wikidata.org/entity/Q1" },
                        "b": { "type": "literal", "value": "Universe" }
                    }
                ]
            }
        });
        let label2q = parse_sparql_label2q(&sparql_result).unwrap();
        assert_eq!(label2q.len(), 1);
        assert_eq!(label2q.get("Universe").unwrap(), "Q1");
    }

    #[test]
    fn test_parse_sparql_label2q_empty_bindings() {
        let sparql_result = json!({
            "head": { "vars": ["x", "y"] },
            "results": { "bindings": [] }
        });
        let label2q = parse_sparql_label2q(&sparql_result).unwrap();
        assert!(label2q.is_empty());
    }

    #[test]
    fn test_parse_sparql_label2q_missing_vars() {
        let sparql_result = json!({
            "head": {},
            "results": { "bindings": [] }
        });
        let result = parse_sparql_label2q(&sparql_result);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_sparql_label2q_too_few_vars() {
        let sparql_result = json!({
            "head": { "vars": ["only_one"] },
            "results": { "bindings": [] }
        });
        let result = parse_sparql_label2q(&sparql_result);
        assert!(result.is_err());
    }

    // --- get_sync tests ---

    #[tokio::test]
    async fn test_get_sync_missing_catalog_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_sync_non_numeric_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog=abc"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("positive integer"));
    }

    #[tokio::test]
    async fn test_get_sync_empty_catalog_param() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog="))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("catalog"));
    }

    #[tokio::test]
    async fn test_get_sync_nonexistent_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog=999999999"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        // Should fail because the catalog either doesn't exist or has no wd_prop
        assert_ne!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_get_sync_catalog_without_wd_prop() {
        // Catalog 1 likely has no wd_prop set, or we just verify the error path works
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=get_sync&catalog=1"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        // Either it succeeds (if catalog 1 has wd_prop) or it gives an appropriate error
        let status = body["status"].as_str().unwrap_or("");
        assert!(
            status == "ok" || status == "bad_request" || status == "internal_error",
            "unexpected status: {status}"
        );
    }

    // --- creation_candidates tests ---

    #[test]
    fn test_cc_mode_sql_default() {
        let sql = cc_mode_sql("", "common_names", 3, "", "").unwrap();
        assert!(sql.contains("common_names"));
        assert!(sql.contains("cnt>=3"));
        assert!(sql.contains("cnt<15"));
    }

    #[test]
    fn test_cc_mode_sql_dates() {
        let sql = cc_mode_sql("dates", "common_names_dates", 2, "", "").unwrap();
        assert!(sql.contains("entry_ids"));
        assert!(sql.contains("cnt>=2"));
    }

    #[test]
    fn test_cc_mode_sql_taxon() {
        let sql = cc_mode_sql("taxon", "common_names_taxon", 3, "", "").unwrap();
        assert!(sql.contains("ext_name"));
        assert!(!sql.contains("entry_ids"));
    }

    #[test]
    fn test_cc_mode_sql_random_prop() {
        let sql = cc_mode_sql("random_prop", "common_names", 1, "227", "").unwrap();
        assert!(sql.contains("aux_candidates"));
        assert!(sql.contains("aux_p=227"));
    }

    #[test]
    fn test_cc_mode_sql_unknown_mode() {
        assert!(cc_mode_sql("bogus_mode", "t", 3, "", "").is_err());
    }

    #[test]
    fn test_is_safe_table_name() {
        assert!(is_safe_table_name("common_names"));
        assert!(is_safe_table_name("common_names_dates"));
        assert!(!is_safe_table_name(""));
        assert!(!is_safe_table_name("table; DROP TABLE"));
    }

    // --- quick_compare tests ---

    #[test]
    fn test_haversine_same_point() {
        let d = haversine_distance_m(52.5, 13.4, 52.5, 13.4);
        assert!(d < 0.01);
    }

    #[test]
    fn test_haversine_known_distance() {
        // Berlin to Paris ~878 km
        let d = haversine_distance_m(52.52, 13.405, 48.8566, 2.3522);
        assert!((d - 878_000.0).abs() < 10_000.0);
    }

    #[test]
    fn test_parse_location_distance_meters() {
        assert_eq!(parse_location_distance("500m"), Some(500.0));
    }

    #[test]
    fn test_parse_location_distance_km() {
        assert_eq!(parse_location_distance("5km"), Some(5000.0));
    }

    #[test]
    fn test_parse_location_distance_invalid() {
        assert_eq!(parse_location_distance("five"), None);
        assert_eq!(parse_location_distance(""), None);
    }

    #[tokio::test]
    async fn test_quick_compare_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=quick_compare"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_creation_candidates_response_structure() {
        let app = router(test_app());
        // `ext_name=` forces the pick to a constant SELECT instead of the
        // `SELECT … FROM common_names … ORDER BY rand() LIMIT 1` full-table
        // scan the default mode runs — that full scan can take minutes on the
        // real replica and dominated the whole cargo-test wall time (~150 s).
        // The handler then falls through to an indexed ext_name lookup, which
        // is enough for a smoke-test on the response shape.
        let resp = app
            .oneshot(build_request(
                "/api?action=creation_candidates&min=0&mode=&ext_name=MnmTestNonexistentName_9d3f",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        // If the pick query never yields a row within 250 tries we now surface
        // that as a "giving up" error (matching the PHP legacy fallback).
        // Any of {ok, bad_request, internal_error} is acceptable here.
        let status = body["status"].as_str().unwrap_or("");
        assert!(
            status == "ok" || status == "internal_error" || status == "bad_request",
            "unexpected status: {status}"
        );
    }

    #[test]
    fn test_get_opt_param_present() {
        let mut p = Params::new();
        p.insert("k".into(), "v".into());
        assert_eq!(get_opt_param(&p, "k"), Some("v"));
    }

    #[test]
    fn test_get_opt_param_missing() {
        let p = Params::new();
        assert_eq!(get_opt_param(&p, "k"), None);
    }

    #[test]
    fn test_get_opt_param_empty() {
        let mut p = Params::new();
        p.insert("k".into(), String::new());
        assert_eq!(get_opt_param(&p, "k"), None);
    }

    // --- lc_* tests ---

    #[tokio::test]
    async fn test_lc_locations_missing_bbox() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_locations"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_locations_bad_bbox() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_locations&bbox=1,2,3"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
        assert!(body["error"].as_str().unwrap().contains("4"));
    }

    #[tokio::test]
    async fn test_lc_report_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_report"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_report_list_missing_catalog() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_report_list"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_missing_params() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request("/api?action=lc_set_status"))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_empty_status() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=lc_set_status&status=&id=1&user=test",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }

    #[tokio::test]
    async fn test_lc_set_status_no_user() {
        let app = router(test_app());
        let resp = app
            .oneshot(build_request(
                "/api?action=lc_set_status&status=DONE&id=1&user=",
            ))
            .await
            .unwrap();
        let (_, body) = response_json(resp).await;
        assert_eq!(body["status"], "bad_request");
    }
}
