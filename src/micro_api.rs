use crate::app_state::AppState;
use crate::code_fragment::{
    self, LuaCommand, LuaEntry,
};
use crate::entry::Entry;
use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

type SharedState = Arc<AppState>;
type Params = HashMap<String, String>;

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
            eprintln!("micro_api: listening on http://0.0.0.0:{port}");
            if let Err(e) = axum::serve(listener, router).await {
                eprintln!("micro_api server error: {e}");
            }
        }
        Err(e) => {
            eprintln!("micro_api: failed to bind to {addr}: {e}");
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
    status: axum::http::StatusCode,
}

impl ApiError {
    fn new(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            status: axum::http::StatusCode::BAD_REQUEST,
        }
    }

    fn not_found(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            status: axum::http::StatusCode::NOT_FOUND,
        }
    }

    fn internal(msg: &str) -> Self {
        Self {
            message: msg.to_string(),
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": self.message });
        (self.status, Json(body)).into_response()
    }
}

fn success(data: Value) -> Result<Response, ApiError> {
    Ok(Json(json!({ "status": "ok", "data": data })).into_response())
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "oops");
    }

    #[test]
    fn test_not_found_status() {
        let err = ApiError::not_found("gone");
        let resp = err.into_response();
        let (status, _) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_internal_error_status() {
        let err = ApiError::internal("boom");
        let resp = err.into_response();
        let (status, _) = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response_json(resp));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }
}
