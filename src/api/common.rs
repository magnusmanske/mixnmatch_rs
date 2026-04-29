use crate::app_state::ExternalServicesContext;
use crate::auth;
use crate::entry::Entry;
use axum::Json;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::collections::HashSet;
use tower_sessions::Session;

/// Query parameter map used by API handlers.
pub type Params = HashMap<String, String>;

// ---------------------------------------------------------------------------
// Parameter extraction helpers
// ---------------------------------------------------------------------------

pub fn get_param(params: &Params, key: &str, default: &str) -> String {
    params
        .get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

pub fn get_param_int(params: &Params, key: &str, default: i64) -> i64 {
    params
        .get(key)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

pub fn get_catalog(params: &Params) -> Result<usize, ApiError> {
    let id = get_param_int(params, "catalog", 0);
    if id <= 0 {
        return Err(ApiError("Invalid catalog ID".into()));
    }
    Ok(id as usize)
}

/// Parse a non-negative integer parameter; return an error if the value is
/// missing or negative.  Use for IDs and other mandatory unsigned fields.
pub fn get_param_usize(params: &Params, key: &str) -> Result<usize, ApiError> {
    let v = get_param_int(params, key, -1);
    if v < 0 {
        return Err(ApiError(format!("Invalid or missing parameter: {key}")));
    }
    Ok(v as usize)
}

/// Parse a `limit` parameter clamped to `[1, max]`, defaulting to `default`.
pub fn get_limit(params: &Params, default: usize, max: usize) -> usize {
    let v = get_param_int(params, "limit", default as i64);
    v.max(1).min(max as i64) as usize
}

/// Parse an `offset` parameter, flooring at 0.
pub fn get_offset(params: &Params) -> usize {
    get_param_int(params, "offset", 0).max(0) as usize
}

/// Require a logged-in user and return their MnM user id.  Convenience
/// wrapper around `auth::guard::require_user_from_params` for handlers that
/// only need the numeric id, not the full `AuthedUser` struct.
pub async fn require_user_id(
    app: &dyn ExternalServicesContext,
    session: &Session,
    params: &Params,
) -> Result<usize, ApiError> {
    Ok(auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id)
}

// ---------------------------------------------------------------------------
// ApiError
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ApiError(pub String);

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self(err.to_string())
    }
}

impl From<String> for ApiError {
    fn from(msg: String) -> Self {
        Self(msg)
    }
}

impl From<&str> for ApiError {
    fn from(msg: &str) -> Self {
        Self(msg.to_string())
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = json!({ "status": self.0 });
        (axum::http::StatusCode::OK, Json(body)).into_response()
    }
}

// ---------------------------------------------------------------------------
// Success helper
// ---------------------------------------------------------------------------

pub fn success_with_data(data: Value) -> Json<Value> {
    Json(json!({ "status": "OK", "data": data }))
}

/// Wrap an arbitrary JSON value as an axum response.
pub fn json_resp(v: Value) -> Response {
    Json(v).into_response()
}

/// Standard `{"status":"OK","data":…}` envelope. Most handlers use this.
pub fn ok(data: Value) -> Response {
    success_with_data(data).into_response()
}

// ---------------------------------------------------------------------------
// Entries to JSON data helper
// ---------------------------------------------------------------------------

/// Convert a slice of Entry objects into the standard `{"entries": {...}, "users": {...}}` JSON structure.
/// Entries are keyed by their id. Users are looked up from the entry user fields.
pub async fn entries_to_json_data(entries: &[Entry], app: &dyn ExternalServicesContext) -> Result<Value, ApiError> {
    let mut entries_map = serde_json::Map::new();
    let mut user_ids = HashSet::new();
    for entry in entries {
        if let Some(id) = entry.id {
            entries_map.insert(
                id.to_string(),
                serde_json::to_value(entry).unwrap_or(json!(null)),
            );
            if let Some(user) = entry.user {
                user_ids.insert(user);
            }
        }
    }
    let users = get_users(app, &user_ids).await?;
    Ok(json!({
        "entries": Value::Object(entries_map),
        "users": users,
    }))
}

/// Convert entries to JSON and immediately enrich with extended data.
/// Returns the combined `{"entries": …, "users": …}` value ready for `ok()`.
/// Use this instead of calling `entries_to_json_data` + `add_extended_entry_data`
/// separately when no further mutations to `data` are needed before the response.
pub async fn entries_with_extended_data(
    entries: &[Entry],
    app: &dyn ExternalServicesContext,
) -> Result<Value, ApiError> {
    let mut data = entries_to_json_data(entries, app).await?;
    add_extended_entry_data(app, &mut data).await?;
    Ok(data)
}

// ---------------------------------------------------------------------------
// Extended entry data (mirrors PHP add_extended_entry_data)
// ---------------------------------------------------------------------------

/// Apply a per-entry enrichment from a storage result map.
/// Skips the whole block if the storage call failed.
fn apply_field<T, F>(entries: &mut Value, rows: anyhow::Result<HashMap<usize, T>>, mut f: F)
where
    F: FnMut(&mut Value, T),
{
    if let Ok(map) = rows {
        for (entry_id, value) in map {
            if let Some(entry) = entries.get_mut(entry_id.to_string()) {
                f(entry, value);
            }
        }
    }
}

fn apply_person_dates(entries: &mut Value, rows: anyhow::Result<HashMap<usize, (String, String)>>) {
    apply_field(entries, rows, |entry, (born, died)| {
        if !born.is_empty() {
            entry["born"] = json!(born);
        }
        if !died.is_empty() {
            entry["died"] = json!(died);
        }
    });
}

fn apply_multi_match(entries: &mut Value, rows: anyhow::Result<HashMap<usize, String>>) {
    apply_field(entries, rows, |entry, candidates| {
        let qs: Vec<Value> = candidates
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|c| json!(format!("Q{c}")))
            .collect();
        entry["multimatch"] = json!(qs);
    });
}

#[allow(clippy::type_complexity)]
fn apply_kv(entries: &mut Value, rows: anyhow::Result<HashMap<usize, Vec<(String, String, u8)>>>) {
    apply_field(entries, rows, |entry, kv_rows| {
        for (kv_key, kv_value, done) in &kv_rows {
            entry[kv_key] = json!([kv_value, done]);
        }
    });
}

pub async fn add_extended_entry_data(app: &dyn ExternalServicesContext, data: &mut Value) -> Result<(), ApiError> {
    let entries = match data.get_mut("entries") {
        Some(e) if e.is_object() => e,
        _ => return Ok(()),
    };

    let entry_ids: Vec<usize> = entries
        .as_object()
        .map(|m| m.keys().filter_map(|k| k.parse::<usize>().ok()).collect())
        .unwrap_or_default();

    if entry_ids.is_empty() {
        return Ok(());
    }

    let s = app.storage();
    let (person_dates, locations, multi_match, auxiliary, aliases, descriptions, kv, mnm_relations) = tokio::join!(
        s.api_get_person_dates_for_entries(&entry_ids),
        s.api_get_locations_for_entries(&entry_ids),
        s.api_get_multi_match_for_entries(&entry_ids),
        s.api_get_auxiliary_for_entries(&entry_ids),
        s.api_get_aliases_for_entries(&entry_ids),
        s.api_get_descriptions_for_entries(&entry_ids),
        s.api_get_kv_for_entries(&entry_ids),
        s.api_get_mnm_relations_for_entries(&entry_ids),
    );

    apply_person_dates(entries, person_dates);
    apply_field(entries, locations, |e, (lat, lon)| {
        e["lat"] = json!(lat);
        e["lon"] = json!(lon);
    });
    apply_multi_match(entries, multi_match);
    apply_field(entries, auxiliary, |e, v| {
        e["aux"] = json!(v);
    });
    apply_field(entries, aliases, |e, v| {
        e["aliases"] = json!(v);
    });
    apply_field(entries, descriptions, |e, v| {
        e["descriptions"] = json!(v);
    });
    apply_kv(entries, kv);
    apply_field(entries, mnm_relations, |e, v| {
        e["relation"] = json!(v);
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// User lookup
// ---------------------------------------------------------------------------

pub async fn get_users(app: &dyn ExternalServicesContext, user_ids: &HashSet<usize>) -> Result<Value, ApiError> {
    if user_ids.is_empty() {
        return Ok(json!({}));
    }
    let ids: Vec<usize> = user_ids.iter().copied().collect();
    let users = app.storage().get_users_by_ids(&ids).await?;
    let mut result = serde_json::Map::new();
    for (id, user_val) in users {
        result.insert(id.to_string(), user_val);
    }
    Ok(Value::Object(result))
}

// Auth is now enforced via `crate::auth::guard::require_user` against the
// session — callers must no longer trust the request-body `username` field.
// The previous `check_user` helper has been removed intentionally: any handler
// that still needs to identify a user must call the guard module.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_param_present() {
        let mut p = Params::new();
        p.insert("foo".into(), "bar".into());
        assert_eq!(get_param(&p, "foo", "default"), "bar");
    }

    #[test]
    fn test_get_param_missing() {
        let p = Params::new();
        assert_eq!(get_param(&p, "foo", "default"), "default");
    }

    #[test]
    fn test_get_param_empty_uses_default() {
        let mut p = Params::new();
        p.insert("foo".into(), String::new());
        assert_eq!(get_param(&p, "foo", "default"), "default");
    }

    #[test]
    fn test_get_param_int_present() {
        let mut p = Params::new();
        p.insert("n".into(), "42".into());
        assert_eq!(get_param_int(&p, "n", 0), 42);
    }

    #[test]
    fn test_get_param_int_negative() {
        let mut p = Params::new();
        p.insert("n".into(), "-7".into());
        assert_eq!(get_param_int(&p, "n", 0), -7);
    }

    #[test]
    fn test_get_param_int_missing() {
        let p = Params::new();
        assert_eq!(get_param_int(&p, "n", 99), 99);
    }

    #[test]
    fn test_get_param_int_non_numeric() {
        let mut p = Params::new();
        p.insert("n".into(), "abc".into());
        assert_eq!(get_param_int(&p, "n", 5), 5);
    }

    #[test]
    fn test_get_catalog_valid() {
        let mut p = Params::new();
        p.insert("catalog".into(), "123".into());
        assert_eq!(get_catalog(&p).unwrap(), 123);
    }

    #[test]
    fn test_get_catalog_zero() {
        let mut p = Params::new();
        p.insert("catalog".into(), "0".into());
        assert!(get_catalog(&p).is_err());
    }

    #[test]
    fn test_get_catalog_missing() {
        let p = Params::new();
        assert!(get_catalog(&p).is_err());
    }

    #[test]
    fn test_api_error_from_string() {
        let err = ApiError::from("something broke".to_string());
        assert_eq!(err.0, "something broke");
    }

    #[test]
    fn test_api_error_from_anyhow() {
        let err = ApiError::from(anyhow::anyhow!("anyhow problem"));
        assert_eq!(err.0, "anyhow problem");
    }

    #[test]
    fn test_api_error_into_response() {
        let err = ApiError("bad request".into());
        let response = err.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[test]
    fn test_success_with_data() {
        let data = json!({"count": 3});
        let Json(envelope) = success_with_data(data);
        assert_eq!(envelope["status"], "OK");
        assert_eq!(envelope["data"]["count"], 3);
    }

    /// Pin the wire format for `entries_to_json_data` — the function
    /// behind every `query=catalog`-shaped response. The frontend reads
    /// `entry.type` everywhere (entry_list_item's Initial-search button,
    /// entry_details' type row, …) so the key has to be `type`, not the
    /// Rust field name `type_name`.
    #[test]
    fn entries_to_json_data_uses_type_not_type_name() {
        let entry = Entry {
            id: Some(42),
            catalog: 1,
            ext_id: "x".into(),
            ext_name: "H.M.Manske".into(),
            type_name: Some("Q5".into()),
            ..Default::default()
        };
        // `entries_to_json_data` is async and takes &dyn ExternalServicesContext
        // to resolve user names; for a single entry with user=None the
        // user lookup is skipped, so we can test the serialisation
        // shape without a DB by mirroring the map-building step.
        let mut entries_map = serde_json::Map::new();
        entries_map.insert(
            entry.id.unwrap().to_string(),
            serde_json::to_value(&entry).unwrap(),
        );
        let data = json!({"entries": Value::Object(entries_map), "users": json!({})});
        let entry_json = &data["entries"]["42"];
        assert_eq!(
            entry_json["type"], json!("Q5"),
            "must serialise as `type` (frontend contract)"
        );
        assert!(
            entry_json.get("type_name").is_none(),
            "must not also emit the Rust field name `type_name`: {entry_json}"
        );
    }
}
