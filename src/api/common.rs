use crate::app_state::AppState;
use crate::entry::Entry;
use axum::Json;
use axum::response::IntoResponse;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::collections::HashSet;

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

// ---------------------------------------------------------------------------
// Entries to JSON data helper
// ---------------------------------------------------------------------------

/// Convert a slice of Entry objects into the standard `{"entries": {...}, "users": {...}}` JSON structure.
/// Entries are keyed by their id. Users are looked up from the entry user fields.
pub async fn entries_to_json_data(entries: &[Entry], app: &AppState) -> Result<Value, ApiError> {
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
            if let Some(entry) = entries.get_mut(&entry_id.to_string()) {
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

fn apply_kv(entries: &mut Value, rows: anyhow::Result<HashMap<usize, Vec<(String, String, u8)>>>) {
    apply_field(entries, rows, |entry, kv_rows| {
        for (kv_key, kv_value, done) in &kv_rows {
            entry[kv_key] = json!([kv_value, done]);
        }
    });
}

pub async fn add_extended_entry_data(app: &AppState, data: &mut Value) -> Result<(), ApiError> {
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

pub async fn get_users(app: &AppState, user_ids: &HashSet<usize>) -> Result<Value, ApiError> {
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

// ---------------------------------------------------------------------------
// Auth helper (simplified — full OAuth would need Widar integration)
// ---------------------------------------------------------------------------

pub async fn check_user(app: &AppState, params: &Params) -> Result<usize, ApiError> {
    let username = get_param(params, "username", "");
    let tusc_user = get_param(params, "tusc_user", "");
    let name = if !username.is_empty() {
        username
    } else {
        tusc_user
    };
    if name.is_empty() || name == "-1" {
        return Err(ApiError("OAuth login required".into()));
    }
    let user_id = app.storage().get_or_create_user_id(&name).await?;
    if user_id == 0 {
        return Err(ApiError("OAuth login failure, please log in again".into()));
    }
    Ok(user_id)
}

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
}
