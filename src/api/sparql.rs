//! `sparql_list`: run a SPARQL query that yields (label, item) pairs and
//! return any *unmatched* MnM entries whose `ext_name` matches one of those
//! labels, with the corresponding Q pre-filled.

use crate::api::common::{ApiError, Params, ok};
use crate::app_state::AppState;
use axum::response::Response;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::OnceLock;

fn re_sparql_q() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"(Q\d+)$").expect("valid regex"))
}

/// Axum-shape entry point for `?query=sparql_list&sparql=…`.
pub async fn query_sparql_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    Ok(ok(list_from_params(app, params).await?))
}

/// Parse SPARQL bindings and build a label-to-Q mapping.
///
/// Each binding must have exactly two variables: one URI and one literal.
/// The Q-number is extracted from the URI via a regex that matches the
/// trailing `Q\d+` portion. Variable order in `head.vars` determines which
/// is treated as the URI and which as the label, but both orderings are
/// accepted (the binding's own type tags decide).
pub fn parse_sparql_label2q(
    sparql_result: &Value,
) -> Result<HashMap<String, String>, ApiError> {
    let head_vars = sparql_result["head"]["vars"]
        .as_array()
        .ok_or_else(|| ApiError("SPARQL result missing head.vars".into()))?;
    if head_vars.len() < 2 {
        return Err(ApiError(
            "SPARQL result must have at least 2 variables".into(),
        ));
    }
    let label_var = head_vars[0]
        .as_str()
        .ok_or_else(|| ApiError("variable name is not a string".into()))?;
    let qnum_var = head_vars[1]
        .as_str()
        .ok_or_else(|| ApiError("variable name is not a string".into()))?;

    let bindings = sparql_result["results"]["bindings"]
        .as_array()
        .ok_or_else(|| ApiError("SPARQL result missing results.bindings".into()))?;

    let re = re_sparql_q();
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

        if let Some(caps) = re.captures(uri_val) {
            label2q.insert(lit_val.to_string(), caps[1].to_string());
        }
    }

    Ok(label2q)
}

/// Run the SPARQL query, look up matching unmatched MnM entries, return the
/// {entries, users} payload (no envelope).
pub async fn list(app: &AppState, sparql: &str) -> Result<Value, ApiError> {
    if sparql.is_empty() {
        return Err(ApiError("missing required parameter: sparql".into()));
    }

    let mw_api = app
        .wikidata()
        .get_mw_api()
        .await
        .map_err(|e| ApiError(format!("failed to get Wikidata API: {e}")))?;
    let sparql_result = mw_api
        .sparql_query(sparql)
        .await
        .map_err(|e| ApiError(format!("SPARQL query failed: {e}")))?;

    let label2q = parse_sparql_label2q(&sparql_result)?;

    if label2q.is_empty() {
        return Ok(json!({ "entries": {}, "users": {} }));
    }

    // Load matching unmatched entries + the user-0 (auto user) record in
    // parallel. They're independent reads on the same DB.
    let labels: Vec<String> = label2q.keys().cloned().collect();
    let s = app.storage();
    let (entries_res, users_res) = tokio::join!(
        s.get_entries_by_ext_names_unmatched(&labels),
        s.get_users_by_ids(&[0]),
    );
    let entries = entries_res.map_err(|e| ApiError(format!("database error: {e}")))?;
    let users = users_res.map_err(|e| ApiError(format!("database error: {e}")))?;

    let mut entry_map: HashMap<String, Value> = HashMap::new();
    for entry in &entries {
        if let Some(q_str) = label2q.get(&entry.ext_name) {
            // Strip leading 'Q'; bad strings collapse to 0 which the
            // frontend treats as "no Q pre-filled".
            let q_numeric: isize = q_str[1..].parse().unwrap_or(0);
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

    let users_json: HashMap<String, Value> = users
        .into_iter()
        .map(|(id, val)| (id.to_string(), val))
        .collect();

    Ok(json!({
        "entries": entry_map,
        "users": users_json,
    }))
}

/// Convenience: pull `sparql=…` out of params, then call `list`.
pub async fn list_from_params(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let sparql = params
        .get("sparql")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError("missing required parameter: sparql".into()))?;
    list(app, sparql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_uri_literal_pairs() {
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
    fn accepts_reversed_var_order() {
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
    fn skips_non_uri_literal_pairs() {
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
    fn empty_bindings_return_empty_map() {
        let sparql_result = json!({
            "head": { "vars": ["x", "y"] },
            "results": { "bindings": [] }
        });
        let label2q = parse_sparql_label2q(&sparql_result).unwrap();
        assert!(label2q.is_empty());
    }

    #[test]
    fn missing_vars_is_an_error() {
        let sparql_result = json!({
            "head": {},
            "results": { "bindings": [] }
        });
        assert!(parse_sparql_label2q(&sparql_result).is_err());
    }

    #[test]
    fn too_few_vars_is_an_error() {
        let sparql_result = json!({
            "head": { "vars": ["only_one"] },
            "results": { "bindings": [] }
        });
        assert!(parse_sparql_label2q(&sparql_result).is_err());
    }
}
