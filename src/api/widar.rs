//! Widar (OAuth) endpoint and the `oauth_verifier` callback that finishes the dance.

use crate::api::common::{ApiError, Params, json_resp};
use crate::app_state::AppState;
use crate::auth;
use axum::response::{IntoResponse, Redirect, Response};
use tower_sessions::Session;

/// Implements `?query=widar&action=…`. Mirrors PHP `query_widar` →
/// `Widar::render_reponse`, which reads the sub-action from the `action`
/// form field and writes its userinfo into the `result` key (not `data`).
/// Sub-actions: `authorize`, `get_rights`, `logout`.
pub async fn query_widar(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cfg = app
        .oauth_config()
        .ok_or_else(|| ApiError("OAuth is not configured on this server".into()))?
        .clone();
    // Match the PHP Widar convention: the sub-action is the `action` parameter.
    // `widar_action` is accepted as a legacy alias so older callers keep working.
    let action = params
        .get("action")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| params.get("widar_action").cloned())
        .unwrap_or_else(|| "get_rights".to_string());
    match action.as_str() {
        // Per-entry match from the Vue frontend: the caller supplies the
        // entry's ext_id (as the new string claim's value), the target item,
        // and the catalog's wd_prop, and we make the edit on behalf of the
        // OAuth-authenticated user. Mirrors PHP `Widar::set_string`, which
        // is what the mnm-mixins `setEntryQ` flow calls when a catalog has
        // a wd_prop set.
        "set_string" => handle_set_string(app, session, params).await,
        // Free-form mutating call: the frontend builds the full MediaWiki
        // API payload (e.g. `wbeditentity new=item` for new-item creation
        // from prep_new_item, or `wbsetclaim`) and we sign + POST it.
        "generic" => handle_generic(app, session, params).await,
        "authorize" => {
            // Off-toolforge the bypass pretends we're already logged in —
            // just redirect home instead of triggering a real OAuth dance.
            if auth::guard::dev_bypass_user().is_some() {
                return Ok(Redirect::to("/").into_response());
            }
            let token = auth::flow::initiate_request_token(&cfg)
                .await
                .map_err(|e| ApiError(format!("OAuth initiate failed: {e}")))?;
            let new_state = auth::session::SessionData {
                state: auth::session::SessionState::PendingVerifier {
                    request_token_key: token.key.clone(),
                    request_token_secret: token.secret.clone(),
                },
            };
            auth::session::store(session, &new_state).await?;
            let url = auth::flow::build_authorize_redirect_url(&cfg, &token.key);
            Ok(Redirect::to(&url).into_response())
        }
        "logout" => {
            auth::session::clear(session).await?;
            Ok(json_resp(serde_json::json!({
                "status": "OK",
                "error": "OK",
                "data": [],
            })))
        }
        // Default (including the explicit "get_rights").
        _ => {
            // Return the shape PHP `Widar::render_reponse` produces:
            // a top-level `result` holding the rights query, not `data`.
            // The Vue frontend reads `d.result.query.userinfo`.
            if let Some(u) = auth::guard::dev_bypass_user() {
                return Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "query": {
                            "userinfo": {
                                "id": u.mnm_user_id,
                                "name": u.wikidata_username,
                            }
                        }
                    }
                })));
            }
            let data = auth::session::load(session).await;
            match data.state {
                auth::session::SessionState::Authenticated {
                    wikidata_user_id,
                    wikidata_username,
                    ..
                } => Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "query": {
                            "userinfo": {
                                "id": wikidata_user_id,
                                "name": wikidata_username,
                            }
                        }
                    }
                }))),
                _ => Ok(json_resp(serde_json::json!({
                    "status": "OK",
                    "error": "OK",
                    "data": [],
                    "result": {
                        "error": {
                            "code": "mwoauth-invalid-authorization",
                            "info": "Not logged in",
                        }
                    }
                }))),
            }
        }
    }
}

/// JSON envelope the frontend `widar.run` callback expects. Success is
/// signalled by `error: "OK"` at the top level; any other string is shown
/// to the user verbatim.
fn widar_ok() -> Response {
    json_resp(serde_json::json!({
        "status": "OK",
        "error": "OK",
        "data": [],
    }))
}

fn widar_error(msg: impl Into<String>) -> Response {
    json_resp(serde_json::json!({
        "status": "OK",
        "error": msg.into(),
        "data": [],
    }))
}

/// Entry-point for `action=set_string`. Writes a string-valued claim on
/// behalf of the logged-in user using the session's OAuth access token.
async fn handle_set_string(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    // In local dev we bypass OAuth and therefore can't make real edits;
    // return OK so the frontend keeps behaving as if the edit succeeded.
    if auth::guard::dev_bypass_user().is_some() {
        return Ok(widar_ok());
    }

    let cfg = app
        .oauth_config()
        .ok_or_else(|| ApiError("OAuth is not configured on this server".into()))?
        .clone();

    let (entity_id, property_id, value, summary) = match read_set_string_params(params) {
        Ok(v) => v,
        Err(msg) => return Ok(widar_error(msg)),
    };

    let access = match auth::session::load(session).await.state {
        auth::session::SessionState::Authenticated {
            access_token_key,
            access_token_secret,
            ..
        } => auth::flow::TokenPair {
            key: access_token_key,
            secret: access_token_secret,
        },
        _ => return Ok(widar_error("Not logged in")),
    };

    // Skip the edit if the item already carries this exact property=value
    // statement, to avoid introducing duplicates. On lookup failure we fall
    // through and let the edit attempt run — the API still rejects true
    // duplicates server-side in most cases.
    if let Ok(true) = wikidata_string_claim_exists(&entity_id, &property_id, &value).await {
        return Ok(widar_ok());
    }

    match auth::flow::wikidata_create_string_claim(
        &cfg,
        &access,
        &entity_id,
        &property_id,
        &value,
        &summary,
    )
    .await
    {
        Ok(v) => {
            if let Some(err) = v.get("error") {
                let info = err
                    .get("info")
                    .and_then(|i| i.as_str())
                    .unwrap_or("Wikidata API error");
                return Ok(widar_error(info.to_string()));
            }
            Ok(widar_ok())
        }
        Err(e) => Ok(widar_error(format!("Wikidata edit failed: {e}"))),
    }
}

/// Entry-point for `action=generic`. Reads the `json` form field — which
/// the frontend builds as `JSON.stringify({action:'wbeditentity', ...})`
/// — and forwards each top-level key as a MediaWiki API form param,
/// signed with the user's OAuth token. Wraps the response under `res` so
/// the JS callback can read `d.res.entity.id` for new-item creation.
async fn handle_generic(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    if auth::guard::dev_bypass_user().is_some() {
        // Dev bypass: pretend the API echoed back a placeholder item so the
        // frontend's success path keeps working without a real edit.
        return Ok(json_resp(serde_json::json!({
            "status": "OK",
            "error": "OK",
            "data": [],
            "res": {"entity": {"id": "Q0"}, "success": 1},
        })));
    }
    let cfg = app
        .oauth_config()
        .ok_or_else(|| ApiError("OAuth is not configured on this server".into()))?
        .clone();

    let (api_params, summary) = match read_generic_params(params) {
        Ok(v) => v,
        Err(msg) => return Ok(widar_error(msg)),
    };

    let access = match auth::session::load(session).await.state {
        auth::session::SessionState::Authenticated {
            access_token_key,
            access_token_secret,
            ..
        } => auth::flow::TokenPair {
            key: access_token_key,
            secret: access_token_secret,
        },
        _ => return Ok(widar_error("Not logged in")),
    };

    match auth::flow::wikidata_generic_edit(&cfg, &access, api_params, &summary).await {
        Ok(v) => {
            if let Some(err) = v.get("error") {
                let info = err
                    .get("info")
                    .and_then(|i| i.as_str())
                    .unwrap_or("Wikidata API error");
                return Ok(widar_error(info.to_string()));
            }
            Ok(json_resp(serde_json::json!({
                "status": "OK",
                "error": "OK",
                "data": [],
                "res": v,
            })))
        }
        Err(e) => Ok(widar_error(format!("Wikidata edit failed: {e}"))),
    }
}

/// Translate the `generic` request body into the (form-param-list, summary)
/// pair we hand to `wikidata_generic_edit`. Pure so it's unit-testable.
///
/// The `json` field is the JSON-stringified MediaWiki API call; nested
/// object/array values are re-serialised because MediaWiki form parameters
/// are scalars (e.g. `wbeditentity data` must be a JSON string).
fn read_generic_params(params: &Params) -> Result<(Vec<(String, String)>, String), String> {
    let json_str = params.get("json").cloned().unwrap_or_default();
    if json_str.is_empty() {
        return Err("missing 'json' for generic action".into());
    }
    let v: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| format!("invalid 'json' payload: {e}"))?;
    let obj = v
        .as_object()
        .ok_or_else(|| "'json' payload must be an object".to_string())?;

    let summary = params.get("summary").cloned().unwrap_or_default();
    let tool_hashtag = params
        .get("tool_hashtag")
        .map(String::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let summary = if tool_hashtag.is_empty() {
        summary
    } else if summary.is_empty() {
        format!("#{tool_hashtag}")
    } else {
        format!("{summary} #{tool_hashtag}")
    };

    let mut api_params: Vec<(String, String)> = Vec::with_capacity(obj.len());
    for (k, val) in obj {
        // Strings come through verbatim (don't double-encode); everything
        // else is JSON-stringified — that's what the API expects for
        // structured params like `wbeditentity data`.
        let s = match val {
            serde_json::Value::String(s) => s.clone(),
            other => serde_json::to_string(other)
                .map_err(|e| format!("re-encoding '{k}' failed: {e}"))?,
        };
        api_params.push((k.clone(), s));
    }
    Ok((api_params, summary))
}

/// Pull the `set_string` params out of the form body and normalise them into
/// the shape `wbcreateclaim` expects. Rejects obviously-bad input up front so
/// we don't sign a request we know will fail. Pure so it can be unit-tested.
fn read_set_string_params(
    params: &Params,
) -> Result<(String, String, String, String), String> {
    let id = params
        .get("id")
        .map(String::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let prop = params
        .get("prop")
        .map(String::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let text = params.get("text").cloned().unwrap_or_default();
    let summary = params.get("summary").cloned().unwrap_or_default();
    let tool_hashtag = params
        .get("tool_hashtag")
        .map(String::as_str)
        .unwrap_or("")
        .trim()
        .to_string();

    if !id.starts_with('Q') || id.len() < 2 || !id[1..].chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("invalid entity id '{id}'"));
    }
    if !prop.starts_with('P') || prop.len() < 2 || !prop[1..].chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("invalid property id '{prop}'"));
    }
    if text.is_empty() {
        return Err("missing 'text' for set_string".into());
    }

    let summary = if tool_hashtag.is_empty() {
        summary
    } else if summary.is_empty() {
        format!("#{tool_hashtag}")
    } else {
        format!("{summary} #{tool_hashtag}")
    };
    Ok((id, prop, text, summary))
}

/// Check whether the Wikidata item already carries a statement on
/// `property_id` with the given string-typed `value`. Uses the
/// unauthenticated `wbgetclaims` API, which is much cheaper than fetching
/// the full entity.
async fn wikidata_string_claim_exists(
    entity_id: &str,
    property_id: &str,
    value: &str,
) -> anyhow::Result<bool> {
    let url = format!(
        "https://www.wikidata.org/w/api.php?action=wbgetclaims\
         &entity={entity_id}&property={property_id}&format=json"
    );
    let client = wikimisc::wikidata::Wikidata::new().reqwest_client()?;
    let json: serde_json::Value = client.get(&url).send().await?.json().await?;
    Ok(claims_contain_string_value(&json, property_id, value))
}

/// Pure parser for a `wbgetclaims` response: does the claims array for
/// `property_id` contain a main snak with the exact `value`? Rank and
/// qualifiers are irrelevant for dedup purposes here.
fn claims_contain_string_value(
    json: &serde_json::Value,
    property_id: &str,
    value: &str,
) -> bool {
    let Some(claims) = json
        .pointer(&format!("/claims/{property_id}"))
        .and_then(|v| v.as_array())
    else {
        return false;
    };
    claims.iter().any(|claim| {
        claim
            .pointer("/mainsnak/datavalue/value")
            .and_then(|v| v.as_str())
            == Some(value)
    })
}

/// Finish the OAuth1 handshake. Runs when the user returns from the authorize
/// step with `oauth_verifier` and `oauth_token` query parameters.
pub async fn handle_oauth_callback(app: &AppState, session: &Session, params: &Params) -> Response {
    let cfg = match app.oauth_config() {
        Some(c) => c.clone(),
        None => return ApiError("OAuth is not configured on this server".into()).into_response(),
    };
    let verifier = params.get("oauth_verifier").cloned().unwrap_or_default();
    let incoming_token = params.get("oauth_token").cloned().unwrap_or_default();

    let data = auth::session::load(session).await;
    let (rk, rs) = match data.state {
        auth::session::SessionState::PendingVerifier {
            request_token_key,
            request_token_secret,
        } => (request_token_key, request_token_secret),
        _ => {
            return ApiError("No pending OAuth login — start over from the authorize link".into())
                .into_response();
        }
    };
    // Session fixation guard: the verifier must match the token we stashed.
    if incoming_token != rk {
        return ApiError("OAuth token mismatch".into()).into_response();
    }
    let pair = auth::flow::TokenPair {
        key: rk,
        secret: rs,
    };
    let access = match auth::flow::exchange_verifier(&cfg, &pair, &verifier).await {
        Ok(a) => a,
        Err(e) => return ApiError(format!("OAuth exchange failed: {e}")).into_response(),
    };
    let user = match auth::flow::fetch_userinfo(&cfg, &access).await {
        Ok(u) => u,
        Err(e) => return ApiError(format!("OAuth userinfo failed: {e}")).into_response(),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let new_data = auth::session::SessionData {
        state: auth::session::SessionState::Authenticated {
            access_token_key: access.key,
            access_token_secret: access.secret,
            wikidata_user_id: user.id,
            wikidata_username: auth::session::normalize_username(&user.name),
            authenticated_at: now,
        },
    };
    if let Err(e) = auth::session::store(session, &new_data).await {
        return e.into_response();
    }
    // Re-cycle session id to prevent session fixation on the now-authenticated session.
    let _ = session.cycle_id().await;
    Redirect::to("/").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn p(pairs: &[(&str, &str)]) -> Params {
        let mut m: HashMap<String, String> = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), (*v).into());
        }
        m
    }

    #[test]
    fn read_set_string_params_happy() {
        let params = p(&[
            ("id", "Q42"),
            ("prop", "P7471"),
            ("text", "ext-id-1234"),
            ("summary", "Matched to foo"),
            ("tool_hashtag", "mix'n'match"),
        ]);
        let (id, prop, text, summary) = read_set_string_params(&params).unwrap();
        assert_eq!(id, "Q42");
        assert_eq!(prop, "P7471");
        assert_eq!(text, "ext-id-1234");
        assert_eq!(summary, "Matched to foo #mix'n'match");
    }

    #[test]
    fn read_set_string_params_without_hashtag_keeps_summary() {
        let params = p(&[
            ("id", "Q1"),
            ("prop", "P2"),
            ("text", "v"),
            ("summary", "plain"),
        ]);
        let (_, _, _, summary) = read_set_string_params(&params).unwrap();
        assert_eq!(summary, "plain");
    }

    #[test]
    fn read_set_string_params_hashtag_without_summary() {
        let params = p(&[
            ("id", "Q1"),
            ("prop", "P2"),
            ("text", "v"),
            ("tool_hashtag", "mix'n'match"),
        ]);
        let (_, _, _, summary) = read_set_string_params(&params).unwrap();
        assert_eq!(summary, "#mix'n'match");
    }

    #[test]
    fn read_set_string_params_rejects_bad_entity() {
        let params = p(&[("id", "42"), ("prop", "P7471"), ("text", "v")]);
        let err = read_set_string_params(&params).unwrap_err();
        assert!(err.contains("entity id"), "got: {err}");
    }

    #[test]
    fn read_set_string_params_rejects_bad_property() {
        let params = p(&[("id", "Q1"), ("prop", "7471"), ("text", "v")]);
        let err = read_set_string_params(&params).unwrap_err();
        assert!(err.contains("property id"), "got: {err}");
    }

    #[test]
    fn read_set_string_params_rejects_empty_text() {
        let params = p(&[("id", "Q1"), ("prop", "P7471"), ("text", "")]);
        let err = read_set_string_params(&params).unwrap_err();
        assert!(err.contains("text"), "got: {err}");
    }

    #[test]
    fn read_set_string_params_rejects_missing_text() {
        let params = p(&[("id", "Q1"), ("prop", "P7471")]);
        let err = read_set_string_params(&params).unwrap_err();
        assert!(err.contains("text"), "got: {err}");
    }

    #[test]
    fn read_generic_params_extracts_form_params_and_serialises_objects() {
        // The frontend's `widar.run({action:'generic', json: JSON.stringify({...})})`
        // posts a stringified JSON body. Object/array values inside the body
        // (e.g. wbeditentity's `data` field) must be re-stringified because
        // MediaWiki form params are scalars.
        let payload = serde_json::json!({
            "action": "wbeditentity",
            "new": "item",
            "data": {"labels": {"en": {"language": "en", "value": "Foo"}}}
        });
        let params = p(&[
            ("json", &payload.to_string()),
            ("summary", "New item from MnM"),
            ("tool_hashtag", "mix'n'match"),
        ]);
        let (api_params, summary) = read_generic_params(&params).unwrap();
        let map: std::collections::HashMap<&str, &str> = api_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(map.get("action"), Some(&"wbeditentity"));
        assert_eq!(map.get("new"), Some(&"item"));
        let data_val: serde_json::Value =
            serde_json::from_str(map.get("data").unwrap()).unwrap();
        assert_eq!(data_val["labels"]["en"]["value"], "Foo");
        assert_eq!(summary, "New item from MnM #mix'n'match");
    }

    #[test]
    fn read_generic_params_rejects_empty_or_invalid_json() {
        assert!(read_generic_params(&p(&[])).is_err());
        assert!(read_generic_params(&p(&[("json", "")])).is_err());
        assert!(read_generic_params(&p(&[("json", "not json")])).is_err());
        // Top-level non-object: not allowed (form params come from object keys).
        assert!(read_generic_params(&p(&[("json", "[1,2,3]")])).is_err());
    }

    #[test]
    fn read_generic_params_summary_without_hashtag() {
        let params = p(&[("json", r#"{"action":"foo"}"#), ("summary", "plain")]);
        let (_, summary) = read_generic_params(&params).unwrap();
        assert_eq!(summary, "plain");
    }

    #[test]
    fn claims_contain_string_value_matches_existing() {
        let json = serde_json::json!({
            "claims": {
                "P7471": [
                    {"mainsnak": {"snaktype": "value",
                                  "datavalue": {"value": "abc", "type": "string"}}},
                    {"mainsnak": {"snaktype": "value",
                                  "datavalue": {"value": "xyz", "type": "string"}}}
                ]
            }
        });
        assert!(claims_contain_string_value(&json, "P7471", "xyz"));
        assert!(claims_contain_string_value(&json, "P7471", "abc"));
        assert!(!claims_contain_string_value(&json, "P7471", "nope"));
    }

    #[test]
    fn claims_contain_string_value_missing_property_returns_false() {
        let json = serde_json::json!({ "claims": {} });
        assert!(!claims_contain_string_value(&json, "P7471", "abc"));
    }

    #[test]
    fn claims_contain_string_value_novalue_snak_ignored() {
        // `somevalue` / `novalue` snaks have no datavalue at all — treat as
        // non-match so the caller still writes the concrete value.
        let json = serde_json::json!({
            "claims": {
                "P7471": [
                    {"mainsnak": {"snaktype": "somevalue"}}
                ]
            }
        });
        assert!(!claims_contain_string_value(&json, "P7471", "abc"));
    }

    #[test]
    fn claims_contain_string_value_exact_match_is_case_sensitive() {
        // External-id comparisons on Wikidata are case-sensitive by datatype;
        // we mirror that to avoid silently masking real differences.
        let json = serde_json::json!({
            "claims": {
                "P7471": [
                    {"mainsnak": {"snaktype": "value",
                                  "datavalue": {"value": "ABC", "type": "string"}}}
                ]
            }
        });
        assert!(claims_contain_string_value(&json, "P7471", "ABC"));
        assert!(!claims_contain_string_value(&json, "P7471", "abc"));
    }
}
