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
