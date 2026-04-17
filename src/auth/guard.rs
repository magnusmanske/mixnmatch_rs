use crate::api::common::{ApiError, Params};
use crate::app_state::AppState;
use crate::auth::session::{SessionData, SessionState, load, normalize_username};
use tower_sessions::Session;

/// Extract the legacy `username` / `tusc_user` form field, if non-empty.
/// These fields are informational — `require_user` still anchors identity
/// to the session — but we verify the claim matches on every write.
fn claimed_username_from(params: &Params) -> Option<&str> {
    params
        .get("username")
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            params
                .get("tusc_user")
                .map(String::as_str)
                .filter(|s| !s.trim().is_empty())
        })
}

/// A verified, logged-in user. The username here is the one MediaWiki gave us
/// during OAuth; it is *not* taken from the request body.
#[derive(Debug, Clone)]
pub struct AuthedUser {
    pub wikidata_username: String,
    pub mnm_user_id: usize,
}

/// Require a logged-in user and return their verified identity.
///
/// The return value is authoritative — callers MUST attribute writes to
/// `mnm_user_id` / `wikidata_username`, and MUST NOT fall back to any
/// `username` / `tusc_user` form parameter.
///
/// If the caller DID pass such a parameter (the legacy PHP frontend always
/// does), we verify it matches the session user and reject mismatches. This
/// mirrors PHP `API::check_and_get_user_id` which compares the claimed name
/// against `Widar::get_username()`.
pub async fn require_user(
    app: &AppState,
    session: &Session,
    claimed_username: Option<&str>,
) -> Result<AuthedUser, ApiError> {
    let data: SessionData = load(session).await;
    let (username, _access_key, _access_secret) = match &data.state {
        SessionState::Authenticated {
            wikidata_username,
            access_token_key,
            access_token_secret,
            ..
        } => (
            wikidata_username.clone(),
            access_token_key.clone(),
            access_token_secret.clone(),
        ),
        _ => return Err(ApiError("OAuth login required".into())),
    };

    if let Some(claim) = claimed_username.map(str::trim).filter(|s| !s.is_empty()) {
        if normalize_username(claim) != normalize_username(&username) {
            return Err(ApiError("OAuth user name problem".into()));
        }
    }

    let user_id = app
        .storage()
        .get_or_create_user_id(&username)
        .await
        .map_err(|e| ApiError(format!("User lookup failed: {e}")))?;
    if user_id == 0 {
        return Err(ApiError("OAuth login failure, please log in again".into()));
    }
    Ok(AuthedUser {
        wikidata_username: username,
        mnm_user_id: user_id,
    })
}

/// Convenience wrapper for the common case of extracting the claim from
/// request parameters. Prefer this in API handlers to keep call sites terse.
pub async fn require_user_from_params(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<AuthedUser, ApiError> {
    require_user(app, session, claimed_username_from(params)).await
}

/// Convenience wrapper matching `require_user_from_params` but for admin-only actions.
pub async fn require_catalog_admin_from_params(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<AuthedUser, ApiError> {
    require_catalog_admin(app, session, claimed_username_from(params)).await
}

/// Require a logged-in catalog admin. Mirrors PHP `API::ensure_user_is_catalog_admin`.
pub async fn require_catalog_admin(
    app: &AppState,
    session: &Session,
    claimed_username: Option<&str>,
) -> Result<AuthedUser, ApiError> {
    let user = require_user(app, session, claimed_username).await?;
    let info = app
        .storage()
        .get_user_by_name(&user.wikidata_username)
        .await
        .map_err(|e| ApiError(format!("Admin lookup failed: {e}")))?;
    match info {
        Some((_id, _name, true)) => Ok(user),
        Some(_) => Err(ApiError(format!(
            "'{}' is not a catalog admin",
            user.wikidata_username
        ))),
        None => Err(ApiError(format!(
            "No such user '{}'",
            user.wikidata_username
        ))),
    }
}
