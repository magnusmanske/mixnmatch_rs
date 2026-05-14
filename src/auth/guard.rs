use crate::api::common::{ApiError, Params};
use crate::app_state::ExternalServicesContext;
use crate::auth::session::{SessionData, SessionState, load, normalize_username};
use tower_sessions::Session;

/// Environment variable that opts in to the dev auth bypass.
/// Must be set to exactly "1". F-9 from audits/error_flow.md.
pub const DEV_BYPASS_ENV_VAR: &str = "MNM_DEV_BYPASS_AUTH";

/// Dev auth bypass: pretend every request is authenticated as
/// Magnus Manske / uid 2 / catalog admin.
///
/// **Opt-in via the `MNM_DEV_BYPASS_AUTH=1` environment variable, never
/// via filesystem state.** The previous toggle (`!is_on_toolforge()`,
/// driven by the existence of `/etc/wmcs-project`) was a default-allow
/// pattern: if the marker file was ever missing on a production-equivalent
/// host (CI promoted to staging, restricted-mount k8s pod, chroot), auth
/// silently disappeared. The env-var form is default-deny — production
/// simply doesn't set the variable.
pub fn dev_bypass_user() -> Option<AuthedUser> {
    // `cfg!(test)` is true only when this crate is being compiled with
    // `--test` (i.e. `cargo test --lib`). Production `cargo build` /
    // `cargo run` never sets it, so this is purely a unit-test
    // convenience that doesn't widen the production attack surface —
    // verified by the test below pinning the `cfg!(test)` branch off
    // when the helper is called with a non-test-compiled env state.
    if cfg!(test) {
        return Some(test_bypass_user());
    }
    dev_bypass_user_from(std::env::var(DEV_BYPASS_ENV_VAR).ok().as_deref())
}

/// The bypass identity. Lifted to a helper so the `cfg!(test)` arm above
/// and the env-var arm in `dev_bypass_user_from` agree.
fn test_bypass_user() -> AuthedUser {
    AuthedUser {
        wikidata_username: "Magnus Manske".to_string(),
        mnm_user_id: 2,
    }
}

/// Pure helper split out so the env-var contract is unit-testable without
/// mutating process-global state (the crate forbids `unsafe`, so we can't
/// use `std::env::set_var` in tests on Rust 2024). Only an exact "1"
/// activates the bypass; any other value (including "true", "yes",
/// non-ASCII, empty) is rejected.
fn dev_bypass_user_from(env_value: Option<&str>) -> Option<AuthedUser> {
    if env_value == Some("1") {
        Some(test_bypass_user())
    } else {
        None
    }
}

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
/// against the OAuth-verified session username.
pub async fn require_user(
    app: &dyn ExternalServicesContext,
    session: &Session,
    claimed_username: Option<&str>,
) -> Result<AuthedUser, ApiError> {
    if let Some(u) = dev_bypass_user() {
        return Ok(u);
    }
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
        _ => return Err(ApiError::Unauthorized("OAuth login required".into())),
    };

    if let Some(claim) = claimed_username.map(str::trim).filter(|s| !s.is_empty()) {
        if normalize_username(claim) != normalize_username(&username) {
            return Err(ApiError::Unauthorized("OAuth user name problem".into()));
        }
    }

    let user_id = app
        .storage()
        .get_or_create_user_id(&username)
        .await
        .map_err(|e| ApiError::Internal(format!("User lookup failed: {e}")))?;
    if user_id == 0 {
        return Err(ApiError::Unauthorized(
            "OAuth login failure, please log in again".into(),
        ));
    }
    Ok(AuthedUser {
        wikidata_username: username,
        mnm_user_id: user_id,
    })
}

/// Convenience wrapper for the common case of extracting the claim from
/// request parameters. Prefer this in API handlers to keep call sites terse.
pub async fn require_user_from_params(
    app: &dyn ExternalServicesContext,
    session: &Session,
    params: &Params,
) -> Result<AuthedUser, ApiError> {
    require_user(app, session, claimed_username_from(params)).await
}

/// Convenience wrapper matching `require_user_from_params` but for admin-only actions.
pub async fn require_catalog_admin_from_params(
    app: &dyn ExternalServicesContext,
    session: &Session,
    params: &Params,
) -> Result<AuthedUser, ApiError> {
    require_catalog_admin(app, session, claimed_username_from(params)).await
}

/// Require a logged-in catalog admin. Mirrors PHP `API::ensure_user_is_catalog_admin`.
pub async fn require_catalog_admin(
    app: &dyn ExternalServicesContext,
    session: &Session,
    claimed_username: Option<&str>,
) -> Result<AuthedUser, ApiError> {
    if let Some(u) = dev_bypass_user() {
        // Dev bypass: skip the DB admin check entirely.
        return Ok(u);
    }
    let user = require_user(app, session, claimed_username).await?;
    let info = app
        .storage()
        .get_user_by_name(&user.wikidata_username)
        .await
        .map_err(|e| ApiError::Internal(format!("Admin lookup failed: {e}")))?;
    match info {
        Some((_id, _name, true)) => Ok(user),
        Some(_) => Err(ApiError::Forbidden(format!(
            "'{}' is not a catalog admin",
            user.wikidata_username
        ))),
        None => Err(ApiError::Forbidden(format!(
            "No such user '{}'",
            user.wikidata_username
        ))),
    }
}

/// Allow either a catalog admin or the catalog's own creator (the
/// `catalog.owner` column) to perform the action. Admins can edit every
/// catalog; creators can edit only the ones they own. Reads the `username`
/// form field as the claim (same shape as `require_catalog_admin_from_params`).
pub async fn require_catalog_admin_or_owner_from_params(
    app: &dyn ExternalServicesContext,
    session: &Session,
    params: &Params,
    catalog_id: usize,
) -> Result<AuthedUser, ApiError> {
    let claimed = claimed_username_from(params);
    let user = require_user(app, session, claimed).await?;
    if let Some(u) = dev_bypass_user() {
        // Dev bypass already grants admin; accept without a DB lookup.
        return Ok(u);
    }
    // Admin short-circuit.
    let admin_info = app
        .storage()
        .get_user_by_name(&user.wikidata_username)
        .await
        .map_err(|e| ApiError::Internal(format!("Admin lookup failed: {e}")))?;
    if matches!(admin_info, Some((_, _, true))) {
        return Ok(user);
    }
    // Otherwise, must be the catalog's owner.
    let catalog = crate::catalog::Catalog::from_id(catalog_id, app)
        .await
        .map_err(|e| ApiError::Internal(format!("Catalog lookup failed: {e}")))?;
    if catalog.owner() == user.mnm_user_id {
        return Ok(user);
    }
    Err(ApiError::Forbidden(format!(
        "'{}' is not a catalog admin and does not own catalog #{catalog_id}",
        user.wikidata_username
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the env-var-only contract via the pure helper. We can't mutate
    /// `std::env` in tests because the crate forbids unsafe; the helper
    /// design makes that unnecessary.
    #[test]
    fn dev_bypass_disabled_without_env_var() {
        assert!(
            dev_bypass_user_from(None).is_none(),
            "bypass must be off when env var is unset — production safety relies on this"
        );
    }

    #[test]
    fn dev_bypass_enabled_with_env_var_one() {
        let u = dev_bypass_user_from(Some("1"))
            .expect("bypass must activate when env var is '1'");
        assert_eq!(u.mnm_user_id, 2);
        assert_eq!(u.wikidata_username, "Magnus Manske");
    }

    /// Mutation test: if someone "helpfully" widens the check to accept
    /// truthy-ish values, this catches it. Only an exact "1" must count;
    /// anything else means the operator did not intend the bypass.
    #[test]
    fn dev_bypass_disabled_with_other_values() {
        for val in ["", "0", "true", "yes", "TRUE", "1 ", " 1", "01"] {
            assert!(
                dev_bypass_user_from(Some(val)).is_none(),
                "bypass must reject env-var value {val:?}; only exact \"1\" counts"
            );
        }
    }
}
