use crate::api::common::ApiError;
use serde::{Deserialize, Serialize};
use tower_sessions::Session;

/// Key under which the whole `SessionData` blob is stored in the session.
/// A single key keeps the state machine transitions atomic.
const SESSION_KEY: &str = "mnm";

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub enum SessionState {
    #[default]
    Anonymous,
    /// After `Special:OAuth/initiate` succeeded but before the user returned
    /// from the authorize redirect. Request tokens are one-shot.
    PendingVerifier {
        request_token_key: String,
        request_token_secret: String,
    },
    /// Fully authenticated. Access token lives for the session's lifetime.
    Authenticated {
        access_token_key: String,
        access_token_secret: String,
        wikidata_user_id: u64,
        wikidata_username: String, // normalised: underscores → spaces, trimmed
        authenticated_at: i64,     // unix seconds
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SessionData {
    pub state: SessionState,
}

impl SessionData {
    pub fn anonymous() -> Self {
        Self::default()
    }
}

pub async fn load(session: &Session) -> SessionData {
    match session.get::<SessionData>(SESSION_KEY).await {
        Ok(Some(data)) => data,
        Ok(None) => SessionData::anonymous(),
        // A deserialize failure usually means the on-disk SessionData shape
        // changed under us (struct rename, enum-variant rename, base64
        // corruption). Falling through to "anonymous" gives the user a
        // clean re-login path, but the silent path used to make this
        // invisible to operators. Logging at warn keeps the signal grep-able
        // without being a page-worthy error.
        Err(e) => {
            log::warn!("session deserialize failed, treating as anonymous: {e}");
            SessionData::anonymous()
        }
    }
}

pub async fn store(session: &Session, data: &SessionData) -> Result<(), ApiError> {
    session.insert(SESSION_KEY, data).await.map_err(|e| {
        // Underlying tower-sessions error can include the filesystem path
        // of the session store; don't surface that to the client. Log the
        // full chain server-side instead.
        log::error!("session store failed: {e}");
        ApiError::Internal("session store failed".into())
    })
}

pub async fn clear(session: &Session) -> Result<(), ApiError> {
    session.remove::<SessionData>(SESSION_KEY).await.map_err(|e| {
        log::error!("session clear failed: {e}");
        ApiError::Internal("session clear failed".into())
    })?;
    // Also cycle the session id so the cookie is effectively invalidated.
    session.cycle_id().await.map_err(|e| {
        log::error!("session cycle failed: {e}");
        ApiError::Internal("session cycle failed".into())
    })?;
    Ok(())
}

/// Canonical form used for comparing usernames across the boundary.
/// MediaWiki treats `Magnus Manske` and `Magnus_Manske` as the same user;
/// PHP `API::normalize_user_name` maps spaces to underscores then trims.
/// We use the other direction (underscores → spaces, trim) to match the
/// database-side storage convention of the `user.name` column.
pub fn normalize_username(s: &str) -> String {
    s.trim().replace('_', " ")
}
