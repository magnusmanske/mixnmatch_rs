//! Distributed-game endpoints (`?action=…` shortcut → `?query=dg_…`).

use crate::api::common::{self, ApiError, Params, json_resp};
use crate::app_state::AppState;
use crate::auth;
use crate::entry::EntryWriter;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_dg_desc(params: &Params) -> Result<Response, ApiError> {
    let mode = common::get_param(params, "mode", "");
    let (title, sub) = if mode == "person" {
        ("Mix'n'match people game", "of a person in")
    } else {
        ("Mix'n'match game", "in")
    };
    let out = serde_json::json!({
        "label": {"en": title},
        "description": {"en": format!("Verify that an entry {sub} an external catalog matches a given Wikidata item. Decisions count as mix'n'match actions!")},
        "icon": "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2d/Bipartite_graph_with_matching.svg/120px-Bipartite_graph_with_matching.svg.png",
        "options": [
            {"name": "Entry type", "key": "type", "values": {"any": "Any", "person": "Person", "not_person": "Not a person"}}
        ],
    });
    // PHP returns this payload as the top-level response (no "data" envelope).
    Ok(json_resp(out))
}

pub async fn query_dg_tiles(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let num = common::get_param_int(params, "num", 5).clamp(1, 20) as usize;
    let type_filter = common::get_param(params, "type", "");
    let tiles = app.storage().api_dg_tiles(num, &type_filter).await?;
    Ok(json_resp(serde_json::json!(tiles)))
}

pub async fn query_dg_log_action(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    // OAuth required: writes that attribute matches to a user must
    // come from a logged-in session. Treat the legacy `?user=` field
    // as a claim that's verified against the session identity, same
    // shape as `claimed_username_from`.
    let claimed_user = params.get("user").map(String::as_str);
    let authed = auth::guard::require_user(app, session, claimed_user).await?;

    let entry_id = common::get_param_int(params, "tile", -1);
    if entry_id < 0 {
        return Err(ApiError::BadRequest("bad tile".into()));
    }
    let entry_id = entry_id as usize;
    let decision = common::get_param(params, "decision", "");

    let mut entry = crate::entry::Entry::from_id(entry_id, app).await?;
    let uid = authed.mnm_user_id;
    let mut ew = EntryWriter::new(app, &mut entry);
    match decision.as_str() {
        "yes" => {
            if let Some(q) = ew.as_entry().q {
                ew.set_match(&format!("Q{q}"), uid).await?;
            }
        }
        "no" => {
            ew.unmatch().await?;
        }
        "n_a" => {
            ew.set_match("Q-1", uid).await?;
        }
        _ => {}
    }
    Ok(json_resp(serde_json::json!([])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Regression pin for the OAuth-bypass fix. Pre-fix the handler read
    /// `?user=` as the literal attribution and silently created a user
    /// row for whatever string the caller supplied. Post-fix it routes
    /// through `auth::guard::require_user`, which in dev/test mode
    /// returns uid 2 (Magnus) regardless of the claim. The test asserts
    /// the post-fix property — if anyone reverts the handler to using
    /// `get_or_create_user_id(&user)`, the attribution would track the
    /// claim again and this test fails.
    #[tokio::test]
    #[ignore = "requires database / external services — run with cargo test -- --ignored"]
    async fn dg_log_action_attribution_uses_session_user_not_query_user() {
        let app = test_support::test_app().await;
        let (_catalog_id, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();

        let store = Arc::new(tower_sessions::MemoryStore::default());
        let session = tower_sessions::Session::new(None, store, None);

        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("tile".into(), entry_id.to_string());
        params.insert("decision".into(), "n_a".into());
        params.insert("user".into(), "AttackerSuppliedName".into());

        let resp = query_dg_log_action(&app, &session, &params).await;
        assert!(
            resp.is_ok(),
            "handler must succeed under dev bypass: {:?}",
            resp.err()
        );

        let entry = crate::entry::Entry::from_id(entry_id, &app).await.unwrap();
        assert_eq!(
            entry.q,
            Some(-1),
            "decision=n_a writes the Q-1 sentinel match"
        );
        assert_eq!(
            entry.user,
            Some(2),
            "attribution must come from the session (dev bypass uid 2), \
             never from the ?user= query parameter"
        );
    }
}
