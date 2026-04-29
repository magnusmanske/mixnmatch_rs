//! Distributed-game endpoints (`?action=…` shortcut → `?query=dg_…`).

use crate::api::common::{self, ApiError, Params, json_resp};
use crate::app_state::AppState;
use crate::entry::EntryWriter;
use axum::response::Response;

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

pub async fn query_dg_log_action(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let user = common::get_param(params, "user", "");
    let entry_id = common::get_param_int(params, "tile", -1);
    if entry_id < 0 {
        return Err(ApiError("bad tile".into()));
    }
    let entry_id = entry_id as usize;
    let decision = common::get_param(params, "decision", "");

    // The user-id resolution and entry-load are independent — fire both in
    // parallel. They tend to be the two slowest steps in this handler.
    let (uid_res, entry_res) = tokio::join!(
        app.storage().get_or_create_user_id(&user),
        crate::entry::Entry::from_id(entry_id, app),
    );
    let uid = uid_res?;
    let mut entry = entry_res?;

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
