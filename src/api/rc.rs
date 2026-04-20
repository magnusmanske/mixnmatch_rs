//! Recent-changes endpoints (frontend feed + Atom XML).

use crate::api::common::{self, ApiError, Params, get_users, ok};
use crate::app_state::AppState;
use axum::response::{IntoResponse, Response};

const RC_LIMIT: usize = 100;

/// Sort the merged events by timestamp DESC and trim to `limit` items.
/// `sort_unstable_by` is fine here — timestamp ties have no meaningful order
/// to preserve, and unstable sort is faster on Vec<Value>.
fn sort_and_truncate(events: &mut Vec<serde_json::Value>, limit: usize) {
    events.sort_unstable_by(|a, b| {
        let ta = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        tb.cmp(ta)
    });
    events.truncate(limit);
}

pub async fn query_rc(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let (entry_evts, log_evts) = app
        .storage()
        .api_get_recent_changes(&ts, catalog, RC_LIMIT)
        .await?;
    let mut events: Vec<serde_json::Value> = entry_evts.into_iter().chain(log_evts).collect();
    sort_and_truncate(&mut events, RC_LIMIT);
    let uids: std::collections::HashSet<usize> = events
        .iter()
        .filter_map(|e| e.get("user").and_then(|v| v.as_u64()).map(|v| v as usize))
        .collect();
    let users = get_users(app, &uids).await?;
    Ok(ok(serde_json::json!({"events": events, "users": users})))
}

pub async fn query_rc_atom(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let ts = common::get_param(params, "ts", "");
    let catalog = common::get_param_int(params, "catalog", 0) as usize;
    let (entry_evts, log_evts) = app
        .storage()
        .api_get_recent_changes(&ts, catalog, RC_LIMIT)
        .await?;
    let mut events: Vec<serde_json::Value> = entry_evts.into_iter().chain(log_evts).collect();
    sort_and_truncate(&mut events, RC_LIMIT);

    // XML serialization is pure CPU work — small here (≤100 entries) so we
    // do it inline rather than spawn_blocking.
    let now = chrono::Utc::now().to_rfc3339();
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\">\n\
         <title>Mix'n'match</title>\n\
         <subtitle>Recent updates by humans (auto-matching not shown)</subtitle>\n\
         <link href=\"https://mix-n-match.toolforge.org/api.php?query=rc_atom\" rel=\"self\" />\n\
         <link href=\"https://mix-n-match.toolforge.org/\" />\n\
         <id>urn:uuid:{}</id>\n\
         <updated>{now}</updated>\n",
        uuid::Uuid::new_v4()
    );
    for e in &events {
        let id = e.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        let name = e.get("ext_name").and_then(|v| v.as_str()).unwrap_or("");
        let event_type = e
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("match");
        let timestamp = e.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
        let title_prefix = if event_type == "remove_q" {
            "Match was removed for "
        } else {
            "New match for "
        };
        xml.push_str(&format!(
            "<entry>\n<title>{title_prefix}\"{name}\"</title>\n\
             <link rel=\"alternate\" href=\"https://mix-n-match.toolforge.org/#/entry/{id}\" />\n\
             <id>urn:uuid:{}</id>\n\
             <updated>{timestamp}</updated>\n\
             </entry>\n",
            uuid::Uuid::new_v4()
        ));
    }
    xml.push_str("</feed>");
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/atom+xml; charset=UTF-8",
        )],
        xml,
    )
        .into_response())
}
