//! Recent-changes endpoints (frontend feed + Atom XML).

use crate::api::common::{self, ApiError, Params, get_users, ok};
use crate::app_state::AppState;
use atom_syndication::{Entry, Feed, Link, Text};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, FixedOffset, NaiveDateTime, TimeZone, Utc};

const RC_LIMIT: usize = 100;
const FEED_BASE_URL: &str = "https://mix-n-match.toolforge.org/";
const FEED_SELF_URL: &str = "https://mix-n-match.toolforge.org/api.php?query=rc_atom";

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

    let xml = build_atom_feed(&events);

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/atom+xml; charset=UTF-8",
        )],
        xml,
    )
        .into_response())
}

/// Parse a MediaWiki-style `YYYYMMDDHHMMSS` timestamp as UTC and convert to
/// RFC 3339. Falls back to `default` for empty/unparseable input — the Atom
/// spec requires an `updated` element on every entry, so we never want to
/// emit a dangling None here.
fn parse_mw_timestamp(s: &str, default: DateTime<FixedOffset>) -> DateTime<FixedOffset> {
    NaiveDateTime::parse_from_str(s, "%Y%m%d%H%M%S")
        .ok()
        .map(|naive| Utc.from_utc_datetime(&naive).fixed_offset())
        .unwrap_or(default)
}

/// Build the Atom feed via `atom_syndication`. The crate handles XML escaping
/// of `<title>` text and timestamp formatting, both of which the previous
/// hand-rolled `format!()` version got wrong (raw `ext_name` could break the
/// XML; raw DB timestamps weren't RFC 3339).
fn build_atom_feed(events: &[serde_json::Value]) -> String {
    let now = Utc::now().fixed_offset();
    let entries: Vec<Entry> = events
        .iter()
        .map(|e| {
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

            let mut entry = Entry {
                id: format!("urn:uuid:{}", uuid::Uuid::new_v4()),
                title: Text::plain(format!("{title_prefix}\"{name}\"")),
                updated: parse_mw_timestamp(timestamp, now),
                ..Default::default()
            };
            entry.links = vec![Link {
                href: format!("{FEED_BASE_URL}#/entry/{id}"),
                rel: "alternate".to_string(),
                ..Default::default()
            }];
            entry
        })
        .collect();

    let feed = Feed {
        id: format!("urn:uuid:{}", uuid::Uuid::new_v4()),
        title: Text::plain("Mix'n'match"),
        subtitle: Some(Text::plain(
            "Recent updates by humans (auto-matching not shown)",
        )),
        updated: now,
        links: vec![
            Link {
                href: FEED_SELF_URL.to_string(),
                rel: "self".to_string(),
                ..Default::default()
            },
            Link {
                href: FEED_BASE_URL.to_string(),
                ..Default::default()
            },
        ],
        entries,
        ..Default::default()
    };
    feed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_mediawiki_timestamp() {
        let fallback = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().fixed_offset();
        let parsed = parse_mw_timestamp("20240419183022", fallback);
        assert_eq!(parsed.to_rfc3339(), "2024-04-19T18:30:22+00:00");
    }

    #[test]
    fn falls_back_for_unparseable_timestamp() {
        let fallback = Utc.with_ymd_and_hms(2030, 5, 1, 12, 0, 0).unwrap().fixed_offset();
        assert_eq!(parse_mw_timestamp("not a ts", fallback), fallback);
        assert_eq!(parse_mw_timestamp("", fallback), fallback);
    }

    #[test]
    fn feed_has_required_atom_structure() {
        let xml = build_atom_feed(&[]);
        assert!(xml.contains(r#"xmlns="http://www.w3.org/2005/Atom""#));
        // The apostrophe in "Mix'n'match" gets escaped to &apos; by quick-xml.
        assert!(xml.contains("Mix") && xml.contains("match"));
        // Self-link is required by Atom for paged feeds.
        assert!(xml.contains(r#"href="https://mix-n-match.toolforge.org/api.php?query=rc_atom""#));
    }

    #[test]
    fn entry_title_and_link_use_event_data() {
        let events = vec![json!({
            "id": 42,
            "ext_name": "Hauk Aabel",
            "event_type": "match",
            "timestamp": "20240419183022",
        })];
        let xml = build_atom_feed(&events);
        assert!(xml.contains("New match for"));
        assert!(xml.contains("Hauk Aabel"));
        assert!(xml.contains(r#"href="https://mix-n-match.toolforge.org/#/entry/42""#));
        assert!(xml.contains("2024-04-19T18:30:22"));
    }

    #[test]
    fn xml_escapes_dangerous_characters_in_ext_name() {
        // Catches the bug the hand-rolled formatter had: raw `<` / `&` would
        // break the document. atom_syndication writes them as entities.
        let events = vec![json!({
            "id": 1,
            "ext_name": "Smith & <Sons>",
            "event_type": "match",
            "timestamp": "20240101000000",
        })];
        let xml = build_atom_feed(&events);
        assert!(!xml.contains("Smith & <Sons>"));
        assert!(xml.contains("Smith &amp; &lt;Sons&gt;"));
    }

    #[test]
    fn remove_q_event_uses_remove_title_prefix() {
        let events = vec![json!({
            "id": 7,
            "ext_name": "Foo",
            "event_type": "remove_q",
            "timestamp": "20240101000000",
        })];
        let xml = build_atom_feed(&events);
        assert!(xml.contains("Match was removed for"));
    }
}
