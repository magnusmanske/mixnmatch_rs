//! Download / export endpoints. Both build potentially large strings, so we
//! offload formatting to `spawn_blocking` to keep the async reactor responsive.

use crate::api::common::{self, ApiError, Params, get_users};
use crate::app_state::AppState;
use axum::response::{IntoResponse, Response};
use std::collections::HashSet;

pub async fn query_download(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let cat = crate::catalog::Catalog::from_id(cid, app).await?;
    let filename = cat
        .name()
        .unwrap_or(&"download".to_string())
        .replace(' ', "_")
        + ".tsv";
    let rows = app.storage().api_get_download_entries(cid).await?;
    let uids: HashSet<usize> = rows.iter().filter_map(|(_, _, _, _, u)| *u).collect();
    let users = get_users(app, &uids).await?;

    // The TSV body for a large catalog can be tens of MB — build it on a
    // blocking thread so we don't stall the runtime while we format strings.
    let out = tokio::task::spawn_blocking(move || {
        let mut out = String::from("Q\tID\tURL\tName\tUser\n");
        for (q, ext_id, ext_url, ext_name, user_id) in &rows {
            let uname = user_id
                .and_then(|u| users.get(u.to_string().as_str()))
                .and_then(|v| v.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            out.push_str(&format!(
                "{q}\t{ext_id}\t{ext_url}\t{ext_name}\t{uname}\n"
            ));
        }
        out
    })
    .await
    .map_err(|e| ApiError(format!("download formatting panic: {e}")))?;

    Ok((
        [
            (
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=UTF-8",
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                &format!("attachment;filename=\"{filename}\""),
            ),
        ],
        out,
    )
        .into_response())
}

pub async fn query_download2(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',')
        .collect();
    let format = common::get_param(params, "format", "tab");
    let columns: serde_json::Value =
        serde_json::from_str(&common::get_param(params, "columns", "{}"))
            .unwrap_or(serde_json::json!({}));
    let hidden: serde_json::Value =
        serde_json::from_str(&common::get_param(params, "hidden", "{}"))
            .unwrap_or(serde_json::json!({}));
    // PHP emits the column/hidden flags as either booleans or 0/1 integers
    // depending on the caller. Accept both.
    let flag = |obj: &serde_json::Value, key: &str| -> bool {
        obj.get(key)
            .and_then(|v| v.as_bool())
            .or(obj.get(key).and_then(|v| v.as_i64()).map(|v| v != 0))
            .unwrap_or(false)
    };

    let limit = common::get_param_int(params, "limit", 100_000).clamp(1, 1_000_000) as u64;
    let offset = common::get_param_int(params, "offset", 0).max(0) as u64;

    let filter = crate::storage::Download2Filter {
        catalogs,
        include_ext_url: flag(&columns, "exturl"),
        include_username: flag(&columns, "username"),
        include_dates: flag(&columns, "dates"),
        include_location: flag(&columns, "location"),
        hide_any_matched: flag(&hidden, "any_matched"),
        hide_firmly_matched: flag(&hidden, "firmly_matched"),
        hide_user_matched: flag(&hidden, "user_matched"),
        hide_unmatched: flag(&hidden, "unmatched"),
        hide_no_multiple: flag(&hidden, "no_multiple"),
        hide_name_date_matched: flag(&hidden, "name_date_matched"),
        hide_automatched: flag(&hidden, "automatched"),
        hide_aux_matched: flag(&hidden, "aux_matched"),
        limit,
        offset,
    };

    let rows = app.storage().api_download2(&filter).await?;
    let ct = if format == "json" {
        "application/json; charset=UTF-8"
    } else {
        "text/plain; charset=UTF-8"
    };

    // Format off the reactor — both branches do per-row string concat that
    // can dominate the response time for large dumps.
    let format_owned = format.clone();
    let out = tokio::task::spawn_blocking(move || {
        let mut out = String::new();
        for (i, row) in rows.iter().enumerate() {
            if i == 0 {
                if format_owned == "tab" {
                    out.push('#');
                    out.push_str(&row.keys().cloned().collect::<Vec<_>>().join("\t"));
                    out.push('\n');
                }
                if format_owned == "json" {
                    out.push_str("[\n");
                }
            }
            if format_owned == "json" {
                if i > 0 {
                    out.push_str(",\n");
                }
                out.push_str(&serde_json::to_string(row).unwrap_or_default());
            } else {
                out.push_str(
                    &row.values()
                        .map(|v| v.replace(['\t', '\n', '\r'], " "))
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
                out.push('\n');
            }
        }
        if rows.is_empty() && format_owned == "json" {
            out.push_str("[\n");
        }
        if format_owned == "json" {
            out.push_str("\n]");
        }
        out
    })
    .await
    .map_err(|e| ApiError(format!("download2 formatting panic: {e}")))?;

    Ok(([(axum::http::header::CONTENT_TYPE, ct)], out).into_response())
}
