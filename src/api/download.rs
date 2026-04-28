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
            // Scrub embedded tabs/newlines out of every cell so an
            // ext_name / ext_url containing stray whitespace can't tear
            // a row into two.
            out.push_str(&scrub_tsv_cell(&q.to_string()));
            out.push('\t');
            out.push_str(&scrub_tsv_cell(ext_id));
            out.push('\t');
            out.push_str(&scrub_tsv_cell(ext_url));
            out.push('\t');
            out.push_str(&scrub_tsv_cell(ext_name));
            out.push('\t');
            out.push_str(&scrub_tsv_cell(uname));
            out.push('\n');
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

/// `?query=download2` — bulk export of catalog entries as TSV (default) or
/// JSON. Output is always capped to a single page, so callers fetching very
/// large catalogs need to walk pages with `limit` + `offset`.
///
/// Parameters:
/// - `catalogs` — comma-separated catalog ids (digits and commas only;
///   anything else is silently stripped). Required.
/// - `format` — `tab` (default, TSV) or `json`.
/// - `columns` — JSON object of optional-column flags (`exturl`, `username`,
///   `dates`, `location`); each value may be `true`/`false` or `1`/`0`.
///   Defaults to `{}` (all off).
/// - `hidden` — JSON object of row-filter flags (`any_matched`,
///   `firmly_matched`, `user_matched`, `unmatched`, `no_multiple`,
///   `name_date_matched`, `automatched`, `aux_matched`); same value shape as
///   `columns`.
/// - `limit` — page size. Default `100000`, clamped to `[1, 1000000]`.
/// - `offset` — number of rows to skip. Default `0`. Combined with `limit`,
///   this is the pagination contract: a caller wanting every row must keep
///   incrementing `offset` by `limit` until a page comes back with fewer than
///   `limit` rows.
///
/// The result columns are ordered deterministically (matching the SQL
/// SELECT list); both TSV and JSON share that ordering, so downstream
/// consumers can rely on column position.
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

    let (cols, rows) = app.storage().api_download2(&filter).await?;
    let ct = if format == "json" {
        "application/json; charset=UTF-8"
    } else {
        "text/plain; charset=UTF-8"
    };

    // Format off the reactor — both branches do per-row string concat that
    // can dominate the response time for large dumps.
    let format_owned = format.clone();
    let out = tokio::task::spawn_blocking(move || {
        if format_owned == "json" {
            write_json(&cols, &rows)
        } else {
            write_tsv(&cols, &rows)
        }
    })
    .await
    .map_err(|e| ApiError(format!("download2 formatting panic: {e}")))?;

    Ok(([(axum::http::header::CONTENT_TYPE, ct)], out).into_response())
}

/// Strip characters that would tear a TSV row apart. Tabs and newlines
/// (LF, CR, NEL U+0085, LINE-SEPARATOR U+2028, PARA-SEPARATOR U+2029) get
/// replaced with a single space — same behaviour for every column.
fn scrub_tsv_cell(v: &str) -> String {
    v.chars()
        .map(|c| match c {
            '\t' | '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}' => ' ',
            other => other,
        })
        .collect()
}

/// Build the TSV body with `#` + tab-joined columns as the header, one
/// row per line. Columns follow the SQL SELECT order exactly, so each
/// value lines up with its header across every row.
fn write_tsv(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::new();
    if !columns.is_empty() {
        out.push('#');
        out.push_str(&columns.join("\t"));
        out.push('\n');
    }
    for row in rows {
        // If a row is shorter than the header (shouldn't happen — defensive),
        // pad with empties so cells stay aligned with the header.
        for i in 0..columns.len() {
            if i > 0 {
                out.push('\t');
            }
            if let Some(cell) = row.get(i) {
                out.push_str(&scrub_tsv_cell(cell));
            }
        }
        out.push('\n');
    }
    out
}

/// Build the JSON body as an array of objects. Each object's keys follow
/// the SELECT column order (not HashMap random order), so consumers get
/// deterministic output that also matches the TSV dump.
fn write_json(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::from("[\n");
    for (ri, row) in rows.iter().enumerate() {
        if ri > 0 {
            out.push_str(",\n");
        }
        out.push('{');
        for (ci, col) in columns.iter().enumerate() {
            if ci > 0 {
                out.push(',');
            }
            out.push_str(&serde_json::to_string(col).unwrap_or_default());
            out.push(':');
            let value = row.get(ci).map(String::as_str).unwrap_or("");
            out.push_str(&serde_json::to_string(value).unwrap_or_default());
        }
        out.push('}');
    }
    out.push_str("\n]");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_replaces_tabs_and_every_newline_flavour() {
        // LF, CR, NEL, LINE-SEPARATOR, PARA-SEPARATOR all become spaces.
        let input = "a\tb\nc\rd\u{0085}e\u{2028}f\u{2029}g";
        assert_eq!(scrub_tsv_cell(input), "a b c d e f g");
    }

    #[test]
    fn scrub_leaves_ordinary_text_untouched() {
        assert_eq!(scrub_tsv_cell("hello world"), "hello world");
        assert_eq!(scrub_tsv_cell(""), "");
    }

    #[test]
    fn tsv_has_one_newline_per_row_and_aligned_columns() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let rows = vec![
            vec!["1".to_string(), "2".to_string(), "3".to_string()],
            vec!["x".to_string(), "y".to_string(), "z".to_string()],
        ];
        let out = write_tsv(&cols, &rows);
        // Exactly header + 2 data lines = 3 newlines, no blanks.
        assert_eq!(out.matches('\n').count(), 3);
        assert_eq!(out, "#a\tb\tc\n1\t2\t3\nx\ty\tz\n");
    }

    #[test]
    fn tsv_scrubs_embedded_newlines_and_tabs() {
        let cols = vec!["name".to_string(), "url".to_string()];
        let rows = vec![vec![
            "foo\nbar\tbaz".to_string(),
            "http://example.com".to_string(),
        ]];
        let out = write_tsv(&cols, &rows);
        // Header + one data row → exactly 2 newlines, no extras from
        // the embedded \n or \t in the cell.
        assert_eq!(out.matches('\n').count(), 2);
        assert!(out.contains("foo bar baz\thttp://example.com"));
    }

    #[test]
    fn tsv_emits_header_even_when_rows_is_empty() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let out = write_tsv(&cols, &[]);
        assert_eq!(out, "#a\tb\n");
    }

    #[test]
    fn json_preserves_column_order_across_rows() {
        // The old implementation serialised HashMap<String, String>, whose
        // iteration order varies per-instance, so two rows could have
        // different key orders in the same JSON payload. Lock it down.
        let cols = vec!["z".to_string(), "a".to_string(), "m".to_string()];
        let rows = vec![
            vec!["z1".to_string(), "a1".to_string(), "m1".to_string()],
            vec!["z2".to_string(), "a2".to_string(), "m2".to_string()],
        ];
        let out = write_json(&cols, &rows);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        // Check key-order in the emitted text (not via parsed, since
        // serde_json re-orders maps).
        assert!(out.contains(r#""z":"z1","a":"a1","m":"m1""#));
        assert!(out.contains(r#""z":"z2","a":"a2","m":"m2""#));
        assert_eq!(parsed.as_array().map(|a| a.len()), Some(2));
    }

    #[test]
    fn json_escapes_quotes_and_backslashes_in_values() {
        let cols = vec!["v".to_string()];
        let rows = vec![vec![r#"he said "hi" \ foo"#.to_string()]];
        let out = write_json(&cols, &rows);
        // Round-trips through serde_json, so it stays valid JSON.
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed[0]["v"], r#"he said "hi" \ foo"#);
    }

    #[test]
    fn json_empty_rows_still_valid() {
        let cols = vec!["a".to_string()];
        let out = write_json(&cols, &[]);
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed.as_array().map(|a| a.len()), Some(0));
    }
}
