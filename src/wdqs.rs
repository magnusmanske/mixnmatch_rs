//! Streamed-TSV access to the Wikidata Query Service.
//!
//! Sister of the JSON-binding plumbing in the `mediawiki` crate: TSV
//! is roughly 3× more compact and decodes incrementally, which matters
//! for queries that return tens of thousands of rows (sync of a popular
//! property, the property-cache rebuild, etc.). Those queries reliably
//! tip JSON responses past the timeout / chunked-decode envelope; TSV
//! sails through.
//!
//! The decoded form is a `Vec<Vec<String>>` — one row of cell values
//! per result, with IRI wrappers stripped and literal escapes resolved
//! per the W3C SPARQL 1.1 TSV format
//! ([spec](https://www.w3.org/TR/sparql11-results-csv-tsv/)). Lang
//! tags / typed-literal datatype suffixes are dropped, since none of
//! the in-tree callers project them.

use anyhow::{Result, anyhow};
use std::time::Duration;

use crate::util::circuit::Breaker;

/// Circuit breaker fronting the WDQS endpoint. After five consecutive
/// failures the breaker opens for 60 s, during which `run_tsv_query`
/// fast-fails without touching the network. Tuning rationale:
/// - WDQS has been observed to suffer 10–30 min outages; a 60 s open
///   window means we re-probe twelve times an hour rather than retrying
///   every queued query.
/// - Five failures rather than three keeps a noisy-but-not-broken
///   WDQS (occasional 502 in a recovering cluster) from tripping the
///   breaker against the operator's intent.
static WDQS_BREAKER: Breaker = Breaker::new("wdqs", 5, 60);

/// WDQS production endpoint. POSTs are signed-form-encoded for parity
/// with the mediawiki crate's `sparql_query` (keeps the network path
/// uniform whether we go through the crate or directly).
pub const WDQS_URL: &str = "https://query.wikidata.org/sparql";

/// Per-attempt request budget. WDQS itself enforces a 60 s wall-clock
/// limit on the query, so giving the client another 30 s of slack
/// covers TLS handshake, body streaming and gzip de-framing. Anything
/// longer just waits for a response that's already been abandoned
/// upstream.
pub const SPARQL_TIMEOUT_SECS: u64 = 90;

/// One retry per logical request — enough to dodge a momentary upstream
/// blip without wasting a full minute when WDQS is genuinely down.
pub const SPARQL_MAX_ATTEMPTS: usize = 2;

/// User-agent string used for both browser-style WDQS calls and bot
/// writes. Stays terse and includes a contact URL per the WMF UA policy.
pub const USER_AGENT: &str = "mix-n-match (https://mix-n-match.toolforge.org)";

/// Build a freshly-configured reqwest client suitable for WDQS calls.
/// The timeout is the per-request limit; gzip/deflate are negotiated
/// automatically so the caller never has to think about the wire
/// encoding.
pub fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(SPARQL_TIMEOUT_SECS))
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| anyhow!("WDQS HTTP client init failed: {e}"))
}

/// Run `sparql` against WDQS in TSV mode, retrying once on transient
/// failure. Returns one inner vector per result row (header line
/// dropped); cells are decoded — IRIs without `<>`, literal text
/// without quotes, lang/datatype suffixes stripped.
///
/// A request error is surfaced as `Err`; an empty result set is `Ok`
/// with zero rows. Callers needing a sanity floor (e.g. "this query
/// must produce N+ rows or something is very wrong") should check the
/// length themselves — see `Maintenance::update_property_cache` for
/// the canonical pattern.
pub async fn run_tsv_query(client: &reqwest::Client, sparql: &str) -> Result<Vec<Vec<String>>> {
    // Fast-fail when the breaker is open. Saves a 90 s timeout per
    // call against an upstream that's already known to be down — the
    // dominant cost during a WDQS outage.
    if WDQS_BREAKER.is_open() {
        WDQS_BREAKER.record_rejected();
        crate::metrics::record_sparql_failure("breaker_open");
        return Err(anyhow!("WDQS request rejected: breaker open"));
    }
    let mut last_err: Option<String> = None;
    for attempt in 0..SPARQL_MAX_ATTEMPTS {
        match send_once(client, sparql).await {
            Ok(rows) => {
                WDQS_BREAKER.record_success();
                return Ok(rows);
            }
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < SPARQL_MAX_ATTEMPTS {
                    // Short pause; not a real backoff — WDQS errors
                    // are usually all-or-nothing, so a long sleep
                    // wouldn't help much.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
    WDQS_BREAKER.record_failure();
    let reason = classify_sparql_failure(last_err.as_deref());
    crate::metrics::record_sparql_failure(reason);
    Err(anyhow!(
        "WDQS request failed after {SPARQL_MAX_ATTEMPTS} attempts: {}",
        last_err.unwrap_or_else(|| "unknown error".into())
    ))
}

/// Bucket the per-attempt error string into a small set of label values
/// so the failure counter stays low-cardinality. Anything we can't
/// classify lands in `other`.
fn classify_sparql_failure(msg: Option<&str>) -> &'static str {
    let Some(msg) = msg else {
        return "unknown";
    };
    if msg.contains("timed out") || msg.contains("timeout") || msg.contains("operation timed out") {
        "timeout"
    } else if msg.contains("HTTP 5") {
        "http_5xx"
    } else if msg.contains("HTTP 4") {
        "http_4xx"
    } else if msg.contains("decode") || msg.contains("body") {
        "decode"
    } else if msg.contains("connect") || msg.contains("dns") {
        "connect"
    } else {
        "other"
    }
}

async fn send_once(client: &reqwest::Client, sparql: &str) -> Result<Vec<Vec<String>>, String> {
    let resp = client
        .post(WDQS_URL)
        .header(reqwest::header::ACCEPT, "text/tab-separated-values")
        .form(&[("query", sparql)])
        .send()
        .await
        .map_err(|e| format!("send: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let body = resp.text().await.map_err(|e| format!("read body: {e}"))?;
    Ok(parse_tsv(&body))
}

/// Parse a W3C SPARQL TSV body into one `Vec<String>` per row. The
/// header line (which holds the variable names) is consumed and
/// discarded; blank trailing lines (typical of `body.lines()` when the
/// payload ends with a newline) are skipped.
pub fn parse_tsv(body: &str) -> Vec<Vec<String>> {
    let mut iter = body.lines();
    iter.next(); // header (?var1\t?var2\t…)
    iter.filter_map(|line| {
        if line.is_empty() {
            return None;
        }
        Some(line.split('\t').map(unwrap_cell).collect())
    })
    .collect()
}

/// Decode one TSV cell: an IRI wrapped in `<...>`, a literal wrapped in
/// `"..."` (with the spec's `\t \n \r \" \\` escapes), or anything else
/// passed through unchanged. Trailing `@lang` / `^^<datatype>` suffixes
/// on literals are intentionally discarded — none of the callers in
/// this tree project them.
fn unwrap_cell(cell: &str) -> String {
    let cell = cell.trim();
    if cell.len() >= 2 && cell.starts_with('<') && cell.ends_with('>') {
        cell[1..cell.len() - 1].to_string()
    } else if cell.starts_with('"') {
        unescape_literal(cell)
    } else {
        cell.to_string()
    }
}

fn unescape_literal(cell: &str) -> String {
    let mut chars = cell.chars();
    chars.next(); // opening "
    let mut out = String::with_capacity(cell.len());
    while let Some(c) = chars.next() {
        if c == '"' {
            break;
        }
        if c == '\\' {
            match chars.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => break,
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Strip the `Q` / `P` prefix off an entity URI and parse the numeric
/// id. Convenience for the common pattern where a SPARQL row holds an
/// entity URI (e.g. `http://www.wikidata.org/entity/Q42`) and the
/// caller wants the bare 42.
///
/// Returns `None` for any URI that doesn't end with the expected
/// prefix + digits — including malformed responses, redirect IRIs and
/// the special `Q-1` no-Wikidata sentinel.
pub fn entity_id_from_uri(uri: &str, expected_prefix: char) -> Option<usize> {
    let last = uri.rsplit('/').next()?;
    let rest = last.strip_prefix(expected_prefix)?;
    rest.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unwrap_iri_cell_strips_brackets() {
        assert_eq!(
            unwrap_cell("<http://www.wikidata.org/entity/Q42>"),
            "http://www.wikidata.org/entity/Q42"
        );
    }

    #[test]
    fn unwrap_literal_cell_handles_escapes_and_suffixes() {
        assert_eq!(unwrap_cell(r#""hello""#), "hello");
        assert_eq!(unwrap_cell(r#""a\tb""#), "a\tb");
        // Lang tag and datatype suffix are intentionally dropped.
        assert_eq!(unwrap_cell(r#""hello"@en"#), "hello");
        assert_eq!(unwrap_cell(r#""5"^^<http://www.w3.org/2001/XMLSchema#integer>"#), "5");
    }

    #[test]
    fn unwrap_unknown_cell_passes_through() {
        // Numbers, blank nodes and similar are returned as-is.
        assert_eq!(unwrap_cell("42"), "42");
        assert_eq!(unwrap_cell("_:b1"), "_:b1");
    }

    #[test]
    fn parse_tsv_drops_header_and_blank_lines() {
        let body = "?p\t?v\n\
                    <http://www.wikidata.org/entity/Q1>\t\"v1\"\n\
                    <http://www.wikidata.org/entity/Q2>\t\"v2\"\n\
                    \n";
        let rows = parse_tsv(body);
        assert_eq!(
            rows,
            vec![
                vec![
                    "http://www.wikidata.org/entity/Q1".to_string(),
                    "v1".to_string()
                ],
                vec![
                    "http://www.wikidata.org/entity/Q2".to_string(),
                    "v2".to_string()
                ],
            ]
        );
    }

    #[test]
    fn parse_tsv_handles_three_columns() {
        let body = "?p\t?v\t?vLabel\n\
                    <http://example/P1>\t<http://example/Q5>\t\"human\"\n";
        let rows = parse_tsv(body);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert_eq!(rows[0][2], "human");
    }

    #[test]
    fn classify_sparql_failure_buckets_known_shapes() {
        // The classifier feeds the `mnm_sparql_failures_total` counter's
        // `reason` label — keep this low-cardinality and stable so
        // dashboards don't churn.
        assert_eq!(classify_sparql_failure(None), "unknown");
        assert_eq!(
            classify_sparql_failure(Some("send: error sending request: operation timed out")),
            "timeout"
        );
        assert_eq!(
            classify_sparql_failure(Some("HTTP 504 Gateway Timeout")),
            "http_5xx"
        );
        assert_eq!(
            classify_sparql_failure(Some("HTTP 429 Too Many Requests")),
            "http_4xx"
        );
        assert_eq!(
            classify_sparql_failure(Some("read body: error decoding response")),
            "decode"
        );
        assert_eq!(
            classify_sparql_failure(Some("error trying to connect: dns lookup failed")),
            "connect"
        );
        assert_eq!(
            classify_sparql_failure(Some("something totally weird")),
            "other"
        );
    }

    #[test]
    fn entity_id_from_uri_extracts_q_number() {
        assert_eq!(
            entity_id_from_uri("http://www.wikidata.org/entity/Q42", 'Q'),
            Some(42)
        );
        assert_eq!(
            entity_id_from_uri("http://www.wikidata.org/entity/P31", 'P'),
            Some(31)
        );
    }

    #[test]
    fn entity_id_from_uri_rejects_wrong_prefix() {
        assert_eq!(
            entity_id_from_uri("http://www.wikidata.org/entity/P31", 'Q'),
            None
        );
    }

    #[test]
    fn entity_id_from_uri_rejects_non_numeric() {
        assert_eq!(entity_id_from_uri("http://example.com/no-qid", 'Q'), None);
        assert_eq!(entity_id_from_uri("Q-1", 'Q'), None);
    }
}
