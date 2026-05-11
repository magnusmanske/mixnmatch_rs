//! Prometheus exporter + the codebase's instrumentation entry points.
//!
//! Initialised by `cli::init_observability`. The Prometheus recorder is
//! installed as the global `metrics` recorder, so any `metrics::counter!`
//! / `histogram!` call anywhere in the crate flows to the same store.
//! `/metrics` on the webserver renders the current snapshot in the
//! Prometheus text-exposition format.
//!
//! Cardinality is the usual hazard — every label combination is a
//! separate time-series. Only emit labels with bounded value spaces:
//!   - `query` is one of the ~175 routes in `api::router::ROUTES`,
//!   - `status` is the small set of HTTP status codes we actually
//!     return (400/401/403/404/500/200…),
//!   - `reason` is a hand-curated short enum.
//! Don't add high-cardinality labels like entry id, catalog id, or
//! arbitrary user input.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the Prometheus recorder as the global `metrics` recorder.
/// Safe to call more than once — subsequent calls are no-ops because
/// the global recorder slot can only be set once.
pub fn init() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    if metrics::set_global_recorder(recorder).is_ok() {
        let _ = HANDLE.set(handle);
    }
}

/// Render the current metric values in Prometheus text-exposition
/// format. Returns an empty string if the recorder wasn't installed
/// (e.g. in tests that don't go through `init`).
pub fn render() -> String {
    HANDLE.get().map(PrometheusHandle::render).unwrap_or_default()
}

/// Record one completed `/api.php?query=…` request. Emits:
///   - `mnm_api_requests_total{query}` (always)
///   - `mnm_api_errors_total{query, status}` (only when status >= 400)
///   - `mnm_api_request_duration_seconds{query}` (always, histogram)
pub fn record_api_request(query: &str, status_code: u16, latency_secs: f64) {
    let query_label = query.to_string();
    metrics::counter!("mnm_api_requests_total", "query" => query_label.clone()).increment(1);
    if status_code >= 400 {
        metrics::counter!(
            "mnm_api_errors_total",
            "query" => query_label.clone(),
            "status" => status_code.to_string(),
        )
        .increment(1);
    }
    metrics::histogram!("mnm_api_request_duration_seconds", "query" => query_label)
        .record(latency_secs);
}

/// Record a WDQS query failure. The `reason` is a hand-curated short
/// label (e.g. "timeout", "decode", "http_5xx") — keep cardinality
/// bounded.
pub fn record_sparql_failure(reason: &'static str) {
    metrics::counter!("mnm_sparql_failures_total", "reason" => reason).increment(1);
}
