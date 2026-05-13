//! Minimal failure-count circuit breaker for external dependencies.
//!
//! Wraps a single named upstream (WDQS, WDRC, …). After
//! `threshold` consecutive failures the breaker opens for
//! `open_for_secs`: `is_open()` returns `true` and callers should
//! fast-fail without touching the upstream. Once the window elapses
//! the breaker closes again — the next call flows through and either
//! resets the failure counter (`record_success`) or starts rebuilding
//! toward the threshold (`record_failure`).
//!
//! This is deliberately not a full Hystrix-style implementation:
//! - no half-open / probe-request gating
//! - no rolling window (consecutive count only)
//! - no per-call latency accounting
//!
//! It exists to put a hard ceiling on the cost of a multi-hour upstream
//! outage when an unbounded retry loop would otherwise burn the whole
//! job-runner pool. For richer behaviour, extend this module rather than
//! pulling in a crate.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// One named circuit-breaker instance. Designed for `static` storage —
/// every field uses interior mutability via atomics, so `&'static Breaker`
/// is enough to record outcomes from any task.
#[derive(Debug)]
pub struct Breaker {
    /// Stable label emitted with breaker metrics. Hand-curated;
    /// don't pipe user input here (cardinality hazard).
    name: &'static str,
    /// Consecutive failure count. Reset to 0 on every success and on
    /// every open transition.
    failures: AtomicUsize,
    /// Epoch-ms wall-clock at which `is_open` should start returning
    /// `false` again. Zero = breaker is closed.
    open_until_epoch_ms: AtomicU64,
    /// Consecutive failures required to open.
    threshold: usize,
    /// How long an opened breaker stays open before the next request
    /// is allowed through.
    open_for: Duration,
}

impl Breaker {
    /// Construct a breaker. `const fn` so callers can declare static
    /// breakers without `Lazy` / `OnceLock`.
    pub const fn new(name: &'static str, threshold: usize, open_for_secs: u64) -> Self {
        Self {
            name,
            failures: AtomicUsize::new(0),
            open_until_epoch_ms: AtomicU64::new(0),
            threshold,
            open_for: Duration::from_secs(open_for_secs),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// True iff the breaker is currently rejecting calls.
    pub fn is_open(&self) -> bool {
        self.is_open_at(now_epoch_ms())
    }

    /// Time-injected variant of [`is_open`] for unit tests.
    fn is_open_at(&self, now_ms: u64) -> bool {
        now_ms < self.open_until_epoch_ms.load(Ordering::Relaxed)
    }

    /// Note a successful call. Always closes the breaker.
    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
        // No need to clear open_until — `is_open_at` already returns
        // false after a success because we got here from a closed
        // breaker.
    }

    /// Note a failed call. Opens the breaker iff this is the
    /// `threshold`-th consecutive failure.
    pub fn record_failure(&self) {
        self.record_failure_at(now_epoch_ms());
    }

    fn record_failure_at(&self, now_ms: u64) {
        let new_count = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
        if new_count >= self.threshold {
            let open_until = now_ms.saturating_add(self.open_for.as_millis() as u64);
            self.open_until_epoch_ms
                .store(open_until, Ordering::Relaxed);
            self.failures.store(0, Ordering::Relaxed);
            metrics::counter!(
                "mnm_breaker_opened_total",
                "breaker" => self.name,
            )
            .increment(1);
        }
    }

    /// Count this call as rejected by the open breaker. Pure metric
    /// helper — the caller is expected to short-circuit on
    /// [`is_open`] and emit this to attribute the cost.
    pub fn record_rejected(&self) {
        metrics::counter!(
            "mnm_breaker_rejected_total",
            "breaker" => self.name,
        )
        .increment(1);
    }
}

fn now_epoch_ms() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_breaker_is_closed() {
        let b = Breaker::new("t", 3, 30);
        assert!(!b.is_open());
    }

    #[test]
    fn failures_below_threshold_keep_it_closed() {
        let b = Breaker::new("t", 3, 30);
        b.record_failure();
        b.record_failure();
        assert!(!b.is_open());
    }

    #[test]
    fn threshold_failures_open_the_breaker() {
        let b = Breaker::new("t", 3, 30);
        b.record_failure();
        b.record_failure();
        b.record_failure();
        assert!(b.is_open());
    }

    #[test]
    fn success_resets_the_failure_counter() {
        let b = Breaker::new("t", 3, 30);
        b.record_failure();
        b.record_failure();
        b.record_success();
        b.record_failure();
        b.record_failure();
        // Only two consecutive failures since the success — still closed.
        assert!(!b.is_open());
    }

    #[test]
    fn breaker_closes_after_open_window_elapses() {
        let b = Breaker::new("t", 1, 1);
        // Use the time-injected helpers so the test never sleeps.
        let opened_at = 1_000_000;
        b.record_failure_at(opened_at);
        // Immediately after opening: rejecting.
        assert!(b.is_open_at(opened_at + 100));
        // Just before the window closes: still rejecting.
        assert!(b.is_open_at(opened_at + 999));
        // Window elapsed: closed again.
        assert!(!b.is_open_at(opened_at + 1_001));
    }

    #[test]
    fn open_resets_failure_count_so_subsequent_failures_must_re_accumulate() {
        let b = Breaker::new("t", 2, 1);
        let opened_at = 1_000_000;
        b.record_failure_at(opened_at);
        b.record_failure_at(opened_at); // Opens
        assert!(b.is_open_at(opened_at + 100));
        // After window elapses, breaker closes; we need `threshold`
        // *new* failures to re-open. One failure must not re-open.
        let after_window = opened_at + 2_000;
        b.record_failure_at(after_window);
        assert!(!b.is_open_at(after_window + 100));
        // The second failure does re-open.
        b.record_failure_at(after_window);
        assert!(b.is_open_at(after_window + 100));
    }

    #[test]
    fn name_is_carried_through() {
        let b = Breaker::new("wdqs", 1, 1);
        assert_eq!(b.name(), "wdqs");
    }
}
