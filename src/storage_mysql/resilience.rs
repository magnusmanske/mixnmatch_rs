//! Shared primitives for surviving MariaDB error 1969
//! ("max_statement_time exceeded") on long-running read-only queries.
//!
//! Two distinct call-site shapes are supported:
//!
//! * **Split-a-slice** — caller has a fixed input slice and a "process
//!   this slice" closure that fans out at the SQL level. On 1969 we
//!   split the slice in half and retry both halves; bottom out at
//!   length 1 (further split is impossible) and propagate the error.
//!   See [`process_slice_adaptive`].
//!
//! * **Halve-a-batch** — caller has a paginating fetcher with a
//!   `batch_size` parameter. On 1969 we halve the batch and retry the
//!   same offset; bottom out at `batch_size == 1` and propagate.
//!   See [`fetch_with_adaptive_batch`].
//!
//! The primitives `is_max_statement_time_err`, `halve_batch_for_retry`,
//! and `split_for_retry` are exposed individually for call sites with
//! bespoke retry shapes that don't quite fit either combinator.
//!
//! Detection walks the `anyhow::Error` chain because real call sites
//! surface the underlying `mysql_async::Error::Server { code: 1969 }`
//! wrapped in one or more `.context(...)` layers.

use anyhow::Result;
use futures::future::BoxFuture;

/// MariaDB server error code for "Query execution was interrupted
/// (max_statement_time exceeded)" — raised when a SELECT runs past the
/// per-statement budget configured by `mysql_misc::DEFAULT_MAX_STATEMENT_TIME_SECS`.
const MARIADB_ERR_QUERY_INTERRUPTED: u16 = 1969;

/// Returns true if `err`'s anyhow chain contains a `mysql_async::Error::Server`
/// with code `1969` (max_statement_time exceeded). Walks the chain because
/// real call sites surface this wrapped in `.context(...)`.
pub(crate) fn is_max_statement_time_err(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<mysql_async::Error>()
            .and_then(|me| match me {
                mysql_async::Error::Server(se) => Some(se.code),
                _ => None,
            })
            == Some(MARIADB_ERR_QUERY_INTERRUPTED)
    })
}

/// Adaptive-retry batch sizing: halve, floored at 1 so we never retry
/// with a no-op `LIMIT 0` that would stall a paginating loop.
pub(crate) fn halve_batch_for_retry(batch_size: usize) -> usize {
    (batch_size / 2).max(1)
}

/// Halve a slice length for a split-and-retry on 1969. Returns
/// `(left_len, right_len)` with `left + right == n` and both halves
/// non-empty, or `None` when `n <= 1` (the bottom-out signal: a
/// single-id slice has nowhere smaller to go, so the caller must
/// propagate the error instead of looping forever).
pub(crate) fn split_for_retry(n: usize) -> Option<(usize, usize)> {
    if n <= 1 {
        return None;
    }
    let left = n / 2;
    Some((left, n - left))
}

/// Recovery policy for [`process_slice_adaptive`] when the split-and-retry
/// loop bottoms out at slice length 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnIrreducible {
    /// Propagate the 1969 as the function's error. Right when the
    /// caller cannot tolerate missing rows (each input element MUST
    /// be processed for the result to be correct). No current caller
    /// uses this — they're all best-effort matchers — but the variant
    /// is the API-explicit "strict" choice for future call sites.
    #[allow(dead_code)]
    Propagate,
    /// Log a warning naming the offending element index and continue.
    /// The element contributes nothing to the output. Use when the
    /// caller's result is intrinsically best-effort — e.g. a candidate
    /// finder where "no matches found for this row" is an acceptable
    /// outcome that the rest of the pipeline already handles.
    Skip,
}

/// Iteratively process `slice` by repeatedly invoking `process` on
/// sub-slices, splitting any sub-slice in half on MariaDB error 1969.
///
/// `context` is included in retry log messages so operators can see
/// which caller triggered the split.
///
/// Order: left-to-right. The work stack is LIFO; we push the right
/// half first so the left half is popped (and processed) before it,
/// preserving the output order for tracing.
///
/// Bottom-out at length 1: behaviour is controlled by
/// [`OnIrreducible`] — propagate the error or skip the single element
/// and continue.
///
/// `process` returns a [`BoxFuture`] rather than using `AsyncFnMut`
/// to give the compiler an explicit per-call `Send` bound; the latter
/// fails to compose with `#[async_trait]`'s HRTB Send requirement on
/// trait-method bodies.
pub(crate) async fn process_slice_adaptive<'a, T, R, F>(
    slice: &'a [T],
    context: &str,
    on_irreducible: OnIrreducible,
    mut process: F,
) -> Result<Vec<R>>
where
    T: Sync + std::fmt::Debug + 'a,
    F: FnMut(&'a [T]) -> BoxFuture<'a, Result<Vec<R>>> + Send,
{
    let mut out: Vec<R> = Vec::new();
    let mut stack: Vec<&[T]> = vec![slice];
    while let Some(chunk) = stack.pop() {
        if chunk.is_empty() {
            continue;
        }
        match process(chunk).await {
            Ok(rows) => out.extend(rows),
            Err(e) if is_max_statement_time_err(&e) => {
                let Some((left_len, _)) = split_for_retry(chunk.len()) else {
                    match on_irreducible {
                        OnIrreducible::Propagate => return Err(e),
                        OnIrreducible::Skip => {
                            log::warn!(
                                "{context}: 1969 max_statement_time at irreducible single \
                                 element {:?}; skipping (best-effort policy)",
                                chunk
                            );
                            continue;
                        }
                    }
                };
                let (a, b) = chunk.split_at(left_len);
                log::warn!(
                    "{context}: 1969 max_statement_time at {} items; splitting into {} + {}",
                    chunk.len(),
                    a.len(),
                    b.len(),
                );
                stack.push(b);
                stack.push(a);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Per-statement timeout ladder used by [`with_escalating_timeout`].
/// Values in seconds; the first entry is the "baseline" (matches the
/// pool's default `max_statement_time`). Each subsequent entry doubles
/// the previous, capped at the Toolforge-friendly ceiling of 900 s.
///
/// Why these levels: doubling lets the call clear the query in at most
/// `log2(ceiling / base)` retries; 240 → 480 → 900 covers the common
/// "almost-finished-when-time-ran-out" cases without burning a long
/// pathological query for an order of magnitude longer than the
/// baseline.
pub(crate) const ESCALATION_LADDER_SECS: &[u64] = &[240, 480, 900];

/// Retry an SQL action with progressively higher per-statement budgets
/// on MariaDB error 1969.
///
/// `attempt(timeout_secs)` is invoked first with the baseline timeout
/// (`ESCALATION_LADDER_SECS[0]`) — the caller is responsible for
/// applying the timeout to the actual SQL, typically by prefixing
/// `SET STATEMENT max_statement_time={timeout_secs} FOR <select>`.
/// On 1969 we re-invoke `attempt` with the next ladder entry; on the
/// last entry we propagate.
///
/// Non-1969 errors short-circuit immediately — escalation only helps
/// for genuine timeouts.
///
/// `context` is included in retry log messages.
pub(crate) async fn with_escalating_timeout<'a, R, F>(context: &str, mut attempt: F) -> Result<R>
where
    F: FnMut(u64) -> BoxFuture<'a, Result<R>> + Send + 'a,
{
    let mut iter = ESCALATION_LADDER_SECS.iter().copied().peekable();
    loop {
        // Always at least one entry; the const slice is non-empty (compile-time enforced below).
        let secs = iter.next().expect("ESCALATION_LADDER_SECS is non-empty");
        match attempt(secs).await {
            Ok(r) => return Ok(r),
            Err(e) if is_max_statement_time_err(&e) && iter.peek().is_some() => {
                let next = *iter.peek().expect("checked Some");
                log::warn!(
                    "{context}: 1969 max_statement_time at budget={secs}s; escalating to {next}s"
                );
            }
            Err(e) => return Err(e),
        }
    }
}

const _: () = assert!(
    !ESCALATION_LADDER_SECS.is_empty(),
    "escalation ladder must have at least one entry"
);

/// Run `fetch(batch_size)`, halving `batch_size` on MariaDB error 1969
/// until either the call succeeds or `batch_size == 1` (then propagate).
///
/// `context` is included in retry log messages. The fetcher must be
/// idempotent across retries — only the LIMIT changes; any offset /
/// cursor must stay constant so retries can't skip rows.
pub(crate) async fn fetch_with_adaptive_batch<'a, R, F>(
    initial_batch: usize,
    context: &str,
    mut fetch: F,
) -> Result<R>
where
    F: FnMut(usize) -> BoxFuture<'a, Result<R>> + Send + 'a,
{
    let mut current = initial_batch;
    loop {
        match fetch(current).await {
            Ok(r) => return Ok(r),
            Err(e) if is_max_statement_time_err(&e) && current > 1 => {
                let smaller = halve_batch_for_retry(current);
                log::warn!(
                    "{context}: 1969 max_statement_time at batch_size={current}; halving to {smaller}"
                );
                current = smaller;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_1969() -> anyhow::Error {
        let me = mysql_async::Error::Server(mysql_async::ServerError {
            code: MARIADB_ERR_QUERY_INTERRUPTED,
            message: "Query execution was interrupted (max_statement_time exceeded)".into(),
            state: "70100".into(),
        });
        anyhow::Error::from(me)
    }

    #[test]
    fn is_max_statement_time_err_recognises_1969() {
        assert!(is_max_statement_time_err(&make_1969()));
    }

    #[test]
    fn is_max_statement_time_err_rejects_other_server_errors() {
        let me = mysql_async::Error::Server(mysql_async::ServerError {
            code: 1062,
            message: "duplicate".into(),
            state: "23000".into(),
        });
        let ae: anyhow::Error = me.into();
        assert!(!is_max_statement_time_err(&ae));
    }

    #[test]
    fn is_max_statement_time_err_rejects_non_mysql_error() {
        let ae = anyhow::anyhow!("plain non-mysql error");
        assert!(!is_max_statement_time_err(&ae));
    }

    #[test]
    fn is_max_statement_time_err_finds_1969_through_anyhow_context() {
        let ae = make_1969().context("automatch_by_search_fetch_page");
        assert!(is_max_statement_time_err(&ae));
    }

    #[test]
    fn split_for_retry_returns_none_at_or_below_one() {
        assert_eq!(split_for_retry(0), None);
        assert_eq!(split_for_retry(1), None);
    }

    #[test]
    fn split_for_retry_halves_even_lengths() {
        assert_eq!(split_for_retry(2), Some((1, 1)));
        assert_eq!(split_for_retry(1000), Some((500, 500)));
    }

    #[test]
    fn split_for_retry_odd_length_gives_smaller_half_first() {
        assert_eq!(split_for_retry(3), Some((1, 2)));
        assert_eq!(split_for_retry(999), Some((499, 500)));
    }

    #[test]
    fn split_for_retry_halves_sum_to_input() {
        for n in [2_usize, 3, 7, 64, 999, 1000, 1_000_000] {
            let (a, b) = split_for_retry(n).unwrap();
            assert_eq!(a + b, n, "halves of {n} must sum to {n}");
            assert!(a > 0 && b > 0, "neither half may be empty for n={n}");
        }
    }

    #[test]
    fn halve_batch_for_retry_halves_normal_sizes() {
        assert_eq!(halve_batch_for_retry(5000), 2500);
        assert_eq!(halve_batch_for_retry(1000), 500);
        assert_eq!(halve_batch_for_retry(2), 1);
    }

    #[test]
    fn halve_batch_for_retry_floors_at_one() {
        assert_eq!(halve_batch_for_retry(1), 1);
    }

    #[test]
    fn halve_batch_descending_sequence_terminates_at_one() {
        let mut current = 1000_usize;
        let mut steps = vec![current];
        while current > 1 {
            let next = halve_batch_for_retry(current);
            assert!(
                next < current,
                "halver must strictly decrease above 1: {current} → {next}"
            );
            current = next;
            steps.push(current);
        }
        assert_eq!(*steps.last().unwrap(), 1);
        assert!(
            steps.len() <= 12,
            "log2(1000) ≈ 10; descent should be ≤ 12 steps, got {steps:?}"
        );
    }

    // -----------------------------------------------------------------
    // process_slice_adaptive
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn process_slice_adaptive_no_errors_returns_all_rows_in_order() {
        let ids: Vec<usize> = (0..16).collect();
        let out: Vec<usize> =
            process_slice_adaptive(&ids, "test", OnIrreducible::Propagate, |s| {
                async move { Ok(s.iter().map(|x| x * 10).collect()) }.boxed()
            })
            .await
            .unwrap();
        let expected: Vec<usize> = (0..16).map(|x| x * 10).collect();
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn process_slice_adaptive_splits_on_1969_then_succeeds() {
        // Simulate: any slice with len > 2 trips 1969; smaller slices succeed.
        // Expected behaviour: the function recurses down until every leaf is
        // small enough, then returns all rows in the original order.
        let ids: Vec<usize> = (0..8).collect();
        let calls = AtomicUsize::new(0);
        let out: Vec<usize> =
            process_slice_adaptive(&ids, "test", OnIrreducible::Propagate, |s| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if s.len() > 2 {
                        Err(make_1969())
                    } else {
                        Ok(s.to_vec())
                    }
                }
                .boxed()
            })
            .await
            .unwrap();
        assert_eq!(out, (0..8).collect::<Vec<_>>());
        assert!(calls.load(Ordering::SeqCst) > 1);
    }

    #[tokio::test]
    async fn process_slice_adaptive_propagates_1969_at_length_one() {
        let ids = vec![42_usize];
        let result: Result<Vec<usize>> = process_slice_adaptive(
            &ids,
            "test",
            OnIrreducible::Propagate,
            |_| async { Err(make_1969()) }.boxed(),
        )
        .await;
        assert!(result.is_err());
        assert!(is_max_statement_time_err(&result.unwrap_err()));
    }

    #[tokio::test]
    async fn process_slice_adaptive_skip_drops_irreducible_and_continues() {
        // The element 42 always trips 1969 even at length 1; the rest
        // succeed normally. Skip policy → 42 contributes nothing,
        // every other id is processed.
        let ids: Vec<usize> = vec![1, 42, 3, 4];
        let out: Vec<usize> = process_slice_adaptive(&ids, "test", OnIrreducible::Skip, |s| {
            async move {
                if s.contains(&42) {
                    Err(make_1969())
                } else {
                    Ok(s.to_vec())
                }
            }
            .boxed()
        })
        .await
        .unwrap();
        // Output is best-effort: 42 is dropped, everything else returns.
        let mut sorted = out;
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 3, 4]);
    }

    #[tokio::test]
    async fn process_slice_adaptive_skip_returns_empty_when_all_irreducible() {
        // Even when every input is pathological, Skip mode returns Ok(vec![])
        // — the caller's job is to treat "no rows" as the normal no-match case.
        let ids: Vec<usize> = vec![1, 2, 3];
        let out: Vec<usize> = process_slice_adaptive(
            &ids,
            "test",
            OnIrreducible::Skip,
            |_| async { Err(make_1969()) }.boxed(),
        )
        .await
        .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn process_slice_adaptive_skip_still_propagates_non_1969() {
        // Skip policy is opt-in for 1969 only — every other error
        // category must still propagate. (A surprise propagation here
        // would silently hide bugs.)
        let ids: Vec<usize> = vec![1, 2, 3];
        let result: Result<Vec<usize>> = process_slice_adaptive(
            &ids,
            "test",
            OnIrreducible::Skip,
            |_| async { Err(anyhow::anyhow!("connection lost")) }.boxed(),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("connection lost"));
    }

    #[tokio::test]
    async fn process_slice_adaptive_propagates_non_1969_immediately() {
        let ids: Vec<usize> = (0..16).collect();
        let result: Result<Vec<usize>> = process_slice_adaptive(
            &ids,
            "test",
            OnIrreducible::Propagate,
            |_| async { Err(anyhow::anyhow!("some other error")) }.boxed(),
        )
        .await;
        let err = result.unwrap_err();
        assert!(!is_max_statement_time_err(&err));
        assert!(err.to_string().contains("some other error"));
    }

    // -----------------------------------------------------------------
    // fetch_with_adaptive_batch
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn fetch_with_adaptive_batch_succeeds_first_try_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let out: usize = fetch_with_adaptive_batch(1000, "test", |n| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(n) }.boxed()
        })
        .await
        .unwrap();
        assert_eq!(out, 1000);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_with_adaptive_batch_halves_until_threshold() {
        // Pass when batch <= 100, fail otherwise. Expect 1000 → 500 → 250 → 125 → 62.
        let calls = AtomicUsize::new(0);
        let out: usize = fetch_with_adaptive_batch(1000, "test", |n| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { if n <= 100 { Ok(n) } else { Err(make_1969()) } }.boxed()
        })
        .await
        .unwrap();
        assert!(out <= 100);
        assert!(calls.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn fetch_with_adaptive_batch_propagates_when_batch_one_still_fails() {
        let result: Result<usize> =
            fetch_with_adaptive_batch(8, "test", |_| async { Err(make_1969()) }.boxed()).await;
        assert!(result.is_err());
        assert!(is_max_statement_time_err(&result.unwrap_err()));
    }

    // -----------------------------------------------------------------
    // with_escalating_timeout
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn with_escalating_timeout_succeeds_at_baseline_no_retry() {
        let calls = AtomicUsize::new(0);
        let observed_budgets = std::sync::Mutex::new(Vec::<u64>::new());
        let out: u64 = with_escalating_timeout("test", |secs| {
            calls.fetch_add(1, Ordering::SeqCst);
            observed_budgets.lock().unwrap().push(secs);
            async move { Ok(secs) }.boxed()
        })
        .await
        .unwrap();
        assert_eq!(out, ESCALATION_LADDER_SECS[0]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            observed_budgets.lock().unwrap().clone(),
            vec![ESCALATION_LADDER_SECS[0]]
        );
    }

    #[tokio::test]
    async fn with_escalating_timeout_walks_full_ladder_on_repeated_1969() {
        // Always fail with 1969; we should see exactly len(ladder) attempts,
        // each at the next ladder budget, and the final error propagates.
        let observed_budgets = std::sync::Mutex::new(Vec::<u64>::new());
        let result: Result<()> = with_escalating_timeout("test", |secs| {
            observed_budgets.lock().unwrap().push(secs);
            async move { Err::<(), _>(make_1969()) }.boxed()
        })
        .await;
        assert!(result.is_err());
        assert!(is_max_statement_time_err(&result.unwrap_err()));
        let budgets = observed_budgets.lock().unwrap().clone();
        assert_eq!(budgets, ESCALATION_LADDER_SECS.to_vec());
    }

    #[tokio::test]
    async fn with_escalating_timeout_propagates_non_1969_without_escalating() {
        let calls = AtomicUsize::new(0);
        let result: Result<()> = with_escalating_timeout("test", |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(anyhow::anyhow!("connection refused")) }.boxed()
        })
        .await;
        assert!(result.is_err());
        assert!(!is_max_statement_time_err(&result.unwrap_err()));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-1969 must not escalate"
        );
    }

    #[tokio::test]
    async fn with_escalating_timeout_succeeds_at_higher_rung() {
        // 1969 at the first budget, success at the second.
        let calls = AtomicUsize::new(0);
        let out: u64 = with_escalating_timeout("test", |secs| {
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(make_1969())
                } else {
                    Ok(secs)
                }
            }
            .boxed()
        })
        .await
        .unwrap();
        assert_eq!(out, ESCALATION_LADDER_SECS[1]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn escalation_ladder_is_monotonic_and_capped() {
        let ladder = ESCALATION_LADDER_SECS;
        assert!(!ladder.is_empty());
        for w in ladder.windows(2) {
            assert!(
                w[0] < w[1],
                "ladder must be strictly increasing: {ladder:?}"
            );
        }
        assert!(
            *ladder.last().unwrap() <= 900,
            "top of ladder must respect Toolforge-friendly ceiling (got {ladder:?})"
        );
    }

    #[tokio::test]
    async fn fetch_with_adaptive_batch_propagates_non_1969_without_halving() {
        let calls = AtomicUsize::new(0);
        let result: Result<usize> = fetch_with_adaptive_batch(1000, "test", |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(anyhow::anyhow!("connection refused")) }.boxed()
        })
        .await;
        assert!(result.is_err());
        assert!(!is_max_statement_time_err(&result.unwrap_err()));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "non-1969 must not trigger halving"
        );
    }
}
