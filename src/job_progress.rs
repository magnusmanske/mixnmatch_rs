//! Typed progress payload stored under the `"progress"` key in `jobs.json`.
//!
//! Schema written to disk:
//! ```json
//! {
//!   "offset":    1234,         // legacy resume cursor; kept in sync with `processed`
//!   "progress": { "processed": 1234, "total": 5000, "percent": 24.68 },
//!   "levels":   [ ... ]        // autoscrape only; opaque to the merge layer
//! }
//! ```
//!
//! Backward compatibility — readers MUST tolerate:
//! - the old bare `{"offset": N}` shape (pre-progress jobs);
//! - the old bare `[...]` array shape (pre-progress autoscrape).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// Typed progress payload. All three fields are independently optional so
/// callers can publish just a counter (`processed` only) when the total
/// isn't known, or a full percentage when it is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct JobProgress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percent: Option<f32>,
}

impl JobProgress {
    /// Build a progress payload from raw counts. Derives `percent` only
    /// when `total` is `Some` and non-zero; clamps to `[0.0, 100.0]` so a
    /// `processed > total` race doesn't surface as "117 %" in the UI.
    pub fn from_counts(processed: u64, total: Option<u64>) -> Self {
        let percent = total.and_then(|t| {
            if t == 0 {
                None
            } else {
                let raw = (processed as f64 / t as f64) * 100.0;
                Some(raw.clamp(0.0, 100.0) as f32)
            }
        });
        Self {
            processed: Some(processed),
            total,
            percent,
        }
    }

    /// Parse a `progress` sub-object out of a `jobs.json` document. Returns
    /// `None` for legacy shapes (bare offset object, bare autoscrape array)
    /// where there is no progress payload to surface.
    pub fn from_json(json: &Value) -> Option<Self> {
        json.as_object()?
            .get("progress")
            .and_then(|p| serde_json::from_value(p.clone()).ok())
    }
}

/// Merge a `progress` payload (and a mirrored `offset` cursor) into the
/// existing `jobs.json` document, preserving any other keys that already
/// live there (e.g. autoscrape's `levels` array).
///
/// Legacy shapes are normalised to the new object form:
/// - `null` / `None` → new object.
/// - bare `{"offset": N}` → object retains `offset`, gains `progress`.
/// - bare `[...]` (autoscrape's old level array) → wrapped as `{"levels": [...], ...}`.
///
/// The mirrored `offset` is kept so `Jobbable::get_last_job_offset()` (and
/// any external readers that already parse `json.offset`) continue to work
/// unchanged.
pub fn merge_progress_into_json(existing: Option<&Value>, progress: &JobProgress) -> Value {
    let mut obj = match existing {
        Some(Value::Object(map)) => map.clone(),
        Some(Value::Array(arr)) => {
            // Legacy autoscrape shape: wrap the level array under "levels"
            // so future writes are in the new object form.
            let mut m = Map::new();
            m.insert("levels".to_string(), Value::Array(arr.clone()));
            m
        }
        _ => Map::new(),
    };

    // Mirror `processed` into `offset` so resume-on-restart keeps working
    // for callers that read `json.offset` directly.
    if let Some(p) = progress.processed {
        obj.insert("offset".to_string(), json!(p));
    }
    obj.insert(
        "progress".to_string(),
        serde_json::to_value(progress).unwrap_or(Value::Null),
    );
    Value::Object(obj)
}

/// Merge an `offset` resume cursor into the existing `jobs.json`
/// document **without** publishing a `progress` payload. Use for
/// strategies whose offset isn't a count of processed rows
/// (e.g. an entry_id watermark): persisting it as `processed` would
/// surface as a misleading counter in the UI.
///
/// Same legacy-shape normalisation as [`merge_progress_into_json`].
pub fn merge_offset_into_json(existing: Option<&Value>, offset: u64) -> Value {
    let mut obj = match existing {
        Some(Value::Object(map)) => map.clone(),
        Some(Value::Array(arr)) => {
            let mut m = Map::new();
            m.insert("levels".to_string(), Value::Array(arr.clone()));
            m
        }
        _ => Map::new(),
    };
    obj.insert("offset".to_string(), json!(offset));
    // Drop any stale `progress` from an earlier `report_progress` call —
    // if the strategy has reverted to offset-only mode, the old percent
    // would be misleading.
    obj.remove("progress");
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_counts_derives_percent_when_total_known() {
        let p = JobProgress::from_counts(25, Some(100));
        assert_eq!(p.processed, Some(25));
        assert_eq!(p.total, Some(100));
        assert_eq!(p.percent, Some(25.0));
    }

    #[test]
    fn from_counts_omits_percent_when_total_unknown() {
        let p = JobProgress::from_counts(42, None);
        assert_eq!(p.processed, Some(42));
        assert_eq!(p.total, None);
        assert_eq!(p.percent, None);
    }

    #[test]
    fn from_counts_omits_percent_when_total_zero() {
        // total=0 would divide-by-zero; report processed but no percent.
        let p = JobProgress::from_counts(0, Some(0));
        assert_eq!(p.percent, None);
    }

    #[test]
    fn from_counts_clamps_overflow_to_100_percent() {
        // Defensive: if a strategy somehow reports processed > total
        // (e.g. retry-after-skip races), we cap the bar at 100% rather
        // than render "117 %".
        let p = JobProgress::from_counts(117, Some(100));
        assert_eq!(p.percent, Some(100.0));
    }

    #[test]
    fn merge_into_empty_creates_object_with_offset_and_progress() {
        let progress = JobProgress::from_counts(50, Some(200));
        let merged = merge_progress_into_json(None, &progress);
        let obj = merged.as_object().expect("object");
        assert_eq!(obj.get("offset"), Some(&json!(50)));
        let prog = obj.get("progress").expect("progress").as_object().unwrap();
        assert_eq!(prog.get("processed"), Some(&json!(50)));
        assert_eq!(prog.get("total"), Some(&json!(200)));
        assert!(prog.get("percent").is_some());
    }

    #[test]
    fn merge_preserves_other_keys() {
        // Critical: autoscrape's "levels" key must survive a progress merge.
        let existing = json!({
            "levels": [{"position": 3}, {"current_value": 2024}],
            "some_other_key": "keep me"
        });
        let progress = JobProgress::from_counts(10, Some(40));
        let merged = merge_progress_into_json(Some(&existing), &progress);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj.get("some_other_key"), Some(&json!("keep me")));
        assert_eq!(
            obj.get("levels"),
            Some(&json!([{"position": 3}, {"current_value": 2024}]))
        );
        assert!(obj.get("progress").is_some());
    }

    #[test]
    fn merge_wraps_legacy_autoscrape_array_under_levels_key() {
        // Old autoscrape shape: bare array of level states.
        let legacy = json!([{"position": 0}, {"current_value": 2000}]);
        let progress = JobProgress::from_counts(0, None);
        let merged = merge_progress_into_json(Some(&legacy), &progress);
        let obj = merged.as_object().expect("must become object");
        assert_eq!(
            obj.get("levels"),
            Some(&json!([{"position": 0}, {"current_value": 2000}]))
        );
    }

    #[test]
    fn merge_overwrites_offset_with_new_value() {
        // Existing offset=10 must be replaced by the new processed=42.
        let existing = json!({"offset": 10});
        let progress = JobProgress::from_counts(42, None);
        let merged = merge_progress_into_json(Some(&existing), &progress);
        assert_eq!(merged.get("offset"), Some(&json!(42)));
    }

    #[test]
    fn from_json_returns_none_for_legacy_offset_only() {
        let legacy = json!({"offset": 12345});
        assert_eq!(JobProgress::from_json(&legacy), None);
    }

    #[test]
    fn from_json_returns_none_for_legacy_autoscrape_array() {
        let legacy = json!([{"position": 0}]);
        assert_eq!(JobProgress::from_json(&legacy), None);
    }

    #[test]
    fn from_json_roundtrip() {
        let original = JobProgress::from_counts(123, Some(456));
        let merged = merge_progress_into_json(None, &original);
        let parsed = JobProgress::from_json(&merged).expect("parses back");
        assert_eq!(parsed, original);
    }

    // merge_offset_into_json ────────────────────────────────────────────

    #[test]
    fn merge_offset_writes_offset_only() {
        let merged = merge_offset_into_json(None, 12_345);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj.get("offset"), Some(&json!(12_345)));
        assert!(obj.get("progress").is_none(), "must not publish progress");
    }

    #[test]
    fn merge_offset_strips_stale_progress() {
        // If a prior report_progress wrote `progress`, subsequent
        // offset-only writes should remove it — otherwise the UI keeps
        // showing an out-of-date percent.
        let existing = json!({
            "offset": 100,
            "progress": {"processed": 100, "total": 200, "percent": 50.0}
        });
        let merged = merge_offset_into_json(Some(&existing), 150);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj.get("offset"), Some(&json!(150)));
        assert!(obj.get("progress").is_none());
    }

    #[test]
    fn merge_offset_preserves_other_keys() {
        let existing = json!({"levels": [{"position": 3}]});
        let merged = merge_offset_into_json(Some(&existing), 7);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj.get("levels"), Some(&json!([{"position": 3}])));
        assert_eq!(obj.get("offset"), Some(&json!(7)));
    }

    #[test]
    fn merge_offset_wraps_legacy_autoscrape_array() {
        let legacy = json!([{"position": 0}, {"current_value": 2000}]);
        let merged = merge_offset_into_json(Some(&legacy), 42);
        let obj = merged.as_object().unwrap();
        assert_eq!(
            obj.get("levels"),
            Some(&json!([{"position": 0}, {"current_value": 2000}]))
        );
        assert_eq!(obj.get("offset"), Some(&json!(42)));
    }
}
