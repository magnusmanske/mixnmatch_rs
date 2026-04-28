//! Free helper functions and constants shared across the StorageMySQL
//! impl. Pulled out so the trait impl in `mod.rs` doesn't have to live
//! next to row-decoding helpers and SQL escape utilities.

use mysql_async::Row;
use serde_json::{Value, json};

pub(super) const TABLES_WITH_ENTRY_ID_FIELDS: &[&str] = &[
    "aliases",
    "descriptions",
    "auxiliary",
    "issues",
    "kv_entry",
    "mnm_relation",
    "multi_match",
    "person_dates",
    "location",
    "log",
    "entry_creation",
    "entry2given_name",
    "statement_text",
];

/// Catalogs to skip in `maintenance_common_names_birth_year` — large or
/// otherwise noisy catalogs where name+birth-year collisions aren't a
/// useful matching signal and would swamp the aggregation.
pub(super) const NAME_BIRTH_YEAR_EXCLUDED_CATALOGS: &[usize] =
    &[4837, 5580, 6094, 3247, 7480, 7562];

/// Escape a string so it's safe to embed between single quotes in a MySQL
/// statement built with `format!` (for the narrow set of cases where the
/// `exec_iter` interface can't take a bound parameter — e.g. arguments to
/// MATCH AGAINST or LIKE patterns). Escapes both `'` (via doubling) and
/// backslash (via doubling) so `\'` tricks can't slip through.
pub(super) fn escape_sql_literal(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// Coerce a Wikidata property-number field (`wd_prop` / `wd_qual`) to NULL
/// when the incoming value is None or zero. Callers elsewhere in the
/// codebase use `wd_prop IS NOT NULL` or `wd_prop > 0` interchangeably to
/// gate "catalog has a property" logic — a stored `0` muddles both, and
/// (having been observed in the wild on catalogs created via scrapers)
/// needs to be filtered at every write path, not just at read time.
pub(super) fn normalize_wd_prop(v: Option<usize>) -> Option<usize> {
    v.filter(|n| *n > 0)
}

/// Convert a MySQL Row to a JSON object, preserving column names and basic types.
pub(super) fn row_to_json(row: Row) -> Value {
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns_ref().iter().enumerate() {
        let name = col.name_str().to_string();
        let val = match &row[i] {
            mysql_async::Value::NULL => Value::Null,
            mysql_async::Value::Int(n) => json!(*n),
            mysql_async::Value::UInt(n) => json!(*n),
            mysql_async::Value::Float(n) => json!(*n),
            mysql_async::Value::Double(n) => json!(*n),
            mysql_async::Value::Bytes(b) => json!(String::from_utf8_lossy(b).to_string()),
            other => json!(format!("{other:?}")),
        };
        obj.insert(name, val);
    }
    Value::Object(obj)
}
