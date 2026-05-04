//! Row → typed-struct decoders for `StorageMySQL`.
//!
//! Pulled out of `mod.rs` so the trait impl doesn't have to sit
//! alongside the per-row column-name plumbing.

use super::StorageMySQL;
use crate::catalog::Catalog;
use crate::coordinates::LocationRow;
use crate::entry::Entry;
use mysql_async::Row;
use serde_json::json;

fn get_str(row: &Row, col: &str) -> String {
    row.get::<Option<String>, _>(col).flatten().unwrap_or_default()
}

fn get_opt_usize(row: &Row, col: &str) -> Option<usize> {
    row.get::<Option<usize>, _>(col).flatten()
}

fn get_u8(row: &Row, col: &str) -> u8 {
    row.get::<Option<u8>, _>(col).flatten().unwrap_or(0)
}

fn get_isize(row: &Row, col: &str) -> isize {
    row.get::<Option<isize>, _>(col).flatten().unwrap_or(0)
}

impl StorageMySQL {
    pub(super) fn location_row_from_row(row: &Row) -> Option<LocationRow> {
        Some(LocationRow {
            lat: row.get("lat")?,
            lon: row.get("lon")?,
            entry_id: row.get("id")?,
            catalog_id: row.get("catalog")?,
            ext_name: row.get("ext_name")?,
            entry_type: row.get("type")?,
            q: row.get("q")?,
        })
    }

    pub(super) fn catalog_from_row(row: &Row) -> Option<Catalog> {
        Catalog::from_mysql_row(row)
    }

    pub(super) fn entry_from_row(row: &Row) -> Option<Entry> {
        // Read by column name — positional reads break whenever a SELECT
        // prepends extra columns (e.g. `cnt`, `property`, `source_entry_id`)
        // before `entry.*`, which several API endpoints do.
        Some(Entry {
            id: row.get("id")?,
            catalog: row.get("catalog")?,
            ext_id: row.get("ext_id")?,
            ext_url: row.get("ext_url")?,
            ext_name: row.get("ext_name")?,
            ext_desc: row.get("ext_desc")?,
            q: Entry::value2opt_isize(row.get("q")?).ok()?,
            user: Entry::value2opt_usize(row.get("user")?).ok()?,
            timestamp: Entry::value2opt_string(row.get("timestamp")?).ok()?,
            // `random` is nullable in the entry table; reading it as f64
            // panics on Null, so pull through Option<f64> first. The SELECT
            // exposes the coalesced value as `random_v` to avoid shadowing
            // the column name in WHERE clauses; fall back to `random` for
            // callers that don't go through `entry_sql_select` (e.g. raw
            // `SELECT *`).
            random: row
                .get::<Option<f64>, _>("random_v")
                .or_else(|| row.get::<Option<f64>, _>("random"))
                .flatten()
                .unwrap_or(0.0),
            type_name: Entry::value2opt_string(row.get("type")?).ok()?,
        })
    }

    pub(super) fn overview_row_to_json(row: Row) -> serde_json::Value {
        // Catalog fields — every column is Option<T> to survive NULLs.
        let mut out = json!({
            "id":              get_opt_usize(&row, "c_id").unwrap_or(0),
            "name":            get_str(&row, "c_name"),
            "url":             get_str(&row, "c_url"),
            "desc":            get_str(&row, "c_desc"),
            "type":            get_str(&row, "c_type"),
            "wd_prop":         get_opt_usize(&row, "c_wd_prop"),
            "wd_qual":         get_opt_usize(&row, "c_wd_qual"),
            "search_wp":       get_str(&row, "c_search_wp"),
            "active":          get_u8(&row, "c_active"),
            "owner":           get_opt_usize(&row, "c_owner"),
            "note":            get_str(&row, "c_note"),
            "source_item":     get_opt_usize(&row, "c_source_item"),
            "has_person_date": get_str(&row, "c_has_person_date"),
            "taxon_run":       get_u8(&row, "c_taxon_run"),
        });

        // Username — only surface when the owner FK actually resolved,
        // matching the prior INNER-JOIN-based behaviour (missing user =
        // no `username` key in the JSON at all). Gate on the user PK
        // rather than `name`, so a user row with a NULL/empty name is
        // still treated as "the join succeeded".
        if get_opt_usize(&row, "u_id").is_some() {
            out["username"] = json!(get_str(&row, "u_name"));
        }

        // Overview fields — merged iff the overview row existed. Gate on
        // `overview.catalog` (the PK, NOT NULL), not on any of the count
        // columns, so a legitimately zero `total` doesn't look like a miss.
        if get_opt_usize(&row, "o_catalog").is_some() {
            out["total"]       = json!(get_isize(&row, "o_total"));
            out["noq"]         = json!(get_isize(&row, "o_noq"));
            out["autoq"]       = json!(get_isize(&row, "o_autoq"));
            out["na"]          = json!(get_isize(&row, "o_na"));
            out["manual"]      = json!(get_isize(&row, "o_manual"));
            out["nowd"]        = json!(get_isize(&row, "o_nowd"));
            out["multi_match"] = json!(get_isize(&row, "o_multi_match"));
            out["types"]       = json!(get_str(&row, "o_types"));
        }

        // Autoscrape fields — merged iff the autoscrape row existed.
        // Gate on `autoscrape.catalog` (the PK) so a NULL last_update
        // doesn't suppress an otherwise-present row.
        if get_opt_usize(&row, "a_catalog").is_some() {
            out["last_update"]     = json!(get_str(&row, "a_last_update"));
            out["do_auto_update"]  = json!(get_u8(&row, "a_do_auto_update"));
            out["autoscrape_json"] = json!(get_str(&row, "a_json"));
        }

        out
    }
}
