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

    // #lizard forgives
    pub(super) fn catalog_from_row(row: &Row) -> Option<Catalog> {
        Catalog::from_mysql_row(row)
    }

    // #lizard forgives
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
        let id: usize = row.get::<Option<usize>, _>("c_id").flatten().unwrap_or(0);
        let name: String = row
            .get::<Option<String>, _>("c_name")
            .flatten()
            .unwrap_or_default();
        let url: String = row
            .get::<Option<String>, _>("c_url")
            .flatten()
            .unwrap_or_default();
        let desc: String = row
            .get::<Option<String>, _>("c_desc")
            .flatten()
            .unwrap_or_default();
        let type_name: String = row
            .get::<Option<String>, _>("c_type")
            .flatten()
            .unwrap_or_default();
        let wd_prop: Option<usize> = row.get::<Option<usize>, _>("c_wd_prop").flatten();
        let wd_qual: Option<usize> = row.get::<Option<usize>, _>("c_wd_qual").flatten();
        let search_wp: String = row
            .get::<Option<String>, _>("c_search_wp")
            .flatten()
            .unwrap_or_default();
        let active: u8 = row.get::<Option<u8>, _>("c_active").flatten().unwrap_or(0);
        let owner: Option<usize> = row.get::<Option<usize>, _>("c_owner").flatten();
        let note: String = row
            .get::<Option<String>, _>("c_note")
            .flatten()
            .unwrap_or_default();
        let source_item: Option<usize> = row.get::<Option<usize>, _>("c_source_item").flatten();
        let has_person_date: String = row
            .get::<Option<String>, _>("c_has_person_date")
            .flatten()
            .unwrap_or_default();
        let taxon_run: u8 = row
            .get::<Option<u8>, _>("c_taxon_run")
            .flatten()
            .unwrap_or(0);

        let mut out = json!({
            "id": id, "name": name, "url": url, "desc": desc, "type": type_name,
            "wd_prop": wd_prop, "wd_qual": wd_qual, "search_wp": search_wp,
            "active": active, "owner": owner, "note": note,
            "source_item": source_item, "has_person_date": has_person_date,
            "taxon_run": taxon_run,
        });

        // Username — only surface when the owner FK actually resolved,
        // matching the prior INNER-JOIN-based behaviour (missing user =
        // no `username` key in the JSON at all). Gate on the user PK
        // rather than `name`, so a user row with a NULL/empty name is
        // still treated as "the join succeeded".
        let user_joined = row.get::<Option<usize>, _>("u_id").flatten().is_some();
        if user_joined {
            let username: String = row
                .get::<Option<String>, _>("u_name")
                .flatten()
                .unwrap_or_default();
            out["username"] = json!(username);
        }

        // Overview fields — merged iff the overview row existed. Gate on
        // `overview.catalog` (the PK, NOT NULL), not on any of the count
        // columns, so a legitimately zero `total` doesn't look like a miss.
        let overview_joined = row.get::<Option<usize>, _>("o_catalog").flatten().is_some();
        if overview_joined {
            out["total"] = json!(
                row.get::<Option<isize>, _>("o_total")
                    .flatten()
                    .unwrap_or(0)
            );
            out["noq"] = json!(row.get::<Option<isize>, _>("o_noq").flatten().unwrap_or(0));
            out["autoq"] = json!(
                row.get::<Option<isize>, _>("o_autoq")
                    .flatten()
                    .unwrap_or(0)
            );
            out["na"] = json!(row.get::<Option<isize>, _>("o_na").flatten().unwrap_or(0));
            out["manual"] = json!(
                row.get::<Option<isize>, _>("o_manual")
                    .flatten()
                    .unwrap_or(0)
            );
            out["nowd"] = json!(row.get::<Option<isize>, _>("o_nowd").flatten().unwrap_or(0));
            out["multi_match"] = json!(
                row.get::<Option<isize>, _>("o_multi_match")
                    .flatten()
                    .unwrap_or(0)
            );
            out["types"] = json!(
                row.get::<Option<String>, _>("o_types")
                    .flatten()
                    .unwrap_or_default()
            );
        }

        // Autoscrape fields — merged iff the autoscrape row existed.
        // Gate on `autoscrape.catalog` (the PK) so a NULL last_update
        // doesn't suppress an otherwise-present row.
        let autoscrape_joined = row.get::<Option<usize>, _>("a_catalog").flatten().is_some();
        if autoscrape_joined {
            out["last_update"] = json!(
                row.get::<Option<String>, _>("a_last_update")
                    .flatten()
                    .unwrap_or_default()
            );
            out["do_auto_update"] = json!(
                row.get::<Option<u8>, _>("a_do_auto_update")
                    .flatten()
                    .unwrap_or(0)
            );
            out["autoscrape_json"] = json!(
                row.get::<Option<String>, _>("a_json")
                    .flatten()
                    .unwrap_or_default()
            );
        }

        out
    }
}
