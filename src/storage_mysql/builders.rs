//! SQL builder helpers for `StorageMySQL`.
//!
//! Pure (mostly static) functions that turn typed filter / query
//! structs into the SQL strings consumed by the trait impl in
//! `mod.rs`. Pulled out so adding a new clause or filter doesn't
//! require scrolling past 6,000 lines of trait methods.

use super::StorageMySQL;
use super::util::escape_sql_literal;
use crate::entry_query::EntryQuery;
use crate::job_status::JobStatus;
use crate::match_state::MatchState;
use crate::mysql_misc::MySQLMisc;
use crate::storage::{CatalogEntryListFilter, Download2Filter};
use anyhow::{Result, anyhow};
use itertools::Itertools;
use rand::prelude::*;

impl StorageMySQL {
    pub(super) fn coordinate_matcher_main_query_sql(
        catalog_id: &Option<usize>,
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> String {
        let conditions_catalog_id = match catalog_id {
            Some(catalog_id) => format!("`catalog`={catalog_id}"),
            None => Self::coordinate_matcher_main_query_sql_subquery(bad_catalogs, max_results),
        };
        let conditions_not_fully_matched = &MatchState::not_fully_matched().get_sql();
        format!(
            "SELECT `lat`,`lon`,`id`,`catalog`,`ext_name`,`type`,`q` FROM `vw_location` WHERE `ext_name`!='' AND {conditions_catalog_id} {conditions_not_fully_matched}"
        )
    }

    pub(super) fn coordinate_matcher_main_query_sql_subquery(
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> String {
        let r: f64 = rand::rng().random();
        let mut sql = format!("`random`>={r} ORDER BY `random` LIMIT {max_results}");
        if !bad_catalogs.is_empty() {
            let s = bad_catalogs.iter().join(",");
            sql += &format!("AND `catalog` NOT IN ({s})");
        }
        sql
    }

    pub(super) fn jobs_get_next_job_construct_sql(
        status: JobStatus,
        depends_on: Option<JobStatus>,
        no_actions: &[String],
        next_ts: Option<String>,
    ) -> String {
        let mut sql = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}'",
            status.as_str()
        );
        sql += r#" AND NOT EXISTS (SELECT * FROM catalog WHERE catalog.id=jobs.catalog AND active!=1)"#; // No inactive catalogs
        match depends_on {
            Some(other_status) => {
                sql += &format!(
                    " AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}')",
                    other_status.as_str()
                );
            }
            None => match &next_ts {
                Some(ts) => {
                    sql += &format!(" AND `next_ts`!='' AND `next_ts`<='{ts}'");
                }
                None => {
                    sql += " AND `depends_on` IS NULL";
                }
            },
        }
        if !no_actions.is_empty() {
            let actions = no_actions.join("','");
            sql += &format!(" AND `action` NOT IN ('{actions}')");
        }
        if next_ts.is_some() {
            sql += " ORDER BY `next_ts` LIMIT 1";
        } else {
            sql += " ORDER BY `last_ts` LIMIT 1";
        }
        sql
    }

    /// Build the SQL for `api_search_entries`. Pure so we can unit-test the
    /// composition (including escaping of user-supplied search terms) without
    /// a live DB. Returns `None` when nothing useful can be queried
    /// (no words, or all search targets disabled).
    pub(super) fn build_api_search_entries_sql(
        words: &[String],
        description_search: bool,
        no_label_search: bool,
        exclude: &[usize],
        include: &[usize],
        max_results: usize,
    ) -> Option<String> {
        if words.is_empty() {
            return None;
        }
        // Boolean-mode MATCH terms are space-separated (not comma-separated; that was a PHP bug).
        // Escape single quotes and backslashes in each word so user input can't
        // break out of the surrounding single-quoted AGAINST() literal.
        let ft_words = words
            .iter()
            .map(|w| format!("+{}", escape_sql_literal(w)))
            .join(" ");
        let mut conditions: Vec<String> = Vec::new();
        if !no_label_search {
            conditions.push(format!(
                "MATCH(`ext_name`) AGAINST('{ft_words}' IN BOOLEAN MODE)"
            ));
        }
        if description_search {
            conditions.push(format!(
                "MATCH(`ext_desc`) AGAINST('{ft_words}' IN BOOLEAN MODE)"
            ));
        }
        if conditions.is_empty() {
            return None;
        }
        let match_clause = conditions.join(" OR ");
        let mut sql = format!("{} WHERE ({match_clause})", Self::entry_sql_select());
        if !exclude.is_empty() {
            let excl = exclude.iter().join(",");
            sql += &format!(" AND `catalog` NOT IN ({excl})");
        }
        if !include.is_empty() {
            let incl = include.iter().join(",");
            sql += &format!(" AND `catalog` IN ({incl})");
        }
        sql += &format!(" LIMIT {max_results}");
        Some(sql)
    }

    pub(super) fn entry_sql_select() -> String {
        // The `random` column is sometimes NULL (older rows). For ordering
        // purposes we coalesce to a fresh `rand()` so NULL entries don't all
        // sort to the same spot. Earlier this was aliased back to `random`,
        // but that name shadows the column and made the optimiser evaluate
        // the IF for every row in `WHERE random>=…` queries — defeating
        // the `random_2` index and turning the global random pick into a
        // 30s+ scan. Use a different alias and read it back explicitly.
        r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,if(isnull(random),rand(),random) as random_v,`type` FROM `entry`".into()
    }

    /// Build the WHERE-clause body for `api_get_catalog_entries`.
    ///
    /// Note: text filters (`entry_type`, `title_match`, `keyword`) are
    /// interpolated with single-quote doubling. The MySQL `exec_iter` API does
    /// not accept positional placeholders inside `LIKE '%...%'` without
    /// restructuring every call site, so we use PHP-era escaping here — the
    /// inputs reach us via user-supplied query strings, so the escape keeps
    /// pre-existing behaviour. Numeric inputs are `format!`-ed after parsing
    /// to integers, so they cannot inject.
    pub(super) fn catalog_entries_where_clause(filter: &CatalogEntryListFilter) -> String {
        let mut conds = vec![format!("catalog={}", filter.catalog_id)];
        conds.extend(Self::match_state_conds(filter));
        conds.extend(Self::text_filter_conds(filter));
        if let Some(c) = Self::user_id_cond(filter.user_id) {
            conds.push(c);
        }
        conds.join(" AND ")
    }

    /// Conditions selecting entries by their match state (Q-value/user
    /// flags). Returns the fast-path single-condition form when exactly one
    /// of `show_noq`/`show_na`/`show_nowd` is set, since that is the common
    /// case (e.g. "Unmatched only") and the simpler `q IS NULL` /
    /// `q = 0` / `q = -1` lets the MySQL optimiser pick an index on
    /// `(catalog, q)` if one is present.
    fn match_state_conds(filter: &CatalogEntryListFilter) -> Vec<String> {
        if filter.show_multiple {
            return vec![
                "EXISTS (SELECT 1 FROM multi_match WHERE entry_id=entry.id) AND (user<=0 OR user is null)"
                    .into(),
            ];
        }
        if let Some(fast) = Self::match_state_fast_path(filter) {
            return vec![fast.into()];
        }
        let mut out = Vec::new();
        if !filter.show_noq {
            out.push("q IS NOT NULL".into());
        }
        if !filter.show_autoq {
            out.push("(q is null OR user!=0)".into());
        }
        if !filter.show_userq {
            out.push("(user<=0 OR user is null)".into());
        }
        if !filter.show_na {
            out.push("(q!=0 or q is null)".into());
        }
        out
    }

    /// Returns the single-condition fast-path SQL when the filter is one of
    /// the three "exclusive" listings (only show_noq, only show_na, or only
    /// show_nowd) and nothing else is selected. `None` means the general
    /// per-flag conjunction is needed.
    fn match_state_fast_path(filter: &CatalogEntryListFilter) -> Option<&'static str> {
        match (
            filter.show_noq,
            filter.show_autoq,
            filter.show_userq,
            filter.show_na,
            filter.show_nowd,
        ) {
            (true, false, false, false, false) => Some("q IS NULL"),
            (false, false, false, true, false) => Some("q=0"),
            (false, false, false, false, true) => Some("q=-1"),
            _ => None,
        }
    }

    fn text_filter_conds(filter: &CatalogEntryListFilter) -> Vec<String> {
        let mut out = Vec::new();
        if !filter.entry_type.is_empty() {
            out.push(format!(
                "`type`='{}'",
                filter.entry_type.replace('\'', "''")
            ));
        }
        if !filter.title_match.is_empty() {
            out.push(format!(
                "`ext_name` LIKE '%{}%'",
                filter.title_match.replace('\'', "''")
            ));
        }
        if !filter.keyword.is_empty() {
            let kw = filter.keyword.replace('\'', "''");
            out.push(format!(
                "(`ext_name` LIKE '%{kw}%' OR `ext_desc` LIKE '%{kw}%')"
            ));
        }
        out
    }

    fn user_id_cond(user_id: Option<i64>) -> Option<String> {
        match user_id {
            Some(uid) if uid > 0 => Some(format!("`user`={uid}")),
            Some(0) => Some("`user`=0".into()),
            _ => None,
        }
    }

    /// The column names emitted by `build_download2_sql`, in the exact
    /// order they appear in the SELECT list. Kept next to the SQL builder
    /// so the two must be edited together — having them drift apart is
    /// exactly what caused the TSV columns-vs-header misalignment.
    pub(super) fn download2_columns(filter: &Download2Filter) -> Vec<String> {
        let mut cols: Vec<String> = vec!["entry_id".into(), "catalog".into(), "external_id".into()];
        if filter.include_ext_url {
            cols.extend([
                "external_url".to_string(),
                "name".to_string(),
                "description".to_string(),
                "entry_type".to_string(),
                "mnm_user_id".to_string(),
            ]);
        }
        cols.push("q".into());
        cols.push("matched_on".into());
        if filter.include_username {
            cols.push("matched_by_username".into());
        }
        if filter.include_dates {
            cols.push("born".into());
            cols.push("died".into());
        }
        if filter.include_location {
            cols.push("lat".into());
            cols.push("lon".into());
        }
        cols
    }

    /// Build the full SELECT for `api_download2`. All conditional joins,
    /// columns, and row filters are derived from the typed filter — callers
    /// never see raw SQL.
    pub(super) fn build_download2_sql(filter: &Download2Filter) -> String {
        // Defensive re-filter: the API layer already strips non-digits/commas,
        // but the backend must not trust that.
        let catalogs: String = filter
            .catalogs
            .chars()
            .filter(|c| c.is_ascii_digit() || *c == ',')
            .collect();

        let mut sql = "SELECT entry.id AS entry_id,entry.catalog,ext_id AS external_id".to_string();
        if filter.include_ext_url {
            sql.push_str(
                ",ext_url AS external_url,ext_name AS `name`,ext_desc AS description,`type` AS entry_type,entry.user AS mnm_user_id",
            );
        }
        sql.push_str(
            ",(CASE WHEN q IS NULL THEN NULL else concat('Q',q) END) AS q,`timestamp` AS matched_on",
        );
        if filter.include_username {
            sql.push_str(",user.name AS matched_by_username");
        }
        if filter.include_dates {
            sql.push_str(",person_dates.born,person_dates.died");
        }
        if filter.include_location {
            sql.push_str(",location.lat,location.lon");
        }

        sql.push_str(" FROM entry");
        if filter.include_dates {
            sql.push_str(" LEFT JOIN person_dates ON (entry.id=person_dates.entry_id)");
        }
        if filter.include_location {
            sql.push_str(" LEFT JOIN location ON (entry.id=location.entry_id)");
        }
        if filter.include_username {
            sql.push_str(" LEFT JOIN user ON (entry.user=user.id)");
        }

        sql.push_str(&format!(" WHERE entry.catalog IN ({catalogs})"));
        if filter.hide_any_matched {
            sql.push_str(" AND entry.q IS NULL");
        }
        if filter.hide_firmly_matched {
            sql.push_str(" AND (entry.q IS NULL OR entry.user=0)");
        }
        if filter.hide_user_matched {
            sql.push_str(" AND (entry.user IS NULL OR entry.user IN (0,3,4))");
        }
        if filter.hide_unmatched {
            sql.push_str(" AND entry.q IS NOT NULL");
        }
        if filter.hide_no_multiple {
            sql.push_str(
                " AND NOT EXISTS (SELECT 1 FROM multi_match WHERE entry.id=multi_match.entry_id)",
            );
        }
        if filter.hide_name_date_matched {
            sql.push_str(" AND entry.user!=3");
        }
        if filter.hide_automatched {
            sql.push_str(" AND entry.user!=0");
        }
        if filter.hide_aux_matched {
            sql.push_str(" AND entry.user!=4");
        }
        sql.push_str(&format!(" LIMIT {} OFFSET {}", filter.limit, filter.offset));
        sql
    }

    pub(super) fn get_entry_query_sql(query: &EntryQuery) -> Result<(String, Vec<String>)> {
        let (sql, parts) =
            Self::get_entry_query_sql_where(query, Self::entry_sql_select(), vec![])?;
        Ok((sql, parts))
    }

    pub(super) fn get_entry_query_sql_where(
        query: &EntryQuery,
        mut sql: String,
        mut parts: Vec<String>,
    ) -> Result<(String, Vec<String>)> {
        // Paranoia
        if query.catalog_id.is_none() && query.ext_ids.is_some() {
            return Err(anyhow!("Catalog ID is required when using external IDs"));
        }

        if !sql.trim().ends_with(" WHERE") {
            sql += " WHERE";
        }

        if let Some(catalog_id) = query.catalog_id {
            sql += &format!(" catalog={catalog_id}");
        }
        if let Some(entry_type) = &query.entry_type {
            parts.push(entry_type.to_string());
            sql += " AND `type`=?";
        }
        if let Some(name_regexp) = &query.name_regexp {
            parts.push(name_regexp.to_string());
            sql += " AND `ext_name` RLIKE ?";
        }
        if let Some(ext_ids) = &query.ext_ids {
            let placeholders = Self::sql_placeholders(ext_ids.len());
            sql += &format!(" AND `ext_id` IN ({placeholders})");
            parts.extend(ext_ids.clone());
        }
        if let Some(num_dates) = query.min_dates {
            if num_dates == 0 {
                // Skip, same as None but might be more convenient for the caller
            } else if num_dates == 1 {
                sql += " AND EXISTS (SELECT * FROM `person_dates` WHERE entry_id=entry.id AND (year_born!='' OR year_died!=''))";
            } else if num_dates == 2 {
                sql += " AND EXISTS (SELECT * FROM `person_dates` WHERE entry_id=entry.id AND (year_born!='' AND year_died!=''))";
            } else {
                return Err(anyhow!(
                    "Invalid number of dates, should be 1 or 2 but id {num_dates}"
                ));
            }
        }
        if let Some(num_aux) = query.min_aux {
            sql += &format!(
                " AND (SELECT count(*) FROM auxiliary WHERE entry_id=entry.id)>={num_aux}"
            );
        }
        if let Some(match_state) = query.match_state {
            sql += " ";
            sql += &match_state.get_sql();
        }
        if query.has_description {
            sql += " AND ext_desc!=''";
        }
        if query.has_coordinates {
            sql += " AND EXISTS (SELECT * FROM `location` WHERE entry_id=entry.id)";
        }
        if let Some(desc_hint) = &query.desc_hint {
            sql += " AND ext_desc LIKE ?";
            parts.push(format!("%{desc_hint}%"));
        }
        if let Some(limit) = query.limit {
            sql += &format!(" LIMIT {limit}");
        }
        if let Some(offset) = query.offset {
            sql += &format!(" OFFSET {offset}");
        }
        Ok((sql, parts))
    }
}
