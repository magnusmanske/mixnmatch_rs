pub use crate::storage::Storage;

mod autoscrape_queries;
mod builders;
mod cersei_queries;
mod coordinate_matcher_queries;
mod issue_queries;
mod job_queries;
mod row_mappers;
mod taxon_queries;
mod util;

use crate::{
    ItemId,
    app_state::USER_AUTO,
    auxiliary_data::AuxiliaryRow,
    auxiliary_matcher::AuxiliaryResults,
    catalog::Catalog,
    coordinates::CoordinateLocation,
    entry::{Entry, EntryError},
    entry_query::EntryQuery,
    match_state::MatchState,
    meta_entry::{MetaIssue, MetaKvEntry, MetaLogEntry, MetaMnmRelation, MetaStatementText},
    mnm_link::MnmLink,
    mysql_misc::MySQLMisc,
    prop_todo::PropTodo,
    storage::{
        AutomatchSearchRow, AutoscrapeQueries, CandidateDatesRow, CatalogEntryListFilter,
        DescriptionAuxRule, Download2Filter, EXT_URL_UNIQUE_SEPARATOR, GroupedEntry,
        MergeableMatch, OverviewTableRow, PersonDateMatchRow, PropertyCacheRow,
        ResultInOriginalCatalog, ResultInOtherCatalog, WdMatchRow,
    },
    taxon_matcher::TaxonNameField,
    update_catalog::UpdateInfo,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::Params::Empty;
use mysql_async::{Params, Row, from_row, futures::GetConn, prelude::*};
use rand::prelude::*;
use serde_json::{Value, json};
use std::collections::HashMap;
use util::{
    NAME_BIRTH_YEAR_EXCLUDED_CATALOGS, TABLES_WITH_ENTRY_ID_FIELDS, escape_sql_literal,
    normalize_wd_prop, row_to_json,
};
use wikimisc::{timestamp::TimeStamp, wikibase::LocaleString};

#[derive(Debug)]
pub struct StorageMySQL {
    pool: mysql_async::Pool,
    pool_ro: mysql_async::Pool,
}

impl MySQLMisc for StorageMySQL {
    fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }
}

impl StorageMySQL {
    pub fn new(j: &Value, j_ro: &Value) -> Self {
        Self {
            pool: Self::create_pool(j),
            pool_ro: Self::create_pool(j_ro),
        }
    }

    /// `pub(super)` so per-trait impl blocks living in sibling submodules
    /// (e.g. `storage_mysql::issue_queries`) can borrow a writable
    /// connection without re-implementing the pool plumbing.
    pub(super) fn get_conn(&self) -> GetConn {
        self.pool.get_conn()
    }

    /// Read-only counterpart of `get_conn`. Same `pub(super)` rationale.
    pub(super) fn get_conn_ro(&self) -> GetConn {
        self.pool_ro.get_conn()
    }

    async fn entry_set_match_cleanup(
        &self,
        entry: &Entry,
        user_id: usize,
        q_numeric: isize,
    ) -> Result<bool> {
        // Update overview table and misc cleanup
        let entry_id = entry.get_valid_id()?;
        self.update_overview_table(entry, Some(user_id), Some(q_numeric))
            .await?;
        let is_full_match = user_id > 0 && q_numeric > 0;
        let is_matched = if is_full_match { 1 } else { 0 };
        self.entry_set_match_status(entry_id, "UNKNOWN", is_matched)
            .await?;
        if user_id != USER_AUTO {
            self.entry_remove_multi_match(entry_id).await?;
        }
        self.queue_reference_fixer(q_numeric).await?;
        Ok(true)
    }

    pub(super) async fn match_taxa_get_ranked_names_batch_get_results(
        &self,
        ranks: &[&str],
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<(usize, String, String)>> {
        let taxon_name_column = field.as_str();
        let sql = format!(
            "SELECT `id`,`{taxon_name_column}` AS taxon_name,`type` FROM `entry`
            	WHERE `catalog` IN ({catalog_id})
             	AND (`q` IS NULL OR `user`=0)
              	AND `type` IN ('{}')
            	LIMIT {batch_size} OFFSET {offset}",
            ranks.join("','")
        );
        let results = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, Empty)
            .await?
            .map_and_drop(from_row::<(usize, String, String)>)
            .await?;
        Ok(results)
    }
}

// STORAGE TRAIT IMPLEMENTATION

#[async_trait]
impl Storage for StorageMySQL {
    async fn disconnect(&self) -> Result<()> {
        self.disconnect_db().await?;
        Ok(())
    }

    async fn get_user_name_from_id(&self, user_id: usize) -> Option<String> {
        let sql = format!("SELECT name FROM user WHERE id = {user_id}");
        self.get_conn_ro()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<String>)
            .await
            .ok()?
            .first()
            .cloned()
    }

    async fn entry_query(&self, query: &EntryQuery) -> Result<Vec<Entry>> {
        let (sql, parts) = Self::get_entry_query_sql(query)?;
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, parts)
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        Ok(ret)
    }

    async fn get_entry_ids_by_aux(&self, prop_numeric: usize, value: &str) -> Result<Vec<usize>> {
        let sql = r"SELECT DISTINCT entry_id from auxiliary where aux_p=:prop_numeric and aux_name=:value
        	UNION
         	SELECT DISTINCT entry.id FROM entry,catalog WHERE active=1 AND wd_prop=:prop_numeric AND wd_qual IS NULL
          	AND entry.catalog=catalog.id AND ext_id=:value";
        let mut conn = self.get_conn_ro().await?;
        let mut results = conn
            .exec_iter(sql, params! {prop_numeric, value})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        results.sort();
        results.dedup();
        Ok(results)
    }

    // Taxon-matcher methods now live on a separate `impl
    // TaxonQueries for StorageMySQL` block (further down).

    async fn catalog_get_entries_of_people_with_initials(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<Entry>> {
        let query = EntryQuery::default()
            .with_catalog_id(catalog_id)
            .with_type("Q5")
            .with_match_state(MatchState::unmatched())
            .with_name_regexp("\\. ");
        self.entry_query(&query).await
    }

    // Coordinate Matcher methods now live on a separate `impl
    // CoordinateMatcherQueries for StorageMySQL` block (further down)
    // so the trait can be depended on independently of `Storage`.

    // Data source

    async fn get_data_source_type_for_uuid(&self, uuid: &str) -> Result<Vec<String>> {
        let results = "SELECT `type` FROM `import_file` WHERE `uuid`=:uuid"
            .with(params! {uuid})
            .map(self.get_conn().await?, |type_name| type_name)
            .await?;
        Ok(results)
    }

    async fn get_import_file_info(&self, uuid: &str) -> Result<Option<(String, usize)>> {
        let results: Vec<(String, usize)> =
            "SELECT `type`, `user` FROM `import_file` WHERE `uuid`=:uuid"
                .with(params! {uuid})
                .map(
                    self.get_conn().await?,
                    |(file_type, user): (String, usize)| (file_type, user),
                )
                .await?;
        Ok(results.into_iter().next())
    }

    async fn save_import_file(&self, uuid: &str, file_type: &str, user_id: usize) -> Result<()> {
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
        let mut conn = self.get_conn().await?;
        conn.exec_drop(
            "INSERT INTO `import_file` (`uuid`,`user`,`timestamp`,`type`) VALUES (:uuid,:user,:timestamp,:file_type)",
            params! {uuid, "user" => user_id, timestamp, file_type},
        )
        .await?;
        Ok(())
    }

    async fn save_scraper(&self, catalog_id: usize, json: &str, owner: usize) -> Result<()> {
        // `autoscrape.catalog` is UNIQUE, so a straight INSERT ... ON DUPLICATE KEY UPDATE
        // gives us upsert semantics without a separate round-trip. Using
        // VALUES(col) in the UPDATE clause avoids binding the same named
        // parameter twice.
        let mut conn = self.get_conn().await?;
        conn.exec_drop(
            "INSERT INTO `autoscrape` (`catalog`,`json`,`status`,`owner`,`notes`) VALUES (:catalog_id,:json,'',:owner,'') \
             ON DUPLICATE KEY UPDATE `json`=VALUES(`json`), `owner`=VALUES(`owner`)",
            params! {catalog_id, json, owner},
        )
        .await?;
        Ok(())
    }

    async fn create_catalog_from_meta(
        &self,
        name: &str,
        desc: &str,
        url: &str,
        type_name: &str,
        wd_prop: Option<usize>,
        owner: usize,
    ) -> Result<usize> {
        let mut conn = self.get_conn().await?;
        // `name` is UNIQUE in the catalog table, so a duplicate wizard submission
        // would silently return the existing id rather than erroring.
        // Normalise wd_prop so 0 / Some(0) are stored as NULL — other queries
        // elsewhere use `wd_prop IS NOT NULL` as the "this catalog has a
        // property" sentinel, and a zero slipping through would masquerade
        // as a real property there.
        let wd_prop = normalize_wd_prop(wd_prop);
        conn.exec_drop(
            "INSERT INTO `catalog` (`name`,`url`,`desc`,`type`,`wd_prop`,`active`,`owner`,`note`,`has_person_date`) \
             VALUES (:name,:url,:desc,:type_name,:wd_prop,1,:owner,'','no')",
            params! {name, url, desc, type_name, wd_prop, owner},
        )
        .await?;
        let id: Option<u64> = conn.last_insert_id();
        let id = id.unwrap_or(0) as usize;
        if id > 0 {
            return Ok(id);
        }
        // Already existed — look it up by name.
        let rows: Vec<usize> = conn
            .exec_iter(
                "SELECT `id` FROM `catalog` WHERE `name`=:name LIMIT 1",
                params! {name},
            )
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow!("catalog insert returned no id and no existing row found"))
    }

    async fn get_existing_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<String>> {
        let existing_ext_ids = self
            .get_entry_ids_for_ext_ids(catalog_id, ext_ids)
            .await?
            .into_iter()
            .map(|(ext_id, _entry_id)| ext_id)
            .collect();
        Ok(existing_ext_ids)
    }

    async fn update_catalog_get_update_info(&self, catalog_id: usize) -> Result<Vec<UpdateInfo>> {
        let results = "SELECT id, catalog, json, note, user_id, is_current FROM `update_info` WHERE `catalog`=:catalog_id AND `is_current`=1 LIMIT 1"
            .with(params!{catalog_id})
            .map(self.get_conn().await?,
                |(id, catalog, json, note, user_id, is_current)|{
                UpdateInfo{id, catalog, json, note, user_id, is_current}
            })
            .await?;
        Ok(results)
    }

    async fn update_catalog_set_update_info(
        &self,
        catalog_id: usize,
        json: &str,
        user_id: usize,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;
        // Mark any existing current row as stale so get_update_info only sees
        // the latest configuration.
        conn.exec_drop(
            "UPDATE `update_info` SET `is_current`=0 WHERE `catalog`=:catalog_id AND `is_current`=1",
            params! {catalog_id},
        )
        .await?;
        conn.exec_drop(
            "INSERT INTO `update_info` (`catalog`,`json`,`note`,`user_id`,`is_current`) VALUES (:catalog_id,:json,'',:user_id,1)",
            params! {catalog_id, json, "user_id" => user_id},
        )
        .await?;
        Ok(())
    }

    // Catalog

    async fn create_catalog(&self, catalog: &Catalog) -> Result<usize> {
        if catalog.id().is_some() {
            return Err(anyhow!("Catalog ID is not blank"));
        }

        let mut conn = self.get_conn().await?;
        let sql = r"INSERT IGNORE INTO `catalog` (`name`, `url`, `desc`, `type`, `wd_prop`, `wd_qual`, `search_wp`,`active`,`owner`,`note`,`source_item`,`has_person_date`,`taxon_run`)
        	VALUES (:name, :url, :desc, :type_name, :wd_prop, :wd_qual, :search_wp, :active, :owner, :note, :source_item, :has_person_date, :taxon_run)";
        let name = catalog.name();
        let url = catalog.url();
        let desc = catalog.desc();
        let type_name = catalog.type_name();
        // Normalise to NULL when 0 / None — see `normalize_wd_prop` for why
        // a stored 0 would break `wd_prop IS NOT NULL`-style callers.
        let wd_prop = normalize_wd_prop(catalog.wd_prop());
        let wd_qual = catalog.wd_qual();
        let search_wp = catalog.search_wp();
        let active = catalog.is_active();
        let owner = catalog.owner();
        let note = catalog.note();
        let source_item = catalog.source_item();
        let has_person_date = catalog.has_person_date();
        let taxon_run = catalog.taxon_run();
        conn.exec_drop(
            sql,
            params! {
                name,
                url,
                desc,
                type_name,
                wd_prop,
                wd_qual,
                search_wp,
                active,
                owner,
                note,
                source_item,
                has_person_date,
                taxon_run,
            },
        )
        .await?;
        let id = conn
            .last_insert_id()
            .map(|id| id as usize)
            .ok_or_else(|| anyhow!("Could not insert catalog"))?;
        Ok(id)
    }

    /// Get all external IDs for a catalog (ext_id => entry_id)
    async fn get_all_external_ids(&self, catalog_id: usize) -> Result<HashMap<String, usize>> {
        let eq = EntryQuery::default().with_catalog_id(catalog_id);
        let sql = "SELECT ext_id,id FROM entry WHERE".to_string();
        let (sql, parts) = Self::get_entry_query_sql_where(&eq, sql, vec![])?;
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, parts)
            .await?
            .map_and_drop(from_row::<(String, usize)>)
            .await?
            .into_iter()
            .collect();
        Ok(ret)
    }

    /// This deletes a catalog and all its associated entries.
    /// USE WITH GREAT CARE!
    async fn delete_catalog(&self, catalog_id: usize) -> Result<()> {
        const TABLES_CATALOG_ID: &[&str] = &[
            "code_fragments",
            "autoscrape",
            "jobs",
            "overview",
            "wd_matches",
            "update_info",
            "catalog_default_statement",
            "entry",
        ];
        const TABLES_ENTRY_ID: &[&str] = &[
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
        const BATCH_SIZE: usize = 10000;

        // Delete entry-associated data
        loop {
            let eq = EntryQuery::default()
                .with_catalog_id(catalog_id)
                .with_limit(BATCH_SIZE);
            let entry_ids = self
                .entry_query(&eq)
                .await?
                .iter()
                .filter_map(|entry| entry.id)
                .collect::<Vec<usize>>();
            if entry_ids.is_empty() {
                break;
            }
            let entry_ids = Itertools::join(&mut entry_ids.iter(), ",");
            for table in TABLES_ENTRY_ID {
                let sql = format!("DELETE FROM `{table}` WHERE `entry_id` IN ({entry_ids})");
                self.get_conn().await?.exec_drop(sql, ()).await?;
            }
            let sql = format!("DELETE FROM `entry` WHERE `id` IN ({entry_ids})");
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }

        // Delete catalog-associated data
        for table in TABLES_CATALOG_ID {
            let sql = format!("DELETE FROM `{table}` WHERE `catalog`={catalog_id}");
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }

        // Delete catalog
        let sql = format!("DELETE FROM `catalog` WHERE `id`={catalog_id}");
        self.get_conn().await?.exec_drop(sql, ()).await?;

        Ok(())
    }

    async fn number_of_entries_in_catalog(&self, catalog_id: usize) -> Result<usize> {
        let results: Vec<usize> = "SELECT count(*) AS cnt FROM `entry` WHERE `catalog`=:catalog_id"
            .with(params! {catalog_id})
            .map(self.get_conn_ro().await?, |num| num)
            .await?;
        Ok(*results.first().unwrap_or(&0))
    }

    async fn get_catalog_from_id(&self, catalog_id: usize) -> Result<Catalog> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `id`=:catalog_id";
        let mut conn = self.get_conn_ro().await?;
        // Take ownership of the result vec and pull the first Some out
        // directly. The previous `.iter().filter_map(|row| row.to_owned()).collect()`
        // cloned every Catalog only to drop all but one, then `.pop().to_owned()`
        // cloned that one a second time.
        let row = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(|row| Self::catalog_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .next();
        drop(conn);
        row.ok_or_else(|| anyhow!("No catalog #{catalog_id}"))
    }

    async fn get_catalog_from_name(&self, name: &str) -> Result<Catalog> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `name`=:name";
        let mut conn = self.get_conn_ro().await?;
        let row = conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(|row| Self::catalog_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .next();
        drop(conn);
        row.ok_or_else(|| anyhow!("No catalog '{name}'"))
    }

    async fn get_catalog_key_value_pairs(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, String>> {
        let sql = r#"SELECT `kv_key`,`kv_value` FROM `kv_catalog` WHERE `catalog_id`=:catalog_id"#;
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        let ret: HashMap<String, String> = results.into_iter().collect();
        Ok(ret)
    }

    // async fn remove_inactive_catalogs_from_overview(&self) -> Result<()> {
    //     let sql = r#"DELETE FROM `overview` WHERE `catalog` IN (SELECT `id` FROM `catalog` WHERE `active`=0)"#;
    //     self.get_conn().await?.exec_drop(sql, ()).await?;
    //     Ok(())
    // }

    async fn replace_nowd_with_noq(&self) -> Result<()> {
        let sql = r"UPDATE entry SET q=NULL,user=NULL,timestamp=NULL WHERE q=-1";
        self.get_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn catalog_refresh_overview_table(&self, catalog_id: usize) -> Result<()> {
        // Predicates come from OverviewColumn::entry_predicate() — the
        // same source of truth used by the incremental update_overview_table
        // path, so Refresh and post-click counts can't disagree. The
        // predicates are mutually exclusive: every entry row contributes
        // to exactly one bucket (total = noq + autoq + na + nowd + manual).
        use crate::overview::OverviewColumn;
        let pred = |c: OverviewColumn| {
            format!(
                "(SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND {})",
                c.entry_predicate()
            )
        };
        let sql = format!(
            "REPLACE INTO `overview` (catalog,total,noq,autoq,na,manual,nowd,multi_match,types) VALUES (\n\
                :catalog_id,\n\
                (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id),\n\
                {noq},\n\
                {autoq},\n\
                {na},\n\
                {manual},\n\
                {nowd},\n\
                (SELECT count(*) FROM `multi_match` WHERE `catalog`=:catalog_id),\n\
                (SELECT group_concat(DISTINCT `type` SEPARATOR '|') FROM `entry` WHERE `catalog`=:catalog_id)\n\
            )",
            noq = pred(OverviewColumn::Noq),
            autoq = pred(OverviewColumn::Autoq),
            na = pred(OverviewColumn::Na),
            manual = pred(OverviewColumn::Manual),
            nowd = pred(OverviewColumn::Nowd),
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id}).await?;
        Ok(())
    }

    async fn do_catalog_entries_have_person_date(&self, catalog_id: usize) -> Result<bool> {
        let has_dates: Option<Row> = self
            .get_conn_ro()
            .await?
            .exec_first(
                "SELECT * FROM vw_dates WHERE catalog=:catalog_id LIMIT 1",
                params! {catalog_id},
            )
            .await?;
        Ok(has_dates.is_some())
    }

    async fn set_has_person_date(
        &self,
        catalog_id: usize,
        new_has_person_date: &str,
    ) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `catalog` SET `has_person_date`=:new_has_person_date WHERE `id`=:catalog_id",
                params! {catalog_id, new_has_person_date},
            )
            .await?;
        Ok(())
    }

    // Microsync

    async fn microsync_load_entry_names(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, String>> {
        let placeholders = Self::sql_placeholders(entry_ids.len());
        let sql = format!("SELECT `id`,`ext_name` FROM `entry` WHERE `id` IN ({placeholders})");
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, entry_ids.to_vec())
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?
            .iter()
            .map(|(entry_id, ext_name)| (*entry_id, ext_name.to_owned()))
            .collect();
        Ok(results)
    }

    async fn microsync_get_multiple_q_in_mnm(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(isize, String, String)>> {
        let sql = format!(
            "SELECT q,group_concat(id) AS ids,group_concat(ext_id SEPARATOR '{EXT_URL_UNIQUE_SEPARATOR}') AS ext_ids FROM entry WHERE catalog=:catalog_id AND q IS NOT NULL and q>0 AND user>0 GROUP BY q HAVING count(id)>1 ORDER BY q"
        );
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(from_row::<(isize, String, String)>)
            .await?;
        Ok(results)
    }

    async fn microsync_get_entries_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[&String],
    ) -> Result<Vec<(usize, Option<isize>, Option<usize>, String, String)>> {
        let placeholders: Vec<&str> = ext_ids.iter().map(|_| "BINARY ?").collect();
        let placeholders = placeholders.join(",");
        let sql = format!(
            "SELECT `id`,`q`,`user`,`ext_id`,`ext_url` FROM `entry` WHERE `catalog`={catalog_id} AND `ext_id` IN ({placeholders})"
        );
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ext_ids.to_vec())
            .await?
            .map_and_drop(from_row::<(usize, Option<isize>, Option<usize>, String, String)>)
            .await?;
        Ok(results)
    }

    // MixNMatch

    /// Updates the overview table for a catalog, given the old Entry object, and the user ID and new item.
    ///
    /// Classification is delegated to `crate::overview::OverviewColumn`,
    /// the single source of truth shared with
    /// `catalog_refresh_overview_table`. When the state change doesn't
    /// actually move a row between buckets (e.g. re-confirming an
    /// existing match), we short-circuit — the previous implementation
    /// emitted `SET col=col+1, col=col-1` which MySQL accepts but is
    /// pointless.
    async fn update_overview_table(
        &self,
        old_entry: &Entry,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()> {
        use crate::overview::OverviewColumn;
        let to_col = OverviewColumn::classify(user_id, q);
        let from_col = OverviewColumn::classify(old_entry.user, old_entry.q);
        if to_col == from_col {
            return Ok(());
        }
        let add_column = to_col.column();
        let reduce_column = from_col.column();
        let catalog_id = old_entry.catalog;
        let sql = format!(
            "UPDATE overview SET `{}`=`{}`+1, `{}`=`{}`-1 WHERE catalog=:catalog_id",
            add_column, add_column, reduce_column, reduce_column
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id}).await?;
        Ok(())
    }

    async fn overview_apply_insert(
        &self,
        catalog_id: usize,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()> {
        use crate::overview::OverviewColumn;
        let col = OverviewColumn::classify(user_id, q).column();
        // No-op if the catalog has never been refreshed (no overview
        // row yet) — UPDATE just affects 0 rows. The first refresh will
        // populate it from `entry` and subsequent inserts will track.
        let sql = format!(
            "UPDATE `overview` SET `total`=`total`+1, `{col}`=`{col}`+1 WHERE `catalog`=:catalog_id"
        );
        self.get_conn()
            .await?
            .exec_drop(sql, params! {catalog_id})
            .await?;
        Ok(())
    }

    async fn overview_apply_delete(
        &self,
        catalog_id: usize,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()> {
        use crate::overview::OverviewColumn;
        let col = OverviewColumn::classify(user_id, q).column();
        let sql = format!(
            "UPDATE `overview` SET `total`=`total`-1, `{col}`=`{col}`-1 WHERE `catalog`=:catalog_id"
        );
        self.get_conn()
            .await?
            .exec_drop(sql, params! {catalog_id})
            .await?;
        Ok(())
    }

    async fn get_overview_table(&self) -> Result<Vec<OverviewTableRow>> {
        let sql = "SELECT * FROM `overview`";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| OverviewTableRow::from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        Ok(ret)
    }

    async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<()> {
        // The reference_fixer.q column is INT UNSIGNED and represents the
        // Wikidata item whose references should be re-checked. For N/A
        // (q=0) there's no item to re-check, and for no-Wikidata (q=-1)
        // the insert would overflow the UNSIGNED column and raise
        // ERROR 22003. Skip both: there's nothing meaningful to queue.
        if q_numeric <= 0 {
            return Ok(());
        }
        self.get_conn().await?.exec_drop(r"INSERT INTO `reference_fixer` (`q`,`done`) VALUES (:q_numeric,0) ON DUPLICATE KEY UPDATE `done`=0",params! {q_numeric}).await?;
        Ok(())
    }

    async fn reference_fixer_pending(&self, limit: usize) -> Result<Vec<usize>> {
        let sql = "SELECT `q` FROM `reference_fixer` WHERE `done`=0 ORDER BY `q` DESC LIMIT :limit";
        let qs: Vec<usize> = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! {limit})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(qs)
    }

    async fn reference_fixer_mark_done(&self, q: usize) -> Result<()> {
        let sql = "UPDATE `reference_fixer` SET `done`=1 WHERE `q`=:q";
        self.get_conn().await?.exec_drop(sql, params! {q}).await?;
        Ok(())
    }

    /// Checks if the log already has a removed match for this entry.
    /// If a q_numeric item is given, and a specific one is in the log entry, it will only trigger on this combination.
    async fn avoid_auto_match(&self, entry_id: usize, q_numeric: Option<isize>) -> Result<bool> {
        let mut sql = format!("SELECT id FROM `log` WHERE `entry_id`={entry_id}");
        if let Some(q) = q_numeric {
            sql += &format!(" AND (q IS NULL OR q={})", &q);
        }
        sql += " LIMIT 1";
        let has_rows = !self
            .get_conn_ro()
            .await?
            .exec_iter(sql, Empty)
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .is_empty();
        Ok(has_rows)
    }

    //TODO test
    async fn get_random_active_catalog_id_with_property(&self) -> Option<usize> {
        let sql = "SELECT /* get_random_active_catalog_id_with_property */ id FROM catalog WHERE active=1 AND wd_prop IS NOT NULL and wd_qual IS NULL ORDER by rand() LIMIT 1";
        self.get_conn_ro()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .first()
            .map(|x| x.to_owned())
    }

    async fn get_random_active_catalog_id(&self) -> Option<usize> {
        let sql = "SELECT /* get_random_active_catalog_id */ id FROM catalog WHERE active=1 ORDER by rand() LIMIT 1";
        self.get_conn_ro()
            .await
            .ok()?
            .exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .first()
            .copied()
    }

    async fn get_kv_value(&self, key: &str) -> Result<Option<String>> {
        let sql = r"SELECT `kv_value` FROM `kv` WHERE `kv_key`=:key";
        Ok(self
            .get_conn()
            .await?
            .exec_iter(sql, params! {key})
            .await?
            .map_and_drop(from_row::<String>)
            .await?
            .pop())
    }

    async fn set_kv_value(&self, key: &str, value: &str) -> Result<()> {
        let sql = r"INSERT INTO `kv` (`kv_key`,`kv_value`) VALUES (:key,:value) ON DUPLICATE KEY UPDATE `kv_value`=:value";
        self.get_conn()
            .await?
            .exec_drop(sql, params! {key,value})
            .await?;
        Ok(())
    }

    // Issue methods now live on a separate `impl IssueQueries for
    // StorageMySQL` block (further down) so the trait can be
    // depended on independently of `Storage`.

    // Autoscrape methods now live on a separate `impl AutoscrapeQueries
    // for StorageMySQL` block (further down).

    // Auxiliary-matcher methods now live on `impl AuxiliaryMatcherQueries
    // for StorageMySQL` (further down in this file).

    // Maintenance

    async fn maintenance_update_auxiliary_props(
        &self,
        prop2type: &[(String, String)],
    ) -> Result<()> {
        if prop2type.is_empty() {
            return Ok(());
        }
        let mut parts = vec![];
        let mut params = vec![];
        for (prop, prop_type) in prop2type {
            let prop = prop.as_str()[1..].to_string(); // Remove leading P
            parts.push(format!("({prop},?)"));
            params.push(prop_type.clone());
        }
        let sql = format!(
            "INSERT INTO `auxiliary_props` (`p`, `type`) VALUES {}",
            parts.join(",")
        );

        let mut conn = self.get_conn().await?;
        conn.exec_drop("TRUNCATE `auxiliary_props`", ()).await?;
        conn.exec_drop(sql, params).await?;
        Ok(())
    }

    async fn maintenance_use_auxiliary_broken(&self) -> Result<()> {
        let sqls = [
            r#"UPDATE auxiliary a INNER JOIN auxiliary_fix af ON a.aux_p=af.aux_p AND a.aux_name=af.label SET a.aux_name=af.aux_name"#,
            r#"INSERT IGNORE INTO auxiliary_broken SELECT * FROM auxiliary WHERE aux_name NOT RLIKE "^Q\\d+$" AND aux_p IN (SELECT q FROM auxiliary_props WHERE `type`="WikibaseItem")"#,
            r#"DELETE FROM auxiliary WHERE aux_name NOT RLIKE "^Q\\d+$" AND aux_p IN (SELECT p FROM auxiliary_props WHERE `type`="WikibaseItem")"#,
            r#"UPDATE auxiliary_broken a INNER JOIN auxiliary_fix af ON a.aux_p=af.aux_p AND a.aux_name=af.label SET a.aux_name=af.aux_name"#,
            r#"INSERT IGNORE INTO auxiliary SELECT * FROM auxiliary_broken WHERE aux_name RLIKE "^Q\\d+$""#,
            r#"DELETE FROM auxiliary_broken WHERE aux_name RLIKE "^Q\\d+$""#,
        ];
        for sql in sqls {
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn maintenance_common_names_dates(&self) -> Result<()> {
        // Build results into a session-local TEMPORARY TABLE first (no global DDL lock
        // contention on `entry`/`person_dates`).  A single GROUP BY with
        // HAVING MAX(matched)=0 replaces the old CREATE→index→self-join DELETE pipeline.
        let sqls = [
            r#"DROP TEMPORARY TABLE IF EXISTS tmp_cnd"#,
            r#"CREATE TEMPORARY TABLE tmp_cnd AS
                SELECT /* maintenance_common_names_dates */
                    ext_name AS `name`,
                    count(DISTINCT catalog) AS cnt,
                    group_concat(entry_id) AS entry_ids,
                    concat(year_born,'-',year_died) AS dates
                FROM entry
                JOIN person_dates ON person_dates.entry_id = entry.id
                WHERE year_born != '' AND year_died != ''
                  AND catalog NOT IN (SELECT id FROM catalog WHERE active=0)
                GROUP BY ext_name, year_born, year_died
                HAVING cnt >= 3
                   AND max(if(q IS NOT NULL AND user > 0, 1, 0)) = 0"#,
            // Slow work is done; minimise the window where common_names_dates is empty.
            r#"TRUNCATE common_names_dates"#,
            r#"INSERT INTO common_names_dates (`name`,cnt,entry_ids,dates)
                SELECT `name`,cnt,entry_ids,dates FROM tmp_cnd"#,
            r#"DROP TEMPORARY TABLE tmp_cnd"#,
        ];
        for sql in sqls {
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn maintenance_common_aux(&self) -> Result<()> {
        let sqls = [
            r#"DROP TABLE IF EXISTS common_aux_tmp"#,
            r#"CREATE TABLE common_aux_tmp
			    SELECT /* maintenance_common_aux */ aux_p,aux_name,group_concat(id) AS entry_ids,count(*) as cnt,sum(q is null or user=0) as unmatched,
			    group_concat(DISTINCT IF(q is not null AND user is not null and user>0,q,null)) as fully_matched_qs
			    FROM vw_aux WHERE aux_p IN (214,227,1207,1273,244)
			    GROUP BY aux_p,aux_name HAVING cnt>=3 AND unmatched>0"#,
            r#"DROP TABLE IF EXISTS common_aux"#,
            r#"RENAME TABLE common_aux_tmp to common_aux"#,
        ];
        for sql in sqls {
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn property_cache_replace(&self, rows: &[PropertyCacheRow]) -> Result<()> {
        // Refuse to wipe the table when there's nothing to put back —
        // a transient SPARQL failure shouldn't leave the cache empty
        // and break every property-aware UI for hours until the next
        // run. The Maintenance caller's row-count gate is the primary
        // defence; this is belt-and-braces.
        if rows.is_empty() {
            return Ok(());
        }
        // Per-batch row count. ~80 chars per VALUES tuple keeps each
        // INSERT well under MySQL's default `max_allowed_packet`
        // (~4 MB on toolforge), with plenty of headroom for long
        // labels.
        const BATCH_ROWS: usize = 1000;

        let mut conn = self.get_conn().await?;
        conn.exec_drop("TRUNCATE `property_cache`", ()).await?;
        for chunk in rows.chunks(BATCH_ROWS) {
            // Manually build the multi-VALUES list — exec_batch in
            // mysql_async fires N round-trips behind the scenes; one
            // INSERT with N tuples is materially faster.
            let placeholders = std::iter::repeat_n("(?,?,?,?)", chunk.len()).join(",");
            let sql = format!(
                "INSERT INTO `property_cache` \
                    (`prop_group`,`property`,`item`,`label`) \
                 VALUES {placeholders}"
            );
            // mysql_async's positional Params::Positional flattens a
            // Vec<Value> into one bind list — exactly what we need.
            let mut bind: Vec<mysql_async::Value> = Vec::with_capacity(chunk.len() * 4);
            for r in chunk {
                bind.push(mysql_async::Value::UInt(r.prop_group as u64));
                bind.push(mysql_async::Value::UInt(r.property as u64));
                bind.push(mysql_async::Value::UInt(r.item as u64));
                bind.push(mysql_async::Value::Bytes(r.label.as_bytes().to_vec()));
            }
            conn.exec_drop(sql, bind).await?;
        }
        Ok(())
    }

    async fn maintenance_common_names_human(&self) -> Result<()> {
        // Collect candidate rows in a TEMPORARY TABLE first so the
        // window where `common_names_human` is empty stays as small as
        // possible (TRUNCATE→INSERT direct from the heavy SELECT would
        // leave readers staring at zero rows for the duration of the
        // scan). Mirrors the dates / birth_year pipelines.
        //
        // Filters mirror PHP `Maintenance::updateCommonNames`'s human
        // arm:
        //   - type='Q5'                   ← only humans
        //   - q IS NULL                   ← only never-matched entries
        //   - ext_name LIKE '____% ____% ____%' ← three space-separated
        //                                   tokens, each ≥4 chars (avoids
        //                                   "J. R. R. Tolkien"-style
        //                                   initials and very short
        //                                   middle names that would
        //                                   produce too-broad groups)
        //   - ext_desc != ''              ← skip rows we have no detail on
        //   - active catalog              ← stale catalogs shouldn't
        //                                   create candidate noise
        //   - HAVING cnt >= 5             ← only names that show up in
        //                                   5+ different catalogs (i.e.
        //                                   plausibly the same person)
        let sqls = [
            "DROP TEMPORARY TABLE IF EXISTS tmp_cnh",
            r"CREATE TEMPORARY TABLE tmp_cnh AS
                SELECT /* maintenance_common_names_human */
                    ext_name AS `name`,
                    count(DISTINCT catalog) AS cnt,
                    group_concat(id) AS entry_ids
                FROM entry
                WHERE `type` = 'Q5'
                  AND q IS NULL
                  AND ext_name LIKE '____% ____% ____%'
                  AND ext_desc != ''
                  AND catalog IN (SELECT id FROM catalog WHERE active = 1)
                GROUP BY ext_name
                HAVING cnt >= 5",
            "TRUNCATE common_names_human",
            "INSERT IGNORE INTO common_names_human (`name`, cnt, entry_ids) \
             SELECT `name`, cnt, entry_ids FROM tmp_cnh",
            "DROP TEMPORARY TABLE tmp_cnh",
        ];
        let mut conn = self.get_conn().await?;
        for sql in sqls {
            conn.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn maintenance_common_names_birth_year(&self) -> Result<()> {
        // Correlated-subquery → JOIN: the old `(SELECT ext_name FROM entry
        // WHERE entry.id=entry_id ...)` ran once per matching person_dates
        // row. A plain INNER JOIN lets MySQL plan a single index scan and
        // also lets the q/catalog filters shrink the aggregated set before
        // GROUP BY instead of being a HAVING afterthought.
        let excluded = NAME_BIRTH_YEAR_EXCLUDED_CATALOGS.iter().join(",");
        let main_sql = format!(
            r#"INSERT /* maintenance_common_names_birth_year */ IGNORE INTO common_names_birth_year_tmp (name,cnt,entry_ids,dates)
	        SELECT /* maintenance_common_names_birth_year */ SQL_NO_CACHE
	            e.ext_name AS name,
	            count(DISTINCT pd.entry_id) AS cnt,
	            group_concat(pd.entry_id) AS entry_ids,
	            concat(pd.year_born,'-') AS dates
	        FROM person_dates pd
	        JOIN entry e ON e.id = pd.entry_id
	        WHERE pd.is_matched = 0
	          AND pd.year_born != ''
	          AND pd.year_born < 1960
	          AND e.q IS NULL
	          AND e.catalog NOT IN ({excluded})
	        GROUP BY e.ext_name, pd.year_born
	        HAVING cnt >= 3 AND name NOT RLIKE "^\\S*$""#
        );
        let sqls: [&str; 6] = [
            "SET SESSION group_concat_max_len = 1000000000",
            "TRUNCATE common_names_birth_year_tmp",
            &main_sql,
            "TRUNCATE common_names_birth_year",
            "INSERT INTO common_names_birth_year SELECT * FROM common_names_birth_year_tmp",
            "TRUNCATE common_names_birth_year_tmp",
        ];
        let mut conn = self.get_conn().await?; // One connection because of SESSION
        for sql in sqls {
            conn.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn maintenance_taxa(&self) -> Result<()> {
        let sqls = [
            r#"DROP TABLE IF EXISTS tmp_taxa"#,
            r#"CREATE TABLE tmp_taxa AS
		        SELECT /* maintenance_taxa */ ext_name,count(distinct catalog) AS cnt FROM entry
		        WHERE `type`="Q16521"
		        AND catalog IN (select id from catalog WHERE active=1 and taxon_run=1)
		        AND q is null
		        AND ext_name RLIKE "^\\S+ \\S+$"
		        GROUP BY ext_name
		        HAVING cnt>=4"#,
            r#"TRUNCATE common_names_taxon"#,
            r#"INSERT IGNORE INTO common_names_taxon (`name`,cnt) SELECT ext_name,cnt FROM tmp_taxa"#,
            r#"DROP TABLE tmp_taxa"#,
        ];
        for sql in sqls {
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn maintenance_artwork(&self) -> Result<()> {
        let sqls = [
            r#"TRUNCATE common_names_artwork"#,
            r#"INSERT IGNORE INTO common_names_artwork (`name`,entry_ids,cnt)
            SELECT /* maintenance_artwork */ artwork.ext_name,group_concat(artwork.id),count(DISTINCT artwork.catalog) as cnt
            FROM entry artwork,entry creator,mnm_relation
            WHERE artwork.id=mnm_relation.entry_id AND creator.id=mnm_relation.target_entry_id AND mnm_relation.property=170
            AND (artwork.q is null or artwork.user=0)
            AND creator.q>0 AND creator.user>0
            AND artwork.ext_name NOT IN ("Sans titre","(Sans titre)","Untitled","No title","Composition","Self Portrait","Self-Portrait")
            GROUP BY artwork.ext_name,creator.q
            HAVING cnt>=2 AND cnt=count(DISTINCT artwork.ext_url)"#,
        ];
        for sql in sqls {
            self.get_conn().await?.exec_drop(sql, ()).await?;
        }
        Ok(())
    }

    async fn import_relations_into_aux(&self) -> Result<()> {
        let sql = r#"INSERT IGNORE INTO auxiliary (entry_id,aux_p,aux_name)
        				SELECT mnm_relation.entry_id,property,concat("Q",entry.q)
            			FROM entry,mnm_relation
               			WHERE target_entry_id=entry.id AND user>0 AND q>0
                  		AND NOT EXISTS (SELECT * FROM auxiliary WHERE auxiliary.entry_id=mnm_relation.entry_id AND aux_p=property)
                    	AND property IN (50,170)"#;
        self.get_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn get_props_todo(&self) -> Result<Vec<PropTodo>> {
        let sql = r#"SELECT id,property_num,property_name,default_type,status,note,user_id,items_using,number_of_records FROM props_todo"#;
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(PropTodo::from_row)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(results)
    }

    async fn add_props_todo(&self, new_props: Vec<PropTodo>) -> Result<()> {
        if new_props.is_empty() {
            return Ok(());
        }
        let mut conn = self.get_conn().await?;
        r"INSERT IGNORE INTO `props_todo` (property_num,property_name,default_type,`status`,note,user_id,items_using)
         		VALUES (:property_num,:property_name,:default_type,:status,:note,:user_id,:items_using)"
        .with(new_props.iter().map(|prop|
            params! {
            "property_num" => prop.prop_num,
            "property_name" => prop.name.to_owned(),
            "default_type" => prop.default_type.to_owned(),
            "status" => prop.status.to_owned(),
            "note" => prop.note.to_owned(),
            "user_id" => prop.user_id,
            "items_using" => prop.items_using
            }))
        .batch(&mut conn)
        .await?;
        Ok(())
    }

    async fn mark_props_todo_as_has_catalog(&self) -> Result<()> {
        let sql = r#"UPDATE `props_todo` SET status="HAS_CATALOG",note="Auto-matched to catalog",user_id=0
        WHERE `status`="NO_CATALOG" AND property_num IN
        (select distinct wd_prop from catalog where active=1 and wd_qual is NULL and wd_prop is not null)"#;
        self.get_conn().await?.exec_drop(sql, Empty).await?;
        Ok(())
    }

    async fn set_props_todo_items_using(&self, prop_numeric: u64, cnt: u64) -> Result<()> {
        let sql = r#"UPDATE `props_todo` SET items_using=:cnt WHERE property_num=:prop_numeric"#;
        self.get_conn()
            .await?
            .exec_drop(sql, params! {prop_numeric,cnt})
            .await?;
        Ok(())
    }

    /// Removes P17 auxiliary values for entryies of type Q5 (human)
    async fn remove_p17_for_humans(&self) -> Result<()> {
        let sql = r#"DELETE FROM auxiliary WHERE aux_p=17 AND EXISTS (SELECT * FROM entry WHERE entry_id=entry.id AND `type`="Q5")"#;
        self.get_conn().await?.exec_drop(sql, Empty).await?;
        Ok(())
    }

    async fn use_automatchers(&self, catalog_id: usize, use_automatchers: u8) -> Result<()> {
        // Stored as a string in kv_catalog; "0" means the generic
        // automatchers are disabled for this catalog.
        let value = use_automatchers.to_string();
        self.set_catalog_kv(catalog_id, "use_automatchers", &value)
            .await
    }

    async fn set_catalog_kv(&self, catalog_id: usize, key: &str, value: &str) -> Result<()> {
        let sql = "INSERT INTO `kv_catalog` (`catalog_id`,`kv_key`,`kv_value`) \
                   VALUES (:catalog_id,:key,:value) \
                   ON DUPLICATE KEY UPDATE `kv_value`=:value";
        self.get_conn()
            .await?
            .exec_drop(sql, params! {catalog_id, key, value})
            .await?;
        Ok(())
    }

    async fn delete_catalog_kv(&self, catalog_id: usize, key: &str) -> Result<()> {
        let sql = "DELETE FROM `kv_catalog` WHERE `catalog_id`=:catalog_id AND `kv_key`=:key";
        self.get_conn()
            .await?
            .exec_drop(sql, params! {catalog_id, key})
            .await?;
        Ok(())
    }

    async fn maintenance_automatch_people_via_year_born(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // DEACTIVATED THIS TAKES TOO LONG

        // Reset
        let sql1 = r#"DROP TABLE IF EXISTS tmp_automatches"#;
        conn.exec_drop(sql1, Empty).await?;

        // Generate sub-list of potential matches
        let sql2 = r#"CREATE table tmp_automatches
	       SELECT DISTINCT e2.id AS entry_id,e1.q AS q
	       FROM entry e1,entry e2,person_dates p1,person_dates p2,catalog c1,catalog c2
	       WHERE p1.entry_id=e1.id AND p2.entry_id=e2.id AND p1.year_born=p2.year_born
	       AND e1.ext_name=e2.ext_name
	       AND e1.q>0 AND e1.user>0
	       AND (e2.q IS NULL or e2.user=0)
	       AND e1.q!=e2.q
	       AND e1.catalog=c1.id AND c1.active=1
	       AND e2.catalog=c2.id AND c2.active=1
	       limit 1000"#;
        conn.exec_drop(sql2, Empty).await?;

        // Apply sub-list
        let sql3 = r#"UPDATE entry
        	INNER JOIN tmp_automatches ON entry.id=entry_id
        	SET entry.q=tmp_automatches.q,user=0,timestamp=date_format(now(),"%Y%m%d%H%i%S")
         	WHERE entry.q!=tmp_automatches.q AND (entry.q IS NULL or entry.user=0)
          	AND NOT EXISTS (SELECT * FROM log WHERE entry.id=log.entry_id AND `action`='remove_q' AND log.q=tmp_automatches.q);"#;
        conn.exec_drop(sql3, Empty).await?;

        // Cleanup
        let sql4 = r#"DROP TABLE IF EXISTS tmp_automatches"#;
        conn.exec_drop(sql4, Empty).await?;

        Ok(())
    }

    async fn cleanup_mnm_relations(&self) -> Result<()> {
        let sql = "DELETE from mnm_relation WHERE entry_id=0 or target_entry_id=0";
        self.get_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn create_match_person_dates_jobs_for_catalogs(&self) -> Result<()> {
        let sql = r#"INSERT IGNORE INTO jobs (`action`,catalog,`status`)
        	SELECT 'match_person_dates',id,'TODO' FROM catalog
         	WHERE has_person_date='yes'
          	AND id NOT IN (SELECT catalog FROM jobs WHERE `action`='match_person_dates')"#;
        self.get_conn().await?.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn maintenance_sync_redirects(&self, redirects: HashMap<isize, isize>) -> Result<()> {
        let mut conn = self.get_conn().await?;
        for (old_q, new_q) in redirects {
            let sql = "UPDATE `entry` SET `q`=:new_q WHERE `q`=:old_q";
            conn.exec_drop(sql, params! {old_q,new_q}).await?;
        }
        Ok(())
    }

    // Unlink deleted Wikidata items (item IDs in `deletions`).
    // Returns the catalog ID that were affected by this.
    async fn maintenance_apply_deletions(&self, deletions: Vec<isize>) -> Result<Vec<usize>> {
        let deletions_string = deletions.iter().join(",");
        let sql1 =
            format!("SELECT DISTINCT `catalog` FROM `entry` WHERE `q` IN ({deletions_string})");
        let mut conn = self.get_conn().await?;
        let catalog_ids = conn
            .exec_iter(sql1, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        let sql2 = format!(
            "UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({deletions_string})"
        );
        conn.exec_drop(sql2, ()).await?;
        Ok(catalog_ids)
    }

    // Returns a list of active catalog IDs that have a WD property set but no WD qualifier.
    // Return items are tuples of (catalog_id, wd_prop)
    async fn maintenance_get_prop2catalog_ids(&self) -> Result<Vec<(usize, usize)>> {
        let sql = r"SELECT `id`,`wd_prop` FROM `catalog` WHERE `wd_prop` IS NOT NULL AND `wd_qual` IS NULL AND `active`=1";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize)>)
            .await?;
        Ok(results)
    }

    async fn maintenance_sync_property(
        &self,
        catalogs: &[usize],
        ext_ids: Vec<String>,
    ) -> Result<Vec<(usize, String, Option<usize>)>> {
        let catalogs_str: String = catalogs.iter().join(",");
        let qm_propvals = Self::sql_placeholders(ext_ids.len());
        let sql = format!(
            r"SELECT `id`,`ext_id`,`user` FROM `entry` WHERE `catalog` IN ({catalogs_str}) AND `ext_id` IN ({qm_propvals})"
        );
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ext_ids)
            .await?
            .map_and_drop(from_row::<(usize, String, Option<usize>)>)
            .await?;
        Ok(results)
    }

    async fn maintenance_fix_redirects(&self, from: isize, to: isize) -> Result<()> {
        let sql = "UPDATE `entry` SET `q`=:to WHERE `q`=:from";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {from,to}).await?;
        Ok(())
    }

    async fn maintenance_unlink_item_matches(&self, items: Vec<String>) -> Result<()> {
        let sql = format!(
            "UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({})",
            items.join(",")
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, Empty).await?;
        Ok(())
    }

    async fn automatch_entry_by_sparql(
        &self,
        catalog_id: usize,
        q_numeric: usize,
        label: &str,
    ) -> Result<()> {
        let timestamp = TimeStamp::now();
        let sql = "UPDATE `entry` SET `q`=:q_numeric,`user`=0,`timestamp`=:timestamp
        	WHERE `catalog`=:catalog_id AND `ext_name`=:label AND `q` IS NULL";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {label,q_numeric,catalog_id,timestamp})
            .await?;
        Ok(())
    }

    /// Finds some unmatched (Q5) entries where there is a (unique) full match for that name,
    /// and uses it as an auto-match
    async fn maintenance_automatch(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let sql1 = "SELECT /* maintenance_automatch */ e1.id,e2.q FROM entry e1,entry e2
            WHERE e1.ext_name=e2.ext_name AND e1.id!=e2.id
            AND e1.type='Q5' AND e2.type='Q5'
            AND e1.q IS NULL
            AND e2.type IS NOT NULL AND e2.user>0
            HAVING
            (SELECT count(DISTINCT q) FROM entry e3 WHERE e3.ext_name=e2.ext_name AND e3.type=e2.type AND e3.q IS NOT NULL AND e3.user>0)=1
            LIMIT 500";
        let new_automatches = conn
            .exec_iter(sql1, ())
            .await?
            .map_and_drop(from_row::<(usize, isize)>)
            .await?;
        let sql2 = "UPDATE `entry` SET `q`=:q,`user`=0,`timestamp`=:timestamp WHERE `id`=:entry_id AND `q` IS NULL";
        for (entry_id, q) in &new_automatches {
            let timestamp = TimeStamp::now();
            conn.exec_drop(sql2, params! {entry_id,q,timestamp}).await?;
        }
        Ok(())
    }

    /// Returns tuples of entry IDs and their prospective q matches,
    /// based on other entriews with the same name, birth date, and death date
    /// (both dates are day precision).
    async fn maintenance_match_people_via_name_and_full_dates(
        &self,
        batch_size: usize,
    ) -> Result<Vec<(usize, usize)>> {
        let mut conn = self.get_conn().await?;
        // Join base tables directly instead of going through vw_dates twice.
        // vw_dates embeds a correlated `catalog IN (SELECT … active=1)` subquery
        // that the optimizer evaluates per row on each alias; explicit JOIN to
        // catalog lets it be evaluated once as a semi-join or hash join.
        // Also fixes d2.died=d2.died (always true) → pd2.died=pd1.died.
        let sql = format!(
            "SELECT /* maintenance_match_people_via_name_and_full_dates */ e1.id AS entry_id, e2.q
            FROM person_dates pd1
            JOIN entry e1 ON e1.id = pd1.entry_id
                          AND (e1.user = 0 OR e1.user IS NULL)
            JOIN catalog c1 ON c1.id = e1.catalog AND c1.active = 1
            JOIN entry e2 ON e2.ext_name = e1.ext_name
                          AND e2.user > 0
                          AND e2.q > 0
            JOIN catalog c2 ON c2.id = e2.catalog AND c2.active = 1
            JOIN person_dates pd2 ON pd2.entry_id = e2.id
                                 AND pd2.born = pd1.born
                                 AND pd2.died = pd1.died
                                 AND pd2.is_matched = 1
            WHERE pd1.is_matched = 0
              AND LENGTH(pd1.born) = 10
              AND LENGTH(pd1.died) = 10
            LIMIT {batch_size}"
        );
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, usize)>)
            .await?;
        Ok(results)
    }

    /// Retrieves a batch of (unique) Wikidata items, in a given matching state.
    async fn get_items(
        &self,
        catalog_id: usize,
        offset: usize,
        state: &MatchState,
    ) -> Result<Vec<String>> {
        let batch_size = 5000;
        let sql = format!(
            "SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            state.get_sql()
        );
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .iter()
            .map(|q| format!("Q{q}"))
            .collect();
        Ok(ret)
    }

    async fn get_catalogs_with_person_dates_without_flag(&self) -> Result<Vec<usize>> {
        let sql = "SELECT DISTINCT catalog FROM vw_dates WHERE catalog IN (SELECT id FROM catalog WHERE has_person_date!='yes' AND has_person_date!='no' AND active=1)";
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(ret)
    }

    async fn add_mnm_relation(
        &self,
        entry_id: usize,
        prop_numeric: usize,
        target_entry_id: usize,
    ) -> Result<()> {
        let sql = "INSERT IGNORE INTO mnm_relation (entry_id, property, target_entry_id) VALUES (?, ?, ?)";
        self.get_conn()
            .await?
            .exec_drop(sql, params![entry_id, prop_numeric, target_entry_id])
            .await?;
        Ok(())
    }

    // Job-table methods now live on `impl JobQueries for StorageMySQL`
    // (further down in this file).

    // Automatch

    /// Auto-matches unmatched and automatched people to fully matched entries that have the same name and birth year.
    async fn automatch_people_with_birth_year(&self, catalog_id: usize) -> Result<()> {
        // Split the OR (year_born / year_died) into two UNION branches so each
        // branch can use the `year_born (year_born, year_died)` composite index on
        // person_dates.  Direct table joins replace vw_dates to avoid the hidden
        // per-row `catalog IN (SELECT … active=1)` subquery inside the view.
        let sql_select = r#"
            SELECT /* automatch_people_with_birth_year */ entry_id, group_concat(DISTINCT q) AS q
            FROM (
                SELECT e0.id AS entry_id, e1.q
                FROM entry e0
                JOIN person_dates pd0 ON pd0.entry_id = e0.id AND pd0.year_born != ''
                JOIN catalog c0 ON c0.id = e0.catalog AND c0.active = 1
                JOIN person_dates pd1 ON pd1.year_born = pd0.year_born
                JOIN entry e1 ON e1.id = pd1.entry_id
                             AND e1.ext_name = e0.ext_name
                             AND e1.catalog != e0.catalog
                             AND e1.user > 0 AND e1.q > 0
                JOIN catalog c1 ON c1.id = e1.catalog AND c1.active = 1
                WHERE e0.catalog = :catalog_id
                  AND (e0.user = 0 OR e0.user IS NULL)

                UNION

                SELECT e0.id AS entry_id, e1.q
                FROM entry e0
                JOIN person_dates pd0 ON pd0.entry_id = e0.id AND pd0.year_died != ''
                JOIN catalog c0 ON c0.id = e0.catalog AND c0.active = 1
                JOIN person_dates pd1 ON pd1.year_died = pd0.year_died
                JOIN entry e1 ON e1.id = pd1.entry_id
                             AND e1.ext_name = e0.ext_name
                             AND e1.catalog != e0.catalog
                             AND e1.user > 0 AND e1.q > 0
                JOIN catalog c1 ON c1.id = e1.catalog AND c1.active = 1
                WHERE e0.catalog = :catalog_id
                  AND (e0.user = 0 OR e0.user IS NULL)
            ) AS combined
            GROUP BY entry_id
            HAVING count(DISTINCT q) = 1"#;

        let mut conn = self.get_conn().await?;
        let entry_id2q = conn
            .exec_iter(sql_select, params! {catalog_id})
            .await?
            .map_and_drop(from_row::<(usize, usize)>)
            .await?;

        for (entry_id, q) in &entry_id2q {
            let sql_update = r#"UPDATE entry e1
            SET e1.q=:q,e1.user=0,timestamp=date_format(now(),'%Y%m%d%H%i%S')
            WHERE e1.type='Q5' AND e1.id=:entry_id AND (e1.q is null or e1.user=0)
            AND NOT EXISTS (SELECT * FROM log WHERE log.entry_id=e1.id AND log.q=:q)"#;
            conn.exec_drop(sql_update, params! {entry_id, q}).await?;
        }

        // DEACTIVATED
        // too expensive
        // https://phabricator.wikimedia.org/T409716
        //   let sql = r#"UPDATE entry e1
        // INNER JOIN person_dates p1 ON p1.entry_id=e1.id AND p1.year_born IS NOT NULL
        // INNER JOIN vw_dates p2 ON p2.ext_name=e1.ext_name AND p2.year_born=p1.year_born AND p2.q IS NOT NULL AND p2.user>0 AND p2.entry_id!=e1.id AND p2.user IS NOT NULL
        // SET e1.q=p2.q,e1.user=0,timestamp=date_format(now(),'%Y%m%d%H%i%S')
        // WHERE e1.type='Q5' AND e1.catalog=:catalog_id AND (e1.q is null or e1.user=0)
        // AND NOT EXISTS (SELECT * FROM log WHERE log.entry_id=e1.id AND log.q=p2.q)"#;
        // self.get_conn_ro()
        //     .await?
        //     .exec_drop(sql, params! {catalog_id})
        //     .await?;
        Ok(())
    }

    async fn automatch_by_sitelink_get_entries(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String)>> {
        let sql = format!("SELECT `id`,`ext_name` FROM entry WHERE catalog=:catalog_id AND q IS NULL
	            AND NOT EXISTS (SELECT * FROM `log` WHERE log.entry_id=entry.id AND log.action='remove_q')
	            {}
	            ORDER BY `id` LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let entries = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(entries)
    }

    async fn automatch_by_search_get_results(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AutomatchSearchRow>> {
        let sql = format!("SELECT /* automatch_by_search_get_results */ `id`,`ext_name`,`type`,
	            IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases`
	            FROM `entry` WHERE `catalog`=:catalog_id {}
	            /* ORDER BY `id` */
	            LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(|row| {
                let (id, name, type_name, aliases) =
                    from_row::<(usize, String, String, String)>(row);
                AutomatchSearchRow::new(id, name, type_name, aliases)
            })
            .await?;
        Ok(results)
    }

    async fn automatch_creations_get_results(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(String, usize, String)>> {
        let sql = "SELECT object_title,object_entry_id,search_query FROM vw_object_creator WHERE object_catalog={} AND object_q IS NULL
                UNION
                SELECT object_title,object_entry_id,search_query FROM vw_object_creator_aux WHERE object_catalog={} AND object_q IS NULL";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(from_row::<(String, usize, String)>)
            .await?;
        Ok(results)
    }

    async fn automatch_simple_get_results(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AutomatchSearchRow>> {
        let sql = format!("SELECT /* automatch_simple_get_results */ `id`,`ext_name`,`type`,
                IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases`
                FROM `entry` WHERE `catalog`=:catalog_id {}
                /* ORDER BY `id` */
                LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(|row| {
                let (id, name, type_name, aliases) =
                    from_row::<(usize, String, String, String)>(row);
                AutomatchSearchRow::new(id, name, type_name, aliases)
            })
            .await?;
        Ok(results)
    }

    async fn automatch_from_other_catalogs_get_results(
        &self,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<ResultInOriginalCatalog>> {
        let sql = "SELECT /* automatch_from_other_catalogs_get_results */ `id`,`ext_name`,`type` FROM entry WHERE catalog=:catalog_id AND q IS NULL LIMIT :batch_size OFFSET :offset";
        let conn = self.get_conn_ro().await?;
        let results_in_original_catalog: Vec<ResultInOriginalCatalog> = sql
            .with(params! {catalog_id,batch_size,offset})
            .map(conn, |(entry_id, ext_name, type_name)| {
                ResultInOriginalCatalog {
                    entry_id,
                    ext_name,
                    type_name,
                }
            })
            .await?;
        Ok(results_in_original_catalog)
    }

    async fn automatch_from_other_catalogs_get_results2(
        &self,
        results_in_original_catalog: &[ResultInOriginalCatalog],
        ext_names: Vec<String>,
    ) -> Result<Vec<ResultInOtherCatalog>> {
        let ext_names: Vec<mysql_async::Value> = ext_names
            .iter()
            .map(|ext_name| mysql_async::Value::Bytes(ext_name.as_bytes().to_vec()))
            .collect();
        let params = Params::Positional(ext_names);
        let placeholders = Self::sql_placeholders(results_in_original_catalog.len());
        let sql = "SELECT /* automatch_from_other_catalogs_get_results2 */ `id`,`ext_name`,`type`,q FROM entry
            WHERE ext_name IN ("
            .to_string()
            + &placeholders
            + ")
            AND q IS NOT NULL AND q > 0 AND user IS NOT NULL AND user>0
            AND catalog IN (SELECT id from catalog WHERE active=1)
            GROUP BY ext_name,type HAVING count(DISTINCT q)=1";
        let conn = self.get_conn_ro().await?;
        let results_in_other_catalogs: Vec<ResultInOtherCatalog> = sql
            .with(params)
            .map(conn, |(entry_id, ext_name, type_name, q)| {
                ResultInOtherCatalog {
                    entry_id,
                    ext_name,
                    type_name,
                    q,
                }
            })
            .await?;
        Ok(results_in_other_catalogs)
    }

    async fn purge_automatches(&self, catalog_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        conn.exec_drop("UPDATE entry SET q=NULL,user=NULL,`timestamp`=NULL WHERE catalog=:catalog_id AND user=0", params! {catalog_id}).await?;
        conn.exec_drop(
            "DELETE FROM multi_match WHERE catalog=:catalog_id",
            params! {catalog_id},
        )
        .await?;
        Ok(())
    }

    async fn match_person_by_dates_get_results(
        &self,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<PersonDateMatchRow>> {
        let sql = "SELECT entry_id,ext_name,born,died
            FROM (`entry` join `person_dates`)
            WHERE `person_dates`.`entry_id` = `entry`.`id`
            AND `catalog`=:catalog_id AND (q IS NULL or user=0) AND born!='' AND died!=''
            LIMIT :batch_size OFFSET :offset";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id,batch_size,offset})
            .await?
            .map_and_drop(|row| {
                let (entry_id, ext_name, born, died) =
                    from_row::<(usize, String, String, String)>(row);
                PersonDateMatchRow::new(entry_id, ext_name, born, died)
            })
            .await?;
        Ok(results)
    }

    async fn match_person_by_single_date_get_results(
        &self,
        match_field: &str,
        catalog_id: usize,
        precision: i32,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<CandidateDatesRow>> {
        let sql = format!("(
	                SELECT multi_match.entry_id AS entry_id,born,died,candidates AS qs FROM person_dates,multi_match,entry
	                WHERE (q IS NULL OR user=0) AND person_dates.entry_id=multi_match.entry_id AND multi_match.catalog=:catalog_id AND length({match_field})=:precision
	                AND entry.id=person_dates.entry_id
	            ) UNION (
	                SELECT entry_id,born,died,q qs FROM person_dates,entry
	                WHERE (q is not null and user=0) AND catalog=:catalog_id AND length({match_field})=:precision AND entry.id=person_dates.entry_id
	            )
	            ORDER BY entry_id LIMIT :batch_size OFFSET :offset");
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(
                sql.clone(),
                params! {catalog_id,precision,batch_size,offset},
            )
            .await?
            .map_and_drop(|row| {
                let (entry_id, born, died, candidates) =
                    from_row::<(usize, String, String, String)>(row);
                CandidateDatesRow::new(entry_id, born, died, candidates)
            })
            .await?;
        Ok(results)
    }

    async fn automatch_complex_get_el_chunk(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<(usize, String)>> {
        let sql = format!("SELECT `id`,`ext_name` FROM entry WHERE catalog=:catalog_id AND q IS NULL
            AND NOT EXISTS (SELECT * FROM `log` WHERE log.entry_id=entry.id AND log.action='remove_q')
            {}
            ORDER BY `id` LIMIT :batch_size OFFSET :offset",MatchState::unmatched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let el_chunk = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(el_chunk)
    }

    // Entry

    async fn entry_from_id(&self, entry_id: usize) -> Result<Entry> {
        let sql = format!("{} WHERE `id`=:entry_id", Self::entry_sql_select());
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql, params! {entry_id})
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| anyhow!("No entry #{}", entry_id))?;
        Ok(ret)
    }

    async fn entry_from_ext_id(&self, catalog_id: usize, ext_id: &str) -> Result<Entry> {
        let sql = format!(
            "{} WHERE `catalog`=:catalog_id AND `ext_id`=:ext_id",
            Self::entry_sql_select()
        );
        self.get_conn_ro()
            .await?
            .exec_iter(sql, params! {catalog_id,ext_id})
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| anyhow!("No ext_id '{}' in catalog #{}", ext_id, catalog_id))
    }

    async fn get_entry_batch(
        &self,
        catalog_id: usize,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entry>> {
        let query = EntryQuery::default()
            .with_catalog_id(catalog_id)
            .with_limit(limit)
            .with_offset(offset);
        self.entry_query(&query).await
    }

    async fn multiple_from_ids(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Entry>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let entry_ids = entry_ids.iter().join(",");
        let sql = format!("{} WHERE `id` IN ({})", Self::entry_sql_select(), entry_ids);
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Entry> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        let ret = rows
            .into_iter()
            .map(|entry| (entry.id.unwrap_or(0), entry))
            .collect();
        Ok(ret)
    }

    async fn entry_insert_as_new(&self, entry: &Entry) -> Result<Option<usize>> {
        let sql = "INSERT IGNORE INTO `entry` (`catalog`,`ext_id`,`ext_url`,`ext_name`,`ext_desc`,`q`,`user`,`timestamp`,`random`,`type`) VALUES (:catalog,:ext_id,:ext_url,:ext_name,:ext_desc,:q,:user,:timestamp,:random,:type_name)";
        let type_name = crate::entry::normalize_entry_type(entry.type_name.as_deref());
        let params = params! {
            "catalog" => entry.catalog,
            "ext_id" => entry.ext_id.to_owned(),
            "ext_url" => entry.ext_url.to_owned(),
            "ext_name" => entry.ext_name.to_owned(),
            "ext_desc" => entry.ext_desc.to_owned(),
            "q" => entry.q,
            "user" => entry.user,
            "timestamp" => entry.timestamp.to_owned(),
            "random" => entry.random,
            "type_name" => type_name,
        };
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params).await?;
        let id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(Some(id))
    }

    async fn entry_delete(&self, entry_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        for table in TABLES_WITH_ENTRY_ID_FIELDS {
            let sql = format!("DELETE FROM `{table}` WHERE `entry_id`=:entry_id");
            conn.exec_drop(sql, params! {entry_id}).await?;
        }
        let sql = "DELETE FROM `entry` WHERE `id`=:entry_id";
        conn.exec_drop(sql, params! {entry_id}).await?;
        Ok(())
    }

    async fn entry_get_creation_time(&self, entry_id: usize) -> Option<String> {
        let mut conn = self.get_conn_ro().await.ok()?;
        let results = conn
            .exec_iter(
                r"SELECT `timestamp` FROM `entry_creation` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .ok()?
            .map_and_drop(from_row::<String>)
            .await
            .ok()?;
        results.first().map(|s| s.to_owned())
    }

    async fn entry_set_ext_name(&self, ext_name: &str, entry_id: usize) -> Result<()> {
        let sql = "UPDATE `entry` SET `ext_name`=SUBSTR(:ext_name,1,127) WHERE `id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {ext_name,entry_id}).await?;
        Ok(())
    }

    async fn entry_set_auxiliary_in_wikidata(
        &self,
        in_wikidata: bool,
        aux_id: usize,
    ) -> Result<()> {
        let sql = "UPDATE `auxiliary` SET `in_wikidata`=:in_wikidata WHERE `id`=:aux_id AND `in_wikidata`!=:in_wikidata";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {in_wikidata,aux_id}).await?;
        Ok(())
    }

    async fn entry_set_ext_desc(&self, ext_desc: &str, entry_id: usize) -> Result<()> {
        let sql = "UPDATE `entry` SET `ext_desc`=SUBSTR(:ext_desc,1,254) WHERE `id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {ext_desc,entry_id}).await?;
        Ok(())
    }

    async fn entry_set_ext_id(&self, ext_id: &str, entry_id: usize) -> Result<()> {
        let sql = "UPDATE `entry` SET `ext_id`=:ext_id WHERE `id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {ext_id,entry_id}).await?;
        Ok(())
    }

    async fn entry_set_ext_url(&self, ext_url: &str, entry_id: usize) -> Result<()> {
        let sql = "UPDATE `entry` SET `ext_url`=:ext_url WHERE `id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {ext_url,entry_id}).await?;
        Ok(())
    }

    async fn entry_set_type_name(&self, type_name: Option<String>, entry_id: usize) -> Result<()> {
        let sql = "UPDATE `entry` SET `type`=:type_name WHERE `id`=:entry_id";
        // Enforce the storage contract: either "" or Qxxx. The legacy
        // label "person" is translated to "Q5" for back-compat with
        // pre-Rust imports that may still hand it in.
        let type_name = crate::entry::normalize_entry_type(type_name.as_deref());
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {type_name,entry_id}).await?;
        Ok(())
    }

    async fn entry_delete_person_dates(&self, entry_id: usize) -> Result<()> {
        let sql = "DELETE FROM `person_dates` WHERE `entry_id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id}).await?;
        Ok(())
    }

    async fn entry_set_person_dates(
        &self,
        entry_id: usize,
        born: String,
        died: String,
    ) -> Result<()> {
        let sql =
            "REPLACE INTO `person_dates` (`entry_id`,`born`,`died`) VALUES (:entry_id,:born,:died)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,born,died}).await?;
        Ok(())
    }

    async fn entry_get_person_dates(
        &self,
        entry_id: usize,
    ) -> Result<(Option<String>, Option<String>)> {
        let mut conn = self.get_conn_ro().await?;
        let mut rows: Vec<(String, String)> = conn
            .exec_iter(
                r"SELECT `born`,`died` FROM `person_dates` WHERE `entry_id`=:entry_id LIMIT 1",
                params! {entry_id},
            )
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        match rows.pop() {
            Some(bd) => {
                let born = if bd.0.is_empty() { None } else { Some(bd.0) };
                let died = if bd.1.is_empty() { None } else { Some(bd.1) };
                Ok((born, died))
            }
            None => Ok((None, None)),
        }
    }

    async fn entry_remove_language_description(
        &self,
        entry_id: usize,
        language: &str,
    ) -> Result<()> {
        let sql = "DELETE FROM `descriptions` WHERE `entry_id`=:entry_id AND `language`=:language";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,language}).await?;
        Ok(())
    }

    async fn entry_set_language_description(
        &self,
        entry_id: usize,
        language: &str,
        text: String,
    ) -> Result<()> {
        let sql = "REPLACE INTO `descriptions` (`entry_id`,`language`,`label`) VALUES (:entry_id,:language,:text)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,language,text})
            .await?;
        Ok(())
    }

    /// Returns a LocaleString Vec of all aliases of the entry
    async fn entry_get_aliases(&self, entry_id: usize) -> Result<Vec<LocaleString>> {
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<(String, String)> = conn
            .exec_iter(
                r"SELECT `language`,`label` FROM `aliases` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        let ret = rows.iter().map(|(k, v)| LocaleString::new(k, v)).collect();
        Ok(ret)
    }

    async fn entry_add_alias(&self, entry_id: usize, language: &str, label: &str) -> Result<()> {
        let sql = "INSERT IGNORE INTO `aliases` (`entry_id`,`language`,`label`) VALUES (:entry_id,:language,:label)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,language,label})
            .await?;
        Ok(())
    }

    async fn entry_get_language_descriptions(
        &self,
        entry_id: usize,
    ) -> Result<HashMap<String, String>> {
        let rows: Vec<(String, String)> = self
            .get_conn_ro()
            .await?
            .exec_iter(
                r"SELECT /* rust:storage:entry_get_language_descriptions */ `language`,`label` FROM `descriptions` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        let mut map: HashMap<String, String> = HashMap::new();
        rows.iter().for_each(|(k, v)| {
            map.insert(k.to_string(), v.to_string());
        });
        Ok(map)
    }

    async fn entry_remove_auxiliary(&self, entry_id: usize, prop_numeric: usize) -> Result<()> {
        let sql = "DELETE /* rust:storage:entry_remove_auxiliary */ FROM `auxiliary` WHERE `entry_id`=:entry_id AND `aux_p`=:prop_numeric";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,prop_numeric}).await?;
        Ok(())
    }

    async fn entry_set_auxiliary(
        &self,
        entry_id: usize,
        prop_numeric: usize,
        value: String,
    ) -> Result<()> {
        let sql = "REPLACE /* rust:storage:entry_set_auxiliary */ INTO `auxiliary` (`entry_id`,`aux_p`,`aux_name`) VALUES (:entry_id,:prop_numeric,:value)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,prop_numeric,value})
            .await?;
        Ok(())
    }

    async fn entry_remove_coordinate_location(&self, entry_id: usize) -> Result<()> {
        let sql = "DELETE /* rust:storage:entry_remove_coordinate_location */ FROM `location` WHERE `entry_id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id}).await?;
        Ok(())
    }

    async fn entry_set_coordinate_location(
        &self,
        entry_id: usize,
        lat: f64,
        lon: f64,
        precision: Option<f64>,
    ) -> Result<()> {
        let sql = "REPLACE /* rust:storage:entry_set_coordinate_location */ INTO `location` (`entry_id`,`lat`,`lon`,`precision`) VALUES (:entry_id,:lat,:lon,:precision)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,lat,lon,precision})
            .await?;
        Ok(())
    }

    async fn entry_get_coordinate_location(
        &self,
        entry_id: usize,
    ) -> Result<Option<CoordinateLocation>> {
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(
                r"SELECT /* rust:storage:entry_get_coordinate_location */ `lat`,`lon`,`precision` FROM `location` WHERE `entry_id`=:entry_id LIMIT 1",
                params! {entry_id},
            )
            .await?
            .map_and_drop(|row| {
                let lat: f64 = row.get::<Option<f64>, _>(0).flatten().unwrap_or_default();
                let lon: f64 = row.get::<Option<f64>, _>(1).flatten().unwrap_or_default();
                let precision: Option<f64> = row.get_opt(2).and_then(|r| r.ok());
                CoordinateLocation::new_with_precision(lat, lon, precision)
            })
            .await?
            .pop();
        Ok(ret)
    }

    async fn entry_get_aux(&self, entry_id: usize) -> Result<Vec<AuxiliaryRow>> {
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(r"SELECT /* rust:storage:entry_get_aux */ `id`,`aux_p`,`aux_name`,`in_wikidata`,`entry_is_matched` FROM `auxiliary` WHERE `entry_id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(|row| AuxiliaryRow::from_row(&row)).await?
            .into_iter().flatten().collect();
        Ok(ret)
    }

    // Returns "was changed" (true/false)
    async fn entry_set_match(
        &self,
        entry: &Entry,
        user_id: usize,
        q_numeric: isize,
        timestamp: &str,
    ) -> Result<bool> {
        let entry_id = entry.get_valid_id()?;
        let mut sql = "UPDATE /* rust:storage:entry_set_match */ `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id AND (`q` IS NULL OR `q`!=:q_numeric OR `user`!=:user_id)".to_string();
        if user_id == USER_AUTO {
            if self.avoid_auto_match(entry_id, Some(q_numeric)).await? {
                return Ok(false); // Nothing wrong but shouldn't be matched
            }
            sql += &MatchState::not_fully_matched().get_sql();
        }
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {q_numeric,user_id,timestamp,entry_id})
            .await?;
        let nothing_changed = conn.affected_rows() == 0;
        drop(conn);
        if nothing_changed {
            return Ok(false);
        }
        self.entry_set_match_cleanup(entry, user_id, q_numeric)
            .await
    }

    async fn entry_set_match_status(
        &self,
        entry_id: usize,
        status: &str,
        is_matched: i32,
    ) -> Result<()> {
        let f1 = async {
            let timestamp = TimeStamp::now();
            let mut conn = self.get_conn().await?;
            conn.exec_drop(r"INSERT INTO `wd_matches` (`entry_id`,`status`,`timestamp`,`catalog`) VALUES (:entry_id,:status,:timestamp,(SELECT entry.catalog FROM entry WHERE entry.id=:entry_id)) ON DUPLICATE KEY UPDATE `status`=:status,`timestamp`=:timestamp",params! {entry_id,status,timestamp}).await
        };
        let f2 = async {
            let mut conn = self.get_conn().await?;
            conn.exec_drop(
                r"UPDATE `person_dates` SET is_matched=:is_matched WHERE entry_id=:entry_id",
                params! {is_matched,entry_id},
            )
            .await
        };
        let f3 = async {
            let mut conn = self.get_conn().await?;
            conn.exec_drop(
                r"UPDATE `auxiliary` SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",
                params! {is_matched,entry_id},
            )
            .await
        };
        let f4 = async {
            let mut conn = self.get_conn().await?;
            conn.exec_drop(
            r"UPDATE `statement_text` SET entry_is_matched=:is_matched WHERE entry_id=:entry_id",
            params! {is_matched,entry_id},
            )
            .await
        };
        let _ = tokio::try_join!(f1, f2, f3, f4)?;
        Ok(())
    }

    /// Removes multi-matches for an entry, eg when the entry has been fully matched.
    async fn entry_remove_multi_match(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                r"DELETE FROM multi_match WHERE entry_id=:entry_id",
                params! {entry_id},
            )
            .await?;
        Ok(())
    }

    async fn entry_unmatch(&self, entry_id: usize) -> Result<()> {
        let f1 = async {
            let mut conn = self.get_conn().await?;
            conn.exec_drop(
                r"UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `id`=:entry_id",
                params! {entry_id},
            )
            .await
            .map_err(|e| anyhow!(e))
        };
        let f2 = async { self.entry_set_match_status(entry_id, "UNKNOWN", 0).await };
        let _ = tokio::try_join!(f1, f2)?;
        Ok(())
    }

    async fn entry_get_multi_matches(&self, entry_id: usize) -> Result<Vec<String>> {
        Ok(self
            .get_conn_ro()
            .await?
            .exec_iter(
                r"SELECT candidates FROM multi_match WHERE entry_id=:entry_id",
                params! {entry_id},
            )
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    async fn entry_set_multi_match(
        &self,
        entry_id: usize,
        candidates: String,
        candidates_count: usize,
    ) -> Result<()> {
        let sql = r"REPLACE INTO `multi_match` (entry_id,catalog,candidates,candidate_count) VALUES (:entry_id,(SELECT catalog FROM entry WHERE id=:entry_id),:candidates,:candidates_count)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,candidates,candidates_count})
            .await?;
        Ok(())
    }

    async fn app_state_seppuku_get_running(&self, ts: &str) -> (usize, usize) {
        let sql = format!("SELECT
                        (SELECT count(*) FROM jobs WHERE `status` IN ('RUNNING')) AS running,
                        (SELECT count(*) FROM jobs WHERE `status` IN ('RUNNING') AND last_ts>='{ts}') AS running_recent");
        let mut conn = self.get_conn_ro().await.expect("seppuku: No DB connection");
        let (running, running_recent) = *conn
            .exec_iter(sql, ())
            .await
            .expect("seppuku: No results")
            .map_and_drop(from_row::<(usize, usize)>)
            .await
            .expect("seppuku: Result retrieval failure")
            .first()
            .expect("seppuku: No DB results");
        (running, running_recent)
    }

    // CERSEI methods now live on a separate `impl CerseiQueries for
    // StorageMySQL` block (further down).

    // MetaEntry methods now live on `impl MetaEntryQueries for StorageMySQL`
    // (further down in this file).

    // ===== API support methods =====

    async fn get_user_by_name(&self, name: &str) -> Result<Option<(usize, String, bool)>> {
        let sql = "SELECT `id`,`name`,`is_catalog_admin` FROM `user` WHERE `name`=:name LIMIT 1";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { name })
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let uname: String = row
                    .get::<Option<String>, _>("name")
                    .flatten()
                    .unwrap_or_default();
                let is_admin: u8 = row
                    .get::<Option<u8>, _>("is_catalog_admin")
                    .flatten()
                    .unwrap_or(0);
                (id, uname, is_admin != 0)
            })
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn get_or_create_user_id(&self, name: &str) -> Result<usize> {
        let sql = "SELECT `id` FROM `user` WHERE `name`=:name LIMIT 1";
        let mut conn = self.get_conn().await?;
        let rows = conn
            .exec_iter(sql, params! { name })
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        if let Some(id) = rows.first() {
            return Ok(*id);
        }
        // Insert new user
        conn.exec_drop(
            "INSERT INTO `user` (`name`) VALUES (:name)",
            params! { name },
        )
        .await?;
        Ok(conn.last_insert_id().unwrap_or(0) as usize)
    }

    async fn get_users_by_ids(
        &self,
        user_ids: &[usize],
    ) -> Result<HashMap<usize, serde_json::Value>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = user_ids.iter().join(",");
        let sql = format!("SELECT * FROM `user` WHERE `id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let name: String = row
                    .get::<Option<String>, _>("name")
                    .flatten()
                    .unwrap_or_default();
                let is_catalog_admin: u8 = row
                    .get::<Option<u8>, _>("is_catalog_admin")
                    .flatten()
                    .unwrap_or(0);
                (
                    id,
                    json!({
                        "id": id,
                        "name": name,
                        "is_catalog_admin": is_catalog_admin
                    }),
                )
            })
            .await?;
        Ok(rows.into_iter().collect())
    }

    async fn api_get_person_dates_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, (String, String)>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!("SELECT * FROM `person_dates` WHERE `entry_id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let born: String = row
                    .get::<Option<String>, _>("year_born")
                    .flatten()
                    .unwrap_or_default();
                let died: String = row
                    .get::<Option<String>, _>("year_died")
                    .flatten()
                    .unwrap_or_default();
                (entry_id, born, died)
            })
            .await?;
        let mut ret = HashMap::new();
        for (entry_id, born, died) in rows {
            if !born.is_empty() || !died.is_empty() {
                ret.insert(entry_id, (born, died));
            }
        }
        Ok(ret)
    }

    async fn api_get_locations_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, (f64, f64)>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!("SELECT * FROM `location` WHERE `entry_id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let lat: f64 = row.get::<Option<f64>, _>("lat").flatten().unwrap_or(0.0);
                let lon: f64 = row.get::<Option<f64>, _>("lon").flatten().unwrap_or(0.0);
                (entry_id, lat, lon)
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(|(eid, lat, lon)| (eid, (lat, lon)))
            .collect())
    }

    async fn api_get_multi_match_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, String>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!("SELECT * FROM `multi_match` WHERE `entry_id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let candidates: String = row
                    .get::<Option<String>, _>("candidates")
                    .flatten()
                    .unwrap_or_default();
                (entry_id, candidates)
            })
            .await?;
        Ok(rows.into_iter().collect())
    }

    async fn api_get_auxiliary_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, Vec<serde_json::Value>>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!("SELECT * FROM `auxiliary` WHERE `entry_id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let aux_p: usize = row.get::<Option<usize>, _>("aux_p").flatten().unwrap_or(0);
                let aux_name: String = row
                    .get::<Option<String>, _>("aux_name")
                    .flatten()
                    .unwrap_or_default();
                let in_wikidata: u8 = row
                    .get::<Option<u8>, _>("in_wikidata")
                    .flatten()
                    .unwrap_or(0);
                (
                    entry_id,
                    json!({
                        "id": id,
                        "entry_id": entry_id,
                        "aux_p": aux_p,
                        "aux_name": aux_name,
                        "in_wikidata": in_wikidata
                    }),
                )
            })
            .await?;
        let mut ret: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();
        for (entry_id, val) in rows {
            ret.entry(entry_id).or_default().push(val);
        }
        Ok(ret)
    }

    async fn api_get_aliases_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, Vec<serde_json::Value>>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!(
            "SELECT * FROM `aliases` WHERE `entry_id` IN ({ids_str}) ORDER BY `entry_id`,`language`,`label`"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let language: String = row
                    .get::<Option<String>, _>("language")
                    .flatten()
                    .unwrap_or_default();
                let label: String = row
                    .get::<Option<String>, _>("label")
                    .flatten()
                    .unwrap_or_default();
                (
                    entry_id,
                    json!({
                        "id": id,
                        "entry_id": entry_id,
                        "language": language,
                        "label": label
                    }),
                )
            })
            .await?;
        let mut ret: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();
        for (entry_id, val) in rows {
            ret.entry(entry_id).or_default().push(val);
        }
        Ok(ret)
    }

    async fn api_get_descriptions_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, Vec<serde_json::Value>>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!(
            "SELECT * FROM `descriptions` WHERE `entry_id` IN ({ids_str}) ORDER BY `entry_id`,`language`,`label`"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let language: String = row
                    .get::<Option<String>, _>("language")
                    .flatten()
                    .unwrap_or_default();
                let label: String = row
                    .get::<Option<String>, _>("label")
                    .flatten()
                    .unwrap_or_default();
                (
                    entry_id,
                    json!({
                        "id": id,
                        "entry_id": entry_id,
                        "language": language,
                        "label": label
                    }),
                )
            })
            .await?;
        let mut ret: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();
        for (entry_id, val) in rows {
            ret.entry(entry_id).or_default().push(val);
        }
        Ok(ret)
    }

    async fn api_get_kv_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, Vec<(String, String, u8)>>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!("SELECT * FROM `kv_entry` WHERE `entry_id` IN ({ids_str})");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let key: String = row
                    .get::<Option<String>, _>("kv_key")
                    .flatten()
                    .unwrap_or_default();
                let value: String = row
                    .get::<Option<String>, _>("kv_value")
                    .flatten()
                    .unwrap_or_default();
                let done: u8 = row.get::<Option<u8>, _>("done").flatten().unwrap_or(0);
                (entry_id, key, value, done)
            })
            .await?;
        let mut ret: HashMap<usize, Vec<(String, String, u8)>> = HashMap::new();
        for (entry_id, key, value, done) in rows {
            ret.entry(entry_id).or_default().push((key, value, done));
        }
        Ok(ret)
    }

    async fn api_get_mnm_relations_for_entries(
        &self,
        entry_ids: &[usize],
    ) -> Result<HashMap<usize, Vec<serde_json::Value>>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_str = entry_ids.iter().join(",");
        let sql = format!(
            "SELECT `property`,`mnm_relation`.`entry_id` AS `source_entry_id`,`entry`.* FROM `mnm_relation`,`entry` WHERE `entry`.`id`=`mnm_relation`.`target_entry_id` AND `mnm_relation`.`entry_id` IN ({ids_str})"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let property: usize = row
                    .get::<Option<usize>, _>("property")
                    .flatten()
                    .unwrap_or(0);
                let source_entry_id: usize = row
                    .get::<Option<usize>, _>("source_entry_id")
                    .flatten()
                    .unwrap_or(0);
                let target_entry = Self::entry_from_row(&row);
                (source_entry_id, property, target_entry)
            })
            .await?;
        let mut ret: HashMap<usize, Vec<serde_json::Value>> = HashMap::new();
        for (source_entry_id, property, target_entry) in rows {
            if let Some(entry) = target_entry {
                let val = json!({
                    "property": property,
                    "source_entry_id": source_entry_id,
                    "target_entry_id": entry.id,
                    "target_catalog": entry.catalog,
                    "target_ext_id": entry.ext_id,
                    "target_ext_url": entry.ext_url,
                    "target_ext_name": entry.ext_name,
                    "target_ext_desc": entry.ext_desc,
                    "target_q": entry.q,
                    "target_user": entry.user,
                    "target_type": entry.type_name,
                });
                ret.entry(source_entry_id).or_default().push(val);
            }
        }
        Ok(ret)
    }

    async fn api_get_catalog_overview(&self) -> Result<Vec<serde_json::Value>> {
        self.api_get_catalog_overview_impl(None).await
    }

    async fn api_get_single_catalog_overview(
        &self,
        catalog_id: usize,
    ) -> Result<serde_json::Value> {
        // Push the id filter into SQL — avoids scanning the entire active
        // catalog set (which made this call take ~10s on prod data).
        let rows = self
            .api_get_catalog_overview_impl(Some(&[catalog_id]))
            .await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow!("Catalog {catalog_id} not found in overview"))
    }

    async fn api_get_catalog_info(&self, catalog_id: usize) -> Result<serde_json::Value> {
        let sql = format!("SELECT * FROM `catalog` WHERE `id`={catalog_id} AND `active`>=1");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow!("Catalog {catalog_id} not found"))
    }

    async fn api_get_catalog_type_counts(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT `type`,count(*) AS `cnt` FROM `entry` WHERE `catalog`=:catalog_id GROUP BY `type` ORDER BY `cnt` DESC";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let type_name: String = row
                    .get::<Option<String>, _>("type")
                    .flatten()
                    .unwrap_or_default();
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                json!({"type": type_name, "cnt": cnt})
            })
            .await?;
        Ok(rows)
    }

    async fn api_get_catalog_match_by_month(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT substring(`timestamp`,1,6) AS `ym`,count(*) AS `cnt` FROM `entry` WHERE `catalog`=:catalog_id AND `timestamp` IS NOT NULL AND `user`!=0 GROUP BY `ym` ORDER BY `ym`";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let ym: String = row
                    .get::<Option<String>, _>("ym")
                    .flatten()
                    .unwrap_or_default();
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                json!({"ym": ym, "cnt": cnt})
            })
            .await?;
        Ok(rows)
    }

    async fn api_get_catalog_matcher_by_user(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT `name` AS `username`,`entry`.`user` AS `uid`,count(*) AS `cnt` FROM `entry`,`user` WHERE `catalog`=:catalog_id AND `entry`.`user`=`user`.`id` AND `user`!=0 AND `entry`.`user` IS NOT NULL GROUP BY `uid` ORDER BY `cnt` DESC";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let username: String = row
                    .get::<Option<String>, _>("username")
                    .flatten()
                    .unwrap_or_default();
                let uid: usize = row.get::<Option<usize>, _>("uid").flatten().unwrap_or(0);
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                json!({"username": username, "uid": uid, "cnt": cnt})
            })
            .await?;
        Ok(rows)
    }

    async fn api_get_jobs(
        &self,
        catalog_id: usize,
        start: usize,
        max: usize,
        status_filter: &str,
    ) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>, usize)> {
        let mut conn = self.get_conn_ro().await?;

        // Stats (only when catalog_id==0, as arrays [status, cnt] for the frontend)
        let job_stats = if catalog_id == 0 {
            conn.exec_iter(
                "SELECT `status`,count(*) AS `cnt` FROM `jobs` WHERE `status`!='BLOCKED' GROUP BY `status` ORDER BY `status`",
                (),
            )
            .await?
            .map_and_drop(|row: Row| {
                let status: String = row.get::<Option<String>, _>("status").flatten().unwrap_or_default();
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                json!([status, cnt])
            })
            .await?
        } else {
            vec![]
        };

        // Build WHERE clause
        let mut filters = vec!["1=1".to_string(), "`status`!='BLOCKED'".to_string()];
        if catalog_id > 0 {
            filters.push(format!("`catalog`={catalog_id}"));
        }
        // `status_filter` is a comma-separated list of statuses to include.
        // Empty = no filter. Single value still works (one-element list).
        // Each value is validated against `JobStatus::new` so unknown
        // statuses can't smuggle SQL through the IN list.
        let allowed_statuses: Vec<&str> = status_filter
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .filter(|s| crate::job_status::JobStatus::new(s).is_some())
            .collect();
        if !allowed_statuses.is_empty() {
            let in_list = allowed_statuses
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(",");
            filters.push(format!("`status` IN ({in_list})"));
        }
        let where_clause = filters.join(" AND ");

        // Total count for pagination
        let count_sql = format!("SELECT count(*) AS cnt FROM `jobs` WHERE {where_clause}");
        let total: usize = conn
            .exec_iter(count_sql, ())
            .await?
            .map_and_drop(|row: Row| row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0))
            .await?
            .into_iter()
            .next()
            .unwrap_or(0);

        // Jobs with catalog name
        let jobs_sql = format!(
            "SELECT `jobs`.*,\
            (SELECT `user`.`name` FROM `user` WHERE `user`.`id`=`jobs`.`user_id`) AS `user_name`,\
            (SELECT `catalog`.`name` FROM `catalog` WHERE `catalog`.`id`=`jobs`.`catalog`) AS `catalog_name` \
            FROM `jobs` WHERE {where_clause} \
            ORDER BY FIELD(`status`,'RUNNING','FAILED','TODO','LOW_PRIORITY','PAUSED','DONE'),\
            `last_ts` DESC,`next_ts` DESC LIMIT {max} OFFSET {start}"
        );
        let jobs = conn
            .exec_iter(jobs_sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let catalog: usize = row
                    .get::<Option<usize>, _>("catalog")
                    .flatten()
                    .unwrap_or(0);
                let action: String = row
                    .get::<Option<String>, _>("action")
                    .flatten()
                    .unwrap_or_default();
                let status: String = row
                    .get::<Option<String>, _>("status")
                    .flatten()
                    .unwrap_or_default();
                let last_ts: String = row
                    .get::<Option<String>, _>("last_ts")
                    .flatten()
                    .unwrap_or_default();
                let next_ts: String = row
                    .get::<Option<String>, _>("next_ts")
                    .flatten()
                    .unwrap_or_default();
                let repeat_after_sec: Option<usize> =
                    row.get::<Option<usize>, _>("repeat_after_sec").flatten();
                let depends_on: Option<usize> = row.get::<Option<usize>, _>("depends_on").flatten();
                let user_id: Option<usize> = row.get::<Option<usize>, _>("user_id").flatten();
                let user_name: Option<String> = row.get::<Option<String>, _>("user_name").flatten();
                let catalog_name: Option<String> =
                    row.get::<Option<String>, _>("catalog_name").flatten();
                let note: Option<String> = row.get::<Option<String>, _>("note").flatten();
                let json_str: Option<String> = row.get::<Option<String>, _>("json").flatten();
                json!({
                    "id": id, "catalog": catalog, "catalog_name": catalog_name,
                    "action": action, "status": status,
                    "last_ts": last_ts, "next_ts": next_ts,
                    "repeat_after_sec": repeat_after_sec, "depends_on": depends_on,
                    "user_id": user_id, "user_name": user_name, "note": note, "json": json_str
                })
            })
            .await?;

        Ok((job_stats, jobs, total))
    }

    async fn api_get_issues_count(&self, issue_type: &str, catalogs: &str) -> Result<usize> {
        let mut sql = "SELECT count(*) AS `cnt` FROM `issues` WHERE `status`='OPEN'".to_string();
        if !issue_type.is_empty() {
            sql += &format!(" AND `type`='{}'", issue_type.replace('\'', "''"));
        }
        if !catalogs.is_empty() {
            sql += &format!(" AND `catalog` IN ({catalogs})");
        }
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(*rows.first().unwrap_or(&0))
    }

    async fn api_get_issues(
        &self,
        issue_type: &str,
        catalogs: &str,
        limit: usize,
        offset: usize,
        random_threshold: f64,
    ) -> Result<Vec<serde_json::Value>> {
        let mut sql = format!(
            "SELECT * FROM `issues` WHERE `status`='OPEN' AND `random`>={random_threshold}"
        );
        if !issue_type.is_empty() {
            sql += &format!(" AND `type`='{}'", issue_type.replace('\'', "''"));
        }
        if !catalogs.is_empty() {
            sql += &format!(" AND `catalog` IN ({catalogs})");
        }
        sql += &format!(" ORDER BY `random` LIMIT {limit} OFFSET {offset}");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let entry_id: usize = row
                    .get::<Option<usize>, _>("entry_id")
                    .flatten()
                    .unwrap_or(0);
                let row_issue_type: String = row
                    .get::<Option<String>, _>("type")
                    .flatten()
                    .unwrap_or_default();
                let json_str: String = row
                    .get::<Option<String>, _>("json")
                    .flatten()
                    .unwrap_or_default();
                let json_val: serde_json::Value =
                    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null);
                let status: String = row
                    .get::<Option<String>, _>("status")
                    .flatten()
                    .unwrap_or_default();
                let catalog: usize = row
                    .get::<Option<usize>, _>("catalog")
                    .flatten()
                    .unwrap_or(0);
                let random: f64 = row.get::<Option<f64>, _>("random").flatten().unwrap_or(0.0);
                json!({
                    "id": id, "entry_id": entry_id, "type": row_issue_type,
                    "json": json_val, "status": status, "catalog": catalog, "random": random
                })
            })
            .await?;
        Ok(rows)
    }

    async fn api_get_all_issues(&self, mode: &str) -> Result<Vec<serde_json::Value>> {
        // Only allow known safe view names to prevent SQL injection
        let view_name = match mode {
            "duplicate_items" => "vw_issues_duplicate_items",
            "mismatched_items" => "vw_issues_mismatched_items",
            "time_mismatch" => "vw_issues_time_mismatch",
            _ => return Err(anyhow!("Invalid issues mode: {mode}")),
        };
        let sql = format!("SELECT * FROM `{view_name}`");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_search_entries(
        &self,
        words: &[String],
        description_search: bool,
        no_label_search: bool,
        exclude: &[usize],
        include: &[usize],
        max_results: usize,
    ) -> Result<Vec<Entry>> {
        let Some(sql) = Self::build_api_search_entries_sql(
            words,
            description_search,
            no_label_search,
            exclude,
            include,
            max_results,
        ) else {
            return Ok(vec![]);
        };
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    async fn api_search_by_q(&self, q: isize, exclude_catalogs: &[usize]) -> Result<Vec<Entry>> {
        let mut sql = format!("{} WHERE `q`={q}", Self::entry_sql_select());
        if !exclude_catalogs.is_empty() {
            let list = exclude_catalogs
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(",");
            sql += &format!(" AND `catalog` NOT IN ({list})");
        }
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    async fn api_get_recent_changes(
        &self,
        ts: &str,
        catalog_id: usize,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<serde_json::Value>, usize)> {
        let ts_safe = escape_sql_literal(ts);
        // Human-matched entries only (user!=0,3,4), with a non-null
        // timestamp — those are the rows in `entry` we treat as "recent
        // changes". `log` covers everything else (removals, N/A marks,
        // historical edits).
        let ts_entry = if ts.is_empty() {
            String::new()
        } else {
            format!(" AND `timestamp`>='{ts_safe}'")
        };
        let ts_log = if ts.is_empty() {
            String::new()
        } else {
            format!(" AND log.timestamp>='{ts_safe}'")
        };
        let entry_catalog_filter = if catalog_id > 0 {
            format!(" AND `catalog`={catalog_id}")
        } else {
            String::new()
        };
        let log_catalog_filter = if catalog_id > 0 {
            format!(" AND entry.catalog={catalog_id}")
        } else {
            String::new()
        };

        // Push ORDER BY timestamp DESC LIMIT into each UNION branch so
        // each side uses the `timestamp` index for an early-terminating
        // reverse scan, instead of materialising the full union of both
        // tables and sorting it. The outer LIMIT then takes the top
        // `limit` of the merged (already-sorted) stream.
        //
        // Each branch gets LIMIT offset+limit so that the outer LIMIT
        // OFFSET can still land on the correct slice when one branch
        // dominates the other. With a tighter window (user's `ts`
        // filter in place) this is a near-instant indexed pick; without
        // it, it's still the difference between a full-table sort and a
        // bounded reverse scan.
        let branch_limit = limit.saturating_add(offset);
        let page_sql = format!(
            "SELECT * FROM ( \
                (SELECT entry.id AS id, entry.catalog AS catalog, \
                        entry.ext_id AS ext_id, entry.ext_url AS ext_url, \
                        entry.ext_name AS ext_name, entry.ext_desc AS ext_desc, \
                        entry.q AS q, entry.user AS user, entry.timestamp AS timestamp, \
                        'match' AS event_type \
                   FROM entry \
                  WHERE entry.user!=0 AND entry.user!=3 AND entry.user!=4 \
                    AND entry.timestamp IS NOT NULL{ts_entry}{entry_catalog_filter} \
                  ORDER BY entry.timestamp DESC LIMIT {branch_limit}) \
                UNION ALL \
                (SELECT entry.id AS id, entry.catalog AS catalog, \
                        entry.ext_id AS ext_id, entry.ext_url AS ext_url, \
                        entry.ext_name AS ext_name, entry.ext_desc AS ext_desc, \
                        entry.q AS q, log.user AS user, log.timestamp AS timestamp, \
                        log.action AS event_type \
                   FROM log INNER JOIN entry ON log.entry_id=entry.id \
                  WHERE log.timestamp IS NOT NULL{ts_log}{log_catalog_filter} \
                  ORDER BY log.timestamp DESC LIMIT {branch_limit}) \
            ) AS rc ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        // The count queries are the actual bottleneck on the unfiltered
        // global feed — they're full table scans / full joins. Only run
        // them when we have a selective filter (a time bound or a
        // specific catalog). Otherwise return a heuristic "at-least"
        // total so the UI can still paginate forward without pretending
        // to know the exact size of a rolling event stream.
        let run_counts = !ts.is_empty() || catalog_id > 0;
        let count_entry_sql = format!(
            "SELECT COUNT(*) AS cnt FROM entry \
             WHERE user!=0 AND user!=3 AND user!=4 AND timestamp IS NOT NULL{ts_entry}{entry_catalog_filter}"
        );
        let count_log_sql = format!(
            "SELECT COUNT(*) AS cnt FROM log INNER JOIN entry ON log.entry_id=entry.id \
             WHERE log.timestamp IS NOT NULL{ts_log}{log_catalog_filter}"
        );

        // Run page + the two totals concurrently. Count failures degrade to
        // 0 so the UI can still render; a listing failure is hard-fatal.
        let row_to_event = |row: Row| {
            // All nullable columns pulled through Option<T> — `row.get::<T, _>`
            // panics on NULL.
            let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
            let catalog: usize = row
                .get::<Option<usize>, _>("catalog")
                .flatten()
                .unwrap_or(0);
            let ext_id: String = row
                .get::<Option<String>, _>("ext_id")
                .flatten()
                .unwrap_or_default();
            let ext_url: String = row
                .get::<Option<String>, _>("ext_url")
                .flatten()
                .unwrap_or_default();
            let ext_name: String = row
                .get::<Option<String>, _>("ext_name")
                .flatten()
                .unwrap_or_default();
            let ext_desc: String = row
                .get::<Option<String>, _>("ext_desc")
                .flatten()
                .unwrap_or_default();
            let q: Option<isize> = row.get::<Option<isize>, _>("q").flatten();
            let user: Option<usize> = row.get::<Option<usize>, _>("user").flatten();
            let timestamp: String = row
                .get::<Option<String>, _>("timestamp")
                .flatten()
                .unwrap_or_default();
            let event_type: String = row
                .get::<Option<String>, _>("event_type")
                .flatten()
                .unwrap_or_default();
            json!({
                "id": id, "catalog": catalog,
                "ext_id": ext_id, "ext_url": ext_url, "ext_name": ext_name, "ext_desc": ext_desc,
                "q": q, "user": user, "timestamp": timestamp,
                "event_type": event_type
            })
        };

        let events = {
            let mut conn = self.get_conn_ro().await?;
            conn.exec_iter(page_sql, ())
                .await?
                .map_and_drop(row_to_event)
                .await?
        };

        let total = if run_counts {
            // Selective filter (time or catalog) — counts return quickly.
            let (count_entry_res, count_log_res) = tokio::join!(
                async {
                    let mut conn = self.get_conn_ro().await?;
                    let cnt = conn
                        .exec_iter(count_entry_sql, ())
                        .await?
                        .map_and_drop(from_row::<usize>)
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or(0);
                    Ok::<usize, anyhow::Error>(cnt)
                },
                async {
                    let mut conn = self.get_conn_ro().await?;
                    let cnt = conn
                        .exec_iter(count_log_sql, ())
                        .await?
                        .map_and_drop(from_row::<usize>)
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or(0);
                    Ok::<usize, anyhow::Error>(cnt)
                },
            );
            count_entry_res.unwrap_or(0) + count_log_res.unwrap_or(0)
        } else {
            // Unfiltered global feed: a precise total would scan the
            // whole entry table and JOIN the whole log table. Return a
            // forward-progressing lower bound so the UI's "load next
            // page" button can keep working without claiming a size
            // we refuse to compute.
            offset
                .saturating_add(events.len())
                .saturating_add(if events.len() == limit { limit } else { 0 })
        };
        Ok((events, total))
    }

    async fn api_get_catalog_entries(
        &self,
        filter: &CatalogEntryListFilter,
    ) -> Result<(Vec<Entry>, usize)> {
        let where_clause = Self::catalog_entries_where_clause(filter);
        let page_sql = format!(
            "SELECT * FROM entry WHERE {where_clause} LIMIT {} OFFSET {}",
            filter.per_page, filter.offset
        );
        let count_sql = format!("SELECT COUNT(*) AS cnt FROM entry WHERE {where_clause}");

        // Independent SELECTs — run them in parallel. Count failures are lossy
        // (yield 0) to match the legacy behaviour; listing failures propagate.
        let (count_res, entries_res) = tokio::join!(
            async {
                let mut conn = self.get_conn_ro().await?;
                let cnt = conn
                    .exec_iter(count_sql, ())
                    .await?
                    .map_and_drop(from_row::<usize>)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or(0);
                Ok::<usize, anyhow::Error>(cnt)
            },
            async {
                let mut conn = self.get_conn_ro().await?;
                let rows: Vec<Entry> = conn
                    .exec_iter(page_sql, ())
                    .await?
                    .map_and_drop(|row| Self::entry_from_row(&row))
                    .await?
                    .into_iter()
                    .flatten()
                    .collect();
                Ok::<Vec<Entry>, anyhow::Error>(rows)
            }
        );
        let total_filtered = count_res.unwrap_or(0);
        let entries = entries_res?;
        Ok((entries, total_filtered))
    }

    async fn api_get_existing_job_actions(&self) -> Result<Vec<String>> {
        let sql =
            "SELECT DISTINCT `action` FROM `jobs` UNION SELECT DISTINCT `action` FROM `job_sizes`";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(rows)
    }

    async fn api_get_random_entry(
        &self,
        catalog_id: usize,
        submode: &str,
        entry_type: &str,
        active_catalogs: &[usize],
    ) -> Result<Option<Entry>> {
        let where_clause: &str = match submode {
            "prematched" => "user=0",
            "no_manual" => "(user=0 OR q IS NULL)",
            _ => "q IS NULL",
        };
        let type_filter = if entry_type.is_empty() {
            String::new()
        } else {
            format!(" AND `type`='{}'", entry_type.replace('\'', "''"))
        };

        let mut conn = self.get_conn_ro().await?;

        if catalog_id > 0 {
            // Catalog-specific: FORCE INDEX (catalog_q_random). Two attempts —
            // first with a random threshold, then wrap to threshold 0.
            let base = format!(
                "{} FORCE INDEX (`catalog_q_random`) WHERE `random`>=%R% AND `catalog`={catalog_id} AND {where_clause}{type_filter} ORDER BY `random` LIMIT 1",
                Self::entry_sql_select()
            );
            for threshold in [rand::random::<f64>(), 0.0] {
                let sql = base.replace("%R%", &format!("{threshold}"));
                let rows = conn
                    .exec_iter(sql, ())
                    .await?
                    .map_and_drop(|row| Self::entry_from_row(&row))
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<Entry>>();
                if let Some(entry) = rows.into_iter().next() {
                    return Ok(Some(entry));
                }
            }
            return Ok(None);
        }

        // Global: FORCE INDEX (random_2), 11 attempts, filter by active_catalogs.
        //
        // We tried collapsing the retry loop into one query that filtered by
        // `catalog.active=1` via an EXISTS subquery — the EXPLAIN looked clean
        // (range scan + eq_ref to catalog.PRIMARY, est. 1 row from `c`) but the
        // optimiser walked massive numbers of `random_2` rows in practice, so
        // the response hung past 30s. Stick with the multi-attempt + Rust-side
        // filter approach which performs reliably on the live replica.
        let base = format!(
            "{} FORCE INDEX (`random_2`) WHERE `random`>=%R% AND {where_clause}{type_filter} ORDER BY `random` LIMIT 10",
            Self::entry_sql_select()
        );
        for attempt in 0..=10 {
            let threshold = if attempt >= 10 {
                0.0
            } else {
                rand::random::<f64>()
            };
            let sql = base.replace("%R%", &format!("{threshold}"));
            let rows = conn
                .exec_iter(sql, ())
                .await?
                .map_and_drop(|row| Self::entry_from_row(&row))
                .await?
                .into_iter()
                .flatten()
                .collect::<Vec<Entry>>();
            for entry in rows {
                if active_catalogs.contains(&entry.catalog) {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    async fn api_get_active_catalog_ids(&self) -> Result<Vec<usize>> {
        let sql = "SELECT `id` FROM `catalog` WHERE `active`=1";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(rows)
    }

    async fn api_get_inactive_catalog_ids(&self) -> Result<Vec<usize>> {
        let sql = "SELECT `id` FROM `catalog` WHERE `active`!=1";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(rows)
    }

    async fn api_get_wd_props(&self) -> Result<Vec<usize>> {
        let sql = "SELECT DISTINCT wd_prop FROM catalog WHERE wd_prop!=0 AND wd_prop IS NOT NULL AND wd_qual IS NULL AND active=1 ORDER BY wd_prop";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(rows)
    }

    async fn api_get_top_missing(&self, catalogs: &str) -> Result<Vec<serde_json::Value>> {
        if catalogs.is_empty() {
            return Ok(vec![]);
        }
        let sql = format!(
            "SELECT ext_name,count(DISTINCT catalog) AS cnt FROM entry WHERE catalog IN ({catalogs}) AND (q IS NULL or user=0) GROUP BY ext_name HAVING cnt>1 ORDER BY cnt DESC LIMIT 500"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let ext_name: String = row
                    .get::<Option<String>, _>("ext_name")
                    .flatten()
                    .unwrap_or_default();
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                json!({"ext_name": ext_name, "cnt": cnt})
            })
            .await?;
        Ok(rows)
    }

    async fn api_get_common_names(
        &self,
        catalog_id: usize,
        type_q: &str,
        other_cats_desc: bool,
        min: usize,
        max: usize,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let cond1 = if other_cats_desc {
            " AND e2.ext_desc!=''"
        } else {
            ""
        };
        let type_filter = if !type_q.is_empty() {
            format!(" AND `type`='{}'", type_q.replace('\'', "''"))
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT /* api_get_common_names */ (SELECT count(*) FROM entry e2 WHERE e1.ext_name=e2.ext_name{cond1}) AS cnt,e1.* FROM entry e1 WHERE catalog={catalog_id} AND q IS NULL AND ext_name NOT LIKE '_. %' AND ext_name NOT LIKE '%?%' AND ext_name NOT LIKE '_ %'{type_filter} HAVING cnt>{min} AND cnt<{max} LIMIT {limit} OFFSET {offset}"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                let entry = Self::entry_from_row(&row);
                (cnt, entry)
            })
            .await?;
        let mut ret = Vec::new();
        for (cnt, entry) in rows {
            if let Some(e) = entry {
                ret.push(json!({
                    "cnt": cnt,
                    "id": e.id,
                    "catalog": e.catalog,
                    "ext_id": e.ext_id,
                    "ext_url": e.ext_url,
                    "ext_name": e.ext_name,
                    "ext_desc": e.ext_desc,
                    "q": e.q,
                    "user": e.user,
                    "timestamp": e.timestamp,
                    "type": e.type_name
                }));
            }
        }
        Ok(ret)
    }

    async fn api_get_locations_bbox(
        &self,
        lon_min: f64,
        lat_min: f64,
        lon_max: f64,
        lat_max: f64,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = format!(
            "SELECT entry.*,location.entry_id,location.lat,location.lon FROM entry,location WHERE location.entry_id=entry.id AND lon>={lon_min} AND lon<={lon_max} AND lat>={lat_min} AND lat<={lat_max} LIMIT 5000"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let lat: f64 = row.get::<Option<f64>, _>("lat").flatten().unwrap_or(0.0);
                let lon: f64 = row.get::<Option<f64>, _>("lon").flatten().unwrap_or(0.0);
                let entry = Self::entry_from_row(&row);
                (lat, lon, entry)
            })
            .await?;
        let mut ret = Vec::new();
        for (lat, lon, entry) in rows {
            if let Some(e) = entry {
                ret.push(json!({
                    "id": e.id,
                    "catalog": e.catalog,
                    "ext_id": e.ext_id,
                    "ext_url": e.ext_url,
                    "ext_name": e.ext_name,
                    "ext_desc": e.ext_desc,
                    "q": e.q,
                    "user": e.user,
                    "timestamp": e.timestamp,
                    "type": e.type_name,
                    "lat": lat,
                    "lon": lon
                }));
            }
        }
        Ok(ret)
    }

    async fn api_get_locations_in_catalog(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = format!("SELECT * FROM vw_location WHERE catalog={catalog_id}");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_get_download_entries(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(isize, String, String, String, Option<usize>)>> {
        let sql = format!(
            "SELECT q,ext_id,ext_url,ext_name,user FROM entry WHERE catalog={catalog_id} AND q IS NOT NULL AND q > 0 AND user!=0"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let q: isize = row.get::<Option<isize>, _>("q").flatten().unwrap_or(0);
                let ext_id: String = row
                    .get::<Option<String>, _>("ext_id")
                    .flatten()
                    .unwrap_or_default();
                let ext_url: String = row
                    .get::<Option<String>, _>("ext_url")
                    .flatten()
                    .unwrap_or_default();
                let ext_name: String = row
                    .get::<Option<String>, _>("ext_name")
                    .flatten()
                    .unwrap_or_default();
                let user: Option<usize> = row.get::<Option<usize>, _>("user").flatten();
                (q, ext_id, ext_url, ext_name, user)
            })
            .await?;
        Ok(rows)
    }

    async fn api_download2(
        &self,
        filter: &Download2Filter,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        // Build the column order up-front (same logic as build_download2_sql)
        // so the header is known even for empty result sets.
        let columns = Self::download2_columns(filter);
        let sql = Self::build_download2_sql(filter);
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let refs = row.columns_ref();
                let mut values = Vec::with_capacity(refs.len());
                for (i, _col) in refs.iter().enumerate() {
                    // Read as a string regardless of DB type — the caller emits
                    // TSV/JSON and reading non-string columns as String would
                    // panic.
                    let s = match &row[i] {
                        mysql_async::Value::NULL => String::new(),
                        mysql_async::Value::Int(n) => n.to_string(),
                        mysql_async::Value::UInt(n) => n.to_string(),
                        mysql_async::Value::Float(n) => n.to_string(),
                        mysql_async::Value::Double(n) => n.to_string(),
                        mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                        other => format!("{other:?}"),
                    };
                    values.push(s);
                }
                values
            })
            .await?;
        Ok((columns, rows))
    }

    async fn api_update_catalog_ext_urls(
        &self,
        catalog_id: usize,
        prefix: &str,
        suffix: &str,
    ) -> Result<()> {
        // The parent API path only passes non-empty, user-supplied template
        // fragments (no way to use named params for the concat() arguments on
        // the legacy MySQL build), so escape single quotes by doubling them —
        // same scheme PHP used. Identifier is an integer so no escaping needed.
        let sql = format!(
            "UPDATE entry SET ext_url=concat('{}',ext_id,'{}') WHERE catalog={catalog_id}",
            prefix.replace('\'', "''"),
            suffix.replace('\'', "''")
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn api_edit_catalog(
        &self,
        catalog_id: usize,
        name: &str,
        url: &str,
        desc: &str,
        type_name: &str,
        search_wp: &str,
        wd_prop: Option<usize>,
        wd_qual: Option<usize>,
        active: bool,
    ) -> Result<()> {
        let active_val: u8 = if active { 1 } else { 0 };
        // Coerce 0 → NULL before the UPDATE; the frontend form yields 0 when
        // a user clears the property field (input type=number), and leaving
        // that as-is would confuse downstream callers that treat `wd_prop
        // IS NOT NULL` as "this catalog has a property".
        let wd_prop = normalize_wd_prop(wd_prop);
        let wd_qual = normalize_wd_prop(wd_qual);
        let sql = "UPDATE catalog SET name=:name,url=:url,`desc`=:desc,`type`=:type_name,search_wp=:search_wp,wd_prop=:wd_prop,wd_qual=:wd_qual,active=:active_val WHERE id=:catalog_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! { name, url, desc, type_name, search_wp, wd_prop, wd_qual, active_val, catalog_id }).await?;
        Ok(())
    }

    async fn api_get_catalog_overview_for_ids(
        &self,
        catalog_ids: &[usize],
    ) -> Result<Vec<serde_json::Value>> {
        // Push the id filter into SQL — batch_catalogs with 5 ids used to
        // re-run four large unfiltered joins per catalog (~50s total).
        self.api_get_catalog_overview_impl(Some(catalog_ids)).await
    }

    async fn api_match_q_multi(
        &self,
        catalog_id: usize,
        ext_id: &str,
        q: isize,
        user_id: usize,
    ) -> Result<bool> {
        let entry = match self.entry_from_ext_id(catalog_id, ext_id).await {
            Ok(e) => e,
            Err(_) => return Ok(false),
        };
        let timestamp = TimeStamp::now();
        self.entry_set_match(&entry, user_id, q, &timestamp).await
    }

    async fn api_remove_all_q(&self, catalog_id: usize, q: isize) -> Result<()> {
        let sql = format!(
            "UPDATE entry SET q=NULL,user=NULL,timestamp=NULL WHERE catalog={catalog_id} AND user=0 AND q={q}"
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn api_remove_all_multimatches(&self, entry_id: usize) -> Result<()> {
        let sql = format!("DELETE FROM multi_match WHERE entry_id={entry_id}");
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn api_suggest(
        &self,
        catalog_id: usize,
        ext_id: &str,
        q: isize,
        overwrite: bool,
    ) -> Result<bool> {
        let ts = TimeStamp::now();
        let overwrite_cond = if overwrite {
            "AND (user=0 OR q IS NULL)"
        } else {
            "AND (q IS NULL)"
        };
        let sql = format!(
            "UPDATE entry SET q={q},user=0,timestamp='{ts}' WHERE catalog=:catalog_id AND ext_id=:ext_id {overwrite_cond}"
        );
        let mut conn = self.get_conn().await?;
        let result = conn.exec_iter(sql, params! { catalog_id, ext_id }).await?;
        let affected = result.affected_rows();
        drop(result);
        Ok(affected > 0)
    }

    async fn api_add_alias(
        &self,
        catalog_id: usize,
        ext_id: &str,
        language: &str,
        label: &str,
        user_id: usize,
    ) -> Result<()> {
        let sql = "INSERT IGNORE INTO aliases (entry_id,language,label,added_by_user) VALUES ((SELECT id FROM entry WHERE catalog=:catalog_id AND ext_id=:ext_id LIMIT 1),:language,:label,:user_id)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(
            sql,
            params! { catalog_id, ext_id, language, label, user_id },
        )
        .await?;
        Ok(())
    }

    async fn api_get_cersei_catalog(&self, scraper_id: usize) -> Result<Option<usize>> {
        let sql = "SELECT catalog_id FROM cersei WHERE cersei_scraper_id=:scraper_id";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { scraper_id })
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn api_get_same_names(&self) -> Result<(String, Vec<Entry>)> {
        let sql = "SELECT ext_name,count(*) AS cnt,SUM(if(q IS NOT NULL OR q=0, 1, 0)) AS matched FROM entry GROUP BY ext_name HAVING cnt>1 AND cnt<10 AND matched>0 AND matched<cnt LIMIT 10000";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let ext_name: String = row
                    .get::<Option<String>, _>("ext_name")
                    .flatten()
                    .unwrap_or_default();
                ext_name
            })
            .await?;
        if rows.is_empty() {
            return Ok((String::new(), vec![]));
        }
        let idx = rand::rng().random_range(0..rows.len());
        let name = rows[idx].clone();
        let entry_sql = format!("{} WHERE ext_name=:name", Self::entry_sql_select());
        let entries = conn
            .exec_iter(entry_sql, params! { name })
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok((rows[idx].clone(), entries))
    }

    async fn api_get_random_person_batch(
        &self,
        gender: &str,
        has_desc: bool,
    ) -> Result<Vec<serde_json::Value>> {
        let mut from = "entry2given_name,entry e1".to_string();
        let mut conditions = vec![
            "entry2given_name.entry_id=e1.id".to_string(),
            "(e1.q IS NULL OR e1.user=0)".to_string(),
        ];
        if !gender.is_empty() {
            from += ",person_dates";
            conditions.push("person_dates.entry_id=e1.id".to_string());
            conditions.push(format!(
                "person_dates.gender='{}'",
                gender.replace('\'', "''")
            ));
        }
        if has_desc {
            conditions.push("e1.ext_desc!=''".to_string());
        }
        let where_clause = conditions.join(" AND ");
        let sql = format!(
            "SELECT e1.*,(select count(*) FROM entry e2 WHERE e1.ext_name=e2.ext_name) AS name_count FROM {from} WHERE {where_clause} ORDER BY entry2given_name.random LIMIT 50"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let name_count: usize = row
                    .get::<Option<usize>, _>("name_count")
                    .flatten()
                    .unwrap_or(0);
                let entry = Self::entry_from_row(&row);
                (name_count, entry)
            })
            .await?;
        let mut ret = Vec::new();
        for (name_count, entry) in rows {
            if let Some(e) = entry {
                ret.push(json!({
                    "id": e.id,
                    "catalog": e.catalog,
                    "ext_id": e.ext_id,
                    "ext_url": e.ext_url,
                    "ext_name": e.ext_name,
                    "ext_desc": e.ext_desc,
                    "q": e.q,
                    "user": e.user,
                    "timestamp": e.timestamp,
                    "type": e.type_name,
                    "name_count": name_count
                }));
            }
        }
        Ok(ret)
    }

    async fn api_get_property_cache(
        &self,
    ) -> Result<(
        HashMap<String, Vec<(usize, usize)>>,
        HashMap<String, String>,
    )> {
        let mut conn = self.get_conn_ro().await?;
        let sql1 = "SELECT DISTINCT prop_group,property,item FROM property_cache WHERE property in (SELECT DISTINCT wd_prop FROM catalog WHERE active=1 AND wd_prop IS NOT NULL AND wd_qual IS NULL)";
        let rows1 = conn
            .exec_iter(sql1, ())
            .await?
            .map_and_drop(|row: Row| {
                let prop_group: String = row
                    .get::<Option<String>, _>("prop_group")
                    .flatten()
                    .unwrap_or_default();
                let property: usize = row
                    .get::<Option<usize>, _>("property")
                    .flatten()
                    .unwrap_or(0);
                let item: usize = row.get::<Option<usize>, _>("item").flatten().unwrap_or(0);
                (prop_group, property, item)
            })
            .await?;
        let mut group_map: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        for (prop_group, property, item) in rows1 {
            group_map
                .entry(prop_group)
                .or_default()
                .push((property, item));
        }

        let sql2 = "SELECT DISTINCT item,label FROM property_cache";
        let rows2 = conn
            .exec_iter(sql2, ())
            .await?
            .map_and_drop(|row: Row| {
                let item: usize = row.get::<Option<usize>, _>("item").flatten().unwrap_or(0);
                let label: String = row
                    .get::<Option<String>, _>("label")
                    .flatten()
                    .unwrap_or_default();
                (format!("{item}"), label)
            })
            .await?;
        let label_map: HashMap<String, String> = rows2.into_iter().collect();

        Ok((group_map, label_map))
    }

    async fn api_get_quick_compare_list(&self) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT * FROM vw_catalogs_for_quick_compare";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_get_mnm_unmatched_relations(
        &self,
        property: usize,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<(usize, usize)>, Vec<Entry>)> {
        let prop_filter = if property > 0 {
            format!(" AND property={property}")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT entry.id,count(*) AS cnt FROM mnm_relation,entry WHERE target_entry_id=entry.id{prop_filter} AND (q is null or user=0) GROUP BY entry.id ORDER BY cnt DESC LIMIT {limit} OFFSET {offset}"
        );
        let mut conn = self.get_conn_ro().await?;
        let id_cnt_pairs: Vec<(usize, usize)> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let cnt: usize = row.get::<Option<usize>, _>("cnt").flatten().unwrap_or(0);
                (id, cnt)
            })
            .await?;

        if id_cnt_pairs.is_empty() {
            return Ok((vec![], vec![]));
        }

        let ids_str = id_cnt_pairs
            .iter()
            .map(|(id, _)| format!("{id}"))
            .collect::<Vec<String>>()
            .join(",");
        let entry_sql = format!("{} WHERE id IN ({ids_str})", Self::entry_sql_select());
        let entries = conn
            .exec_iter(entry_sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok((id_cnt_pairs, entries))
    }

    async fn api_get_top_groups(&self) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT top_missing_groups.*,user.name AS user_name FROM top_missing_groups,user WHERE top_missing_groups.user=user.id AND current=1 ORDER BY name";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_set_top_group(
        &self,
        name: &str,
        catalogs: &str,
        user_id: usize,
        based_on: usize,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;
        if based_on > 0 {
            let sql = format!("UPDATE top_missing_groups SET current=0 WHERE id={based_on}");
            conn.exec_drop(sql, ()).await?;
        }
        let ts = TimeStamp::now();
        let sql = "INSERT IGNORE INTO top_missing_groups (name,catalogs,user,timestamp,current,based_on) VALUES (:name,:catalogs,:user_id,:ts,1,:based_on)";
        conn.exec_drop(sql, params! { name, catalogs, user_id, ts, based_on })
            .await?;
        Ok(())
    }

    async fn api_remove_empty_top_group(&self, group_id: usize) -> Result<()> {
        let sql =
            format!("UPDATE top_missing_groups SET current=0 WHERE catalogs='' AND id={group_id}");
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn api_set_missing_properties_status(
        &self,
        row_id: usize,
        status: &str,
        note: &str,
        user_id: usize,
    ) -> Result<()> {
        let sql =
            "UPDATE props_todo SET status=:status,note=:note,user_id=:user_id WHERE id=:row_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! { status, note, user_id, row_id })
            .await?;
        Ok(())
    }

    async fn api_get_entries_by_q_or_value(
        &self,
        q: isize,
        prop_catalog_map: &HashMap<usize, Vec<usize>>,
        prop_values: &HashMap<usize, Vec<String>>,
    ) -> Result<Vec<Entry>> {
        let mut unions = Vec::new();
        let base_select = Self::entry_sql_select();

        // Match by q — restrict to entries in active catalogs so callers
        // (e.g. the Wikidata gadget) don't see matches on dormant catalogs
        // they can't act on. The prop-values branch is already scoped to
        // active catalogs via `api_get_prop2catalog`.
        if q > 0 {
            unions.push(format!(
                "{base_select} WHERE q={q} \
                 AND catalog IN (SELECT id FROM catalog WHERE active=1)"
            ));
        }

        // Match by prop values
        for (prop, catalogs) in prop_catalog_map {
            if let Some(values) = prop_values.get(prop) {
                if !catalogs.is_empty() && !values.is_empty() {
                    let cats_str = catalogs.iter().join(",");
                    let vals_str = values
                        .iter()
                        .map(|v| format!("'{}'", escape_sql_literal(v)))
                        .join(",");
                    unions.push(format!(
                        "{base_select} WHERE catalog IN ({cats_str}) AND ext_id IN ({vals_str})"
                    ));
                }
            }
        }

        if unions.is_empty() {
            return Ok(vec![]);
        }

        let sql = unions.join(" UNION ");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    async fn api_get_prop2catalog(&self, props: &[usize]) -> Result<HashMap<usize, Vec<usize>>> {
        if props.is_empty() {
            return Ok(HashMap::new());
        }
        let props_str = props.iter().join(",");
        let sql = format!(
            "SELECT id,wd_prop FROM catalog WHERE wd_qual IS NULL AND active=1 AND wd_prop IN ({props_str})"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let wd_prop: usize = row
                    .get::<Option<usize>, _>("wd_prop")
                    .flatten()
                    .unwrap_or(0);
                (wd_prop, id)
            })
            .await?;
        let mut ret: HashMap<usize, Vec<usize>> = HashMap::new();
        for (wd_prop, id) in rows {
            ret.entry(wd_prop).or_default().push(id);
        }
        Ok(ret)
    }

    async fn api_get_missing_properties_raw(&self) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT * FROM props_todo";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_get_rc_log_events(
        &self,
        min_ts: &str,
        max_ts: &str,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let catalog_filter = if catalog_id > 0 {
            format!(" AND catalog={catalog_id}")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT entry.id AS id,catalog,ext_id,ext_url,ext_name,ext_desc,action AS event_type,log.user AS user,log.timestamp AS timestamp FROM log,entry WHERE log.entry_id=entry.id AND log.timestamp BETWEEN '{min_ts}' AND '{max_ts}'{catalog_filter}"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<Option<usize>, _>("id").flatten().unwrap_or(0);
                let catalog: usize = row
                    .get::<Option<usize>, _>("catalog")
                    .flatten()
                    .unwrap_or(0);
                let ext_id: String = row
                    .get::<Option<String>, _>("ext_id")
                    .flatten()
                    .unwrap_or_default();
                let ext_url: String = row
                    .get::<Option<String>, _>("ext_url")
                    .flatten()
                    .unwrap_or_default();
                let ext_name: String = row
                    .get::<Option<String>, _>("ext_name")
                    .flatten()
                    .unwrap_or_default();
                let ext_desc: String = row
                    .get::<Option<String>, _>("ext_desc")
                    .flatten()
                    .unwrap_or_default();
                let event_type: String = row
                    .get::<Option<String>, _>("event_type")
                    .flatten()
                    .unwrap_or_default();
                let user: usize = row.get::<Option<usize>, _>("user").flatten().unwrap_or(0);
                let timestamp: String = row
                    .get::<Option<String>, _>("timestamp")
                    .flatten()
                    .unwrap_or_default();
                json!({
                    "id": id, "catalog": catalog, "ext_id": ext_id, "ext_url": ext_url,
                    "ext_name": ext_name, "ext_desc": ext_desc, "event_type": event_type,
                    "user": user, "timestamp": timestamp
                })
            })
            .await?;
        Ok(rows)
    }

    async fn get_code_fragment_lua(
        &self,
        function: &str,
        catalog_id: usize,
    ) -> Result<Option<String>> {
        let sql = "SELECT `lua` FROM `code_fragments` WHERE `function`=:function AND `catalog`=:catalog_id AND `is_active`=1 LIMIT 1";
        let result: Option<Option<String>> = self
            .get_conn()
            .await?
            .exec_first(sql, params! { function, catalog_id })
            .await?;
        match result {
            Some(lua) => Ok(lua),
            None => Ok(None),
        }
    }

    async fn touch_code_fragment(&self, function: &str, catalog_id: usize) -> Result<()> {
        let sql = "UPDATE `code_fragments` SET `last_run`=NOW() WHERE `function`=:function AND `catalog`=:catalog_id";
        self.get_conn()
            .await?
            .exec_drop(sql, params! { function, catalog_id })
            .await?;
        Ok(())
    }

    async fn clear_person_dates_for_catalog(&self, catalog_id: usize) -> Result<()> {
        let sql = "DELETE person_dates FROM person_dates INNER JOIN entry ON entry.id=person_dates.entry_id WHERE entry.catalog=:catalog_id";
        self.get_conn()
            .await?
            .exec_drop(sql, params! { catalog_id })
            .await?;
        Ok(())
    }

    async fn get_code_fragments_for_catalog(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = "SELECT `id`,`function`,`catalog`,`php`,`json`,`is_active`,`note`,CAST(`last_run` AS CHAR) AS `last_run`,`lua` FROM `code_fragments` WHERE `catalog`=:catalog_id ORDER BY `function`";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<serde_json::Value> = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row| {
                let (id, function, catalog, php, json, is_active, note, last_run, lua): (
                    usize,
                    String,
                    usize,
                    String,
                    String,
                    i8,
                    String,
                    Option<String>,
                    Option<String>,
                ) = from_row(row);
                let note: Option<String> = if note.is_empty() { None } else { Some(note) };
                serde_json::json!({
                    "id": id,
                    "function": function,
                    "catalog": catalog,
                    "php": php,
                    "json": json,
                    "is_active": is_active,
                    "note": note,
                    "last_run": last_run,
                    "lua": lua,
                })
            })
            .await?;
        Ok(rows)
    }

    async fn get_all_code_fragment_functions(&self) -> Result<Vec<String>> {
        let sql = "SELECT DISTINCT `function` FROM `code_fragments`";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<String> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(rows)
    }

    async fn save_code_fragment(&self, fragment: &serde_json::Value) -> Result<usize> {
        let php = fragment["php"].as_str().unwrap_or("");
        let lua_val = &fragment["lua"];
        let lua: Option<&str> =
            if lua_val.is_null() || lua_val.as_str().is_some_and(|s| s.is_empty()) {
                None
            } else {
                lua_val.as_str()
            };
        let json_str = fragment["json"].as_str().unwrap_or("{}");
        let is_active: i8 = if fragment["is_active"].as_bool().unwrap_or(true) {
            1
        } else {
            0
        };
        let note = fragment["note"].as_str().unwrap_or("");
        let function = fragment["function"].as_str().unwrap_or("");
        let catalog = fragment["catalog"].as_u64().unwrap_or(0) as usize;

        let mut conn = self.get_conn().await?;

        let id = fragment["id"].as_u64().unwrap_or(0) as usize;
        if id > 0 {
            // Update existing
            let sql = "UPDATE `code_fragments` SET `php`=:php, `lua`=:lua, `json`=:json_str, `is_active`=:is_active, `note`=:note WHERE `id`=:id";
            conn.exec_drop(sql, params! { php, lua, json_str, is_active, note, id })
                .await?;
            Ok(id)
        } else {
            // Insert new
            let sql = "INSERT INTO `code_fragments` (`function`, `catalog`, `php`, `lua`, `json`, `is_active`, `note`) VALUES (:function, :catalog, :php, :lua, :json_str, :is_active, :note)";
            conn.exec_drop(
                sql,
                params! { function, catalog, php, lua, json_str, is_active, note },
            )
            .await?;
            Ok(conn.last_insert_id().unwrap_or(0) as usize)
        }
    }

    async fn queue_job(
        &self,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
    ) -> Result<usize> {
        let mut conn = self.get_conn().await?;
        match depends_on {
            Some(dep) => {
                let sql = "INSERT IGNORE INTO `jobs` (`action`, `catalog`, `status`, `depends_on`) VALUES (:action, :catalog_id, 'TODO', :dep)";
                conn.exec_drop(sql, params! { action, catalog_id, dep })
                    .await?;
            }
            None => {
                let sql = "INSERT IGNORE INTO `jobs` (`action`, `catalog`, `status`) VALUES (:action, :catalog_id, 'TODO')";
                conn.exec_drop(sql, params! { action, catalog_id }).await?;
            }
        }
        Ok(conn.last_insert_id().unwrap_or(0) as usize)
    }

    // Micro-API: sparql_list
    async fn get_entries_by_ext_names_unmatched(&self, names: &[String]) -> Result<Vec<Entry>> {
        if names.is_empty() {
            return Ok(vec![]);
        }
        let placeholders = names.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "{} WHERE (user=0 OR q IS NULL) AND ext_name IN ({placeholders})",
            Self::entry_sql_select()
        );
        let params: Vec<mysql_async::Value> = names
            .iter()
            .map(|n| mysql_async::Value::from(n.as_str()))
            .collect();
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    // Micro-API: get_sync
    async fn get_catalog_wd_prop(
        &self,
        catalog_id: usize,
    ) -> Result<(Option<usize>, Option<usize>)> {
        let sql = "SELECT COALESCE(wd_prop, 0) AS wd_prop, COALESCE(wd_qual, 0) AS wd_qual FROM catalog WHERE id=:catalog_id";
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let wd_prop: usize = row
                    .get::<Option<usize>, _>("wd_prop")
                    .flatten()
                    .unwrap_or(0);
                let wd_qual: usize = row
                    .get::<Option<usize>, _>("wd_qual")
                    .flatten()
                    .unwrap_or(0);
                let wd_prop = if wd_prop == 0 { None } else { Some(wd_prop) };
                let wd_qual = if wd_qual == 0 { None } else { Some(wd_qual) };
                (wd_prop, wd_qual)
            })
            .await?;
        rows.into_iter()
            .next()
            .ok_or_else(|| anyhow!("Catalog {} not found", catalog_id))
    }

    async fn get_mnm_matched_entries_for_sync(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(isize, String)>> {
        let sql = "SELECT q, ext_id FROM entry WHERE q IS NOT NULL AND q > 0 AND user != 0 AND user IS NOT NULL AND catalog=:catalog_id AND ext_id NOT LIKE 'fake_id_%'";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let q: isize = row.get::<Option<isize>, _>("q").flatten().unwrap_or(0);
                let ext_id: String = row
                    .get::<Option<String>, _>("ext_id")
                    .flatten()
                    .unwrap_or_default();
                (q, ext_id)
            })
            .await?;
        Ok(results)
    }

    async fn get_mnm_double_matches(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, Vec<usize>>> {
        // `user > 0` excludes auto-matched rows (user=0) and never-touched
        // rows (user IS NULL — `NULL > 0` evaluates to NULL → false). The
        // "Multiple external IDs for a single Wikidata item" report is
        // only meaningful for *fully* matched entries; including
        // automatcher hits floods the panel with false positives.
        // Mirrors microsync_get_multiple_q_in_mnm above.
        let sql = "SELECT id, q FROM entry WHERE q IS NOT NULL AND q > 0 AND user > 0 AND catalog=:catalog_id AND ext_id NOT LIKE 'fake_id_%'";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get::<usize, _>("id").unwrap_or(0);
                let q: isize = row.get::<Option<isize>, _>("q").flatten().unwrap_or(0);
                (q, id)
            })
            .await?;
        let mut q2entry_ids: HashMap<String, Vec<usize>> = HashMap::new();
        for (q, id) in results {
            q2entry_ids.entry(q.to_string()).or_default().push(id);
        }
        // Only keep Qs that match more than one MnM entry (the "double" case)
        q2entry_ids.retain(|_, v| v.len() > 1);
        Ok(q2entry_ids)
    }

    // Micro-API: creation_candidates

    async fn cc_random_pick(&self, sql: &str) -> Result<Vec<serde_json::Value>> {
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in row.columns_ref().iter().enumerate() {
                    let name = col.name_str().to_string();
                    let val = match &row[i] {
                        mysql_async::Value::NULL => serde_json::Value::Null,
                        mysql_async::Value::Int(n) => json!(*n),
                        mysql_async::Value::UInt(n) => json!(*n),
                        mysql_async::Value::Float(n) => json!(*n),
                        mysql_async::Value::Double(n) => json!(*n),
                        mysql_async::Value::Bytes(b) => {
                            json!(String::from_utf8_lossy(b).to_string())
                        }
                        other => json!(format!("{other:?}")),
                    };
                    obj.insert(name, val);
                }
                serde_json::Value::Object(obj)
            })
            .await?;
        Ok(rows)
    }

    async fn cc_get_entries_by_ids_active(&self, entry_ids: &str) -> Result<Vec<Entry>> {
        let sql = format!(
            "SELECT entry.* FROM entry INNER JOIN catalog ON catalog.id=entry.catalog AND catalog.active=1 WHERE entry.id IN ({entry_ids})"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    async fn cc_get_entries_by_names_active(
        &self,
        names: &[String],
        type_filter: Option<&str>,
        birth_year: Option<&str>,
        death_year: Option<&str>,
    ) -> Result<Vec<Entry>> {
        if names.is_empty() {
            return Ok(vec![]);
        }
        let placeholders: Vec<String> = names.iter().map(|_| "?".to_string()).collect();
        let placeholders = placeholders.join(",");
        let mut sql = format!(
            "SELECT entry.* FROM entry INNER JOIN catalog ON catalog.id=entry.catalog AND catalog.active=1 WHERE entry.ext_name IN ({placeholders}) AND (entry.q IS NULL OR entry.q!=-1)"
        );
        if let Some(t) = type_filter {
            sql += &format!(" AND entry.`type`='{}'", t.replace('\'', "''"));
        }
        if birth_year.is_some() || death_year.is_some() {
            let mut parts = vec!["entry_id=entry.id".to_string()];
            if let Some(by) = birth_year {
                parts.push(format!("year_born='{by}'"));
            }
            if let Some(dy) = death_year {
                parts.push(format!("year_died='{dy}'"));
            }
            sql += &format!(
                " AND EXISTS (SELECT 1 FROM person_dates WHERE {})",
                parts.join(" AND ")
            );
        }
        let params: Vec<mysql_async::Value> = names
            .iter()
            .map(|n| mysql_async::Value::from(n.as_str()))
            .collect();
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(rows)
    }

    // Micro-API: quick_compare

    async fn qc_get_entries(
        &self,
        catalog_id: usize,
        entry_id: Option<usize>,
        require_image: bool,
        require_coordinates: bool,
        random_threshold: f64,
        max_results: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut select = "SELECT entry.*, catalog.search_wp AS language".to_string();
        let mut from = " FROM entry, catalog".to_string();
        let mut where_clause;

        if let Some(eid) = entry_id {
            where_clause = format!(" WHERE entry.id={eid} AND catalog.id=entry.catalog");
        } else {
            where_clause = format!(
                " WHERE catalog.id=entry.catalog AND catalog.active=1 AND user=0 AND entry.catalog={catalog_id} AND entry.catalog NOT IN (819)"
            );
            if random_threshold > 0.0 {
                where_clause += &format!(" AND random>={random_threshold}");
            }
        }

        if require_image {
            select += ", kv1.kv_value AS image_url";
            from += ", kv_entry kv1";
            where_clause +=
                " AND kv1.entry_id=entry.id AND kv1.kv_key='image_url' AND kv1.kv_value!=''";
        }
        if require_coordinates {
            select += ", location.lat, location.lon";
            from += ", location";
            where_clause += " AND location.entry_id=entry.id";
        }

        if entry_id.is_none() {
            if random_threshold > 0.0 {
                where_clause += " ORDER BY random";
            }
            where_clause += &format!(" LIMIT {max_results}");
        }

        let sql = format!("{select}{from}{where_clause}");
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in row.columns_ref().iter().enumerate() {
                    let name = col.name_str().to_string();
                    let val = match &row[i] {
                        mysql_async::Value::NULL => serde_json::Value::Null,
                        mysql_async::Value::Int(n) => json!(*n),
                        mysql_async::Value::UInt(n) => json!(*n),
                        mysql_async::Value::Float(n) => json!(*n),
                        mysql_async::Value::Double(n) => json!(*n),
                        mysql_async::Value::Bytes(b) => {
                            json!(String::from_utf8_lossy(b).to_string())
                        }
                        other => json!(format!("{other:?}")),
                    };
                    obj.insert(name, val);
                }
                serde_json::Value::Object(obj)
            })
            .await?;
        Ok(rows)
    }

    // ─── Lightweight catalog endpoints ────────────────────────────────────

    async fn api_search_catalogs(&self, q: &str, limit: usize) -> Result<Vec<Value>> {
        let q_like = format!("%{q}%");
        let sql = "SELECT c.id, c.name, c.`desc`, c.type, IFNULL(o.total,0) AS total, IFNULL(o.manual,0) AS manual
            FROM catalog c LEFT JOIN overview o ON o.catalog = c.id
            WHERE c.active = 1 AND (c.name LIKE :q_like OR c.`desc` LIKE :q_like)
            ORDER BY IFNULL(o.total,0) DESC LIMIT :limit";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, params! { "q_like" => q_like, "limit" => limit })
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_catalog_type_counts(&self) -> Result<Vec<Value>> {
        let sql = "SELECT `type`, COUNT(*) AS cnt FROM catalog WHERE active = 1 AND `type` != '' GROUP BY `type` ORDER BY cnt DESC";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_latest_catalogs(&self, limit: usize) -> Result<Vec<Value>> {
        let sql = "SELECT c.id, c.name, c.`desc`, c.type, IFNULL(o.total,0) AS total, IFNULL(o.manual,0) AS manual
            FROM catalog c LEFT JOIN overview o ON o.catalog = c.id
            WHERE c.active = 1 ORDER BY c.id DESC LIMIT :limit";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, params! { limit })
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_catalogs_with_locations(&self) -> Result<Vec<Value>> {
        let sql = "SELECT c.id, c.name, c.`desc`, c.type, IFNULL(o.total,0) AS total, IFNULL(o.manual,0) AS manual
            FROM catalog c INNER JOIN kv_catalog kv ON kv.catalog_id = c.id
            LEFT JOIN overview o ON o.catalog = c.id
            WHERE c.active = 1 AND kv.kv_key = 'has_locations' AND kv.kv_value = 'yes'
            ORDER BY c.name";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_catalog_property_groups(&self) -> Result<Value> {
        let mut conn = self.get_conn_ro().await?;
        // 1. Map active catalogs to their wd_prop (or "no property")
        let rows: Vec<(usize, Option<usize>)> = conn
            .exec_iter("SELECT id, wd_prop FROM catalog WHERE active = 1", ())
            .await?
            .map_and_drop(from_row::<(usize, Option<usize>)>)
            .await?;
        let mut prop2cats: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut no_prop_ids: Vec<usize> = vec![];
        for (id, wd_prop) in rows {
            match wd_prop {
                Some(p) if p > 0 => prop2cats.entry(p).or_default().push(id),
                _ => no_prop_ids.push(id),
            }
        }

        let mut groups = serde_json::Map::new();
        groups.insert(
            "ig_no_property".to_string(),
            json!({
                "label": "Catalogs without Wikidata property",
                "catalogs": no_prop_ids,
            }),
        );

        if !prop2cats.is_empty() {
            let keys: Vec<String> = prop2cats.keys().map(|k| k.to_string()).collect();
            let in_list = keys.join(",");
            let sql_groups = format!(
                "SELECT pc.prop_group, pc.property, pc.item, pc.label FROM property_cache pc WHERE pc.property IN ({in_list})"
            );
            // `prop_group` is the numeric property id the group is keyed to
            // (31 = "instance of" meaning top-level group; anything else is a
            // country/subgroup). Stored as INT on the server; mysql_async
            // would panic if we asked for String.
            let prop_rows: Vec<(usize, usize, usize, String)> = conn
                .exec_iter(sql_groups, ())
                .await?
                .map_and_drop(from_row::<(usize, usize, usize, String)>)
                .await?;
            for (prop_group, property, item, label) in prop_rows {
                let prefix = if prop_group == 31 { "ig_" } else { "country_" };
                let key = format!(
                    "{prefix}{}",
                    label
                        .to_lowercase()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join("_")
                );
                let catalogs = prop2cats.get(&property).cloned().unwrap_or_default();
                let entry = groups.entry(key.clone()).or_insert_with(
                    || json!({"label": label, "catalogs": [], "q": format!("Q{item}")}),
                );
                if let Some(arr) = entry.get_mut("catalogs").and_then(|v| v.as_array_mut()) {
                    for c in catalogs {
                        if !arr.iter().any(|v| v.as_u64() == Some(c as u64)) {
                            arr.push(json!(c));
                        }
                    }
                }
            }
        }

        for (_, v) in groups.iter_mut() {
            if let Some(arr) = v.get("catalogs").and_then(|c| c.as_array()) {
                let cnt = arr.len();
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("count".to_string(), json!(cnt));
                }
            }
        }
        Ok(Value::Object(groups))
    }

    async fn api_check_wd_prop_usage(
        &self,
        wd_prop: usize,
        exclude_catalog: usize,
    ) -> Result<Value> {
        let mut sql = String::from(
            "SELECT id, name FROM catalog WHERE active = 1 AND wd_prop = :wd_prop AND wd_qual IS NULL",
        );
        if exclude_catalog > 0 {
            sql.push_str(&format!(" AND id != {exclude_catalog}"));
        }
        sql.push_str(" LIMIT 1");
        let mut conn = self.get_conn_ro().await?;
        let row: Option<(usize, String)> = conn
            .exec_iter(sql, params! { wd_prop })
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?
            .into_iter()
            .next();
        Ok(match row {
            Some((id, name)) => json!({"used": true, "catalog_id": id, "catalog_name": name}),
            None => json!({"used": false}),
        })
    }

    async fn api_catalog_by_group(&self, group: &str) -> Result<Value> {
        let mut conn = self.get_conn_ro().await?;
        // Build the WHERE clause based on the group token
        let group_safe = group.replace('\'', "''");
        let where_clause: String = if group == "all" {
            "c.active = 1".into()
        } else if group == "ig_no_property" {
            "c.active = 1 AND (c.wd_prop IS NULL OR c.wd_prop = 0)".into()
        } else if group.starts_with("ig_") || group.starts_with("country_") {
            let prop_group = if group.starts_with("ig_") { 31 } else { 17 };
            let label_raw = if let Some(rest) = group.strip_prefix("ig_") {
                rest
            } else {
                group.strip_prefix("country_").unwrap_or("")
            };
            let label = label_raw.replace('_', " ").replace('\'', "''");
            let sql = format!(
                "SELECT DISTINCT property FROM property_cache WHERE prop_group = {prop_group} AND LOWER(label) = LOWER('{label}')"
            );
            let props: Vec<usize> = conn
                .exec_iter(sql, ())
                .await?
                .map_and_drop(from_row::<usize>)
                .await?;
            if props.is_empty() {
                return Ok(json!({}));
            }
            format!(
                "c.active = 1 AND c.wd_qual IS NULL AND c.wd_prop IN ({})",
                props
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        } else {
            // Type group
            format!("c.active = 1 AND c.type = '{group_safe}'")
        };

        let sql = format!(
            "SELECT c.*, o.total, o.noq, o.autoq, o.na, o.manual, o.nowd, o.multi_match, o.types, u.name AS username
                FROM catalog c LEFT JOIN overview o ON o.catalog = c.id LEFT JOIN user u ON u.id = c.owner
                WHERE {where_clause} ORDER BY c.name"
        );
        let rows: Vec<Value> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;

        let mut map = serde_json::Map::new();
        let mut catalog_ids: Vec<usize> = vec![];
        for row in rows {
            if let Some(id) = row.get("id").and_then(|v| v.as_u64()) {
                catalog_ids.push(id as usize);
                map.insert(id.to_string(), row);
            }
        }

        if !catalog_ids.is_empty() {
            let in_list = catalog_ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",");
            // `SELECT *` returned the `id` PK column too, which broke the 3-tuple
            // FromRow conversion. List the columns explicitly to keep the
            // shape stable against schema additions.
            let sql_kv = format!(
                "SELECT catalog_id, kv_key, kv_value FROM kv_catalog WHERE catalog_id IN ({in_list})"
            );
            let kv_rows: Vec<(usize, String, String)> = conn
                .exec_iter(sql_kv, ())
                .await?
                .map_and_drop(from_row::<(usize, String, String)>)
                .await?;
            for (catalog_id, kv_key, kv_value) in kv_rows {
                if kv_key.starts_with("wdrc_") {
                    continue;
                }
                if let Some(entry) = map.get_mut(&catalog_id.to_string()) {
                    let existing = entry.get(&kv_key).cloned();
                    let new_val = match existing {
                        None => json!(kv_value),
                        Some(Value::Array(mut arr)) => {
                            arr.push(json!(kv_value));
                            Value::Array(arr)
                        }
                        Some(other) => json!([other, kv_value]),
                    };
                    if let Some(obj) = entry.as_object_mut() {
                        obj.insert(kv_key, new_val);
                    }
                }
            }
        }

        Ok(Value::Object(map))
    }

    // ─── Other newly ported endpoints ─────────────────────────────────────

    async fn api_create_list(&self, catalog_id: usize) -> Result<Vec<Value>> {
        let sql = "SELECT ext_id, ext_name, ext_desc, ext_url, type FROM entry WHERE q=-1 AND user>0 AND catalog=:catalog_id ORDER BY ext_name";
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<Value> = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(row_to_json)
            .await?;
        Ok(rows)
    }

    async fn api_user_edits(
        &self,
        user_id: usize,
        catalog: usize,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<Value>, Value, usize, Option<Value>)> {
        let mut conn = self.get_conn_ro().await?;
        let cat_cond = if catalog > 0 {
            format!(" AND catalog={catalog}")
        } else {
            String::new()
        };
        let cat_cond2 = if catalog > 0 {
            format!(" AND entry.catalog={catalog}")
        } else {
            String::new()
        };

        // Matches from entry
        let sql_matches = format!(
            "SELECT entry.* FROM entry WHERE user={user_id}{cat_cond} ORDER BY timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        let current_rows: Vec<Value> = conn
            .exec_iter(sql_matches, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;

        // Historical edits
        let sql_log = format!(
            "SELECT entry.id AS id, entry.catalog, entry.ext_id, entry.ext_url, entry.ext_name, entry.ext_desc,
                log.action AS event_type, log.user AS user, log.timestamp AS timestamp, log.q AS q
                FROM log INNER JOIN entry ON log.entry_id = entry.id
                WHERE log.user={user_id}{cat_cond2} ORDER BY log.timestamp DESC LIMIT {limit} OFFSET {offset}"
        );
        let log_rows: Vec<Value> = conn
            .exec_iter(sql_log, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;

        // Merge and sort
        let mut events: Vec<Value> = current_rows
            .into_iter()
            .map(|mut v| {
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("event_type".to_string(), json!("match"));
                }
                v
            })
            .chain(log_rows)
            .collect();
        events.sort_by(|a, b| {
            let ta = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            let tb = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            tb.cmp(ta)
        });
        events.truncate(limit);

        // Total count
        let sql_total = format!("SELECT COUNT(*) AS cnt FROM entry WHERE user={user_id}{cat_cond}");
        let total: usize = conn
            .exec_iter(sql_total, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .into_iter()
            .next()
            .unwrap_or(0);

        // User info
        let sql_user = format!("SELECT * FROM user WHERE id={user_id} LIMIT 1");
        let user_info: Option<Value> = conn
            .exec_iter(sql_user, ())
            .await?
            .map_and_drop(row_to_json)
            .await?
            .into_iter()
            .next();

        // Collect user ids
        let mut uids: std::collections::HashSet<usize> = events
            .iter()
            .filter_map(|e| e.get("user").and_then(|v| v.as_u64()).map(|v| v as usize))
            .collect();
        uids.insert(user_id);
        let uid_vec: Vec<usize> = uids.into_iter().collect();
        let users = self.get_users_by_ids(&uid_vec).await.unwrap_or_default();
        let mut users_map = serde_json::Map::new();
        for (id, v) in users {
            users_map.insert(id.to_string(), v);
        }

        Ok((events, Value::Object(users_map), total, user_info))
    }

    async fn api_get_statement_text_groups(
        &self,
        catalog_id: usize,
        property: usize,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<Value>, Vec<Value>)> {
        let mut conn = self.get_conn_ro().await?;
        // Unmatched entry IDs for this catalog
        let sql_ids = "SELECT id FROM entry WHERE catalog=:catalog_id AND q IS NULL";
        let entry_ids: Vec<usize> = conn
            .exec_iter(sql_ids, params! { catalog_id })
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        if entry_ids.is_empty() {
            return Ok((vec![], vec![]));
        }
        let in_list = entry_ids
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Available properties
        let sql_props = format!(
            "SELECT property, COUNT(DISTINCT text) AS group_count FROM statement_text WHERE entry_id IN ({in_list}) AND q IS NULL GROUP BY property ORDER BY group_count DESC"
        );
        let properties: Vec<Value> = conn
            .exec_iter(sql_props, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;

        // Groups
        let prop_filter = if property > 0 {
            format!(" AND property = {property}")
        } else {
            String::new()
        };
        let sql_groups = format!(
            "SELECT property, text, COUNT(*) AS cnt FROM statement_text WHERE entry_id IN ({in_list}) AND q IS NULL{prop_filter} GROUP BY property, text ORDER BY cnt DESC LIMIT {limit} OFFSET {offset}"
        );
        let mut groups: Vec<Value> = conn
            .exec_iter(sql_groups, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;

        // Add up to 5 samples per group
        for group in groups.iter_mut() {
            let text = group
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .replace('\'', "''");
            let prop = group.get("property").and_then(|v| v.as_u64()).unwrap_or(0);
            let sql_samples = format!(
                "SELECT e.id, e.ext_id, e.ext_name, e.ext_url FROM statement_text st INNER JOIN entry e ON st.entry_id=e.id WHERE st.entry_id IN ({in_list}) AND st.property={prop} AND st.text='{text}' AND st.q IS NULL LIMIT 5"
            );
            let samples: Vec<Value> = conn
                .exec_iter(sql_samples, ())
                .await?
                .map_and_drop(row_to_json)
                .await?;
            if let Some(obj) = group.as_object_mut() {
                obj.insert("samples".to_string(), json!(samples));
            }
        }

        Ok((properties, groups))
    }

    async fn api_set_statement_text_q(
        &self,
        catalog_id: usize,
        property: usize,
        text: &str,
        q: usize,
        user_id: usize,
    ) -> Result<(usize, usize)> {
        let text_safe = text.replace('\'', "''");
        let mut conn = self.get_conn().await?;
        let sql_update = format!(
            "UPDATE statement_text st INNER JOIN entry e ON st.entry_id=e.id
                SET st.q={q}, st.user_id={user_id}
                WHERE e.catalog={catalog_id} AND st.property={property} AND st.text='{text_safe}' AND st.q IS NULL"
        );
        conn.exec_drop(sql_update, ()).await?;
        let rows_updated = conn.affected_rows() as usize;

        let q_str = format!("Q{q}");
        let sql_aux = format!(
            "INSERT IGNORE INTO auxiliary (entry_id, aux_p, aux_name)
                SELECT st.entry_id, {property}, '{q_str}'
                FROM statement_text st INNER JOIN entry e ON st.entry_id=e.id
                WHERE e.catalog={catalog_id} AND st.property={property} AND st.text='{text_safe}' AND st.q={q}"
        );
        conn.exec_drop(sql_aux, ()).await?;
        let aux_rows_added = conn.affected_rows() as usize;
        Ok((rows_updated, aux_rows_added))
    }

    async fn api_missingpages(&self, catalog_id: usize, site: &str) -> Result<(Value, Value)> {
        // NOTE: the Wikidata DB replica lookup from the PHP version is omitted here —
        // the Rust build has no Wikidata item-per-site access at this layer. We return
        // all human-matched entries for the catalog; the frontend can still render them.
        let mut conn = self.get_conn_ro().await?;
        let sql =
            "SELECT * FROM entry WHERE user>0 AND catalog=:catalog_id AND q>0 AND q IS NOT NULL";
        let entries: Vec<Value> = conn
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(row_to_json)
            .await?;
        let _ = site; // Currently unused; listed in signature for API parity.
        let mut entries_map = serde_json::Map::new();
        let mut uids: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for e in entries {
            if let Some(id) = e.get("id").and_then(|v| v.as_u64()) {
                if let Some(u) = e.get("user").and_then(|v| v.as_u64()) {
                    uids.insert(u as usize);
                }
                entries_map.insert(id.to_string(), e);
            }
        }
        let uid_vec: Vec<usize> = uids.into_iter().collect();
        let users = self.get_users_by_ids(&uid_vec).await.unwrap_or_default();
        let mut users_map = serde_json::Map::new();
        for (id, v) in users {
            users_map.insert(id.to_string(), v);
        }
        Ok((Value::Object(entries_map), Value::Object(users_map)))
    }

    async fn api_sitestats(&self, catalog: Option<usize>) -> Result<Value> {
        // PHP version queries the Wikidata DB replica (wb_items_per_site) which the
        // Rust build does not access here. Return an empty object as a safe fallback.
        let _ = catalog;
        Ok(json!({}))
    }

    async fn api_dg_tiles(&self, num: usize, type_filter: &str) -> Result<Vec<Value>> {
        let mut conn = self.get_conn_ro().await?;
        // Load eligible catalogs once
        let sql_cats = "SELECT * FROM catalog WHERE wd_prop IS NOT NULL AND wd_qual IS NULL AND `active`=1 AND id NOT IN (80,150)";
        let catalogs_list: Vec<Value> = conn
            .exec_iter(sql_cats, ())
            .await?
            .map_and_drop(row_to_json)
            .await?;
        let mut catalogs: HashMap<usize, Value> = HashMap::new();
        let mut catalog_ids: Vec<usize> = vec![];
        for c in catalogs_list {
            if let Some(id) = c.get("id").and_then(|v| v.as_u64()) {
                catalog_ids.push(id as usize);
                catalogs.insert(id as usize, c);
            }
        }
        if catalog_ids.is_empty() {
            return Ok(vec![]);
        }
        let cat_in = catalog_ids
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let type_clause = match type_filter {
            "person" => " AND entry.type='Q5'".to_string(),
            "not_person" => " AND entry.type!='Q5'".to_string(),
            _ => String::new(),
        };

        let mut tiles = Vec::with_capacity(num);
        for _ in 0..num {
            let r: f64 = rand::random();
            let sql_tile = format!(
                "SELECT * FROM entry WHERE user=0 AND ext_url!='' AND random>={r} AND catalog IN ({cat_in}) AND ext_desc IS NOT NULL AND ext_desc!=''{type_clause} ORDER BY random LIMIT 1"
            );
            let entry: Option<Value> = conn
                .exec_iter(sql_tile, ())
                .await?
                .map_and_drop(row_to_json)
                .await?
                .into_iter()
                .next();
            let Some(mut entry) = entry else { continue };
            let entry_catalog_id =
                entry.get("catalog").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            let cat_meta = match catalogs.get(&entry_catalog_id) {
                Some(c) => c,
                None => continue,
            };
            let wd_prop = cat_meta
                .get("wd_prop")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let name = cat_meta
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("wd_prop".to_string(), json!(wd_prop));
                obj.insert("name".to_string(), json!(name));
            }
            let q = entry.get("q").and_then(|v| v.as_i64()).unwrap_or(0);
            let q_str = format!("Q{q}");
            let p_str = format!("P{wd_prop}");
            let entry_id = entry.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            let ext_name = entry
                .get("ext_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ext_url = entry
                .get("ext_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ext_desc = entry
                .get("ext_desc")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ext_id = entry
                .get("ext_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let tile = json!({
                "id": entry_id,
                "sections": [
                    {"type": "text", "title": ext_name, "url": ext_url, "text": format!("{ext_desc}\n[from {name} catalog]")},
                    {"type": "item", "q": q_str},
                ],
                "controls": [{
                    "type": "buttons",
                    "entries": [
                        {"type": "green", "decision": "yes", "label": "Yes", "api_action": {"action": "wbcreateclaim", "entity": q_str, "property": p_str, "snaktype": "value", "value": serde_json::to_string(&ext_id).unwrap_or_default()}},
                        {"type": "white", "decision": "skip", "label": "Skip"},
                        {"type": "yellow", "decision": "n_a", "label": "N/A", "shortcut": "n"},
                        {"type": "blue", "decision": "no", "label": "No"},
                    ],
                }],
            });
            tiles.push(tile);
        }
        Ok(tiles)
    }

    async fn wd_matches_get_batch(&self, status: &str, limit: usize) -> Result<Vec<WdMatchRow>> {
        // Filters mirror the PHP `getEntriesWithWdMatches` + active-catalog
        // gate: only fully-matched entries (`user>0 AND q>0`) on active
        // catalogs with a Wikidata property and no qualifier. Anything
        // outside that set can't be classified or pushed back, so we skip
        // it at the SQL level rather than at the Rust level.
        let sql = "SELECT \
                wd_matches.entry_id, \
                entry.catalog AS catalog_id, \
                entry.ext_id, \
                entry.q AS q_numeric, \
                catalog.wd_prop \
            FROM wd_matches \
            INNER JOIN entry ON entry.id = wd_matches.entry_id \
            INNER JOIN catalog ON catalog.id = entry.catalog \
            WHERE wd_matches.status = :status \
              AND entry.user > 0 AND entry.q > 0 \
              AND catalog.active = 1 \
              AND catalog.wd_prop IS NOT NULL \
              AND catalog.wd_qual IS NULL \
            LIMIT :limit";
        let rows: Vec<WdMatchRow> = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { status, limit })
            .await?
            .map_and_drop(|row: Row| WdMatchRow {
                entry_id: row.get("entry_id").unwrap_or(0),
                catalog_id: row.get("catalog_id").unwrap_or(0),
                ext_id: row.get::<String, _>("ext_id").unwrap_or_default(),
                q_numeric: row.get("q_numeric").unwrap_or(0),
                wd_prop: row.get("wd_prop").unwrap_or(0),
            })
            .await?
            .into_iter()
            // Defensive: an INNER JOIN combined with the WHERE filter
            // already excludes nulls, but a partially-malformed row
            // would still come through with zeroed-out fields. Drop
            // those — there's nothing useful to do with `entry_id=0`.
            .filter(|r| r.entry_id > 0 && r.q_numeric > 0 && r.wd_prop > 0)
            .collect();
        Ok(rows)
    }

    async fn wd_matches_set_status(&self, entry_id: usize, status: &str) -> Result<()> {
        // Targeted update — unlike `entry_set_match_status`, this does
        // not touch `person_dates.is_matched` or other side-tables.
        // The classifier only cares about transitioning the status
        // bucket; running the full cleanup path would also wipe
        // multi_match rows on every classification, which is wrong.
        let timestamp = TimeStamp::now();
        let sql = "UPDATE `wd_matches` \
            SET `status` = :status, `timestamp` = :timestamp \
            WHERE `entry_id` = :entry_id";
        self.get_conn()
            .await?
            .exec_drop(sql, params! { entry_id, status, timestamp })
            .await?;
        Ok(())
    }

    async fn catalog_set_active(&self, catalog_id: usize, active: bool) -> Result<()> {
        let active_int: i32 = if active { 1 } else { 0 };
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `catalog` SET `active` = :active_int WHERE `id` = :catalog_id",
                params! { catalog_id, active_int },
            )
            .await?;
        Ok(())
    }

    async fn entry_copy_missing_to_catalog(
        &self,
        source_catalog: usize,
        target_catalog: usize,
    ) -> Result<usize> {
        // Single bulk INSERT…SELECT, materialising one fresh unmatched
        // row in `target` for every source ext_id that isn't already
        // present in target. PHP's per-row loop is gone — same end
        // state, far fewer round-trips on big catalogs.
        //
        // `random` is reset per row so the new entries get the spread
        // the random-pick UI relies on; copying the source's `random`
        // would cluster the imports and skew the random samples.
        let sql = "INSERT INTO `entry` \
                (`catalog`,`ext_id`,`ext_url`,`ext_name`,`ext_desc`,\
                 `q`,`user`,`timestamp`,`random`,`type`) \
            SELECT \
                :target_catalog, src.`ext_id`, src.`ext_url`, src.`ext_name`, \
                src.`ext_desc`, NULL, NULL, NULL, RAND(), src.`type` \
            FROM `entry` AS src \
            WHERE src.`catalog` = :source_catalog \
              AND NOT EXISTS ( \
                SELECT 1 FROM `entry` AS tgt \
                WHERE tgt.`catalog` = :target_catalog \
                  AND tgt.`ext_id` = src.`ext_id` \
              )";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! { source_catalog, target_catalog })
            .await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn entry_get_mergeable_matches(
        &self,
        source_catalog: usize,
        target_catalog: usize,
    ) -> Result<Vec<MergeableMatch>> {
        // Find ext_id pairs where the source carries a confirmed manual
        // match (`user>0`, `q>0`) and the target row is either unmatched
        // (`user IS NULL`) or auto-matched (`user=0`). Skips pairs that
        // already agree (`src.q = tgt.q`) so the merger doesn't churn
        // through no-op writes.
        let sql = "SELECT \
                tgt.`id` AS target_entry_id, \
                src.`q` AS source_q, \
                src.`user` AS source_user, \
                src.`timestamp` AS source_timestamp \
            FROM `entry` AS src \
            INNER JOIN `entry` AS tgt \
                ON tgt.`ext_id` = src.`ext_id` \
            WHERE src.`catalog` = :source_catalog \
              AND tgt.`catalog` = :target_catalog \
              AND src.`user` > 0 \
              AND src.`q` IS NOT NULL AND src.`q` > 0 \
              AND (src.`q` <> tgt.`q` OR tgt.`q` IS NULL) \
              AND (tgt.`user` IS NULL OR tgt.`user` = 0)";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { source_catalog, target_catalog })
            .await?
            .map_and_drop(|row: Row| MergeableMatch {
                target_entry_id: row.get("target_entry_id").unwrap_or(0),
                source_q: row
                    .get::<Option<isize>, _>("source_q")
                    .flatten()
                    .unwrap_or(0),
                source_user: row
                    .get::<Option<usize>, _>("source_user")
                    .flatten()
                    .unwrap_or(0),
                source_timestamp: row
                    .get_opt::<Option<String>, _>("source_timestamp")
                    .and_then(Result::ok)
                    .flatten(),
            })
            .await?;
        Ok(rows
            .into_iter()
            // Defensive: an INNER JOIN combined with the WHERE filter
            // already excludes zero/null source data, but keep the
            // floor positive so a malformed row can't slip through and
            // produce a nonsensical Q0 match downstream.
            .filter(|m| m.target_entry_id > 0 && m.source_q > 0 && m.source_user > 0)
            .collect())
    }

    async fn entry_force_timestamp(&self, entry_id: usize, timestamp: &str) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `entry` SET `timestamp` = :timestamp WHERE `id` = :entry_id",
                params! { entry_id, timestamp },
            )
            .await?;
        Ok(())
    }

    async fn catalog_get_manually_matched_ext_ids(
        &self,
        catalog_id: usize,
    ) -> Result<std::collections::HashSet<String>> {
        let sql = "SELECT `ext_id` FROM `entry` \
            WHERE `catalog` = :catalog_id \
              AND `q` IS NOT NULL AND `q` > 0 \
              AND `user` > 0";
        let rows: Vec<String> = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(rows.into_iter().collect())
    }

    async fn entry_load_for_migration(&self, catalog_id: usize) -> Result<Vec<GroupedEntry>> {
        let sql = "SELECT `id`, `ext_id`, `ext_name`, `ext_desc`, `q`, `user`, `timestamp` \
            FROM `entry` WHERE `catalog` = :catalog_id";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| GroupedEntry {
                id: row.get("id").unwrap_or(0),
                ext_id: row.get::<String, _>("ext_id").unwrap_or_default(),
                ext_name: row.get::<String, _>("ext_name").unwrap_or_default(),
                ext_desc: row.get::<String, _>("ext_desc").unwrap_or_default(),
                q: row
                    .get_opt::<Option<isize>, _>("q")
                    .and_then(Result::ok)
                    .flatten(),
                user: row
                    .get_opt::<Option<usize>, _>("user")
                    .and_then(Result::ok)
                    .flatten(),
                timestamp: row
                    .get_opt::<Option<String>, _>("timestamp")
                    .and_then(Result::ok)
                    .flatten(),
            })
            .await?;
        Ok(rows)
    }

    async fn entry_get_manual_matches_for_catalog(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(usize, String, isize)>> {
        let sql = "SELECT `id`, `ext_id`, `q` FROM `entry` \
            WHERE `catalog` = :catalog_id \
              AND `q` IS NOT NULL AND `q` > 0 \
              AND `user` IS NOT NULL AND `user` > 0";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get("id").unwrap_or(0);
                let ext_id: String = row.get::<String, _>("ext_id").unwrap_or_default();
                let q: isize = row.get::<Option<isize>, _>("q").flatten().unwrap_or(0);
                (id, ext_id, q)
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter(|(id, _, q)| *id > 0 && *q > 0)
            .collect())
    }

    async fn entry_get_algorithmic_human_matches(&self) -> Result<Vec<(usize, isize)>> {
        // `user IN (3,4)` is exactly the audit shape PHP uses —
        // those are the algorithmic-match user ids
        // (USER_DATE_MATCH, USER_AUX_MATCH). Manual matches are
        // trusted; only the algorithmic ones get re-validated.
        let sql = "SELECT `id`, `q` FROM `entry` \
            WHERE `q` IS NOT NULL AND `q` > 0 \
              AND `type` = 'Q5' \
              AND `user` IN (3, 4)";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let id: usize = row.get("id").unwrap_or(0);
                let q: isize = row.get::<Option<isize>, _>("q").flatten().unwrap_or(0);
                (id, q)
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter(|(id, q)| *id > 0 && *q > 0)
            .collect())
    }

    async fn description_aux_get_all(&self) -> Result<Vec<DescriptionAuxRule>> {
        let sql = "SELECT `property`, `value`, `rx`, `type_constraint` \
            FROM `description_aux`";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| DescriptionAuxRule {
                property: row.get("property").unwrap_or(0),
                value: row.get::<String, _>("value").unwrap_or_default(),
                rx: row.get::<String, _>("rx").unwrap_or_default(),
                type_constraint: row.get::<String, _>("type_constraint").unwrap_or_default(),
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter(|r| r.property > 0 && !r.rx.is_empty())
            .collect())
    }

    async fn apply_description_aux_to_catalog(
        &self,
        catalog_id: usize,
        rule: &DescriptionAuxRule,
    ) -> Result<usize> {
        // Build the type filter as a literal SQL fragment because
        // the column we're filtering on (`entry.type`) may be
        // anything the catalog imported. Empty string means "no
        // filter", matching the PHP behaviour. Defensive escape on
        // the type string rather than parameterising — the rest of
        // the SQL is already a format!-built string.
        let type_filter = if rule.type_constraint.is_empty() {
            String::new()
        } else {
            format!(
                " AND `type` = '{}'",
                escape_sql_literal(&rule.type_constraint)
            )
        };
        // RLIKE pattern is parameterised; the property + value land
        // in the SELECT list and need to come through as bound
        // params too so a malformed `value` can't break out into the
        // surrounding SQL.
        let property = rule.property;
        let value = rule.value.trim().to_string();
        let rx = rule.rx.to_lowercase();
        let sql = format!(
            "INSERT IGNORE INTO `auxiliary` (`entry_id`, `aux_p`, `aux_name`) \
             SELECT `id`, :property, :value \
             FROM `entry` \
             WHERE `catalog` = :catalog_id \
               AND lower(`ext_desc`) RLIKE :rx \
               AND NOT EXISTS ( \
                 SELECT 1 FROM `auxiliary` \
                 WHERE `entry_id` = `entry`.`id` \
                   AND `aux_p` = :property \
                   AND `aux_name` = :value \
               ){type_filter}"
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! { catalog_id, property, value, rx })
            .await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn auxiliary_get_crossmatch_groups(
        &self,
        props: &[usize],
    ) -> Result<Vec<(usize, String, Vec<usize>)>> {
        if props.is_empty() {
            return Ok(vec![]);
        }
        // `cnt > entry_is_matched` means at least one row in the group
        // is *not* yet manually matched — i.e. there's something to
        // potentially propagate the match to. The PHP version also
        // accepts `entry_is_matched=0` cases (no manual match
        // anywhere in the group); the maintenance pass filters those
        // out at the Rust level after loading the entries because
        // it needs the per-entry user/q values anyway.
        let props_csv: String = props.iter().join(",");
        let sql = format!(
            "SELECT `aux_p`, `aux_name`, group_concat(`entry_id`) AS `entry_ids` \
             FROM `auxiliary` \
             WHERE `aux_p` IN ({props_csv}) \
             GROUP BY `aux_p`, `aux_name` \
             HAVING count(`entry_id`) > 1 \
                AND count(`entry_id`) > sum(`entry_is_matched`)"
        );
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row: Row| {
                let prop: usize = row.get("aux_p").unwrap_or(0);
                let name: String = row.get::<String, _>("aux_name").unwrap_or_default();
                let entry_ids_csv: String = row.get::<String, _>("entry_ids").unwrap_or_default();
                let entry_ids: Vec<usize> = entry_ids_csv
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                (prop, name, entry_ids)
            })
            .await?;
        Ok(rows
            .into_iter()
            .filter(|(p, n, ids)| *p > 0 && !n.is_empty() && !ids.is_empty())
            .collect())
    }

    async fn auxiliary_select_for_prop(&self, prop: usize) -> Result<Vec<(usize, String)>> {
        let sql = "SELECT `id`, `aux_name` FROM `auxiliary` WHERE `aux_p` = :prop";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { prop })
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(rows)
    }

    async fn auxiliary_delete_row(&self, id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop("DELETE FROM `auxiliary` WHERE `id` = :id", params! { id })
            .await?;
        Ok(())
    }

    async fn entry_select_with_html_entities_in_name(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<(usize, String)>> {
        // LIKE '%&%;%' rather than a regex — false positives are
        // fine (`fix_html_entities_in_catalog` checks each row
        // post-decode and only writes when the name actually
        // changed) and LIKE keeps the scan inside the `(catalog,
        // ext_name)` index instead of forcing a full row read.
        let sql = "SELECT `id`, `ext_name` FROM `entry` \
            WHERE `catalog` = :catalog_id \
              AND `ext_name` LIKE '%&%;%'";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(rows)
    }

    async fn auxiliary_distinct_props(&self) -> Result<Vec<usize>> {
        let sql = "SELECT DISTINCT `aux_p` FROM `auxiliary` ORDER BY `aux_p`";
        let rows = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        Ok(rows)
    }

    async fn maintenance_update_aux_candidates(
        &self,
        props_ext: &[usize],
        min_count: usize,
    ) -> Result<usize> {
        // Refuse to wipe the table when the allowlist is empty —
        // either the SPARQL fetch returned nothing or every aux
        // property got filtered out, and dropping every row would
        // break the random_prop creation_candidates picker until
        // the next successful run.
        if props_ext.is_empty() {
            return Ok(0);
        }

        // Build via TEMPORARY TABLE so the live `aux_candidates`
        // window stays minimal — the random-prop picker reads from
        // it and an empty interval would surface as "no candidates"
        // errors.
        let props_csv: String = props_ext.iter().join(",");
        let create_tmp = format!(
            "CREATE TEMPORARY TABLE aux_candidates_tmp \
             SELECT SQL_NO_CACHE aux_name, aux_p, count(*) AS cnt, \
                    sum(entry_is_matched) AS matched, \
                    group_concat(entry_id) AS entry_ids \
             FROM ( \
                SELECT entry.id AS entry_id, \
                       entry.ext_id AS aux_name, \
                       wd_prop AS aux_p, \
                       IF(q IS NULL OR user = 0, 0, 1) AS entry_is_matched \
                FROM entry, catalog \
                WHERE catalog.id = entry.catalog \
                  AND catalog.active = 1 \
                  AND wd_prop IN ({props_csv}) \
                  AND wd_qual IS NULL \
                  AND entry.ext_id != '' \
                UNION ALL \
                SELECT entry_id, aux_name, aux_p, entry_is_matched \
                FROM auxiliary \
                WHERE aux_p IN ({props_csv}) AND aux_name != '' \
             ) t \
             GROUP BY aux_p, aux_name \
             HAVING cnt >= {min_count} AND matched = 0"
        );

        let mut conn = self.get_conn().await?;
        conn.exec_drop("DROP TEMPORARY TABLE IF EXISTS aux_candidates_tmp", ())
            .await?;
        conn.exec_drop(create_tmp, ()).await?;
        conn.exec_drop("TRUNCATE aux_candidates", ()).await?;
        conn.exec_drop(
            "INSERT INTO aux_candidates SELECT * FROM aux_candidates_tmp",
            (),
        )
        .await?;
        let count: Option<usize> = conn
            .exec_first("SELECT count(*) FROM aux_candidates", ())
            .await?;
        conn.exec_drop("DROP TEMPORARY TABLE aux_candidates_tmp", ())
            .await?;
        Ok(count.unwrap_or(0))
    }

    async fn maintenance_fixup_wd_matches(&self) -> Result<(usize, usize, usize)> {
        let mut conn = self.get_conn().await?;

        // 1. Drop rows from deactivated catalogs — they only burn
        // sweep cycles and surface as "your match was lost" surprises
        // if the catalog ever comes back.
        conn.exec_drop(
            "DELETE FROM `wd_matches` WHERE `catalog` IN \
             (SELECT `id` FROM `catalog` WHERE `active` != 1)",
            (),
        )
        .await?;
        let deleted = conn.affected_rows() as usize;

        // 2. Back-fill catalog=0 rows. Pre-`entry_set_match_cleanup`
        // code paths sometimes inserted with an unset catalog; the
        // current cleanup populates it correctly but legacy rows linger.
        conn.exec_drop(
            "UPDATE `wd_matches` \
             SET `catalog` = (SELECT `entry`.`catalog` FROM `entry` \
                              WHERE `entry`.`id` = `wd_matches`.`entry_id`) \
             WHERE `wd_matches`.`catalog` = 0",
            (),
        )
        .await?;
        let recatalogued = conn.affected_rows() as usize;

        // 3. Flip every row whose catalog has no usable Wikidata
        // property pointer (no `wd_prop` set, or has a `wd_qual`)
        // straight to N/A — wd_match_sync's classifier can't do
        // anything meaningful with those rows.
        conn.exec_drop(
            "UPDATE `wd_matches` SET `status` = 'N/A' \
             WHERE `status` != 'N/A' \
               AND `catalog` IN ( \
                   SELECT `id` FROM `catalog` \
                   WHERE `wd_prop` IS NULL OR `wd_qual` IS NOT NULL \
               )",
            (),
        )
        .await?;
        let marked_na = conn.affected_rows() as usize;

        Ok((deleted, recatalogued, marked_na))
    }

    async fn maintenance_delete_multi_match_for_fully_matched(&self) -> Result<usize> {
        // Mirrors PHP exactly. EXISTS over the indexed (id, q, user)
        // columns lets MySQL short-circuit per row, which beats the
        // JOIN form when `multi_match` is large.
        let sql = "DELETE FROM `multi_match` \
            WHERE EXISTS ( \
                SELECT 1 FROM `entry` \
                WHERE `entry`.`id` = `multi_match`.`entry_id` \
                  AND `entry`.`q` IS NOT NULL \
                  AND `entry`.`q` > 0 \
                  AND `entry`.`user` IS NOT NULL \
                  AND `entry`.`user` > 0 \
            )";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(conn.affected_rows() as usize)
    }

    async fn entry_get_duplicate_qs_in_catalog(&self, catalog_id: usize) -> Result<Vec<isize>> {
        let sql = "SELECT `q` FROM `entry` \
            WHERE `catalog` = :catalog_id \
              AND `user` > 0 \
              AND `q` IS NOT NULL AND `q` > 0 \
            GROUP BY `q` \
            HAVING count(*) > 1";
        let rows: Vec<isize> = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { catalog_id })
            .await?
            .map_and_drop(from_row::<isize>)
            .await?;
        Ok(rows)
    }

    async fn overview_increment_noq(&self, catalog_id: usize, delta: usize) -> Result<()> {
        // No-op when there's nothing to add — saves a round-trip on
        // catalogs whose target already had every source ext_id.
        if delta == 0 {
            return Ok(());
        }
        // Mirrors `overview_apply_insert` but bumps both `total` and
        // `noq` by `delta` in one go: the merger always adds *unmatched*
        // rows, so the Noq bucket is always the right destination.
        // UPDATE on a missing overview row is a 0-row no-op; the next
        // catalog refresh will populate it.
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `overview` SET `total` = `total` + :delta, `noq` = `noq` + :delta \
                 WHERE `catalog` = :catalog_id",
                params! { catalog_id, delta },
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl crate::storage::AuxiliaryMatcherQueries for StorageMySQL {
    async fn auxiliary_matcher_match_via_aux(
        &self,
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
        extid_props: &[String],
        blacklisted_catalogs: &[String],
    ) -> Result<Vec<AuxiliaryResults>> {
        let sql = format!(
            "SELECT auxiliary.id,entry_id,0,aux_p,aux_name FROM entry,auxiliary
        WHERE entry_id=entry.id AND catalog=:catalog_id
        {}
        AND in_wikidata=0
        AND aux_p IN ({})
        AND catalog NOT IN ({})
        /* ORDER BY auxiliary.id */
        LIMIT :batch_size OFFSET :offset",
            MatchState::not_fully_matched().get_sql(),
            extid_props.join(","),
            blacklisted_catalogs.join(",")
        );
        let results = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(|row| {
                let (aux_id, entry_id, q_numeric, property, value) =
                    from_row::<(usize, usize, usize, usize, String)>(row);
                AuxiliaryResults::new(aux_id, entry_id, q_numeric, property, value)
            })
            .await?;
        Ok(results)
    }

    async fn auxiliary_matcher_add_auxiliary_to_wikidata(
        &self,
        blacklisted_properties: &[String],
        catalog_id: usize,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<AuxiliaryResults>> {
        let sql = format!(
            "SELECT auxiliary.id,entry_id,q,aux_p,aux_name FROM entry,auxiliary
            WHERE entry_id=entry.id AND catalog=:catalog_id
            {}
            AND in_wikidata=0
            AND aux_p NOT IN ({})
            AND (aux_p!=17 OR `type`!='Q5')
            ORDER BY auxiliary.id LIMIT :batch_size OFFSET :offset",
            MatchState::fully_matched().get_sql(),
            blacklisted_properties.join(",")
        );
        let results = self
            .get_conn_ro()
            .await?
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(|row| {
                let (aux_id, entry_id, q_numeric, property, value) =
                    from_row::<(usize, usize, usize, usize, String)>(row);
                AuxiliaryResults::new(aux_id, entry_id, q_numeric, property, value)
            })
            .await?;
        Ok(results)
    }
}

#[async_trait]
impl crate::storage::MetaEntryQueries for StorageMySQL {
    async fn meta_entry_get_mnm_relations(&self, entry_id: usize) -> Result<Vec<MetaMnmRelation>> {
        let sql =
            "SELECT `property`, `target_entry_id` FROM `mnm_relation` WHERE `entry_id`=:entry_id";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { entry_id })
            .await?
            .map_and_drop(|row| {
                let property: usize = row.get::<Option<usize>, _>(0).flatten().unwrap_or_default();
                let target_entry_id: usize =
                    row.get::<Option<usize>, _>(1).flatten().unwrap_or_default();
                MetaMnmRelation {
                    property,
                    target: MnmLink::EntryId(target_entry_id),
                }
            })
            .await?;
        Ok(ret)
    }

    async fn meta_entry_get_issues(&self, entry_id: usize) -> Result<Vec<MetaIssue>> {
        let sql = "SELECT `id`, `type`, `json`, `status`, `user_id`, `resolved_ts`, `catalog` FROM `issues` WHERE `entry_id`=:entry_id";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { entry_id })
            .await?
            .map_and_drop(|row| {
                let id: Option<usize> = row.get(0);
                let issue_type: String = row
                    .get::<Option<String>, _>(1)
                    .flatten()
                    .unwrap_or_default();
                let json_str: String = row
                    .get::<Option<String>, _>(2)
                    .flatten()
                    .unwrap_or_default();
                let status: String = row
                    .get::<Option<String>, _>(3)
                    .flatten()
                    .unwrap_or_default();
                let user_id: Option<usize> = row.get::<Option<usize>, _>(4).flatten();
                let resolved_ts: Option<String> = row.get::<Option<String>, _>(5).flatten();
                let catalog_id: usize =
                    row.get::<Option<usize>, _>(6).flatten().unwrap_or_default();
                MetaIssue {
                    id,
                    issue_type,
                    json: serde_json::from_str(&json_str).unwrap_or_default(),
                    status,
                    user_id,
                    resolved_ts,
                    catalog_id,
                }
            })
            .await?;
        Ok(ret)
    }

    async fn meta_entry_get_kv_entries(&self, entry_id: usize) -> Result<Vec<MetaKvEntry>> {
        let sql = "SELECT `kv_key`, `kv_value` FROM `kv_entry` WHERE `entry_id`=:entry_id";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { entry_id })
            .await?
            .map_and_drop(|row| {
                let key: String = row
                    .get::<Option<String>, _>(0)
                    .flatten()
                    .unwrap_or_default();
                let value: String = row
                    .get::<Option<String>, _>(1)
                    .flatten()
                    .unwrap_or_default();
                MetaKvEntry { key, value }
            })
            .await?;
        Ok(ret)
    }

    async fn meta_entry_get_log_entries(&self, entry_id: usize) -> Result<Vec<MetaLogEntry>> {
        let sql = "SELECT `id`, `action`, `user`, `timestamp`, `q` FROM `log` WHERE `entry_id`=:entry_id ORDER BY `id`";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { entry_id })
            .await?
            .map_and_drop(|row| {
                let id: Option<usize> = row.get(0);
                let action: String = row
                    .get::<Option<String>, _>(1)
                    .flatten()
                    .unwrap_or_default();
                let user: Option<usize> = row.get::<Option<usize>, _>(2).flatten();
                let timestamp: Option<String> = row.get::<Option<String>, _>(3).flatten();
                let q: Option<isize> = row.get::<Option<isize>, _>(4).flatten();
                MetaLogEntry {
                    id,
                    action,
                    user,
                    timestamp,
                    q,
                }
            })
            .await?;
        Ok(ret)
    }

    async fn meta_entry_get_statement_text(
        &self,
        entry_id: usize,
    ) -> Result<Vec<MetaStatementText>> {
        let sql = "SELECT `id`, `property`, `text`, `in_wikidata`, `entry_is_matched`, `q` FROM `statement_text` WHERE `entry_id`=:entry_id";
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! { entry_id })
            .await?
            .map_and_drop(|row| {
                let id: Option<usize> = row.get(0);
                let property: usize = row.get::<Option<usize>, _>(1).flatten().unwrap_or_default();
                let text: String = row
                    .get::<Option<String>, _>(2)
                    .flatten()
                    .unwrap_or_default();
                let in_wikidata: bool = row.get::<Option<bool>, _>(3).flatten().unwrap_or_default();
                let entry_is_matched: bool =
                    row.get::<Option<bool>, _>(4).flatten().unwrap_or_default();
                let q: Option<ItemId> = row.get::<Option<ItemId>, _>(5).flatten();
                MetaStatementText {
                    id,
                    property,
                    text,
                    in_wikidata,
                    entry_is_matched,
                    q,
                }
            })
            .await?;
        Ok(ret)
    }

    async fn meta_entry_delete_auxiliary(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "DELETE FROM `auxiliary` WHERE `entry_id`=:entry_id",
                params! { entry_id },
            )
            .await?;
        Ok(())
    }

    async fn meta_entry_delete_aliases(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "DELETE FROM `aliases` WHERE `entry_id`=:entry_id",
                params! { entry_id },
            )
            .await?;
        Ok(())
    }

    async fn meta_entry_delete_descriptions(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "DELETE FROM `descriptions` WHERE `entry_id`=:entry_id",
                params! { entry_id },
            )
            .await?;
        Ok(())
    }

    async fn meta_entry_delete_mnm_relations(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "DELETE FROM `mnm_relation` WHERE `entry_id`=:entry_id",
                params! { entry_id },
            )
            .await?;
        Ok(())
    }

    async fn meta_entry_delete_kv_entries(&self, entry_id: usize) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "DELETE FROM `kv_entry` WHERE `entry_id`=:entry_id",
                params! { entry_id },
            )
            .await?;
        Ok(())
    }

    async fn meta_entry_set_kv_entry(&self, entry_id: usize, key: &str, value: &str) -> Result<()> {
        let sql = "REPLACE INTO `kv_entry` (`entry_id`, `kv_key`, `kv_value`) VALUES (:entry_id, :key, :value)";
        self.get_conn()
            .await?
            .exec_drop(sql, params! { entry_id, key, value })
            .await?;
        Ok(())
    }
}

// Inherent helpers that are not part of the Storage trait.
impl StorageMySQL {
    /// Shared body for `api_get_catalog_overview`,
    /// `api_get_single_catalog_overview`, and `api_get_catalog_overview_for_ids`.
    ///
    /// Previously ran four separate queries (catalog, overview, user,
    /// autoscrape) and merged the results client-side. A single LEFT JOIN
    /// is equivalent data-wise (each side is 1:0-or-1 per catalog) but
    /// saves three round-trips and lets MySQL plan a single index scan
    /// over `catalog` instead of re-filtering it three times.
    ///
    /// When `id_filter` is Some, the filter is pushed into SQL so "fetch
    /// one (or a few) catalog(s)" doesn't scan the whole active set — that
    /// used to make single_catalog take ~10s and batch_catalogs with 5 ids
    /// take ~50s.
    async fn api_get_catalog_overview_impl(
        &self,
        id_filter: Option<&[usize]>,
    ) -> Result<Vec<serde_json::Value>> {
        if matches!(id_filter, Some(ids) if ids.is_empty()) {
            return Ok(vec![]);
        }
        // `catalog.desc` is a reserved word, hence the backticks. Every
        // joined column gets an alias so column-name collisions between
        // `overview.types` and whatever future column lands on `catalog`
        // never cross-contaminate the Row getters.
        let id_filter_clause = match id_filter {
            Some(ids) => format!(" AND c.`id` IN ({})", ids.iter().join(",")),
            None => String::new(),
        };
        let sql = format!(
            "SELECT \
                c.`id` AS c_id, c.`name` AS c_name, c.`url` AS c_url, \
                c.`desc` AS c_desc, c.`type` AS c_type, \
                c.`wd_prop` AS c_wd_prop, c.`wd_qual` AS c_wd_qual, \
                c.`search_wp` AS c_search_wp, c.`active` AS c_active, \
                c.`owner` AS c_owner, c.`note` AS c_note, \
                c.`source_item` AS c_source_item, \
                c.`has_person_date` AS c_has_person_date, \
                c.`taxon_run` AS c_taxon_run, \
                u.`id` AS u_id, u.`name` AS u_name, \
                o.`catalog` AS o_catalog, \
                o.`total` AS o_total, o.`noq` AS o_noq, o.`autoq` AS o_autoq, \
                o.`na` AS o_na, o.`manual` AS o_manual, o.`nowd` AS o_nowd, \
                o.`multi_match` AS o_multi_match, o.`types` AS o_types, \
                a.`catalog` AS a_catalog, \
                a.`last_update` AS a_last_update, \
                a.`do_auto_update` AS a_do_auto_update, \
                a.`json` AS a_json \
             FROM `catalog` c \
               LEFT JOIN `overview` o ON o.`catalog` = c.`id` \
               LEFT JOIN `user` u ON u.`id` = c.`owner` \
               LEFT JOIN `autoscrape` a ON a.`catalog` = c.`id` \
             WHERE c.`active`>=1{id_filter_clause}"
        );
        let mut conn = self.get_conn_ro().await?;
        let rows = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(Self::overview_row_to_json)
            .await?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;
    use crate::job_status::JobStatus;

    #[test]
    fn normalize_wd_prop_zero_and_none_become_none() {
        assert_eq!(normalize_wd_prop(None), None);
        assert_eq!(normalize_wd_prop(Some(0)), None);
    }

    fn base_filter() -> CatalogEntryListFilter {
        CatalogEntryListFilter {
            catalog_id: 42,
            ..Default::default()
        }
    }

    #[test]
    fn catalog_entries_where_clause_show_multiple() {
        let mut f = base_filter();
        f.show_multiple = true;
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(
            sql,
            "catalog=42 AND EXISTS (SELECT 1 FROM multi_match WHERE entry_id=entry.id) AND (user<=0 OR user is null)"
        );
    }

    #[test]
    fn catalog_entries_where_clause_show_noq_fast_path() {
        let mut f = base_filter();
        f.show_noq = true;
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql, "catalog=42 AND q IS NULL");
    }

    #[test]
    fn catalog_entries_where_clause_show_na_fast_path() {
        let mut f = base_filter();
        f.show_na = true;
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql, "catalog=42 AND q=0");
    }

    #[test]
    fn catalog_entries_where_clause_show_nowd_fast_path() {
        let mut f = base_filter();
        f.show_nowd = true;
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql, "catalog=42 AND q=-1");
    }

    #[test]
    fn catalog_entries_where_clause_general_fallback_two_flags() {
        let mut f = base_filter();
        f.show_noq = true;
        f.show_na = true;
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(
            sql,
            "catalog=42 AND (q is null OR user!=0) AND (user<=0 OR user is null)"
        );
    }

    #[test]
    fn catalog_entries_where_clause_all_flags_off() {
        let f = base_filter();
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(
            sql,
            "catalog=42 AND q IS NOT NULL AND (q is null OR user!=0) AND (user<=0 OR user is null) AND (q!=0 or q is null)"
        );
    }

    #[test]
    fn catalog_entries_where_clause_text_filters() {
        let mut f = base_filter();
        f.show_noq = true;
        f.entry_type = "Q5".into();
        f.title_match = "smith".into();
        f.keyword = "writer".into();
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(
            sql,
            "catalog=42 AND q IS NULL AND `type`='Q5' AND `ext_name` LIKE '%smith%' AND (`ext_name` LIKE '%writer%' OR `ext_desc` LIKE '%writer%')"
        );
    }

    #[test]
    fn catalog_entries_where_clause_user_id() {
        let mut f = base_filter();
        f.show_noq = true;
        f.user_id = Some(7);
        let sql_pos = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql_pos, "catalog=42 AND q IS NULL AND `user`=7");

        f.user_id = Some(0);
        let sql_zero = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql_zero, "catalog=42 AND q IS NULL AND `user`=0");

        f.user_id = Some(-1);
        let sql_neg = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql_neg, "catalog=42 AND q IS NULL");
    }

    #[test]
    fn catalog_entries_where_clause_quote_doubling() {
        let mut f = base_filter();
        f.show_noq = true;
        f.entry_type = "O'Reilly".into();
        let sql = StorageMySQL::catalog_entries_where_clause(&f);
        assert_eq!(sql, "catalog=42 AND q IS NULL AND `type`='O''Reilly'");
    }

    #[test]
    fn normalize_wd_prop_positive_passes_through() {
        assert_eq!(normalize_wd_prop(Some(1)), Some(1));
        assert_eq!(normalize_wd_prop(Some(7471)), Some(7471));
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_get_all_external_ids() {
        let app = get_test_app();
        let results = app.storage().get_all_external_ids(1).await.unwrap();
        assert_eq!(results.len(), 67233);
        assert!(results.contains_key("100006"));
    }

    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_get_overview_table() {
        let app = get_test_app();
        let results = app.storage().get_overview_table().await.unwrap();
        assert!(results.len() > 5000);
    }

    // #lizard forgives
    #[test]
    fn test_jobs_get_next_job_construct_sql() {
        let catalog_filter =
            "AND NOT EXISTS (SELECT * FROM catalog WHERE catalog.id=jobs.catalog AND active!=1)";

        // High priority
        let sql1 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::HighPriority, None, &[], None);
        let expected = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
            JobStatus::HighPriority.as_str()
        );
        assert_eq!(sql1, expected);

        // Low priority
        let sql2 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::LowPriority, None, &[], None);
        let expected2 = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
            JobStatus::LowPriority.as_str()
        );
        assert_eq!(sql2, expected2);

        // Next dependent
        let sql2a = StorageMySQL::jobs_get_next_job_construct_sql(
            JobStatus::Todo,
            Some(JobStatus::Done),
            &[],
            None,
        );
        let expected2a = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}') ORDER BY `last_ts` LIMIT 1",
            JobStatus::Todo.as_str(),
            JobStatus::Done.as_str()
        );
        assert_eq!(sql2a, expected2a);

        // get_next_initial_allowed_job
        let avoid = vec!["test1".to_string(), "test2".to_string()];
        let sql3 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &avoid, None);
        let not_in = avoid.join("','");
        let expected3 = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL AND `action` NOT IN ('{}') ORDER BY `last_ts` LIMIT 1",
            JobStatus::Todo.as_str(),
            &not_in
        );
        assert_eq!(sql3, expected3);

        // get_next_initial_job
        let sql4 = StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &[], None);
        let expected4 = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
            JobStatus::Todo.as_str()
        );
        assert_eq!(sql4, expected4);

        // get_next_scheduled_job
        let timestamp = TimeStamp::now();
        let sql5 = StorageMySQL::jobs_get_next_job_construct_sql(
            JobStatus::Done,
            None,
            &[],
            Some(timestamp.to_owned()),
        );
        let expected5 = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `next_ts`!='' AND `next_ts`<='{}' ORDER BY `next_ts` LIMIT 1",
            JobStatus::Done.as_str(),
            &timestamp
        );
        assert_eq!(sql5, expected5);

        // get_next_initial_job with avoid
        let no_actions = vec!["foo".to_string(), "bar".to_string()];
        let sql6 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &no_actions, None);
        let expected6 = format!(
            "SELECT /* jobs_get_next_job */ `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL AND `action` NOT IN ('foo','bar') ORDER BY `last_ts` LIMIT 1",
            JobStatus::Todo.as_str()
        );
        assert_eq!(sql6, expected6);
    }

    #[test]
    fn test_escape_sql_literal() {
        assert_eq!(escape_sql_literal("plain"), "plain");
        assert_eq!(escape_sql_literal("it's"), "it''s");
        assert_eq!(escape_sql_literal(r"back\slash"), r"back\\slash");
        // Backslash is escaped before the quote, so a literal `\'` pair
        // (which MySQL would otherwise treat as an escaped quote) is
        // rendered inert as `\\''`.
        assert_eq!(escape_sql_literal(r"\'"), r"\\''");
    }

    #[test]
    fn catalog_entries_where_show_noq_only_is_q_is_null() {
        // The common "Unmatched only" listing — the slow-on-catalog=6502
        // URL — should emit the compact `q IS NULL` form so MySQL can
        // pick an index on (catalog, q) / (catalog_only) + q-nullness
        // instead of evaluating a 3-conjunct disjunction per row.
        let filter = crate::storage::CatalogEntryListFilter {
            catalog_id: 6502,
            show_noq: true,
            show_autoq: false,
            show_userq: false,
            show_na: false,
            show_nowd: false,
            show_multiple: false,
            ..Default::default()
        };
        let where_clause = StorageMySQL::catalog_entries_where_clause(&filter);
        assert_eq!(where_clause, "catalog=6502 AND q IS NULL");
    }

    #[test]
    fn catalog_entries_where_show_na_only_is_q_zero() {
        // Regression guard for the pre-existing fast paths.
        let filter = crate::storage::CatalogEntryListFilter {
            catalog_id: 1,
            show_na: true,
            ..Default::default()
        };
        assert_eq!(
            StorageMySQL::catalog_entries_where_clause(&filter),
            "catalog=1 AND q=0"
        );
    }

    #[test]
    fn catalog_entries_where_show_nowd_only_is_q_minus_one() {
        let filter = crate::storage::CatalogEntryListFilter {
            catalog_id: 1,
            show_nowd: true,
            ..Default::default()
        };
        assert_eq!(
            StorageMySQL::catalog_entries_where_clause(&filter),
            "catalog=1 AND q=-1"
        );
    }

    #[test]
    fn catalog_entries_where_mixed_uses_general_branch() {
        // Two "show_*" flags on at once must NOT hit the noq-only
        // fast-path — otherwise autoq entries would be hidden.
        let filter = crate::storage::CatalogEntryListFilter {
            catalog_id: 1,
            show_noq: true,
            show_autoq: true,
            ..Default::default()
        };
        let clause = StorageMySQL::catalog_entries_where_clause(&filter);
        // Still filters out userq/na via the general branch.
        assert!(clause.contains("(user<=0 OR user is null)"));
        assert!(clause.contains("(q!=0 or q is null)"));
    }

    #[test]
    fn test_build_api_search_entries_sql_empty_words() {
        assert!(
            StorageMySQL::build_api_search_entries_sql(&[], true, false, &[], &[], 10).is_none()
        );
    }

    #[test]
    fn test_build_api_search_entries_sql_all_disabled() {
        let words = vec!["abc".to_string()];
        assert!(
            StorageMySQL::build_api_search_entries_sql(&words, false, true, &[], &[], 10).is_none()
        );
    }

    #[test]
    fn test_build_api_search_entries_sql_basic() {
        let words = vec!["foo".to_string(), "bar".to_string()];
        let sql = StorageMySQL::build_api_search_entries_sql(&words, false, false, &[], &[], 25)
            .expect("non-empty sql");
        assert!(sql.contains("MATCH(`ext_name`) AGAINST('+foo +bar' IN BOOLEAN MODE)"));
        assert!(!sql.contains("MATCH(`ext_desc`)"));
        assert!(sql.ends_with(" LIMIT 25"));
    }

    #[test]
    fn test_build_api_search_entries_sql_with_description_and_filters() {
        let words = vec!["foo".to_string()];
        let sql =
            StorageMySQL::build_api_search_entries_sql(&words, true, false, &[3, 7], &[1, 2], 10)
                .expect("non-empty sql");
        assert!(sql.contains("MATCH(`ext_name`)"));
        assert!(sql.contains("MATCH(`ext_desc`)"));
        assert!(sql.contains("`catalog` NOT IN (3,7)"));
        assert!(sql.contains("`catalog` IN (1,2)"));
    }

    #[test]
    fn test_build_api_search_entries_sql_escapes_quotes() {
        // A user-supplied word containing a single quote or backslash must not
        // be able to break out of the AGAINST() string literal.
        let words = vec!["ev'il".to_string(), r"b\d".to_string()];
        let sql = StorageMySQL::build_api_search_entries_sql(&words, false, false, &[], &[], 10)
            .expect("non-empty sql");
        // Count single quotes: should be exactly 2 (the outer delimiters of
        // the AGAINST literal); any user-contributed quote is doubled inside.
        let lone_quotes = sql
            .chars()
            .enumerate()
            .filter(|&(i, c)| {
                c == '\''
                    && sql.as_bytes().get(i + 1).copied() != Some(b'\'')
                    && (i == 0 || sql.as_bytes()[i - 1] != b'\'')
            })
            .count();
        assert_eq!(lone_quotes, 2, "unescaped quote in: {sql}");
        assert!(sql.contains("+ev''il"));
        assert!(sql.contains(r"+b\\d"));
    }
}

/* TODO

#[tokio::test]
async fn test_get_overview_column_name_for_user_and_q() {
    let mnm = get_test_mnm();
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(0), None),
        "autoq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), Some(1)),
        "manual"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), Some(0)),
        "na"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), Some(-1)),
        "nowd"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), None),
        "noq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&None, None),
        "noq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&None, Some(1)),
        "noq"
    );
}

#[tokio::test]
async fn test_match_via_auxiliary() {
    let _test_lock = TEST_MUTEX.lock();
    let mnm = get_test_mnm();
    let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
    entry
        .set_auxiliary(214, Some("30701597".to_string()))
        .await
        .unwrap();
    entry.unmatch().await.unwrap();

    // Run matcher
    let mut am = AuxiliaryMatcher::new(&mnm);
    am.match_via_auxiliary(TEST_CATALOG_ID).await.unwrap();

    // Check
    let mut entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
    assert_eq!(entry.q.unwrap(), 13520818);

    // Cleanup
    entry.set_auxiliary(214, None).await.unwrap();
    entry.unmatch().await.unwrap();
    let catalog_id = TEST_CATALOG_ID;
    let mut conn = self.get_conn().await?;
    conn.exec_drop(
            "DELETE FROM `jobs` WHERE `action`='aux2wd' AND `catalog`=:catalog_id",
            params! {catalog_id},
        )
        .await
        .unwrap();
}


#[cfg(test)]
mod tests {

    use super::*;
    use crate::issue::{Issue, IssueType};
    use crate::job_status::JobStatus;
    use mysql_async::from_row;
    use serde_json::json;

    const _TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_issue_insert() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let entry_id = TEST_ENTRY_ID;

        // Cleanup
        mnm.app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_drop(
                "DELETE FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap();

        let issues_for_entry = *mnm
            .app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_iter(
                "SELECT count(*) AS `cnt` FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap()
            .map_and_drop(from_row::<usize>)
            .await
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(issues_for_entry, 0);

        let issue = Issue::new(entry_id, IssueType::Mismatch, json!("!"));
        issue
            .insert(mnm.app.storage().as_ref().as_ref())
            .await
            .unwrap();

        let issues_for_entry = *mnm
            .app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_iter(
                "SELECT count(*) AS `cnt` FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap()
            .map_and_drop(from_row::<usize>)
            .await
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(issues_for_entry, 1);

        // Cleanup
        mnm.app
            .get_mnm_conn()
            .await
            .unwrap()
            .exec_drop(
                "DELETE FROM `issues` WHERE `entry_id`=:entry_id",
                params! {entry_id},
            )
            .await
            .unwrap();
    }
}

*/
