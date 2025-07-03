pub use crate::storage::Storage;
use crate::{
    app_state::USER_AUTO,
    automatch::{ResultInOriginalCatalog, ResultInOtherCatalog},
    auxiliary_matcher::AuxiliaryResults,
    catalog::Catalog,
    cersei::CurrentScraper,
    coordinate_matcher::LocationRow,
    entry::{AuxiliaryRow, CoordinateLocation, Entry, EntryError},
    issue::Issue,
    job_row::JobRow,
    job_status::JobStatus,
    match_state::MatchState,
    microsync::EXT_URL_UNIQUE_SEPARATOR,
    mysql_misc::MySQLMisc,
    prop_todo::PropTodo,
    task_size::TaskSize,
    taxon_matcher::{RankedNames, TaxonMatcher, TaxonNameField, TAXON_RANKS},
    update_catalog::UpdateInfo,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::Params::Empty;
use mysql_async::{from_row, futures::GetConn, prelude::*, Params, Row};
use rand::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use wikimisc::{timestamp::TimeStamp, wikibase::LocaleString};

pub const TABLES_WITH_ENTRY_ID_FIELDS: &[&str] = &[
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

    fn get_conn(&self) -> GetConn {
        self.pool.get_conn()
    }

    fn get_conn_ro(&self) -> GetConn {
        self.pool_ro.get_conn()
    }

    fn coordinate_matcher_main_query_sql(
        catalog_id: &Option<usize>,
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> String {
        let conditions_catalog_id = match catalog_id {
            Some(catalog_id) => format!("`catalog`={catalog_id}"),
            None => Self::coordinate_matcher_main_query_sql_subquery(bad_catalogs, max_results),
        };
        let conditions_not_fully_matched = &MatchState::not_fully_matched().get_sql();
        format!("SELECT `lat`,`lon`,`id`,`catalog`,`ext_name`,`type`,`q` FROM `vw_location` WHERE `ext_name`!='' AND {conditions_catalog_id} {conditions_not_fully_matched}")
    }

    fn location_row_from_row(row: &Row) -> Option<LocationRow> {
        Some(LocationRow {
            lat: row.get(0)?,
            lon: row.get(1)?,
            entry_id: row.get(2)?,
            catalog_id: row.get(3)?,
            ext_name: row.get(4)?,
            entry_type: row.get(5)?,
            q: row.get(6)?,
        })
    }

    // #lizard forgives
    fn catalog_from_row(row: &Row) -> Option<Catalog> {
        Some(Catalog {
            id: row.get(0)?,
            name: row.get(1)?,
            url: row.get(2)?,
            desc: row.get(3)?,
            type_name: row.get(4)?,
            wd_prop: row.get(5)?,
            wd_qual: row.get(6)?,
            search_wp: row.get(7)?,
            active: row.get(8)?,
            owner: row.get(9)?,
            note: row.get(10)?,
            source_item: row.get(11)?,
            has_person_date: row.get(12)?,
            taxon_run: row.get(13)?,
            app: None,
        })
    }

    async fn entry_set_match_cleanup(
        &self,
        entry: &Entry,
        user_id: usize,
        q_numeric: isize,
    ) -> Result<bool> {
        // Update overview table and misc cleanup
        self.update_overview_table(entry, Some(user_id), Some(q_numeric))
            .await?;
        let is_full_match = user_id > 0 && q_numeric > 0;
        let is_matched = if is_full_match { 1 } else { 0 };
        self.entry_set_match_status(entry.id, "UNKNOWN", is_matched)
            .await?;
        if user_id != USER_AUTO {
            self.entry_remove_multi_match(entry.id).await?;
        }
        self.queue_reference_fixer(q_numeric).await?;
        Ok(true)
    }

    /// Computes the column of the overview table that is affected, given a user ID and item ID
    fn get_overview_column_name_for_user_and_q(
        user_id: &Option<usize>,
        q: &Option<isize>,
    ) -> String {
        match (user_id, q) {
            (Some(0), _) => "autoq",
            (Some(_), None) => "noq",
            (Some(_), Some(0)) => "na",
            (Some(_), Some(-1)) => "nowd",
            (Some(_), _) => "manual",
            _ => "noq",
        }
        .to_string()
    }

    fn jobs_get_next_job_construct_sql(
        status: JobStatus,
        depends_on: Option<JobStatus>,
        no_actions: &[String],
        next_ts: Option<String>,
    ) -> String {
        let mut sql = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}'",
            status.as_str()
        );
        sql += r#" AND NOT EXISTS (SELECT * FROM catalog WHERE catalog.id=jobs.catalog AND active!=1)"#; // No inactive catalogs
        match depends_on {
            Some(other_status) => {
                sql += &format!(" AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}')",other_status.as_str());
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

    fn entry_sql_select() -> String {
        r"SELECT id,catalog,ext_id,ext_url,ext_name,ext_desc,q,user,timestamp,if(isnull(random),rand(),random) as random,`type` FROM `entry`".into()
    }

    // #lizard forgives
    fn entry_from_row(row: &Row) -> Option<Entry> {
        Some(Entry {
            id: row.get(0)?,
            catalog: row.get(1)?,
            ext_id: row.get(2)?,
            ext_url: row.get(3)?,
            ext_name: row.get(4)?,
            ext_desc: row.get(5)?,
            q: Entry::value2opt_isize(row.get(6)?).ok()?,
            user: Entry::value2opt_usize(row.get(7)?).ok()?,
            timestamp: Entry::value2opt_string(row.get(8)?).ok()?,
            random: row.get(9).unwrap_or(0.0), // random might be null, who cares
            type_name: Entry::value2opt_string(row.get(10)?).ok()?,
            app: None,
        })
    }

    async fn match_taxa_get_ranked_names_batch_get_results(
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

    fn coordinate_matcher_main_query_sql_subquery(
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> String {
        let r: f64 = rand::rng().random();
        let mut sql = format!("`random`>={r} ORDER BY `random` LIMIT {max_results}");
        if !bad_catalogs.is_empty() {
            let s = bad_catalogs
                .iter()
                .map(|id| format!("{id}"))
                .collect::<Vec<String>>()
                .join(",");
            sql += &format!("AND `catalog` NOT IN ({s})");
        }
        sql
    }
}

// STORAGE TRAIT IMPLEMENTATION

#[async_trait]
impl Storage for StorageMySQL {
    async fn disconnect(&self) -> Result<()> {
        self.disconnect_db().await?;
        Ok(())
    }

    // Taxon matcher
    async fn set_catalog_taxon_run(&self, catalog_id: usize, taxon_run: bool) -> Result<()> {
        let taxon_run = taxon_run as u16;
        let sql =
            "UPDATE `catalog` SET `taxon_run`=1 WHERE `id`=:catalog_id AND `taxon_run`=:taxon_run";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id, taxon_run}).await?;
        Ok(())
    }

    async fn catalog_get_entries_of_people_with_initials(
        &self,
        catalog_id: usize,
    ) -> Result<Vec<Entry>> {
        let select = Self::entry_sql_select();
        let sql = format!(
            "{select} WHERE `type`='Q5' AND (q IS NULL OR user=0) AND ext_name rlike '\\\\. ' AND catalog=:catalog_id",
        );
        let ret = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        Ok(ret)
    }

    async fn match_taxa_get_ranked_names_batch(
        &self,
        ranks: &[&str],
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<(usize, RankedNames)> {
        let results = self
            .match_taxa_get_ranked_names_batch_get_results(
                ranks, field, catalog_id, batch_size, offset,
            )
            .await?;
        let mut ranked_names: RankedNames = HashMap::new();
        for result in &results {
            let entry_id = result.0;
            let taxon_name = match TaxonMatcher::rewrite_taxon_name(catalog_id, &result.1) {
                Some(s) => s,
                None => continue,
            };
            let type_name = &result.2;
            let rank = match TAXON_RANKS.get(type_name.as_str()) {
                Some(rank) => format!(" ; wdt:P105 {rank}"),
                None => "".to_string(),
            };
            ranked_names
                .entry(rank)
                .or_default()
                .push((entry_id, taxon_name));
        }
        Ok((results.len(), ranked_names))
    }

    // Coordinate Matcher

    async fn get_coordinate_matcher_rows(
        &self,
        catalog_id: &Option<usize>,
        bad_catalogs: &[usize],
        max_results: usize,
    ) -> Result<Vec<LocationRow>> {
        let sql = Self::coordinate_matcher_main_query_sql(catalog_id, bad_catalogs, max_results);
        let mut conn = self.get_conn_ro().await?;
        let rows: Vec<LocationRow> = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(|row| Self::location_row_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        Ok(rows)
    }

    async fn get_all_catalogs_key_value_pairs(&self) -> Result<Vec<(usize, String, String)>> {
        let sql = r#"SELECT `catalog_id`,`kv_key`,`kv_value` FROM `kv_catalog`"#;
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(usize, String, String)>)
            .await?;
        Ok(results)
    }

    // Data source

    async fn get_data_source_type_for_uuid(&self, uuid: &str) -> Result<Vec<String>> {
        let results = "SELECT `type` FROM `import_file` WHERE `uuid`=:uuid"
            .with(params! {uuid})
            .map(self.get_conn().await?, |type_name| type_name)
            .await?;
        Ok(results)
    }

    async fn get_existing_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<String>> {
        let placeholders = Self::sql_placeholders(ext_ids.len());
        let sql = format!(
            "SELECT `ext_id` FROM entry WHERE `ext_id` IN ({}) AND `catalog`={}",
            &placeholders, catalog_id
        );
        let existing_ext_ids = sql
            .with(ext_ids.to_vec()) // TODO don't convert to Vec
            .map(self.get_conn_ro().await?, |ext_id| ext_id)
            .await?;
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

    // Catalog

    async fn create_catalog(&self, catalog: &Catalog) -> Result<usize> {
        if catalog.id != crate::catalog::BLANK_CATALOG_ID {
            return Err(anyhow!("Catalog ID is not blank"));
        }

        let mut conn = self.get_conn().await?;
        let sql = r"INSERT IGNORE INTO `catalog` (`name`, `url`, `desc`, `type`, `wd_prop`, `wd_qual`, `search_wp`,`active`,`owner`,`note`,`source_item`,`has_person_date`,`taxon_run`)
        	VALUES (:name, :url, :desc, :type_name, :wd_prop, :wd_qual, :search_wp, :active, :owner, :note, :source_item, :has_person_date, :taxon_run)";
        let name = catalog.name.clone();
        let url = catalog.url.clone();
        let desc = catalog.desc.clone();
        let type_name = catalog.type_name.clone();
        let wd_prop = catalog.wd_prop.clone();
        let wd_qual = catalog.wd_qual.clone();
        let search_wp = catalog.search_wp.clone();
        let active = catalog.active.clone();
        let owner = catalog.owner.clone();
        let note = catalog.note.clone();
        let source_item = catalog.source_item.clone();
        let has_person_date = catalog.has_person_date.clone();
        let taxon_run = catalog.taxon_run.clone();
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
        let mut rows: Vec<Catalog> = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(|row| Self::catalog_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        drop(conn);
        let ret = rows
            .pop()
            .ok_or(anyhow!("No catalog #{catalog_id}"))?
            .to_owned();
        Ok(ret)
    }

    async fn get_catalog_from_name(&self, name: &str) -> Result<Catalog> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `name`=:name";
        let mut conn = self.get_conn_ro().await?;
        let mut rows: Vec<Catalog> = conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(|row| Self::catalog_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        drop(conn);
        let ret = rows.pop().ok_or(anyhow!("No catalog '{name}'"))?.to_owned();
        Ok(ret)
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

    async fn catalog_refresh_overview_table(&self, catalog_id: usize) -> Result<()> {
        let sql = r"REPLACE INTO `overview` (catalog,total,noq,autoq,na,manual,nowd,multi_match,types) VALUES (
	        :catalog_id,
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id),
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q` IS NULL),
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `user`=0),
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q`=0),
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q` IS NOT NULL AND `user`>0),
	        (SELECT count(*) FROM `entry` WHERE `catalog`=:catalog_id AND `q`=-1),
	        (SELECT count(*) FROM `multi_match` WHERE `catalog`=:catalog_id),
	        (SELECT group_concat(DISTINCT `type` SEPARATOR '|') FROM `entry` WHERE `catalog`=:catalog_id)
	        )";
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
        let sql = format!(
            "SELECT `id`,`ext_name` FROM `entry` WHERE `id` IN ({})",
            placeholders
        );
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
        let sql = format!("SELECT q,group_concat(id) AS ids,group_concat(ext_id SEPARATOR '{}') AS ext_ids FROM entry WHERE catalog=:catalog_id AND q IS NOT NULL and q>0 AND user>0 GROUP BY q HAVING count(id)>1 ORDER BY q",EXT_URL_UNIQUE_SEPARATOR);
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
        let sql = format!("SELECT `id`,`q`,`user`,`ext_id`,`ext_url` FROM `entry` WHERE `catalog`={catalog_id} AND `ext_id` IN ({placeholders})");
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
    async fn update_overview_table(
        &self,
        old_entry: &Entry,
        user_id: Option<usize>,
        q: Option<isize>,
    ) -> Result<()> {
        let add_column = Self::get_overview_column_name_for_user_and_q(&user_id, &q);
        let reduce_column =
            Self::get_overview_column_name_for_user_and_q(&old_entry.user, &old_entry.q);
        let catalog_id = old_entry.catalog;
        let sql = format!(
            "UPDATE overview SET {}={}+1,{}={}-1 WHERE catalog=:catalog_id",
            &add_column, &add_column, &reduce_column, &reduce_column
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id}).await?;
        Ok(())
    }

    async fn queue_reference_fixer(&self, q_numeric: isize) -> Result<()> {
        self.get_conn().await?.exec_drop(r"INSERT INTO `reference_fixer` (`q`,`done`) VALUES (:q_numeric,0) ON DUPLICATE KEY UPDATE `done`=0",params! {q_numeric}).await?;
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
        let sql = "SELECT id FROM catalog WHERE active=1 AND wd_prop IS NOT NULL and wd_qual IS NULL ORDER by rand() LIMIT 1" ;
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

    // Issue

    async fn issue_insert(&self, issue: &Issue) -> Result<()> {
        let sql = "INSERT IGNORE INTO `issues` (`entry_id`,`type`,`json`,`random`,`catalog`)
        SELECT :entry_id,:issue_type,:json,rand(),`catalog` FROM `entry` WHERE `id`=:entry_id";
        let params = params! {
            "entry_id" => issue.entry_id,
            "issue_type" => issue.issue_type.to_str(),
            "json" => issue.json.to_string(),
            "catalog" => issue.catalog_id,
        };
        self.get_conn().await?.exec_drop(sql, params).await?;
        Ok(())
    }

    // Autoscrape

    async fn autoscrape_get_for_catalog(&self, catalog_id: usize) -> Result<Vec<(usize, String)>> {
        Ok(self
            .get_conn_ro()
            .await?
            .exec_iter(
                "SELECT `id`,`json` FROM `autoscrape` WHERE `catalog`=:catalog_id",
                params! {catalog_id},
            )
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?)
    }

    async fn autoscrape_get_entry_ids_for_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<(String, usize)>> {
        let placeholders = Self::sql_placeholders(ext_ids.len());
        let sql = format!(
            "SELECT `ext_id`,`id` FROM entry WHERE `ext_id` IN ({placeholders}) AND `catalog`={catalog_id}"
        );
        let existing_ext_ids: Vec<(String, usize)> = self
            .get_conn_ro()
            .await?
            .exec_iter(sql, ext_ids.to_vec())
            .await?
            .map_and_drop(from_row::<(String, usize)>)
            .await?;
        Ok(existing_ext_ids)
    }

    async fn autoscrape_start(&self, autoscrape_id: usize) -> Result<()> {
        let sql = "UPDATE `autoscrape` SET `status`='RUNNING'`last_run_min`=NULL,`last_run_urls`=NULL WHERE `id`=:autoscrape_id" ;
        if let Ok(mut conn) = self.get_conn().await {
            let _ = conn.exec_drop(sql, params! {autoscrape_id}).await; // Ignore error
        }
        Ok(())
    }

    async fn autoscrape_finish(&self, autoscrape_id: usize, last_run_urls: usize) -> Result<()> {
        let sql = "UPDATE `autoscrape` SET `status`='OK',`last_run_min`=NULL,`last_run_urls`=:last_run_urls WHERE `id`=:autoscrape_id" ;
        if let Ok(mut conn) = self.get_conn().await {
            let _ = conn
                .exec_drop(sql, params! {autoscrape_id,last_run_urls})
                .await;
        }
        Ok(())
    }

    // Auxiliary matcher

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
            .map_and_drop(from_row::<(usize, usize, usize, usize, String)>)
            .await?;
        let results: Vec<AuxiliaryResults> =
            results.iter().map(AuxiliaryResults::from_result).collect();
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
            .map_and_drop(from_row::<(usize, usize, usize, usize, String)>)
            .await?;
        let results: Vec<AuxiliaryResults> =
            results.iter().map(AuxiliaryResults::from_result).collect();
        Ok(results)
    }

    // Maintenance

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
        let deletions_string = deletions
            .iter()
            .map(|i| format!("{}", *i))
            .collect::<Vec<String>>()
            .join(",");
        let sql1 =
            format!("SELECT DISTINCT `catalog` FROM `entry` WHERE `q` IN ({deletions_string})");
        let mut conn = self.get_conn().await?;
        let catalog_ids = conn
            .exec_iter(sql1, ())
            .await?
            .map_and_drop(from_row::<usize>)
            .await?;
        let sql2 = format!("UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q` IN ({deletions_string})");
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
        propval2item: &HashMap<String, isize>,
        params: Vec<String>,
    ) -> Result<Vec<(usize, String, Option<usize>, Option<usize>)>> {
        let catalogs_str: String = catalogs.iter().map(|id| format!("{id}")).join(",");
        let qm_propvals = Self::sql_placeholders(propval2item.len());
        let sql = format!(
            r"SELECT `id`,`ext_id`,`user`,`q` FROM `entry` WHERE `catalog` IN ({catalogs_str}) AND `ext_id` IN ({qm_propvals})"
        );
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(from_row::<(usize, String, Option<usize>, Option<usize>)>)
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
        let sql1 = "SELECT e1.id,e2.q FROM entry e1,entry e2
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
        let sql2 = "UPDATE `entry` SET `q`=:q,`user`=0,`timestamp`=:timestamp WHERE `id`=:entry_id AND `q` IS NULL" ;
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
        let sql = format!(
            "SELECT d1.entry_id,d2.q
			FROM vw_dates d1,vw_dates d2
			WHERE length(d1.born)=10 and length(d1.died)=10 and d1.is_matched=false
			AND d1.born=d2.born AND d2.died=d2.died AND d2.is_matched=true
			AND d1.ext_name=d2.ext_name
			AND (d1.user=0 OR d1.user is null)
			AND d2.user>0 and d2.user is not null
			HAVING d2.q>0
			limit {batch_size}
			"
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
        let sql = format!("SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            state.get_sql()
        ) ;
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<usize>)
            .await?
            .iter()
            .map(|q| format!("Q{}", q))
            .collect();
        Ok(ret)
    }

    // Jobs

    async fn jobs_get_tasks(&self) -> Result<HashMap<String, TaskSize>> {
        let sql = "SELECT `action`,`size` FROM `job_sizes`";
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(sql, ())
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?
            .into_iter()
            .map(|(name, size)| (name, TaskSize::new(&size)))
            .filter(|(_name, size)| size.is_some())
            .map(|(name, size)| (name, size.unwrap()))
            .collect();
        Ok(ret)
    }

    /// Resets all RUNNING jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    async fn reset_running_jobs(&self) -> Result<()> {
        let sql = format!(
            "UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Running.as_str()
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    /// Resets all FAILED jobs of certain types to TODO. Used when bot restarts.
    //TODO test
    async fn reset_failed_jobs(&self) -> Result<()> {
        let sql = format!(
            "UPDATE `jobs` SET `status`='{}' WHERE `status`='{}'",
            JobStatus::Todo.as_str(),
            JobStatus::Failed.as_str()
        );
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, ()).await?;
        Ok(())
    }

    async fn jobs_queue_simple_job(
        &self,
        catalog_id: usize,
        action: &str,
        depends_on: Option<usize>,
        status: &str,
        timestamp: String,
    ) -> Result<usize> {
        let sql = "INSERT INTO `jobs` (catalog,action,status,depends_on,last_ts) VALUES (:catalog_id,:action,:status,:depends_on,:timestamp)
            ON DUPLICATE KEY UPDATE status=:status,depends_on=:depends_on,last_ts=:timestamp";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id,action,depends_on,status,timestamp})
            .await?;
        let last_id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(last_id)
    }

    async fn jobs_reset_json(&self, job_id: usize, timestamp: String) -> Result<()> {
        let sql = "UPDATE `jobs` SET `json`=NULL,last_ts=:timestamp WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id, timestamp}).await?;
        Ok(())
    }

    async fn jobs_set_json(
        &self,
        job_id: usize,
        json_string: String,
        timestamp: &str,
    ) -> Result<()> {
        let sql = "UPDATE `jobs` SET `json`=:json_string,last_ts=:timestamp WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id, json_string, timestamp})
            .await?;
        Ok(())
    }

    async fn jobs_row_from_id(&self, job_id: usize) -> Result<JobRow> {
        let sql = r"SELECT id,action,catalog,json,depends_on,status,last_ts,note,repeat_after_sec,next_ts,user_id FROM `jobs` WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        let row = conn
            .exec_iter(sql, params! {job_id})
            .await?
            .map_and_drop(
                from_row::<(
                    usize,
                    String,
                    usize,
                    Option<String>,
                    Option<usize>,
                    String,
                    String,
                    Option<String>,
                    Option<usize>,
                    String,
                    usize,
                )>,
            )
            .await?
            .pop()
            .ok_or(anyhow!("No job with ID {}", job_id))?;
        let job_row = JobRow::from_row(row);
        Ok(job_row)
    }

    async fn jobs_set_status(
        &self,
        status: &JobStatus,
        job_id: usize,
        timestamp: String,
    ) -> Result<()> {
        let status_str = status.as_str();
        let sql = "UPDATE `jobs` SET `status`=:status_str,`last_ts`=:timestamp,`note`=NULL WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,timestamp,status_str})
            .await?;
        Ok(())
    }

    async fn jobs_set_note(&self, note: Option<String>, job_id: usize) -> Result<Option<String>> {
        let note_cloned = note.clone().map(|s| s.get(..127).unwrap_or(&s).to_string());
        let sql = "UPDATE `jobs` SET `note`=:note WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,note}).await?;
        Ok(note_cloned)
    }

    async fn jobs_update_next_ts(&self, job_id: usize, next_ts: String) -> Result<()> {
        let sql = "UPDATE `jobs` SET `next_ts`=:next_ts WHERE `id`=:job_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {job_id,next_ts}).await?;
        Ok(())
    }

    async fn jobs_get_next_job(
        &self,
        status: JobStatus,
        depends_on: Option<JobStatus>,
        no_actions: &[String],
        next_ts: Option<String>,
    ) -> Option<usize> {
        let sql = Self::jobs_get_next_job_construct_sql(status, depends_on, no_actions, next_ts);
        let mut conn = self.get_conn().await.ok()?;
        conn.exec_iter(sql, ())
            .await
            .ok()?
            .map_and_drop(from_row::<usize>)
            .await
            .ok()?
            .pop()
    }

    // Automatch

    /// Auto-matches unmatched and automatched people to fully matched entries that have the same name and birth year.
    async fn automatch_people_with_birth_year(&self, catalog_id: usize) -> Result<()> {
        let sql = r#"UPDATE entry e1
		    INNER JOIN person_dates p1 ON p1.entry_id=e1.id AND p1.year_born IS NOT NULL
		    INNER JOIN vw_dates p2 ON p2.ext_name=e1.ext_name AND p2.year_born=p1.year_born AND p2.q IS NOT NULL AND p2.user>0 AND p2.entry_id!=e1.id AND p2.user IS NOT NULL
		    SET e1.q=p2.q,e1.user=0,timestamp=date_format(now(),'%Y%m%d%H%i%S')
		    WHERE e1.type='Q5' AND e1.catalog=:catalog_id AND (e1.q is null or e1.user=0)
		    AND NOT EXISTS (SELECT * FROM log WHERE log.entry_id=e1.id AND log.q=p2.q)"#;
        self.get_conn_ro()
            .await?
            .exec_drop(sql, params! {catalog_id})
            .await?;
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
    ) -> Result<Vec<(usize, String, String, String)>> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,
	            IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases`
	            FROM `entry` WHERE `catalog`=:catalog_id {}
	            /* ORDER BY `id` */
	            LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<(usize, String, String, String)>)
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
    ) -> Result<Vec<(usize, String, String, String)>> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,
                IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases`
                FROM `entry` WHERE `catalog`=:catalog_id {}
                /* ORDER BY `id` */
                LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql.clone(), params! {catalog_id,offset,batch_size})
            .await?
            .map_and_drop(from_row::<(usize, String, String, String)>)
            .await?;
        Ok(results)
    }

    async fn automatch_from_other_catalogs_get_results(
        &self,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<ResultInOriginalCatalog>> {
        let sql = "SELECT `id`,`ext_name`,`type` FROM entry WHERE catalog=:catalog_id AND q IS NULL LIMIT :batch_size OFFSET :offset" ;
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
        let sql = "SELECT `id`,`ext_name`,`type`,q FROM entry
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
    ) -> Result<Vec<(usize, String, String, String)>> {
        let sql = "SELECT entry_id,ext_name,born,died
            FROM (`entry` join `person_dates`)
            WHERE `person_dates`.`entry_id` = `entry`.`id`
            AND `catalog`=:catalog_id AND (q IS NULL or user=0) AND born!='' AND died!=''
            LIMIT :batch_size OFFSET :offset";
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id,batch_size,offset})
            .await?
            .map_and_drop(from_row::<(usize, String, String, String)>)
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
    ) -> Result<Vec<(usize, String, String, String)>> {
        let sql = format!("(
	                SELECT multi_match.entry_id AS entry_id,born,died,candidates AS qs FROM person_dates,multi_match,entry
	                WHERE (q IS NULL OR user=0) AND person_dates.entry_id=multi_match.entry_id AND multi_match.catalog=:catalog_id AND length({})=:precision
	                AND entry.id=person_dates.entry_id
	            ) UNION (
	                SELECT entry_id,born,died,q qs FROM person_dates,entry
	                WHERE (q is not null and user=0) AND catalog=:catalog_id AND length({})=:precision AND entry.id=person_dates.entry_id
	            )
	            ORDER BY entry_id LIMIT :batch_size OFFSET :offset",match_field,match_field);
        let mut conn = self.get_conn_ro().await?;
        let results = conn
            .exec_iter(
                sql.clone(),
                params! {catalog_id,precision,batch_size,offset},
            )
            .await?
            .map_and_drop(from_row::<(usize, String, String, String)>)
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
            .iter()
            .filter_map(|row| row.to_owned())
            .next()
            .ok_or(anyhow!("No entry #{}", entry_id))?
            .to_owned();
        Ok(ret)
    }

    async fn entry_from_ext_id(&self, catalog_id: usize, ext_id: &str) -> Result<Entry> {
        let sql = format!(
            "{} WHERE `catalog`=:catalog_id AND `ext_id`=:ext_id",
            Self::entry_sql_select()
        );
        let mut conn = self.get_conn_ro().await?;
        let mut rows: Vec<Entry> = conn
            .exec_iter(sql, params! {catalog_id,ext_id})
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        // `catalog`/`ext_id` comprises a unique index, so there can be only zero or one row in rows.
        let ret = rows
            .pop()
            .ok_or(anyhow!("No entry '{}' in catalog #{}", ext_id, catalog_id))?
            .to_owned();
        Ok(ret)
    }

    async fn get_entry_batch(
        &self,
        catalog_id: usize,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entry>> {
        let sql = "SELECT * FROM `entry` WHERE `catalog`=:catalog_id LIMIT :limit OFFSET :offset";
        Ok(self
            .get_conn_ro()
            .await?
            .exec_iter(sql, params! {catalog_id,limit,offset})
            .await?
            .map_and_drop(|row| Self::entry_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect())
    }

    async fn multiple_from_ids(&self, entry_ids: &[usize]) -> Result<HashMap<usize, Entry>> {
        if entry_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let entry_ids = entry_ids
            .iter()
            .map(|id| format!("{id}"))
            .collect::<Vec<String>>()
            .join(",");
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
        let ret = rows.into_iter().map(|entry| (entry.id, entry)).collect();
        Ok(ret)
    }

    async fn entry_insert_as_new(&self, entry: &Entry) -> Result<usize> {
        let sql = "INSERT IGNORE INTO `entry` (`catalog`,`ext_id`,`ext_url`,`ext_name`,`ext_desc`,`q`,`user`,`timestamp`,`random`,`type`) VALUES (:catalog,:ext_id,:ext_url,:ext_name,:ext_desc,:q,:user,:timestamp,:random,:type_name)";
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
            "type_name" => entry.type_name.to_owned(),
        };
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params).await?;
        let id = conn.last_insert_id().ok_or(EntryError::EntryInsertFailed)? as usize;
        Ok(id)
    }

    async fn entry_delete(&self, entry_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        for table in TABLES_WITH_ENTRY_ID_FIELDS {
            let sql = format!("DELETE FROM `{}` WHERE `entry_id`=:entry_id", table);
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
                r"SELECT `language`,`label` FROM `descriptions` WHERE `entry_id`=:entry_id",
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
        let sql = "DELETE FROM `auxiliary` WHERE `entry_id`=:entry_id AND `aux_p`=:prop_numeric";
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
        let sql = "REPLACE INTO `auxiliary` (`entry_id`,`aux_p`,`aux_name`) VALUES (:entry_id,:prop_numeric,:value)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,prop_numeric,value})
            .await?;
        Ok(())
    }

    async fn entry_remove_coordinate_location(&self, entry_id: usize) -> Result<()> {
        let sql = "DELETE FROM `location` WHERE `entry_id`=:entry_id";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id}).await?;
        Ok(())
    }

    async fn entry_set_coordinate_location(
        &self,
        entry_id: usize,
        lat: f64,
        lon: f64,
    ) -> Result<()> {
        let sql = "REPLACE INTO `location` (`entry_id`,`lat`,`lon`) VALUES (:entry_id,:lat,:lon)";
        let mut conn = self.get_conn().await?;
        conn.exec_drop(sql, params! {entry_id,lat,lon}).await?;
        Ok(())
    }

    async fn entry_get_coordinate_location(
        &self,
        entry_id: usize,
    ) -> Result<Option<CoordinateLocation>> {
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(
                r"SELECT `lat`,`lon` FROM `location` WHERE `entry_id`=:entry_id LIMIT 1",
                params! {entry_id},
            )
            .await?
            .map_and_drop(from_row::<(f64, f64)>)
            .await?
            .pop()
            .map(|(lat, lon)| CoordinateLocation { lat, lon });
        Ok(ret)
    }

    async fn entry_get_aux(&self, entry_id: usize) -> Result<Vec<AuxiliaryRow>> {
        let mut conn = self.get_conn_ro().await?;
        let ret = conn
            .exec_iter(r"SELECT `id`,`aux_p`,`aux_name`,`in_wikidata`,`entry_is_matched` FROM `auxiliary` WHERE `entry_id`=:entry_id",params! {entry_id}).await?
            .map_and_drop(|row| AuxiliaryRow::from_row(&row)).await?
            .iter().filter_map(|row|row.to_owned()).collect();
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
        let entry_id = entry.id;
        let mut sql = "UPDATE `entry` SET `q`=:q_numeric,`user`=:user_id,`timestamp`=:timestamp WHERE `id`=:entry_id AND (`q` IS NULL OR `q`!=:q_numeric OR `user`!=:user_id)".to_string();
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

    // CERSEI

    /// Get current scrapers from database
    async fn get_current_scrapers(&self) -> Result<HashMap<usize, CurrentScraper>> {
        let mut conn = self.get_conn_ro().await?;
        let sql = "SELECT * FROM `cersei`";
        let rows: Vec<Row> = conn.query(sql).await?;

        let mut scrapers = HashMap::new();
        for row in rows {
            let scraper = CurrentScraper {
                cersei_scraper_id: row.get("cersei_scraper_id").unwrap(),
                catalog_id: row.get("catalog_id").unwrap(),
                last_sync: row.get("last_sync"),
            };
            scrapers.insert(scraper.cersei_scraper_id, scraper);
        }

        Ok(scrapers)
    }

    async fn add_cersei_catalog(&self, catalog_id: usize, scraper_id: usize) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let sql = "INSERT INTO `cersei` (`catalog_id`, `cersei_scraper_id`) VALUES (:catalog_id, :scraper_id)";
        conn.exec_drop(sql, params! {catalog_id, scraper_id})
            .await?;
        Ok(())
    }

    async fn update_cersei_last_update(&self, scraper_id: usize, last_sync: &str) -> Result<()> {
        self.get_conn()
            .await?
            .exec_drop(
                "UPDATE `cersei` SET `last_sync`=:last_update WHERE `cersei_scraper_id`=:scraper_id",
                params!{last_sync, scraper_id},
            )
            .await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #lizard forgives
    #[test]
    fn test_jobs_get_next_job_construct_sql() {
        let catalog_filter =
            "AND NOT EXISTS (SELECT * FROM catalog WHERE catalog.id=jobs.catalog AND active!=1)";

        // High priority
        let sql1 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::HighPriority, None, &[], None);
        let expected = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
            JobStatus::HighPriority.as_str()
        );
        assert_eq!(sql1, expected);

        // Low priority
        let sql2 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::LowPriority, None, &[], None);
        let expected2 = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
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
        let expected2a = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NOT NULL AND `depends_on` IN (SELECT `id` FROM `jobs` WHERE `status`='{}') ORDER BY `last_ts` LIMIT 1",JobStatus::Todo.as_str(),JobStatus::Done.as_str()) ;
        assert_eq!(sql2a, expected2a);

        // get_next_initial_allowed_job
        let avoid = vec!["test1".to_string(), "test2".to_string()];
        let sql3 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &avoid, None);
        let not_in = avoid.join("','");
        let expected3 = format!("SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL AND `action` NOT IN ('{}') ORDER BY `last_ts` LIMIT 1",JobStatus::Todo.as_str(),&not_in) ;
        assert_eq!(sql3, expected3);

        // get_next_initial_job
        let sql4 = StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &[], None);
        let expected4 = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL ORDER BY `last_ts` LIMIT 1",
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
            "SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `next_ts`!='' AND `next_ts`<='{}' ORDER BY `next_ts` LIMIT 1",
            JobStatus::Done.as_str(),
            &timestamp
        );
        assert_eq!(sql5, expected5);

        // get_next_initial_job with avoid
        let no_actions = vec!["foo".to_string(), "bar".to_string()];
        let sql6 =
            StorageMySQL::jobs_get_next_job_construct_sql(JobStatus::Todo, None, &no_actions, None);
        let expected6 = format!(
            "SELECT `id` FROM `jobs` WHERE `status`='{}' {catalog_filter} AND `depends_on` IS NULL AND `action` NOT IN ('foo','bar') ORDER BY `last_ts` LIMIT 1",
            JobStatus::Todo.as_str()
        );
        assert_eq!(sql6, expected6);
    }
}

/* TODO

#[tokio::test]
async fn test_get_overview_column_name_for_user_and_q() {
    let mnm = get_test_mnm();
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(0), &None),
        "autoq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), &Some(1)),
        "manual"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), &Some(0)),
        "na"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), &Some(-1)),
        "nowd"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&Some(2), &None),
        "noq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&None, &None),
        "noq"
    );
    assert_eq!(
        mnm.get_storage()
            .get_overview_column_name_for_user_and_q(&None, &Some(1)),
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

        let issue = Issue::new(entry_id, IssueType::Mismatch, json!("!"), &mnm)
            .await
            .unwrap();
        issue.insert().await.unwrap();

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
