pub use crate::storage::Storage;
use crate::{
    app_state::AppState,
    catalog::Catalog,
    coordinate_matcher::LocationRow,
    microsync::EXT_URL_UNIQUE_SEPARATOR,
    mixnmatch::MatchState,
    taxon_matcher::{RankedNames, TaxonMatcher, TaxonNameField, TAXON_RANKS},
    update_catalog::UpdateInfo,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::{from_row, prelude::*, Row};
use rand::prelude::*;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StorageMySQL {
    pool: mysql_async::Pool,
}

impl StorageMySQL {
    pub fn new(j: &Value) -> Self {
        Self {
            pool: AppState::create_pool(j),
        }
    }

    pub fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }

    fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num, "?".to_string());
        placeholders.join(",")
    }

    fn coordinate_matcher_main_query_sql(
        catalog_id: &Option<usize>,
        bad_catalogs: &Vec<usize>,
        max_results: usize,
    ) -> String {
        let conditions = match catalog_id {
            Some(catalog_id) => format!("`catalog`={catalog_id}"),
            None => {
                let r: f64 = rand::thread_rng().gen();
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
        } + &MatchState::not_fully_matched().get_sql();
        format!("SELECT `lat`,`lon`,`id`,`catalog`,`ext_name`,`type`,`q` FROM `vw_location` WHERE `ext_name`!='' AND {conditions}",)
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
            mnm: None,
        })
    }
}

#[async_trait]
impl Storage for StorageMySQL {
    async fn disconnect(&self) -> Result<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }

    // Taxon matcher
    async fn set_catalog_taxon_run(&self, catalog_id: usize, taxon_run: bool) -> Result<()> {
        let taxon_run = taxon_run as u16;
        let sql = format!("UPDATE `catalog` SET `taxon_run`=1 WHERE `id`=? AND `taxon_run`=?",);
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id, taxon_run}).await?;
        Ok(())
    }

    async fn match_taxa_get_ranked_names_batch(
        &self,
        ranks: &Vec<&str>,
        field: &TaxonNameField,
        catalog_id: usize,
        batch_size: usize,
        offset: usize,
    ) -> Result<(usize, RankedNames)> {
        let taxon_name_column = match field {
            TaxonNameField::Name => "ext_name",
            TaxonNameField::Description => "ext_desc",
        };
        let sql = format!(
            r"SELECT `id`,`{}` AS taxon_name,`type` FROM `entry`
                	WHERE `catalog` IN (:catalog_id)
                 	AND (`q` IS NULL OR `user`=0)
                  	AND `type` IN ('{}')
                	LIMIT :batch_size OFFSET :offset",
            taxon_name_column,
            ranks.join("','")
        );

        let mut conn = self.pool.get_conn().await?;
        let results = conn
            .exec_iter(sql, params! {catalog_id,batch_size,offset})
            .await?
            .map_and_drop(from_row::<(usize, String, String)>)
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
        bad_catalogs: &Vec<usize>,
        max_results: usize,
    ) -> Result<Vec<LocationRow>> {
        let sql = Self::coordinate_matcher_main_query_sql(catalog_id, bad_catalogs, max_results);
        let mut conn = self.pool.get_conn().await?;
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

    async fn get_coordinate_matcher_permissions(&self) -> Result<Vec<(usize, String, String)>> {
        let sql = r#"SELECT `catalog_id`,`kv_key`,`kv_value` FROM `kv_catalog`"#;
        let mut conn = self.pool.get_conn().await?;
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
            .map(self.pool.get_conn().await?, |type_name| type_name)
            .await?;
        Ok(results)
    }

    async fn get_existing_ext_ids(
        &self,
        placeholders: String,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT `ext_id` FROM entry WHERE `ext_id` IN ({}) AND `catalog`={}",
            &placeholders, catalog_id
        );
        let existing_ext_ids = sql
            .with(ext_ids.to_vec())
            .map(self.pool.get_conn().await?, |ext_id| ext_id)
            .await?;
        Ok(existing_ext_ids)
    }

    async fn update_catalog_get_update_info(&self, catalog_id: usize) -> Result<Vec<UpdateInfo>> {
        let results = "SELECT id, catalog, json, note, user_id, is_current FROM `update_info` WHERE `catalog`=:catalog_id AND `is_current`=1 LIMIT 1"
            .with(params!{catalog_id})
            .map(self.pool.get_conn().await?,
                |(id, catalog, json, note, user_id, is_current)|{
                UpdateInfo{id, catalog, json, note, user_id, is_current}
            })
            .await?;
        Ok(results)
    }

    // Catalog

    async fn number_of_entries_in_catalog(&self, catalog_id: usize) -> Result<usize> {
        let results: Vec<usize> = "SELECT count(*) AS cnt FROM `entry` WHERE `catalog`=:catalog_id"
            .with(params! {catalog_id})
            .map(self.pool.get_conn().await?, |num| num)
            .await?;
        Ok(*results.get(0).unwrap_or(&0))
    }

    async fn get_catalog_from_id(&self, catalog_id: usize) -> Result<Catalog> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `id`=:catalog_id";
        let mut conn = self.pool.get_conn().await?;
        let mut rows: Vec<Catalog> = conn
            .exec_iter(sql, params! {catalog_id})
            .await?
            .map_and_drop(|row| Self::catalog_from_row(&row))
            .await?
            .iter()
            .filter_map(|row| row.to_owned())
            .collect();
        let ret = rows
            .pop()
            .ok_or(anyhow!("No catalog #{}", catalog_id))?
            .to_owned();
        Ok(ret)
    }

    async fn get_catalog_key_value_pairs(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, String>> {
        let sql = r#"SELECT `kv_key`,`kv_value` FROM `kv_catalog` WHERE `catalog_id`=:catalog_id"#;
        let mut conn = self.pool.get_conn().await?;
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
        let mut conn = self.pool.get_conn().await?;
        conn.exec_drop(sql, params! {catalog_id}).await?;
        Ok(())
    }

    // Microsync

    async fn microsync_load_entry_names(
        &self,
        entry_ids: &Vec<usize>,
    ) -> Result<HashMap<usize, String>> {
        let placeholders = Self::sql_placeholders(entry_ids.len());
        let sql = format!(
            "SELECT `id`,`ext_name` FROM `entry` WHERE `id` IN ({})",
            placeholders
        );
        let mut conn = self.pool.get_conn().await?;
        let results = conn
            .exec_iter(sql, entry_ids)
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
        let mut conn = self.pool.get_conn().await?;
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
        ext_ids: &Vec<&String>,
    ) -> Result<Vec<(usize, Option<isize>, Option<usize>, String, String)>> {
        let placeholders: Vec<&str> = ext_ids.iter().map(|_| "BINARY ?").collect();
        let placeholders = placeholders.join(",");
        let sql = format!("SELECT `id`,`q`,`user`,`ext_id`,`ext_url` FROM `entry` WHERE `catalog`={catalog_id} AND `ext_id` IN ({placeholders})");
        let mut conn = self.pool.get_conn().await?;
        let results = conn
            .exec_iter(sql, ext_ids)
            .await?
            .map_and_drop(from_row::<(usize, Option<isize>, Option<usize>, String, String)>)
            .await?;
        Ok(results)
    }
}
