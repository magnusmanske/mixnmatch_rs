use crate::app_state::AppState;
use anyhow::Result;
use mysql_async::{from_row, futures::GetConn, prelude::*};
use serde_json::Value;

pub const META_ITEMS: &[&str] = &[
    "Q4167410",  // Wikimedia disambiguation page
    "Q11266439", // Wikimedia template
    "Q4167836",  // Wikimedia category
    "Q13406463", // Wikimedia list article
    "Q22808320", // Wikimedia human name disambiguation page
    "Q17362920", // Wikimedia duplicated page
];

#[derive(Debug, Clone)]
pub struct Wikidata {
    pool: mysql_async::Pool,
}

impl Wikidata {
    pub fn new(config: &Value) -> Self {
        Self {
            pool: AppState::create_pool(config),
        }
    }

    pub fn get_conn(&self) -> GetConn {
        self.pool.get_conn()
    }

    fn sql_placeholders(num: usize) -> String {
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(num, "?".to_string());
        placeholders.join(",")
    }

    pub async fn disconnect(&self) -> Result<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }

    pub async fn automatch_by_sitlinks_get_wd_matches(
        &self,
        params: Vec<String>,
        site: &String,
    ) -> Result<Vec<(usize, String)>> {
        let placeholders = Self::sql_placeholders(params.len());
        let sql = format!("SELECT `ips_item_id`,`ips_site_page` FROM `wb_items_per_site` WHERE `ips_site_id`='{}' AND `ips_site_page` IN ({})",&site,placeholders);
        let mut conn = self.get_conn().await?;
        let wd_matches = conn
            .exec_iter(sql, params)
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(wd_matches)
    }

    // `items` should be a unique list of Qids
    pub async fn get_meta_items(&self, items: &Vec<String>) -> Result<Vec<String>> {
        let placeholders = Self::sql_placeholders(items.len());
        let sql = format!(
            "SELECT DISTINCT page_title AS page_title
            FROM page,pagelinks,linktarget
	        WHERE page_namespace=0
	        AND lt_namespace=0
	        AND page_title IN ({placeholders})
	        AND pl_from=page_id
	        AND lt_id=pl_target_id
	        AND lt_title IN ('{}')",
            &META_ITEMS.join("','")
        );
        let mut conn = self.get_conn().await?;
        let meta_items = conn
            .exec_iter(sql, items)
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(meta_items)
    }

    pub async fn search_with_type(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name
                AND EXISTS (SELECT * FROM page,pagelinks,linktarget WHERE page_title=concat('Q',wbit_item_id) AND page_namespace=0 AND pl_target_id=lt_id AND pl_from=page_id AND lt_namespace=0 AND lt_title=:type_q)
                GROUP BY name,q";
        let mut conn = self.get_conn().await?;
        Ok(conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    pub async fn search_without_type(&self, name: &str) -> Result<Vec<String>, anyhow::Error> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id  AND wbx_text=:name GROUP BY name,q";
        let mut conn = self.get_conn().await?;
        Ok(conn
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?)
    }

    pub async fn get_redirected_items(
        &self,
        unique_qs: &Vec<String>,
    ) -> Result<Vec<(String, String)>> {
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect`
                WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0
                AND `page_title` IN ({})",placeholders);
        let mut conn = self.get_conn().await?;
        let page2rd = conn
            .exec_iter(sql, unique_qs)
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        Ok(page2rd)
    }

    pub async fn get_deleted_items(&self, unique_qs: &[String]) -> Result<Vec<String>> {
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!(
            "SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({})",
            placeholders
        );
        let mut conn = self.get_conn().await?;
        let found_items = conn
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q| !found_items.contains(q))
            .cloned()
            .collect();
        Ok(not_found)
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Wikidata::sql_placeholders(0), "".to_string());
        assert_eq!(Wikidata::sql_placeholders(1), "?".to_string());
        assert_eq!(Wikidata::sql_placeholders(3), "?,?,?".to_string());
    }
}
