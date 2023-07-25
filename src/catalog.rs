use mysql_async::prelude::*;
use mysql_async::Row;
use crate::mixnmatch::*;
use crate::app_state::*;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub id: usize,
    pub name: Option<String>,
    pub url: Option<String>,
    pub desc: String,
    pub type_name: String,
    pub wd_prop: Option<usize>,
    pub wd_qual: Option<usize>,
    pub search_wp: String,
    pub active: bool,
    pub owner: usize,
    pub note: String,
    pub source_item: Option<usize>,
    pub has_person_date: String,
    pub taxon_run: bool,
    pub mnm: Option<MixNMatch>
}

impl Catalog {
    //TODO test
    fn from_row(row: &Row) -> Option<Self> {
        Some(Self {
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
            mnm: None
        })
    }

    /// Returns a Catalog object for a given entry ID.
    pub async fn from_id(catalog_id: usize, mnm: &MixNMatch) -> Result<Self,GenericError> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `id`=:catalog_id";
        let mut rows: Vec<Catalog> = mnm.app.get_mnm_conn().await?
            .exec_iter(sql,params! {catalog_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?
            .iter().filter_map(|row|row.to_owned()).collect();
        // `id` is a unique index, so there can be only zero or one row in rows.
        let mut ret = rows.pop().ok_or(format!("No catalog #{}",catalog_id))?.to_owned() ;
        ret.set_mnm(mnm);
        Ok(ret)
    }

    /// Sets the MixNMatch object. Automatically done when created via from_id().
    //TODO test
    pub fn set_mnm(&mut self, mnm: &MixNMatch) {
        self.mnm = Some(mnm.clone());
    }

    fn mnm(&self) -> Result<&MixNMatch,GenericError> {
        match &self.mnm {
            Some(mnm) => Ok(mnm),
            None => Err(format!("Catalog {}: MnM not set",self.id).into())
        }
    }

    //TODO test
    pub async fn refresh_overview_table(&self) -> Result<(),GenericError> {
        let catalog_id = self.id;
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
        self.mnm()?.app.get_mnm_conn().await?.exec_drop(sql,params! {catalog_id}).await?;
        Ok(())
    }
    
    
}


#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const _TEST_ENTRY_ID: usize = 143962196 ;

    #[tokio::test]
    async fn test_catalog_from_id() {
        let mnm = get_test_mnm();
        let catalog = Catalog::from_id(TEST_CATALOG_ID, &mnm).await.unwrap();
        assert_eq!(catalog.name.unwrap(),"TEST CATALOG");
    }

}