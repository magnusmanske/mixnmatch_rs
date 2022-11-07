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
    fn from_row(row: &Row) -> Self {
        Self {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            url: row.get(2).unwrap(),
            desc: row.get(3).unwrap(),
            type_name: row.get(4).unwrap(),
            wd_prop: row.get(5).unwrap(),
            wd_qual: row.get(6).unwrap(),
            search_wp: row.get(7).unwrap(),
            active: row.get(8).unwrap(),
            owner: row.get(9).unwrap(),
            note: row.get(10).unwrap(),
            source_item: row.get(11).unwrap(),
            has_person_date: row.get(12).unwrap(),
            taxon_run: row.get(13).unwrap(),
            mnm: None
        }
    }

    /// Returns a Catalog object for a given entry ID.
    pub async fn from_id(catalog_id: usize, mnm: &MixNMatch) -> Result<Self,GenericError> {
        let sql = r"SELECT id,`name`,url,`desc`,`type`,wd_prop,wd_qual,search_wp,active,owner,note,source_item,has_person_date,taxon_run FROM `catalog` WHERE `id`=:catalog_id";
        let mut rows: Vec<Self> = mnm.app.get_mnm_conn().await?
            .exec_iter(sql,params! {catalog_id}).await?
            .map_and_drop(|row| Self::from_row(&row)).await?;
        // `id` is a unique index, so there can be only zero or one row in rows.
        let mut ret = rows.pop().ok_or(format!("No catalog #{}",catalog_id))?.to_owned() ;
        ret.set_mnm(mnm);
        Ok(ret)
    }

    /// Sets the MixNMatch object. Automatically done when created via from_id().
    pub fn set_mnm(&mut self, mnm: &MixNMatch) {
        self.mnm = Some(mnm.clone());
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