use std::fs::File;
use std::collections::HashMap;
use serde_json::Value;
use mysql_async::prelude::*;
use mysql_async::from_row;
use std::error::Error;
use std::fmt;
use crate::app_state::*;
use crate::catalog::*;
use crate::mixnmatch::*;
use crate::auxiliary_matcher::*;
use crate::entry::*;
use crate::job::*;

const MAX_WIKI_ROWS: usize = 400;
const EXT_URL_UNIQUE_SEPARATOR: &'static str = "!@£$%^&|";
const BLACKLISTED_CATALOGS: &'static [usize] = &[
    506
];

#[derive(Debug)]
pub enum MicrosyncError {
    UnsuitableCatalogProperty
}

impl Error for MicrosyncError {}

impl fmt::Display for MicrosyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}




#[derive(Debug,Clone,Eq,Ord,PartialEq,PartialOrd)]
struct MatchDiffers {
    ext_id: String,
    q_wd: isize,
    q_mnm: isize,
    entry_id: usize,
    ext_url: String,
}

#[derive(Debug,Clone,Eq,Ord,PartialEq,PartialOrd)]
struct SmallEntry {
    id: usize,
    q: Option<isize>,
    user: Option<usize>,
    ext_url: String,
}

#[derive(Debug,Clone,Eq,Ord,PartialEq,PartialOrd)]
struct MultipleExtIdInWikidata {
    ext_id: String,
    items:Vec<String>,
}

#[derive(Debug,Clone,Eq,Ord,PartialEq,PartialOrd)]
struct ExtIdWithMutipleQ {
    q: isize,
    entry2ext_id: Vec<(usize,String)>,
}

#[derive(Debug,Clone,Eq,Ord,PartialEq,PartialOrd)]
struct ExtIdNoMnM {
    q: isize,
    ext_id: String,
}


#[derive(Debug,Clone)]
pub struct Microsync {
    mnm: MixNMatch,
    job: Option<Job>
}

impl Jobbable for Microsync {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}


impl Microsync {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
            job: None
        }
    }

    pub async fn check_catalog(&mut self, catalog_id: usize) -> Result<(),GenericError> {
        if BLACKLISTED_CATALOGS.contains(&catalog_id) {
            return Ok(()) // TODO error?
        }
        let catalog = Catalog::from_id(catalog_id,&self.mnm).await?;
        let property = match (catalog.wd_prop,catalog.wd_qual) {
            (Some(prop),None) => prop,
            _ => return Err(Box::new(MicrosyncError::UnsuitableCatalogProperty))
        };
        self.fix_redirects(catalog_id).await?;
        self.fix_deleted_items(catalog_id).await?;

        let multiple_extid_in_wikidata = self.get_multiple_extid_in_wikidata(property).await?;
        let multiple_q_in_mnm = self.get_multiple_q_in_mnm(catalog_id).await?;
        let (extid_not_in_mnm,match_differs) = self.get_differences_mnm_wd(catalog_id,property).await?;
        let wikitext = self.wikitext_from_issues(&catalog, multiple_extid_in_wikidata,multiple_q_in_mnm,match_differs,extid_not_in_mnm).await?;
        self.update_wiki_page(catalog_id,&wikitext).await?;
        Ok(())
    }

    async fn update_wiki_page(&mut self, catalog_id: usize, wikitext: &str) -> Result<(),GenericError> {
        let page_title = format!("User:Magnus Manske/Mix'n'match report/{}",catalog_id);
        let day = &MixNMatch::get_timestamp()[0..8];
        let comment = format!("Update {}",day);
        self.mnm.set_wikipage_text(&page_title,&wikitext,&comment).await?;
        Ok(())
        //mw_api.
    }

    async fn wikitext_from_issues(&self,
        catalog: &Catalog,
        multiple_extid_in_wikidata: Vec<MultipleExtIdInWikidata>,
        multiple_q_in_mnm: Vec<ExtIdWithMutipleQ>,
        match_differs: Vec<MatchDiffers>,
        extid_not_in_mnm: Vec<ExtIdNoMnM>,
    ) -> Result<String,GenericError> {
        let formatter_url = Self::get_formatter_url_for_prop(catalog.wd_prop.unwrap_or(0)).await?;
        let catalog_name = match &catalog.name {
            Some(s) => s.to_owned(),
            None => String::new()
        };
        let mut ret = String::new();
        ret += &format!("A report for the [{}/ Mix'n'match] tool. '''This page will be replaced regularly!'''\n",MNM_SITE_URL);
        ret += "''Please note:''\n";
        ret += "* If you fix something from this list on Wikidata, please fix it on Mix'n'match as well, if applicable. Otherwise, the error might be re-introduced from there.\n";
        ret += "* 'External ID' refers to the IDs in the original (external) catalog; the same as the statement value for the associated  property.\n\n";
        ret += &format!("==[{MNM_SITE_URL}/#/catalog/{} {}]==\n{}\n\n",catalog.id,&catalog_name,&catalog.desc);

        if !extid_not_in_mnm.is_empty() {
            ret += "== Unknown external ID ==\n";
            if extid_not_in_mnm.len()>MAX_WIKI_ROWS {
                ret += &format!("* {} external IDs in Wikidata but not in Mix'n'Match. Too many to show individually.\n\n",extid_not_in_mnm.len());
            } else {
                ret += "{| class='wikitable'\n! External ID !! Item\n" ;
                for e in &extid_not_in_mnm {
                    let ext_id = self.format_ext_id(&e.ext_id,"",&formatter_url);
                    let s = format!("|-\n| {} || {{{{Q|{}}}}}\n",&ext_id,e.q);
                    ret += &s;
                }
                ret += "|}\n\n";
            }
        }

        if !match_differs.is_empty() {
            ret += "== Different items for the same external ID ==\n";
            if match_differs.len()>MAX_WIKI_ROWS {
                ret += &format!("* {} enties have different items on Mix'n'match and Wikidata. Too many to show individually.\n\n",match_differs.len());
            } else {
                let entry_ids = match_differs.iter().map(|e|e.entry_id).collect();
                let entry2name = self.load_entry_names(&entry_ids).await?;
                ret += "{| class='wikitable'\n! External ID !! External label !! Item in Wikidata !! Item in Mix'n'Match !! Mix'n'match entry\n" ;
                for e in &match_differs {
                    let ext_name=entry2name.get(&e.entry_id).unwrap_or(&e.ext_id);
                    let ext_id = self.format_ext_id(&e.ext_id,&e.ext_url,&formatter_url);
                    let mnm_url = format!("https://mix-n-match.toolforge.org/#/entry/{}",e.entry_id);
                    let s = format!("|-\n| {ext_id} || {ext_name} || {{{{Q|{}}}}} || {{Q|{}}}}} || [{mnm_url} {}]\n",e.q_wd,e.q_mnm,e.entry_id);
                    ret += &s;
                }
                ret += "|}\n\n";
            }
        }

        if !multiple_q_in_mnm.is_empty() {
            ret += "== Same item for multiple external IDs in Mix'n'match ==\n";
            if multiple_q_in_mnm.len()>MAX_WIKI_ROWS {
                ret += &format!("* {} items have more than one match in Mix'n'Match. Too many to show individually.\n\n",multiple_q_in_mnm.len());
            } else {
                let entry_ids = multiple_q_in_mnm
                    .iter()
                    .flat_map(|e|e.entry2ext_id.iter().map(|x|x.0))
                    .collect();
                let entry2name = self.load_entry_names(&entry_ids).await?;
                ret += "{| class='wikitable'\n! Item in Mix'n'Match !! Mix'n'match entry !! External ID !! External label\n" ;
                for e in &multiple_q_in_mnm {
                    let mut first = true;
                    let q_mnm=e.q;
                    for (entry_id,ext_id) in &e.entry2ext_id {
                        let row = if first {
                            first = false;
                            format!("|-\n|rowspan={}|{{{{Q|{}}}}}|| ",e.entry2ext_id.len(),q_mnm)
                        } else {
                            "|-\n|| ".to_string()
                        };
                        let ext_name=entry2name.get(&entry_id).unwrap_or(&ext_id);
                        let ext_id = self.format_ext_id(&ext_id,"",&formatter_url);
                        let mnm_url = format!("https://mix-n-match.toolforge.org/#/entry/{}",entry_id);
                        ret += &format!("{row}[{mnm_url} {entry_id}] || {ext_id} || {ext_name}\n");
                    }
                }
                ret += "|}\n\n";
            }
        }

        if !multiple_extid_in_wikidata.is_empty() {
            ret += "== Multiple items for the same external ID in Wikidata ==\n";
            if multiple_extid_in_wikidata.len()>MAX_WIKI_ROWS {
                ret += &format!("* {} external IDs have at least two items on Wikidata. Too many to show individually.\n\n",multiple_extid_in_wikidata.len());
            } else {
                ret += "{| class='wikitable'\n! External ID !! Items in Mix'n'Match\n" ;
                for e in &multiple_extid_in_wikidata {
                    let ext_id = self.format_ext_id(&e.ext_id,"",&formatter_url);
                    let items: Vec<String> = e.items.iter().map(|q|format!("{{{{Q|{}}}}}",q)).collect();
                    let items = items.join("<br/>");
                    let s = format!("|-\n| {ext_id} || {}\n",items);
                    ret += &s;
                }
                ret += "|}\n\n";
            }
        }


        Ok(ret)
    }

    async fn load_entry_names(&self, entry_ids: &Vec<usize>) -> Result<HashMap<usize,String>,GenericError> {
        let placeholders = MixNMatch::sql_placeholders(entry_ids.len());
        let sql = format!("SELECT `id`,`ext_name` FROM `entry` WHERE `id` IN ({})",placeholders);
        let results = self.mnm.app.get_mnm_conn().await?
            .exec_iter(sql, entry_ids).await?
            .map_and_drop(from_row::<(usize,String)>).await?
            .iter()
            .map(|(entry_id,ext_name)|(*entry_id,ext_name.to_owned()))
            .collect();
        Ok(results)
    }

    fn format_ext_id(&self, ext_id: &str, ext_url: &str, formatter_url: &str) -> String {
        // TODO if ( !preg_match('|^[a-zA-Z0-9._ -]+$|',$ext_id) ) $ext_id = "<nowiki>{$ext_id}</nowiki>" ;
        if !formatter_url.is_empty() {
            format!("[{} {}]",formatter_url.replace("$1",ext_id),ext_id)
        } else if !ext_url.is_empty() {
            format!("[{} {}]",ext_url,ext_id)
        } else {
            ext_id.to_string()
        }
    }

    async fn get_formatter_url_for_prop(property: usize) -> Result<String,GenericError> {
        let url = format!("https://www.wikidata.org/w/api.php?action=wbgetentities&ids=P{property}&format=json") ;
        let json = reqwest::get(&url).await?.json::<Value>().await?;
        let url = match json["entities"][format!("P{property}")]["claims"]["P1630"][0]["mainsnak"]["datavalue"]["value"].as_str() {
            Some(url) => url.to_string(),
            None => String::new()
        };
        Ok(url)
    }

    async fn fix_redirects(&self, catalog_id: usize) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_fully_matched_items(catalog_id, offset).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
            let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect` 
                WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0 
                AND `page_title` IN ({})",placeholders);
            let page2rd = self.mnm.app.get_wd_conn().await?
                .exec_iter(sql, unique_qs).await?
                .map_and_drop(from_row::<(String,String)>).await?;
            for (from,to) in &page2rd {
                if let (Some(from),Some(to)) = (self.mnm.item2numeric(from),self.mnm.item2numeric(to)) {
                    if from>0 && to>0 {
                        let sql = "UPDATE `entry` SET `q`=:to WHERE `q`=:from";
                        self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {from,to,catalog_id}).await?;
                    }
                }
            }
        }
    }

    async fn fix_deleted_items(&self, catalog_id: usize) -> Result<(),GenericError> {
        let mut offset = 0;
        loop {
            let unique_qs = self.get_fully_matched_items(catalog_id, offset).await?;
            if unique_qs.is_empty() {
                return Ok(())
            }
            offset += unique_qs.len();
            let placeholders = MixNMatch::sql_placeholders(unique_qs.len());
            let sql = format!("SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({})",placeholders);
            let found_items = self.mnm.app.get_wd_conn().await?
                .exec_iter(sql, unique_qs.clone()).await?
                .map_and_drop(from_row::<String>).await?;
            let not_found: Vec<isize> = unique_qs
                .iter()
                .filter(|q|!found_items.contains(q))
                .filter_map(|q|self.mnm.item2numeric(q))
                .collect();
            let sql = "UPDATE `entry` SET `q`=NULL,`user`=NULL,`timestamp`=NULL WHERE `q`=:q";
            for q in not_found {
                self.mnm.app.get_mnm_conn().await?.exec_drop(sql, params! {q}).await?;
            }
        }
    }

    async fn get_multiple_extid_in_wikidata(&self, property: usize) -> Result<Vec<MultipleExtIdInWikidata>,GenericError> {
        let mw_api = self.mnm.get_mw_api().await.unwrap();
        // TODO: lcase?
        let sparql = format!("SELECT ?extid (count(?q) AS ?cnt) (GROUP_CONCAT(?q; SEPARATOR = '|') AS ?items) 
            {{ ?q wdt:P{} ?extid }} 
            GROUP BY ?extid HAVING (?cnt>1)
            ORDER BY ?extid",property);
        Ok(self.mnm
            .load_sparql_csv(&sparql)
            .await?
            .records()
            .filter_map(|r|r.ok())
            .filter(|r|r.len()==3)
            .take(MAX_WIKI_ROWS+1) // limit to max results, not point in collecting more
            .map(|r|{
                let ext_id = r.get(0).unwrap(); // Safe
                let items: Vec<String> = r.get(2).unwrap().split("|").filter_map(|s|mw_api.extract_entity_from_uri(s).ok()).collect();
                MultipleExtIdInWikidata{ext_id:ext_id.to_string(),items}
            })
            .collect())
    }

    async fn get_multiple_q_in_mnm(&self, catalog_id: usize) -> Result<Vec<ExtIdWithMutipleQ>,GenericError> {
        let sql = format!("SELECT q,group_concat(id) AS ids,group_concat(ext_id SEPARATOR '{}') AS ext_ids FROM entry WHERE catalog=:catalog_id AND q IS NOT NULL and q>0 AND user>0 GROUP BY q HAVING count(id)>1 ORDER BY q",EXT_URL_UNIQUE_SEPARATOR);
        let results = self.mnm.app.get_mnm_conn().await?
            .exec_iter(sql, params!{catalog_id}).await?
            .map_and_drop(from_row::<(isize,String,String)>).await?;
        let mut results: Vec<ExtIdWithMutipleQ> = results
            .iter()
            .map(|r|{
                let entry_ids: Vec<&str> = r.1.split(",").collect();
                let ext_ids: Vec<&str> = r.2.split(EXT_URL_UNIQUE_SEPARATOR).collect();
                let mut entry2ext_id:Vec<(usize,String)> = entry_ids
                    .iter()
                    .zip(ext_ids.iter())
                    .filter_map(|(entry_id,ext_id)|{
                        match entry_id.parse() {
                            Ok(entry_id) => Some((entry_id,ext_id.to_string())),
                            _ => None
                        }
                    })
                    .collect();
                    entry2ext_id.sort();
                ExtIdWithMutipleQ{q:r.0,entry2ext_id}
            })
            .collect();
        results.sort();
        Ok(results)
    }

    async fn get_fully_matched_items(&self, catalog_id: usize, offset: usize) -> Result<Vec<String>,GenericError> {
        let batch_size = 5000;
        let sql = format!("SELECT DISTINCT `q` FROM `entry` WHERE `catalog`=:catalog_id {} LIMIT :batch_size OFFSET :offset",
            MatchState::fully_matched().get_sql()
        ) ;
        let ret = self.mnm.app.get_mnm_conn().await?
            .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
            .map_and_drop(from_row::<usize>).await?;
        let ret = ret.iter().map(|q|format!("Q{}",q)).collect();
        Ok(ret)
    }

    async fn get_q2ext_id_chunk(&self, reader: &mut csv::Reader<File>,case_insensitive: bool, batch_size: usize) -> Result<Vec<(isize,String)>,GenericError> {
        let mw_api = self.mnm.get_mw_api().await.unwrap();
        Ok(reader
            .records()
            .filter_map(|r|r.ok())
            .filter_map(|r|{
                let q = mw_api.extract_entity_from_uri(r.get(0)?).ok()?;
                let q_numeric = self.mnm.item2numeric(&q)?;
                let value = r.get(1)?;
                let value = if case_insensitive { value.to_lowercase().to_string() } else { value.to_string() } ;
                Some((q_numeric,value))
            })
            .take(batch_size)
            .collect())
    }

    async fn get_differences_mnm_wd(&self, catalog_id: usize, property: usize) -> Result<(Vec<ExtIdNoMnM>,Vec<MatchDiffers>),GenericError> {
        let case_insensitive = AUX_PROPERTIES_ALSO_USING_LOWERCASE.contains(&property);
        let sparql = format!("SELECT ?item ?value {{ ?item wdt:P{property} ?value }} ORDER BY ?item");
        let mut reader = self.mnm.load_sparql_csv(&sparql).await?;
        let mut extid_not_in_mnm: Vec<ExtIdNoMnM> = vec![];
        let mut match_differs = vec![];
        let batch_size: usize = 5000;
        loop {
            let chunk = self.get_q2ext_id_chunk(&mut reader,case_insensitive,batch_size).await?;
            let ext_ids: Vec<&String> = chunk.iter().map(|x|&x.1).collect();
            let ext_id2entry = self.get_entries_for_ext_ids(catalog_id, property, &ext_ids).await?;
            for (q,ext_id) in &chunk {
                match ext_id2entry.get(ext_id) {
                    Some(entry) => {
                        if entry.user.is_none() || entry.user==Some(0) || entry.q.is_none() { // Found a match but not in MnM yet
                            Entry::from_id(entry.id , &self.mnm).await?.set_match(&format!("Q{}",q), 4).await?;
                        } else if Some(*q)!=entry.q { // Fully matched but to different item
                            if let Some(entry_q)=entry.q { // Entry has N/A or Not In Wikidata, overwrite
                                if entry_q<=0 {
                                    Entry::from_id(entry.id , &self.mnm).await?.set_match(&format!("Q{}",q), 4).await?;
                                } else {
                                    let md = MatchDiffers {
                                        ext_id:ext_id.to_owned(),
                                        q_wd: *q,
                                        q_mnm: entry_q,
                                        entry_id: entry.id,
                                        ext_url: entry.ext_url.to_owned()
                                    };
                                    if match_differs.len()<=MAX_WIKI_ROWS {
                                        match_differs.push(md);
                                    }
                                }
                                
                            }
                        }
                    }
                    None => {
                        if extid_not_in_mnm.len()<=MAX_WIKI_ROWS {
                            extid_not_in_mnm.push(ExtIdNoMnM{q:*q,ext_id:ext_id.to_owned()});
                        }
                    }
                }
            }
            if chunk.len()<batch_size {
                break;
            }
        }
        extid_not_in_mnm.sort();
        match_differs.sort();

        Ok((extid_not_in_mnm,match_differs))
    }

    async fn get_entries_for_ext_ids(&self, catalog_id: usize, property: usize, ext_ids:&Vec<&String> ) -> Result<HashMap<String,SmallEntry>,GenericError> {
        let case_insensitive = AUX_PROPERTIES_ALSO_USING_LOWERCASE.contains(&property);
        let placeholders = MixNMatch::sql_placeholders(ext_ids.len());
        let sql = format!("SELECT `id`,`q`,`user`,`ext_id`,`ext_url` FROM `entry` WHERE `catalog`={} AND `ext_id` IN ({})",catalog_id,placeholders);
        let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql,ext_ids).await?
                .map_and_drop(from_row::<(usize,Option<isize>,Option<usize>,String,String)>).await?;
        let ret: HashMap<String,SmallEntry> = results
            .iter()
            .map(|(id,q,user, ext_id,ext_url)|{
                let ext_id = if case_insensitive { ext_id.to_lowercase().to_string() } else { ext_id.to_string() } ;
                (ext_id, SmallEntry{id:*id,q:q.to_owned(),user:user.to_owned(),ext_url:ext_url.to_owned()})
            })
            .collect();
        Ok(ret)
    }

}


#[cfg(test)]
mod tests {

    use super::*;
    //use crate::entry::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;
    const TEST_ENTRY_ID2 : usize = 144000951 ;

    #[tokio::test]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().set_match("Q100000067", 2).await.unwrap();
        let ms = Microsync::new(&mnm);
        ms.fix_redirects(TEST_CATALOG_ID).await.unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(91013264));
    }

    #[tokio::test]
    async fn test_fix_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().set_match("Q115205673", 2).await.unwrap();
        let ms = Microsync::new(&mnm);
        ms.fix_deleted_items(TEST_CATALOG_ID).await.unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,None);
    }

    #[tokio::test]
    async fn test_get_multiple_extid_in_wikidata() {
        let mnm = get_test_mnm();
        let ms = Microsync::new(&mnm);
        let result = ms.get_multiple_extid_in_wikidata(7889).await.unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_get_multiple_q_in_mnm() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        Entry::from_id(TEST_ENTRY_ID , &mnm).await.unwrap().set_match("Q13520818", 2).await.unwrap();
        Entry::from_id(TEST_ENTRY_ID2, &mnm).await.unwrap().set_match("Q13520818", 2).await.unwrap();

        let ms = Microsync::new(&mnm);
        let _results = ms.get_multiple_q_in_mnm(TEST_CATALOG_ID).await.unwrap();

        // Cleanup
        Entry::from_id(TEST_ENTRY_ID , &mnm).await.unwrap().unmatch().await.unwrap();
        Entry::from_id(TEST_ENTRY_ID2, &mnm).await.unwrap().unmatch().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_formatter_url_for_prop() {
        assert_eq!(Microsync::get_formatter_url_for_prop(214).await.unwrap(),"https://viaf.org/viaf/$1/".to_string());
        assert_eq!(Microsync::get_formatter_url_for_prop(215).await.unwrap(),"".to_string());
        assert_eq!(Microsync::get_formatter_url_for_prop(0).await.unwrap(),"".to_string());

    }

    #[tokio::test]
    async fn test_load_entry_names() {
        let mnm = get_test_mnm();
        let ms = Microsync::new(&mnm);
        let result = ms.load_entry_names(&vec![TEST_ENTRY_ID]).await.unwrap();
        assert_eq!(result.get(&TEST_ENTRY_ID),Some(&"Magnus Manske".to_string()));
    }

    #[tokio::test]
    async fn test_format_ext_id() {
        let mnm = get_test_mnm();
        let ms = Microsync::new(&mnm);
        assert_eq!(ms.format_ext_id("gazebo", "http://foo.bar", "http://foo.baz/$1"),"[http://foo.baz/gazebo gazebo]".to_string());
        assert_eq!(ms.format_ext_id("gazebo", "http://foo.bar", ""),"[http://foo.bar gazebo]".to_string());
        assert_eq!(ms.format_ext_id("gazebo", "", "http://foo.baz/$1"),"[http://foo.baz/gazebo gazebo]".to_string());
        assert_eq!(ms.format_ext_id("gazebo", "", ""),"gazebo".to_string());
    }

    #[tokio::test]
    async fn test_check_catalog() {
        let mnm = get_test_mnm();
        let mut ms = Microsync::new(&mnm);
        ms.check_catalog(22).await.unwrap();
    }
    

}