use futures::future::join_all;
use itertools::Itertools;
use serde_json::json;
use std::collections::HashMap;
use regex::Regex;
use chrono::prelude::*;
use lazy_static::lazy_static;
use mysql_async::prelude::*;
use chrono::{Utc, NaiveDateTime};
use mysql_async::{from_row, Params};
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::catalog::*;
use crate::entry::*;
use crate::job::*;
use crate::issue::*;

lazy_static!{
    static ref RE_YEAR : Regex = Regex::new(r"(\d{3,4})").expect("Regexp error");
}

#[derive(Debug, Clone)]
struct CandidateDates {
    pub entry_id: usize,
    pub born: String,
    pub died: String,
    pub matches: Vec<String>
}

impl CandidateDates {
    //TODO test
    fn from_row(r: &(usize,String,String,String)) -> Self {
        Self {
            entry_id: r.0,
            born: r.1.clone(),
            died: r.2.clone(),
            matches: r.3.split(",").filter(|s|!s.is_empty()).map(|s|format!("Q{}",s)).collect()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoMatch {
    mnm: MixNMatch,
    job: Option<Job>
}

impl Jobbable for AutoMatch {
    //TODO test
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    //TODO test
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}

impl AutoMatch {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
            job: None
        }
    }

    pub async fn automatch_by_sitelink(&self, catalog_id: usize) -> Result<(),GenericError> {
        let language = Catalog::from_id(catalog_id, &self.mnm).await?.search_wp;
        let site = format!("{}wiki",&language);
        let sql = format!("SELECT `id`,`ext_name` FROM entry WHERE catalog=:catalog_id AND q IS NULL
            AND NOT EXISTS (SELECT * FROM `log` WHERE log.entry_id=entry.id AND log.action='remove_q')
            {}
            ORDER BY `id` LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 5000 ;
        loop {
            let entries = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,String)>).await?;
            if entries.is_empty() {
                break // Done
            }
            let mut name2entries: HashMap<String,Vec<usize>> = HashMap::new();
            entries
                .iter()
                .for_each(|(id,name)|{
                    name2entries.entry(name.to_owned()).and_modify(|n2e|n2e.push(*id)).or_insert(vec![*id]);
                });
            
            let params:Vec<String> = name2entries.keys().map(|s|s.to_owned()).collect();
            let placeholders = MixNMatch::sql_placeholders(params.len());
            let sql2 = format!("SELECT `ips_item_id`,`ips_site_page` FROM `wb_items_per_site` WHERE `ips_site_id`='{}' AND `ips_site_page` IN ({})",&site,placeholders);
            let wd_matches = self.mnm.app.get_wd_conn().await?
                .exec_iter(sql2,params).await?
                .map_and_drop(from_row::<(usize,String)>).await?;
            
            for (q,title) in wd_matches {
                match name2entries.get(&title) {
                    Some(v) => {
                        for entry_id in v {
                            if let Ok(mut entry) = Entry::from_id(*entry_id, &self.mnm).await {
                                let _ = entry.set_match(&format!("Q{}",q),USER_AUTO).await;
                            }
                                
                        }
                    }
                    None => {}
                }
            }

            
            if entries.len()<batch_size {
                break;
            }
            offset += entries.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    async fn search_with_type_and_entity_id(&self, entry_id: usize, name: &str, type_q: &str) -> Option<(usize,Vec<String>)> {
        let mut items = match self.mnm.wd_search_with_type(name,type_q).await {
            Ok(items) => items,
            Err(_e) => {
                // eprintln!("search_with_type_and_entity_id: {e}");
                return None;
            },
        };
        if items.is_empty() {
            return None;
        }
        items.sort();
        items.dedup();
        Some((entry_id,items))
    }

    async fn match_entries_to_items(&self ,entry_id2items: &HashMap<usize,Vec<String>>) -> Result<(),GenericError> {
        let entry_ids = entry_id2items.keys().map(|id|*id).collect();
        let mut entries = Entry::multiple_from_ids(&entry_ids,&self.mnm).await?;
        let mut futures = vec![];

        for (entry_id,entry) in &mut entries {
            let items = match entry_id2items.get(&entry_id) {
                Some(items) => items,
                None => continue,
            };
            let future = entry.set_auto_and_multi_match(&items);
            futures.push(future);
        }
        
        let _ = join_all(futures).await; // Ignore errors
        Ok(())
    }

    // async fn match_entry_to_items(&self, entry_id: usize, mut items: Vec<String>) -> Result<(),GenericError> {
    //     items.sort();
    //     items.dedup();
    //     let mut entry= Entry::from_id(entry_id, &self.mnm).await?;
    //     if entry.q!=self.mnm.item2numeric(&items[0]) {
    //         entry.set_match(&items[0],USER_AUTO).await?;
    //         if items.len()>1 { // Multi-match
    //             entry.set_multi_match(&items).await?;
    //         }
    //     }
    //     Ok(())
    // }

    pub async fn automatch_by_search(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,
            IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases` 
            FROM `entry` WHERE `catalog`=:catalog_id {} 
            /* ORDER BY `id` */
            LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut offset = self.get_last_job_offset() ;
        let batch_size = *self.mnm.app.task_specific_usize.get("automatch_by_search_batch_size").unwrap_or(&5000) ;
        let search_batch_size = *self.mnm.app.task_specific_usize.get("automatch_by_search_search_batch_size").unwrap_or(&100) ;

        loop {
            // println!("{sql}\n{catalog_id},{offset},{batch_size}");
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;
            // println!("Done.");

            for result_batch in results.chunks(search_batch_size) {
                // println!("Starting batch of {search_batch_size}...");
                let mut futures = vec![];
                for result in result_batch {
                    let entry_id = result.0 ;
                    let label = &result.1 ;
                    let type_q = &result.2 ;
                    let aliases: Vec<&str> = result.3.split("|").filter(|alias|!alias.is_empty()).collect();
                    let future = self.search_with_type_and_entity_id(entry_id,label,type_q);
                    futures.push(future);
                    for alias in &aliases {
                        let future = self.search_with_type_and_entity_id(entry_id,alias,type_q);
                        futures.push(future);
                    }
                }
                // println!("Running {} futures...",futures.len());
                let mut search_results = join_all(futures).await.into_iter()
                    .filter_map(|r|r)
                    .map(|(entry_id,items)| items.into_iter().map(move |q|(entry_id,q.to_string())))
                    .flatten()
                    .collect_vec();
                // println!("Futures complete");

                // Remove meta items
                let mut no_meta_items = search_results.iter().map(|(_entry_id,q)|q).cloned().collect_vec();
                no_meta_items.sort();
                no_meta_items.dedup();
                let _ = self.mnm.remove_meta_items(&mut no_meta_items).await;
                search_results.retain(|(_entry_id,q)| no_meta_items.contains(q));


                // Build entry ID to item IDs Hashmap
                let mut entry_id2items: HashMap<usize,Vec<String>> = HashMap::new();
                for (entry_id,q) in search_results {
                    entry_id2items.entry(entry_id).or_default().push(q);
                }

                // println!("Matching {} entries",entry_id2items.len());
                let _ = self.match_entries_to_items(&entry_id2items).await;
                // println!("Batch completed.");
            }

            if results.len()<batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }


    pub async fn automatch_simple(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql = format!("SELECT `id`,`ext_name`,`type`,
            IFNULL((SELECT group_concat(DISTINCT `label` SEPARATOR '|') FROM aliases WHERE entry_id=entry.id),'') AS `aliases` 
            FROM `entry` WHERE `catalog`=:catalog_id {} 
            /* ORDER BY `id` */
            LIMIT :batch_size OFFSET :offset",MatchState::not_fully_matched().get_sql());
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 5000 ;
        loop {
            // TODO make this more efficient, too many wd replica queries
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,offset,batch_size}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;

            for result in &results {
                let entry_id = result.0 ;
                let label = &result.1 ;
                let type_q = &result.2 ;
                let aliases: Vec<&str> = result.3.split("|").collect();
                let mut items = match self.mnm.wd_search_with_type_db(label,type_q).await {
                    Ok(items) => items,
                    _ => continue // Ignore error
                } ;
                for alias in &aliases {
                    let mut tmp = match self.mnm.wd_search_with_type_db(alias,type_q).await {
                        Ok(tmp) => tmp,
                        _ => continue // Ignore error
                    };
                    items.append(&mut tmp);
                }
                items.sort();
                items.dedup();
                if self.mnm.remove_meta_items(&mut items).await.is_err() {
                    continue ; // Ignore error
                }
                if items.is_empty() {
                    continue ;
                }
                let mut entry= match Entry::from_id(entry_id, &self.mnm).await {
                    Ok(entry) => entry,
                    _ => continue // Ignore error
                };
                if entry.set_match(&items[0],USER_AUTO).await.is_err() {
                    continue // Ignore error
                }
                if items.len()>1 { // Multi-match
                    let _ = entry.set_multi_match(&items).await.is_err(); // Ignore error
                }
            }

            if results.len()<batch_size {
                break;
            }
            offset += results.len();
            let _ = self.remember_offset(offset).await;
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    //TODO test
    pub async fn automatch_from_other_catalogs(&self, catalog_id: usize) -> Result<(),GenericError> {
        let sql1 = "SELECT `id`,`ext_name`,`type` FROM entry WHERE catalog=:catalog_id AND q IS NULL LIMIT :batch_size OFFSET :offset" ;
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 500 ;
        loop {
            #[derive(Debug, PartialEq, Eq, Clone)]
            struct ResultInOriginalCatalog {
                entry_id: usize,
                ext_name: String,
                type_name: String
            }
            let results_in_original_catalog: Vec<ResultInOriginalCatalog> = sql1.with(params! {catalog_id,batch_size,offset})
                .map(self.mnm.app.get_mnm_conn().await?, |(entry_id, ext_name, type_name)|ResultInOriginalCatalog{entry_id, ext_name, type_name})
                .await?;
            if results_in_original_catalog.is_empty() {
                break;
            }
            let ext_names: Vec<mysql_async::Value> = results_in_original_catalog
                .iter()
                .map(|r| {
                    mysql_async::Value::Bytes(r.ext_name.as_bytes().to_vec())
                })
                .collect();
            
            let mut name_type2id: HashMap<(String,String),Vec<usize>> = HashMap::new();
            results_in_original_catalog.iter().for_each(|r|{
                name_type2id
                    .entry((r.ext_name.to_owned(),r.type_name.to_owned()))
                    .and_modify(|v|v.push(r.entry_id))
                    .or_insert(vec![r.entry_id]);
            });
            
            #[derive(Debug, PartialEq, Eq, Clone)]
            struct ResultInOtherCatalog {
                entry_id: usize,
                ext_name: String,
                type_name: String,
                q: Option<isize>
            }

            let params = Params::Positional(ext_names);
            let placeholders = MixNMatch::sql_placeholders(results_in_original_catalog.len());
            let sql2 = "SELECT `id`,`ext_name`,`type`,q FROM entry 
            WHERE ext_name IN (".to_string()+&placeholders+")
            AND q IS NOT NULL AND q > 0 AND user IS NOT NULL AND user>0
            AND catalog IN (SELECT id from catalog WHERE active=1)
            GROUP BY ext_name,type HAVING count(DISTINCT q)=1";
            let results_in_other_catalogs: Vec<ResultInOtherCatalog> = sql2.with(params)
                .map(self.mnm.app.get_mnm_conn().await?, |(entry_id, ext_name, type_name, q)|ResultInOtherCatalog{entry_id, ext_name, type_name, q})
                .await?;
            for r in &results_in_other_catalogs {
                let q = match r.q {
                    Some(q) => format!("Q{}",q),
                    None => continue
                };
                let key = (r.ext_name.to_owned(),r.type_name.to_owned());
                if let Some(v) = name_type2id.get(&key) {
                    for entry_id in v {
                        if let Ok(mut entry) = Entry::from_id(*entry_id, &self.mnm).await {
                            let _ = entry.set_match(&q,USER_AUTO).await;
                        };
                    }
                }
            }
            if results_in_original_catalog.len()<batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results_in_original_catalog.len();
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    pub async fn purge_automatches(&self, catalog_id: usize) -> Result<(),GenericError> {
        let mut conn = self.mnm.app.get_mnm_conn().await?;
        conn.exec_drop("UPDATE entry SET q=NULL,user=NULL,`timestamp`=NULL WHERE catalog=:catalog_id AND user=0", params! {catalog_id}).await?;
        conn.exec_drop("DELETE FROM multi_match WHERE catalog=:catalog_id", params! {catalog_id}).await?;
        Ok(())
    }

    pub async fn match_person_by_dates(&self, catalog_id: usize) -> Result<(),GenericError> {
        let mw_api = self.mnm.get_mw_api().await?;
        let sql = "SELECT entry_id,ext_name,born,died 
            FROM (`entry` join `person_dates`)
            WHERE `person_dates`.`entry_id` = `entry`.`id`
            AND `catalog`=:catalog_id AND (q IS NULL or user=0) AND born!='' AND died!='' 
            LIMIT :batch_size OFFSET :offset";
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 5000 ;
        loop {
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,batch_size,offset}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;
            for result in &results {
                let entry_id = result.0;
                let ext_name = &result.1;
                let birth_year = match Self::extract_sane_year_from_date(&result.2) {
                    Some(year) => year,
                    None => continue
                };
                let death_year = match Self::extract_sane_year_from_date(&result.3) {
                    Some(year) => year,
                    None => continue
                };
                let candidate_items = match self.search_person(ext_name).await {
                    Ok(c) => c,
                    _ => continue // Ignore error
                };
                if candidate_items.is_empty() {
                    continue // No candidate items
                }
                let candidate_items = match self.subset_items_by_birth_death_year(&candidate_items,birth_year,death_year,&mw_api).await {
                    Ok(ci) => ci,
                    _ => continue // Ignore error
                };
                match candidate_items.len() {
                    0 => {} // No results
                    1 => {
                        let q=&candidate_items[0];
                        let _ = Entry::from_id(entry_id, &self.mnm).await?.set_match(&q,USER_DATE_MATCH).await;
                    }
                    _ => {
                        Issue::new(entry_id,IssueType::WdDuplicate,json!(candidate_items),&self.mnm).await?.insert().await?;
                    }
                }
            }
            if results.len()<batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results.len()
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    pub async fn match_person_by_single_date(&self, catalog_id: usize) -> Result<(),GenericError> {
        let precision = 10; // 2022-xx-xx=10; use 4 for just the year
        let match_field = "born" ;
        let match_prop = if match_field=="born" { "P569" }  else { "P570" } ;
        let mw_api = self.mnm.get_mw_api().await?;
        // CAUTION: Do NOT use views in the SQL statement, it will/might throw an "Prepared statement needs to be re-prepared" error
        let sql = format!("(
                SELECT multi_match.entry_id AS entry_id,born,died,candidates AS qs FROM person_dates,multi_match,entry
                WHERE (q IS NULL OR user=0) AND person_dates.entry_id=multi_match.entry_id AND multi_match.catalog=:catalog_id AND length({})=:precision
                AND entry.id=person_dates.entry_id
            ) UNION (
                SELECT entry_id,born,died,q qs FROM person_dates,entry
                WHERE (q is not null and user=0) AND catalog=:catalog_id AND length({})=:precision AND entry.id=person_dates.entry_id
            )
            ORDER BY entry_id LIMIT :batch_size OFFSET :offset",match_field,match_field);
        let mut offset = self.get_last_job_offset() ;
        let batch_size = 100 ;
        loop {
            let results = self.mnm.app.get_mnm_conn().await?
                .exec_iter(sql.clone(),params! {catalog_id,precision,batch_size,offset}).await?
                .map_and_drop(from_row::<(usize,String,String,String)>).await?;
            let results: Vec<CandidateDates> = results.iter().map(|r| CandidateDates::from_row(r)).collect();
            let items_to_load: Vec<String> = results.iter().map(|r|r.matches.clone()).flatten().collect();
            let items = wikibase::entity_container::EntityContainer::new();
            let _ = items.load_entities(&mw_api, &items_to_load).await; // We don't really care if there was an error in the grand scheme of things
            for result in &results {
                let mut candidates = vec![];
                for q in &result.matches {
                    let item = match items.get_entity(q.to_owned()) {
                        Some(item) => item,
                        None => continue
                    } ;
                    let statements = item.claims_with_property(match_prop);
                    for statement in &statements {
                        let main_snak = statement.main_snak();
                        let data_value = match main_snak.data_value() {
                            Some(dv) => dv,
                            None => continue
                        };
                        let time = match data_value.value() {
                            wikibase::value::Value::Time(tv) => tv,
                            _ => continue
                        };
                        let dt = match NaiveDateTime::parse_from_str(time.time(),"+%Y-%m-%dT%H:%M:%SZ") {
                            Ok(dt) => dt,
                            _ => continue // Could not parse date
                        };
                        let date = match precision {
                            4 => format!("{}",dt.format("%Y")),
                            10 => format!("{}",dt.format("%Y-%m-%d")),
                            other => panic!("Bad precision {}",other) // Should never happen
                        };
                        if (match_field=="born"&&date==result.born) || (match_field=="died"&&date==result.died) {
                            candidates.push(q.clone());
                        }
                    }
                }
                if candidates.len()==1 { // TODO >1
                    if let Some(q) = candidates.get(0) {
                        let _ = Entry::from_id(result.entry_id, &self.mnm).await?.set_match(&q,USER_DATE_MATCH).await;
                    }
                }
            }

            if results.len()<batch_size {
                break;
            }
            let _ = self.remember_offset(offset).await;
            offset += results.len()
        }
        let _ = self.clear_offset().await;
        Ok(())
    }

    //TODO test
    async fn search_person(&self, name: &str) -> Result<Vec<String>,GenericError> {
        let name = MixNMatch::sanitize_person_name(&name);
        let name = MixNMatch::simplify_person_name(&name);
        self.mnm.wd_search_with_type(&name,"Q5").await
    }

    //TODO test
    async fn subset_items_by_birth_death_year(&self, items: &Vec<String>, birth_year: i32, death_year: i32, mw_api: &mediawiki::api::Api) -> Result<Vec<String>,GenericError> {
        if items.len()>100 { // TODO chunks but that's a nightly feature
            return Ok(vec![]) ;
        }
        let item_str = items.join(" wd:");
        let sparql = format!("SELECT DISTINCT ?q {{ VALUES ?q {{ wd:{} }} . ?q wdt:P569 ?born ; wdt:P570 ?died. FILTER ( year(?born)={}).FILTER ( year(?died)={} ) }}",&item_str,birth_year,death_year);
        let results = match mw_api.sparql_query(&sparql).await {
            Ok(result) => result,
            _ => return Ok(vec![]) // Ignore error
        } ;
        let items = mw_api.entities_from_sparql_result(&results,"q");
        Ok(items)
    }


    //TODO test
    fn extract_sane_year_from_date(date: &str) -> Option<i32> {
        let captures = RE_YEAR.captures(date)?;
        if captures.len()!=2 {
            return None;
        }
        let year = captures.get(1)?.as_str().parse::<i32>().ok()?;
        if year<0 || year>Utc::now().year() {
            None
        } else {
            Some(year)
        }
    }

}


#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 5526 ;
    const TEST_ENTRY_ID: usize = 143962196 ;
    const TEST_ENTRY_ID2: usize = 144000954 ;

    #[tokio::test]
    async fn test_match_person_by_dates() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        
        // Clear
        Entry::from_id(TEST_ENTRY_ID2, &mnm).await.unwrap().unmatch().await.unwrap();

        // Match by date
        let am = AutoMatch::new(&mnm);
        am.match_person_by_dates(TEST_CATALOG_ID).await.unwrap();

        // Check if set
        let entry = Entry::from_id(TEST_ENTRY_ID2, &mnm).await.unwrap();
        assert!(entry.is_fully_matched());
        assert_eq!(1035,entry.q.unwrap());
    }

    #[tokio::test]
    async fn test_automatch_by_search() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

        // Clear
        Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap().unmatch().await.unwrap();

        // Run automatch
        let am = AutoMatch::new(&mnm);
        am.automatch_by_search(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(13520818));
        assert_eq!(entry.user,Some(USER_AUTO));
        
        // Clear
        entry.unmatch().await.unwrap();
    }

    #[tokio::test]
    async fn test_automatch_by_sitelink() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let am = AutoMatch::new(&mnm);

        // Clear
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Run automatch
        am.automatch_by_sitelink(TEST_CATALOG_ID).await.unwrap();

        // Check in-database changes
        let entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(13520818));
        assert_eq!(entry.user,Some(USER_AUTO));
        
        // Clear
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }

    #[tokio::test]
    async fn test_purge_automatches() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

        // Set a full match
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.unmatch().await.unwrap();
        entry.set_match("Q1",4).await.unwrap();
        assert!(entry.is_fully_matched());

        // Purge catalog
        let am = AutoMatch::new(&mnm);
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is still fully matched
        let entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert!(entry.is_fully_matched());

        // Set an automatch
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.unmatch().await.unwrap();
        entry.set_match("Q1",0).await.unwrap();
        assert!(entry.is_partially_matched());

        // Purge catalog
        let am = AutoMatch::new(&mnm);
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Check that the entry is now unmatched
        let entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert!(entry.is_unmatched());
    }

    #[tokio::test]
    async fn test_match_person_by_single_date() {
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();
        let am = AutoMatch::new(&mnm);
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();

        // Set prelim match
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        entry.set_match("Q13520818",0).await.unwrap();

        // Run automatch
        am.match_person_by_single_date(TEST_CATALOG_ID).await.unwrap();

        // Check match
        let mut entry= Entry::from_id(TEST_ENTRY_ID, &mnm).await.unwrap();
        assert_eq!(entry.q,Some(13520818));
        assert_eq!(entry.user,Some(USER_DATE_MATCH));

        // Cleanup
        entry.unmatch().await.unwrap();
        am.purge_automatches(TEST_CATALOG_ID).await.unwrap();
    }

}