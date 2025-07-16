use crate::app_state::AppState;
use crate::auxiliary_matcher::AUX_PROPERTIES_ALSO_USING_LOWERCASE;
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::job::{Job, Jobbable};
use crate::maintenance::Maintenance;
use crate::match_state::MatchState;
use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use wikimisc::timestamp::TimeStamp;

pub const EXT_URL_UNIQUE_SEPARATOR: &str = "!@£$%^&|";
const MAX_WIKI_ROWS: usize = 400;
const BLACKLISTED_CATALOGS: &[usize] = &[506];
const MNM_SITE_URL: &str = "https://mix-n-match.toolforge.org";

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct MatchDiffers {
    ext_id: String,
    q_wd: isize,
    q_mnm: isize,
    entry_id: usize,
    ext_url: String,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct SmallEntry {
    id: usize,
    q: Option<isize>,
    user: Option<usize>,
    ext_url: String,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct MultipleExtIdInWikidata {
    ext_id: String,
    items: Vec<String>,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ExtIdWithMutipleQ {
    q: isize,
    entry2ext_id: Vec<(usize, String)>,
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ExtIdNoMnM {
    q: isize,
    ext_id: String,
}

#[derive(Debug, Clone)]
pub struct Microsync {
    app: AppState,
    job: Option<Job>,
}

impl Jobbable for Microsync {
    //TODO test
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    //TODO test
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

impl Microsync {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            job: None,
        }
    }

    pub async fn check_catalog(&mut self, catalog_id: usize) -> Result<()> {
        if BLACKLISTED_CATALOGS.contains(&catalog_id) {
            return Ok(()); // TODO error?
        }
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let property = match (catalog.wd_prop(), catalog.wd_qual()) {
            (Some(prop), None) => prop,
            _ => return Ok(()), // Don't fail this job, just silently close it
        };
        let maintenance = Maintenance::new(&self.app);
        maintenance
            .fix_matched_items(catalog_id, &MatchState::fully_matched())
            .await?;

        let multiple_extid_in_wikidata = self.get_multiple_extid_in_wikidata(property).await?;
        let multiple_q_in_mnm = self.get_multiple_q_in_mnm(catalog_id).await?;
        let (extid_not_in_mnm, match_differs) =
            self.get_differences_mnm_wd(catalog_id, property).await?;
        let wikitext = self
            .wikitext_from_issues(
                &catalog,
                multiple_extid_in_wikidata,
                multiple_q_in_mnm,
                match_differs,
                extid_not_in_mnm,
            )
            .await?;
        self.update_wiki_page(catalog_id, &wikitext).await?;
        Ok(())
    }

    //TODO test
    async fn update_wiki_page(&mut self, catalog_id: usize, wikitext: &str) -> Result<()> {
        let page_title = format!("User:Magnus Manske/Mix'n'match report/{catalog_id}");
        let day = &TimeStamp::now()[0..8];
        let comment = format!("Update {day}");
        self.app
            .wikidata_mut()
            .set_wikipage_text(&page_title, wikitext, &comment)
            .await
    }

    //TODO test
    async fn wikitext_from_issues(
        &self,
        catalog: &Catalog,
        multiple_extid_in_wikidata: Vec<MultipleExtIdInWikidata>,
        multiple_q_in_mnm: Vec<ExtIdWithMutipleQ>,
        match_differs: Vec<MatchDiffers>,
        extid_not_in_mnm: Vec<ExtIdNoMnM>,
    ) -> Result<String> {
        let formatter_url =
            Self::get_formatter_url_for_prop(catalog.wd_prop().unwrap_or(0)).await?;
        let mut ret = wikitext_from_issues_get_header(catalog);
        Self::wikitext_from_issues_add_extid_info(extid_not_in_mnm, &mut ret, &formatter_url);
        self.wikitext_from_issues_match_differs(match_differs, &mut ret, &formatter_url)
            .await?;
        ret += &self
            .wikitext_from_issues_multiple_q_in_mnm(multiple_q_in_mnm, &formatter_url)
            .await?;
        ret += &Self::wikitext_from_issues_multiple_extid_in_wd(
            multiple_extid_in_wikidata,
            formatter_url,
        );
        Ok(ret)
    }

    fn wikitext_from_issues_multiple_extid_in_wd(
        multiple_extid_in_wikidata: Vec<MultipleExtIdInWikidata>,
        formatter_url: String,
    ) -> String {
        let mut ret = String::new();
        if !multiple_extid_in_wikidata.is_empty() {
            ret += "== Multiple items for the same external ID in Wikidata ==\n";
            if multiple_extid_in_wikidata.len() > MAX_WIKI_ROWS {
                ret += &format!("* {} external IDs have at least two items on Wikidata. Too many to show individually.\n\n",multiple_extid_in_wikidata.len());
            } else {
                ret += "{| class='wikitable'\n! External ID !! Items in Mix'n'Match\n";
                for e in &multiple_extid_in_wikidata {
                    let ext_id = Self::format_ext_id(&e.ext_id, "", &formatter_url);
                    let items: Vec<String> =
                        e.items.iter().map(|q| format!("{{{{Q|{q}}}}}")).collect();
                    let items = items.join("<br/>");
                    let s = format!("|-\n| {ext_id} || {items}\n");
                    ret += &s;
                }
                ret += "|}\n\n";
            }
        }
        ret
    }

    async fn wikitext_from_issues_multiple_q_in_mnm(
        &self,
        multiple_q_in_mnm: Vec<ExtIdWithMutipleQ>,
        formatter_url: &str,
    ) -> Result<String> {
        let mut ret = String::new();
        if !multiple_q_in_mnm.is_empty() {
            ret += "== Same item for multiple external IDs in Mix'n'match ==\n";
            if multiple_q_in_mnm.len() > MAX_WIKI_ROWS {
                ret += &format!("* {} items have more than one match in Mix'n'Match. Too many to show individually.\n\n",multiple_q_in_mnm.len());
            } else {
                let entry_ids: Vec<usize> = multiple_q_in_mnm
                    .iter()
                    .flat_map(|e| e.entry2ext_id.iter().map(|x| x.0))
                    .collect();
                let entry2name = self
                    .app
                    .storage()
                    .microsync_load_entry_names(&entry_ids)
                    .await?;
                ret += "{| class='wikitable'\n! Item in Mix'n'Match !! Mix'n'match entry !! External ID !! External label\n" ;
                for e in &multiple_q_in_mnm {
                    let mut first = true;
                    let q_mnm = e.q;
                    for (entry_id, ext_id) in &e.entry2ext_id {
                        ret += &Self::wikitext_from_issues_multiple_q_in_mnm_process_row(
                            &mut first,
                            e,
                            q_mnm,
                            &entry2name,
                            entry_id,
                            ext_id,
                            formatter_url,
                        );
                    }
                }
                ret += "|}\n\n";
            }
        }
        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    fn wikitext_from_issues_multiple_q_in_mnm_process_row(
        first: &mut bool,
        e: &ExtIdWithMutipleQ,
        q_mnm: isize,
        entry2name: &HashMap<usize, String>,
        entry_id: &usize,
        ext_id: &String,
        formatter_url: &str,
    ) -> String {
        let row = if *first {
            *first = false;
            format!(
                "|-\n|rowspan={}|{{{{Q|{}}}}}|| ",
                e.entry2ext_id.len(),
                q_mnm
            )
        } else {
            "|-\n|| ".to_string()
        };
        let ext_name = entry2name.get(entry_id).unwrap_or(ext_id);
        let ext_id = Self::format_ext_id(ext_id, "", formatter_url);
        let mnm_url = format!("https://mix-n-match.toolforge.org/#/entry/{entry_id}");
        format!("{row}[{mnm_url} {entry_id}] || {ext_id} || {ext_name}\n")
    }

    async fn wikitext_from_issues_match_differs(
        &self,
        match_differs: Vec<MatchDiffers>,
        ret: &mut String,
        formatter_url: &str,
    ) -> Result<()> {
        if !match_differs.is_empty() {
            *ret += "== Different items for the same external ID ==\n";
            if match_differs.len() > MAX_WIKI_ROWS {
                *ret += &format!("* {} enties have different items on Mix'n'match and Wikidata. Too many to show individually.\n\n",match_differs.len());
            } else {
                let entry_ids: Vec<usize> = match_differs.iter().map(|e| e.entry_id).collect();
                let entry2name = self
                    .app
                    .storage()
                    .microsync_load_entry_names(&entry_ids)
                    .await?;
                *ret += "{| class='wikitable'\n! External ID !! External label !! Item in Wikidata !! Item in Mix'n'Match !! Mix'n'match entry\n" ;
                for e in &match_differs {
                    let ext_name = entry2name.get(&e.entry_id).unwrap_or(&e.ext_id);
                    let ext_id = Self::format_ext_id(&e.ext_id, &e.ext_url, formatter_url);
                    let mnm_url =
                        format!("https://mix-n-match.toolforge.org/#/entry/{}", e.entry_id);
                    let s = format!("|-\n| {ext_id} || {ext_name} || {{{{Q|{}}}}} || {{{{Q|{}}}}} || [{mnm_url} {}]\n",e.q_wd,e.q_mnm,e.entry_id);
                    *ret += &s;
                }
                *ret += "|}\n\n";
            }
        }
        Ok(())
    }

    fn wikitext_from_issues_add_extid_info(
        extid_not_in_mnm: Vec<ExtIdNoMnM>,
        ret: &mut String,
        formatter_url: &str,
    ) {
        if !extid_not_in_mnm.is_empty() {
            *ret += "== Unknown external ID ==\n";
            if extid_not_in_mnm.len() > MAX_WIKI_ROWS {
                *ret += &format!("* {} external IDs in Wikidata but not in Mix'n'Match. Too many to show individually.\n\n",extid_not_in_mnm.len());
            } else {
                *ret += "{| class='wikitable'\n! External ID !! Item\n";
                for e in &extid_not_in_mnm {
                    let ext_id = Self::format_ext_id(&e.ext_id, "", formatter_url);
                    let s = format!("|-\n| {} || {{{{Q|{}}}}}\n", &ext_id, e.q);
                    *ret += &s;
                }
                *ret += "|}\n\n";
            }
        }
    }

    fn format_ext_id(ext_id: &str, ext_url: &str, formatter_url: &str) -> String {
        // TODO if ( !preg_match('|^[a-zA-Z0-9._ -]+$|',$ext_id) ) $ext_id = "<nowiki>{$ext_id}</nowiki>" ;
        if !formatter_url.is_empty() {
            format!("[{} {}]", formatter_url.replace("$1", ext_id), ext_id)
        } else if !ext_url.is_empty() {
            format!("[{ext_url} {ext_id}]")
        } else {
            ext_id.to_string()
        }
    }

    async fn get_formatter_url_for_prop(property: usize) -> Result<String> {
        let url = format!(
            "https://www.wikidata.org/w/api.php?action=wbgetentities&ids=P{property}&format=json"
        );
        let client = wikimisc::wikidata::Wikidata::new().reqwest_client()?;
        let json = client.get(&url).send().await?.json::<Value>().await?;
        let url2 = json["entities"][format!("P{property}")]["claims"]["P1630"][0]["mainsnak"]
            ["datavalue"]["value"]
            .as_str()
            .map_or_else(String::new, |url_tmp| url_tmp.to_string());
        Ok(url2)
    }

    async fn get_multiple_extid_in_wikidata(
        &self,
        property: usize,
    ) -> Result<Vec<MultipleExtIdInWikidata>> {
        let mw_api = self.app.wikidata().get_mw_api().await?;
        // TODO: lcase?
        let sparql = format!(
            "SELECT ?extid (count(?q) AS ?cnt) (GROUP_CONCAT(?q; SEPARATOR = '|') AS ?items)
            {{ ?q wdt:P{property} ?extid }}
            GROUP BY ?extid HAVING (?cnt>1)
            ORDER BY ?extid"
        );
        Ok(self
            .app
            .wikidata()
            .load_sparql_csv(&sparql)
            .await?
            .records()
            .filter_map(|r| r.ok())
            .filter(|r| r.len() == 3)
            .take(MAX_WIKI_ROWS + 1) // limit to max results, not point in collecting more
            .filter_map(|r| match r.get(0) {
                Some(ext_id) => match r.get(2) {
                    Some(item_str) => {
                        let items: Vec<String> = item_str
                            .split('|')
                            .filter_map(|s| mw_api.extract_entity_from_uri(s).ok())
                            .collect();
                        Some(MultipleExtIdInWikidata {
                            ext_id: ext_id.to_string(),
                            items,
                        })
                    }
                    None => None,
                },
                None => None,
            })
            .collect())
    }

    async fn get_multiple_q_in_mnm(&self, catalog_id: usize) -> Result<Vec<ExtIdWithMutipleQ>> {
        let results = self
            .app
            .storage()
            .microsync_get_multiple_q_in_mnm(catalog_id)
            .await?;
        let mut results: Vec<ExtIdWithMutipleQ> = results
            .iter()
            .map(|r| {
                let entry_ids: Vec<&str> = r.1.split(',').collect();
                let ext_ids: Vec<&str> = r.2.split(EXT_URL_UNIQUE_SEPARATOR).collect();
                let mut entry2ext_id: Vec<(usize, String)> = entry_ids
                    .iter()
                    .zip(ext_ids.iter())
                    .filter_map(|(entry_id, ext_id)| {
                        entry_id
                            .parse()
                            .map_or(None, |entry_id2| Some((entry_id2, ext_id.to_string())))
                    })
                    .collect();
                entry2ext_id.sort();
                ExtIdWithMutipleQ {
                    q: r.0,
                    entry2ext_id,
                }
            })
            .collect();
        results.sort();
        Ok(results)
    }

    //TODO test
    async fn get_q2ext_id_chunk(
        &self,
        reader: &mut csv::Reader<File>,
        case_insensitive: bool,
        batch_size: usize,
    ) -> Result<Vec<(isize, String)>> {
        let mw_api = self.app.wikidata().get_mw_api().await?;
        Ok(reader
            .records()
            .filter_map(|r| r.ok())
            .filter_map(|r| {
                let q = mw_api.extract_entity_from_uri(r.get(0)?).ok()?;
                let q_numeric = AppState::item2numeric(&q)?;
                let value = r.get(1)?;
                let value = if case_insensitive {
                    value.to_lowercase().to_string()
                } else {
                    value.to_string()
                };
                Some((q_numeric, value))
            })
            .take(batch_size)
            .collect())
    }

    //TODO test
    async fn get_differences_mnm_wd(
        &self,
        catalog_id: usize,
        property: usize,
    ) -> Result<(Vec<ExtIdNoMnM>, Vec<MatchDiffers>)> {
        let case_insensitive = AUX_PROPERTIES_ALSO_USING_LOWERCASE.contains(&property);
        let sparql = format!("SELECT ?item ?value {{ ?item wdt:P{property} ?value }}"); // "ORDER BY ?item" unnecessary?
        let mut reader = self.app.wikidata().load_sparql_csv(&sparql).await?;
        let mut extid_not_in_mnm: Vec<ExtIdNoMnM> = vec![];
        let mut match_differs = vec![];
        let batch_size: usize = 5000;
        loop {
            let chunk = self
                .get_differences_mnm_wd_process_chunk(
                    &mut reader,
                    case_insensitive,
                    batch_size,
                    catalog_id,
                    property,
                    &mut match_differs,
                    &mut extid_not_in_mnm,
                )
                .await?;
            if chunk.len() < batch_size {
                break;
            }
        }
        extid_not_in_mnm.sort();
        match_differs.sort();
        Ok((extid_not_in_mnm, match_differs))
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_differences_mnm_wd_process_chunk(
        &self,
        reader: &mut csv::Reader<File>,
        case_insensitive: bool,
        batch_size: usize,
        catalog_id: usize,
        property: usize,
        match_differs: &mut Vec<MatchDiffers>,
        extid_not_in_mnm: &mut Vec<ExtIdNoMnM>,
    ) -> Result<Vec<(isize, String)>> {
        let chunk = self
            .get_q2ext_id_chunk(reader, case_insensitive, batch_size)
            .await?;
        let ext_ids: Vec<&String> = chunk.iter().map(|x| &x.1).collect();
        let ext_id2entry = self
            .get_entries_for_ext_ids(catalog_id, property, &ext_ids)
            .await?;
        for (q, ext_id) in &chunk {
            match ext_id2entry.get(ext_id) {
                Some(entry) => {
                    self.get_differences_mnm_wd_process_entry(entry, q, ext_id, match_differs)
                        .await?;
                }
                None => {
                    if extid_not_in_mnm.len() <= MAX_WIKI_ROWS {
                        extid_not_in_mnm.push(ExtIdNoMnM {
                            q: *q,
                            ext_id: ext_id.to_owned(),
                        });
                    }
                }
            }
        }
        Ok(chunk)
    }

    async fn get_differences_mnm_wd_process_entry(
        &self,
        entry: &SmallEntry,
        q: &isize,
        ext_id: &String,
        match_differs: &mut Vec<MatchDiffers>,
    ) -> Result<()> {
        if entry.user.is_none() || entry.user == Some(0) || entry.q.is_none() {
            // Found a match but not in app yet
            Entry::from_id(entry.id, &self.app)
                .await?
                .set_match(&format!("Q{q}"), 4)
                .await?;
        } else if Some(*q) != entry.q {
            // Fully matched but to different item
            if let Some(entry_q) = entry.q {
                // Entry has N/A or Not In Wikidata, overwrite
                self.get_differences_mnm_wd_process_entry_overwrite(
                    entry_q,
                    entry,
                    q,
                    ext_id,
                    match_differs,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn get_differences_mnm_wd_process_entry_overwrite(
        &self,
        entry_q: isize,
        entry: &SmallEntry,
        q: &isize,
        ext_id: &String,
        match_differs: &mut Vec<MatchDiffers>,
    ) -> Result<()> {
        if entry_q <= 0 {
            Entry::from_id(entry.id, &self.app)
                .await?
                .set_match(&format!("Q{q}"), 4)
                .await?;
        } else {
            let md = MatchDiffers {
                ext_id: ext_id.to_owned(),
                q_wd: *q,
                q_mnm: entry_q,
                entry_id: entry.id,
                ext_url: entry.ext_url.to_owned(),
            };
            if match_differs.len() <= MAX_WIKI_ROWS {
                match_differs.push(md);
            }
        }
        Ok(())
    }

    //TODO test
    async fn get_entries_for_ext_ids(
        &self,
        catalog_id: usize,
        property: usize,
        ext_ids: &[&String],
    ) -> Result<HashMap<String, SmallEntry>> {
        if ext_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let results = self
            .app
            .storage()
            .microsync_get_entries_for_ext_ids(catalog_id, ext_ids)
            .await?;
        let case_insensitive = AUX_PROPERTIES_ALSO_USING_LOWERCASE.contains(&property);
        let ret: HashMap<String, SmallEntry> = results
            .iter()
            .map(|(id, q, user, ext_id, ext_url)| {
                let ext_id = if case_insensitive {
                    ext_id.to_lowercase().to_string()
                } else {
                    ext_id.to_string()
                };
                (
                    ext_id,
                    SmallEntry {
                        id: *id,
                        q: q.to_owned(),
                        user: user.to_owned(),
                        ext_url: ext_url.to_owned(),
                    },
                )
            })
            .collect();
        Ok(ret)
    }
}

fn wikitext_from_issues_get_header(catalog: &Catalog) -> String {
    let catalog_name = catalog.name().map_or_else(String::new, |s| s.to_owned());
    let mut ret = String::new();
    ret += &format!(
        "A report for the [{MNM_SITE_URL}/ Mix'n'match] tool. '''This page will be replaced regularly!'''\n"
    );
    ret += "''Please note:''\n";
    ret += "* If you fix something from this list on Wikidata, please fix it on Mix'n'match as well, if applicable. Otherwise, the error might be re-introduced from there.\n";
    ret += "* 'External ID' refers to the IDs in the original (external) catalog; the same as the statement value for the associated  property.\n\n";
    ret += &format!(
        "==[{MNM_SITE_URL}/#/catalog/{} {}]==\n{}\n\n",
        catalog.id(),
        &catalog_name,
        &catalog.desc()
    );
    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{get_test_app, TEST_MUTEX};

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;
    const TEST_ENTRY_ID2: usize = 144000951;

    #[tokio::test]
    async fn test_get_multiple_extid_in_wikidata() {
        let app = get_test_app();
        let ms = Microsync::new(&app);
        let result = ms.get_multiple_extid_in_wikidata(7889).await.unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_get_multiple_q_in_mnm() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .set_match("Q13520818", 2)
            .await
            .unwrap();
        Entry::from_id(TEST_ENTRY_ID2, &app)
            .await
            .unwrap()
            .set_match("Q13520818", 2)
            .await
            .unwrap();

        let ms = Microsync::new(&app);
        let _results = ms.get_multiple_q_in_mnm(TEST_CATALOG_ID).await.unwrap();

        // Cleanup
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();
        Entry::from_id(TEST_ENTRY_ID2, &app)
            .await
            .unwrap()
            .unmatch()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_formatter_url_for_prop() {
        assert_eq!(
            Microsync::get_formatter_url_for_prop(214).await.unwrap(),
            "https://viaf.org/viaf/$1".to_string()
        );
        assert_eq!(
            Microsync::get_formatter_url_for_prop(215).await.unwrap(),
            "".to_string()
        );
        assert_eq!(
            Microsync::get_formatter_url_for_prop(0).await.unwrap(),
            "".to_string()
        );
    }

    #[tokio::test]
    async fn test_load_entry_names() {
        let app = get_test_app();
        let result = app
            .storage()
            .microsync_load_entry_names(&[TEST_ENTRY_ID])
            .await
            .unwrap();
        assert_eq!(
            result.get(&TEST_ENTRY_ID),
            Some(&"Magnus Manske".to_string())
        );
    }

    #[tokio::test]
    async fn test_format_ext_id() {
        assert_eq!(
            Microsync::format_ext_id("gazebo", "http://foo.bar", "http://foo.baz/$1"),
            "[http://foo.baz/gazebo gazebo]".to_string()
        );
        assert_eq!(
            Microsync::format_ext_id("gazebo", "http://foo.bar", ""),
            "[http://foo.bar gazebo]".to_string()
        );
        assert_eq!(
            Microsync::format_ext_id("gazebo", "", "http://foo.baz/$1"),
            "[http://foo.baz/gazebo gazebo]".to_string()
        );
        assert_eq!(
            Microsync::format_ext_id("gazebo", "", ""),
            "gazebo".to_string()
        );
    }

    #[tokio::test]
    async fn test_check_catalog() {
        let app = get_test_app();
        let mut ms = Microsync::new(&app);
        ms.check_catalog(22).await.unwrap();
    }
}
