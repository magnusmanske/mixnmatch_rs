use crate::app_state::*;
use crate::storage::Storage;
use crate::wikidata::META_ITEMS;
use anyhow::Result;
use lazy_static::lazy_static;
use regex::Regex;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub const MNM_SITE_URL: &str = "https://mix-n-match.toolforge.org";

/// Global function for tests.
pub fn get_test_mnm() -> MixNMatch {
    let ret =
        MixNMatch::new(AppState::from_config_file("config.json").expect("Cannot create test MnM"));
    *TESTING.lock().unwrap() = true;
    ret
}

lazy_static! {
    pub static ref TESTING: Mutex<bool> = Mutex::new(false); // To lock the test entry in the database
    pub static ref TEST_MUTEX: Mutex<bool> = Mutex::new(true); // To lock the test entry in the database
}

lazy_static! {
    static ref SANITIZE_PERSON_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"^(Sir|Mme|Dr|Mother|Father)\.{0,1} ").expect("Regex failure"),
        Regex::new(r"\b[A-Z]\. /").expect("Regex failure"),
        Regex::new(r" (\&) ").expect("Regex failure"),
        Regex::new(r"\(.+?\)").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_PERSON_NAME_RES: Vec<Regex> = vec![
        Regex::new(r"\s*\(.*?\)\s*").expect("Regex failure"),
        Regex::new(r"[, ]+(Jr\.{0,1}|Sr\.{0,1}|PhD\.{0,1}|MD|M\.D\.)$").expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"^(Sir|Baron|Baronesse{0,1}|Graf|Gräfin|Prince|Princess|Dr\.|Prof\.|Rev\.)\s+")
            .expect("Regex failure"),
        Regex::new(r"\s*(Ritter|Freiherr)\s+").expect("Regex failure"),
        Regex::new(r"\s+").expect("Regex failure"),
    ];
    static ref SIMPLIFY_PERSON_NAME_TWO_RE: Regex =
        Regex::new(r"^(\S+) .*?(\S+)$").expect("Regex failure");
    static ref RE_ITEM2NUMERIC: Regex = Regex::new(r"(-{0,1}\d+)").expect("Regex failure");
}

pub const Q_NA: isize = 0;
pub const Q_NOWD: isize = -1;
pub const USER_AUTO: usize = 0;
pub const USER_DATE_MATCH: usize = 3;
pub const USER_AUX_MATCH: usize = 4;
pub const USER_LOCATION_MATCH: usize = 5;
pub const WIKIDATA_USER_AGENT: &str = "MixNMmatch_RS/1.0";
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

#[derive(Debug, Clone)]
pub struct MatchState {
    pub unmatched: bool,
    pub partially_matched: bool,
    pub fully_matched: bool,
    // TODO N/A ?
}

impl MatchState {
    pub fn unmatched() -> Self {
        Self {
            unmatched: true,
            partially_matched: false,
            fully_matched: false,
        }
    }

    pub fn fully_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: false,
            fully_matched: true,
        }
    }

    pub fn not_fully_matched() -> Self {
        Self {
            unmatched: true,
            partially_matched: true,
            fully_matched: false,
        }
    }

    pub fn any_matched() -> Self {
        Self {
            unmatched: false,
            partially_matched: true,
            fully_matched: true,
        }
    }

    pub fn get_sql(&self) -> String {
        let mut parts = vec![];
        if self.unmatched {
            parts.push("(`q` IS NULL)")
        }
        if self.partially_matched {
            parts.push("(`q`>0 AND `user`=0)")
        }
        if self.fully_matched {
            parts.push("(`q`>0 AND `user`>0)")
        }
        if parts.is_empty() {
            return "".to_string();
        }
        format!(" AND ({}) ", parts.join(" OR "))
    }
}

#[derive(Debug, Clone)]
pub struct MixNMatch {
    pub app: AppState,
    pub testing: bool,
}

impl MixNMatch {
    pub fn new(app: AppState) -> Self {
        Self {
            app,
            testing: false,
        }
    }

    pub fn get_storage(&self) -> &Arc<Box<dyn Storage>> {
        self.app.get_storage()
    }

    /// Removes "meta items" (eg disambiguation pages) from an item list.
    /// Items are in format "Qxxx".
    pub async fn remove_meta_items(&self, items: &mut Vec<String>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        items.sort();
        items.dedup();
        let meta_items = self.app.wikidata().get_meta_items(items).await?;
        items.retain(|item| !meta_items.iter().any(|q| q == item));
        Ok(())
    }

    /// Converts a string like "Q12345" to the numeric 12334
    pub fn item2numeric(&self, q: &str) -> Option<isize> {
        RE_ITEM2NUMERIC
            .captures_iter(q)
            .next()
            .and_then(|cap| cap[1].parse::<isize>().ok())
    }

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn wd_search_with_type(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        if type_q.is_empty() {
            return self.wd_search(name).await;
        }
        let mut query = format!("{} haswbstatement:P31={}", name, type_q);
        if type_q != "Q13442814" {
            // Exclude "scholarly article"
            query = format!("{} -haswbstatement:P31=Q13442814", query);
        }
        let meta_items: Vec<String> = META_ITEMS
            .iter()
            .map(|q| format!(" -haswbstatement:P31={}", q))
            .collect();
        query += &meta_items.join("");
        self.wd_search(&query).await
    }

    pub async fn wd_search_with_type_db(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        let items = if type_q.is_empty() {
            self.app.wikidata().search_without_type(name).await?
        } else {
            self.app.wikidata().search_with_type(name).await?
        };
        Ok(items)
    }

    /// Performs a Wikidata API search for the query string. Returns item IDs matching the query.
    pub async fn wd_search(&self, query: &str) -> Result<Vec<String>> {
        self.app.wikidata().search_with_limit(query, None).await
    }

    //TODO test
    pub fn sanitize_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SANITIZE_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name.trim().to_string()
    }

    //TODO test
    pub fn simplify_person_name(name: &str) -> String {
        let mut name = name.to_string();
        for re in SIMPLIFY_PERSON_NAME_RES.iter() {
            name = re.replace_all(&name, " ").to_string();
        }
        name = SIMPLIFY_PERSON_NAME_TWO_RE
            .replace_all(&name, "$1 $2")
            .to_string();
        name.trim().to_string()
    }

    pub fn import_file_path(&self) -> &str {
        self.app.import_file_path()
    }

    // /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        wikimisc::wikidata::Wikidata::new()
            .load_sparql_csv(sparql)
            .await
    }

    pub fn tool_root_dir() -> String {
        std::env::var("TOOL_DATA_DIR").unwrap_or("/data/project/mix-n-match".to_string())
    }

    pub fn is_on_toolforge() -> bool {
        std::path::Path::new("/etc/wmcs-project").exists()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const _TEST_CATALOG_ID: usize = 5526;
    const _TEST_ENTRY_ID: usize = 143962196;

    #[test]
    fn test_get_sql() {
        let ms = MatchState {
            unmatched: false,
            fully_matched: false,
            partially_matched: false,
        };
        assert_eq!(ms.get_sql().as_str(), "");
        assert_eq!(
            MatchState::unmatched().get_sql().as_str(),
            " AND ((`q` IS NULL)) "
        );
        assert_eq!(
            MatchState::fully_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`>0)) "
        );
        assert_eq!(
            MatchState::not_fully_matched().get_sql().as_str(),
            " AND ((`q` IS NULL) OR (`q`>0 AND `user`=0)) "
        );
        assert_eq!(
            MatchState::any_matched().get_sql().as_str(),
            " AND ((`q`>0 AND `user`=0) OR (`q`>0 AND `user`>0)) "
        );
    }

    #[tokio::test]
    async fn test_remove_meta_items() {
        let mnm = get_test_mnm();
        let mut items: Vec<String> = ["Q1", "Q3522", "Q2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        mnm.remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items, ["Q1", "Q2"]);
    }

    #[tokio::test]
    async fn test_wd_search() {
        let mnm = get_test_mnm();
        assert!(mnm.wd_search("").await.unwrap().is_empty());
        assert_eq!(
            mnm.wd_search("Magnus Manske haswbstatement:P31=Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
        assert_eq!(
            mnm.wd_search_with_type("Magnus Manske", "Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
    }

    #[test]
    fn test_item2numeric() {
        let mnm = get_test_mnm();
        assert_eq!(mnm.item2numeric("foobar"), None);
        assert_eq!(mnm.item2numeric("12345"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345X"), Some(12345));
        assert_eq!(mnm.item2numeric("Q12345X6"), Some(12345));
    }

    #[test]
    fn test_sanitize_person_name() {
        assert_eq!(
            MixNMatch::sanitize_person_name("Sir John Doe"),
            "John Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Mme. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Mother Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Father Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
        assert_eq!(
            MixNMatch::sanitize_person_name("Jane Doe & John Smith"),
            "Jane Doe John Smith".to_string()
        );
    }

    #[test]
    fn test_simplify_person_name() {
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe (actor)"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, Jr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, Sr."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, PhD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, MD"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Jane Doe, M.D."),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Sir Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Baron Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Baronesse Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Graf Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Gräfin Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Prince Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Princess Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Dr. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Prof. Jane Doe"),
            "Jane Doe".to_string()
        );
        assert_eq!(
            MixNMatch::simplify_person_name("Rev. Jane Doe"),
            "Jane Doe".to_string()
        );
    }
}

unsafe impl Send for MixNMatch {}
unsafe impl Sync for MixNMatch {}
