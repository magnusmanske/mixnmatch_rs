use crate::{
    mysql_misc::MySQLMisc, util::wikidata_props as wp, wikidata_commands::WikidataCommand,
};
use anyhow::{Result, anyhow};
use itertools::Itertools;
use log::error;
use mysql_async::{from_row, prelude::*};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    time::Duration,
};
use urlencoding::encode;
use wikimisc::wikibase::EntityTrait;

pub const WIKIDATA_API_URL: &str = "https://www.wikidata.org/w/api.php";
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
    mw_api: Option<mediawiki::api::Api>,
    bot_name: String,
    bot_password: String,
    api_url: String,
}

impl MySQLMisc for Wikidata {
    fn pool(&self) -> &mysql_async::Pool {
        &self.pool
    }
}

impl Wikidata {
    pub fn new(config: &Value, bot_name: String, bot_password: String) -> Self {
        let api_url = config["api_url"]
            .as_str()
            .unwrap_or(WIKIDATA_API_URL)
            .to_string();
        Self {
            pool: Self::create_pool(config),
            mw_api: None,
            bot_name,
            bot_password,
            api_url,
        }
    }

    fn testing() -> bool {
        *crate::app_state::TESTING.lock().unwrap()
    }

    // Database things

    /// Returns [(`item_id`, `page`)]
    pub async fn get_items_for_pages_on_wiki(
        &self,
        pages: Vec<String>,
        site: &String,
    ) -> Result<Vec<(usize, String)>> {
        let placeholders = Self::sql_placeholders(pages.len());
        let sql = format!(
            "SELECT `ips_item_id`,`ips_site_page`
            FROM `wb_items_per_site`
            WHERE `ips_site_id`='{site}'
            AND `ips_site_page` IN ({placeholders})"
        );
        let wd_matches = self
            .get_conn()
            .await?
            .exec_iter(sql, pages)
            .await?
            .map_and_drop(from_row::<(usize, String)>)
            .await?;
        Ok(wd_matches)
    }

    /// Returns the subset of `unique_qs` whose `instance of` (P31) is one
    /// of the meta classes in [`META_ITEMS`] — i.e. items that should
    /// never be a real Mix'n'match target (disambiguation pages, templates,
    /// categories, list articles, …).
    ///
    /// Implemented against the live `wbgetentities` API rather than the
    /// replica's `pagelinks` table. The previous SQL had two problems
    /// (GitHub #6):
    ///
    /// * It joined `linktarget` into the FROM with no condition tying it
    ///   to the rest of the query — a Cartesian product against a
    ///   multi-million-row table that ballooned the intermediate result
    ///   set, made the query slow and timeout-prone, and offered nothing
    ///   in return.
    /// * It looked at *pagelinks*, not P31. A statement value (qualifier,
    ///   reference, …) that happened to point at a meta-class Q-id was
    ///   enough to flag the source item, even though P31 said nothing of
    ///   the sort. Combined with the bulk-UPDATE blast radius (since
    ///   fixed), one such false positive wiped real matches across every
    ///   catalog in the tool.
    ///
    /// `wbgetentities` answers the question we actually want — "is P31
    /// one of these classes?" — directly. It is slower per call than a
    /// replica query but the volumes in question (per-catalog batches)
    /// stay well inside the API budget; chunking is delegated to
    /// `EntityContainer`.
    pub async fn get_meta_items(&self, unique_qs: &[String]) -> Result<Vec<String>> {
        if unique_qs.is_empty() {
            return Ok(vec![]);
        }
        // wbgetentities caps at 50 ids per request. EntityContainer
        // handles the inner chunking; we batch loosely at 200 to bound
        // the number of in-flight responses kept around per call —
        // matches the pattern in `Maintenance::sanity_check_one_batch`.
        const BATCH_SIZE: usize = 200;
        let api = self
            .get_mw_api()
            .await
            .map_err(|e| anyhow!("get_meta_items: cannot reach Wikidata API: {e}"))?;
        let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
        for chunk in unique_qs.chunks(BATCH_SIZE) {
            entities
                .load_entities(&api, &chunk.to_vec())
                .await
                .map_err(|e| anyhow!("get_meta_items: load_entities failed: {e}"))?;
        }
        let mut meta = Vec::new();
        for q in unique_qs {
            // Items that didn't load (network blip, deletion, redirect
            // missed by `get_redirected_items`) are *not* flagged as
            // meta — let the dedicated paths handle missing entities.
            let Some(entity) = entities.get_entity(q.clone()) else {
                continue;
            };
            if META_ITEMS
                .iter()
                .any(|m| entity.has_target_entity(wp::P_INSTANCE_OF, m))
            {
                meta.push(q.clone());
            }
        }
        Ok(meta)
    }

    pub async fn search_without_type(&self, name: &str) -> Result<Vec<String>> {
        let sql = "SELECT concat('Q',wbit_item_id) AS q
        	FROM wbt_text,wbt_item_terms,wbt_term_in_lang,wbt_text_in_lang
         	WHERE wbit_term_in_lang_id=wbtl_id AND wbtl_text_in_lang_id=wbxl_id AND wbxl_text_id=wbx_id
          	AND wbx_text=:name
           GROUP BY wbx_text,q";
        let results = self
            .get_conn_wbt()
            .await?
            .exec_iter(sql, params! {name})
            .await?
            .map_and_drop(from_row::<String>)
            .await?;
        Ok(results)
    }

    /// Returns a list of redirected items, with their redirect tatget.
    pub async fn get_redirected_items(
        &self,
        unique_qs: &[String],
    ) -> Result<Vec<(String, String)>> {
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!("SELECT page_title,rd_title FROM `page`,`redirect`
                WHERE `page_id`=`rd_from` AND `rd_namespace`=0 AND `page_is_redirect`=1 AND `page_namespace`=0
                AND `page_title` IN ({placeholders})");
        let results = self
            .get_conn()
            .await?
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<(String, String)>)
            .await?;
        Ok(results)
    }

    /// Returns the subset of `unique_qs` that is *not* present in the
    /// Wikidata replica's `page` table — i.e. the items the caller should
    /// treat as "deleted on Wikidata".
    ///
    /// Sanity gate (GitHub #6, Codeberg #124): when too many items appear
    /// missing the underlying query has likely been truncated by replica
    /// lag, query timeout, max_allowed_packet, or a transient replica
    /// problem. In that mode the original "absence = deletion" rule wipes
    /// live matches en masse. Instead, return `Err` so the caller's bulk
    /// UPDATE never runs. The threshold is `max(50, 10% of input)`
    /// missing — small batches keep working at full sensitivity, large
    /// batches must produce a near-complete hit list before any deletion
    /// is confirmed. Real Wikidata-item deletion volume is far below this
    /// floor on any normal day, so a legitimate result will pass.
    pub async fn get_deleted_items(&self, unique_qs: &[String]) -> Result<Vec<String>> {
        if unique_qs.is_empty() {
            return Ok(vec![]);
        }
        let placeholders = Self::sql_placeholders(unique_qs.len());
        let sql = format!(
            "SELECT page_title FROM `page` WHERE `page_namespace`=0 AND `page_title` IN ({placeholders})"
        );
        let found_items: HashSet<String> = self
            .get_conn()
            .await?
            .exec_iter(sql, unique_qs.to_vec())
            .await?
            .map_and_drop(from_row::<String>)
            .await?
            .into_iter()
            .collect();

        let missing = unique_qs.len().saturating_sub(found_items.len());
        let max_allowed_missing = std::cmp::max(50, unique_qs.len() / 10);
        if missing > max_allowed_missing {
            log::warn!(
                "get_deleted_items: {missing} of {} items missing from page table (limit: {max_allowed_missing}); refusing to declare deletions — likely replica/query issue",
                unique_qs.len()
            );
            return Err(anyhow!(
                "get_deleted_items: {missing}/{} items missing exceeds threshold ({max_allowed_missing})",
                unique_qs.len()
            ));
        }

        let not_found: Vec<String> = unique_qs
            .iter()
            .filter(|q| !found_items.contains(*q))
            .cloned()
            .collect();
        Ok(not_found)
    }

    pub async fn search_db_with_type(&self, name: &str, _type_q: &str) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        // let items = if type_q.is_empty() {
        //     self.search_without_type(name).await?
        // } else {
        //     self.search_with_type(name).await?
        // };
        self.search_without_type(name).await
    }

    /// Removes "meta items" (eg disambiguation pages) from an item list.
    /// Items are in format "Qxxx".
    pub async fn remove_meta_items(&self, items: &mut Vec<String>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        items.sort();
        items.dedup();
        let meta_items: HashSet<String> = self.get_meta_items(items).await?.into_iter().collect();
        if !meta_items.is_empty() {
            items.retain(|item| !meta_items.contains(item));
        }
        Ok(())
    }

    // API stuff

    pub fn bot_name(&self) -> &str {
        &self.bot_name
    }

    pub fn bot_password(&self) -> &str {
        &self.bot_password
    }

    pub async fn get_mw_api(
        &self,
    ) -> Result<mediawiki::api::Api, mediawiki::media_wiki_error::MediaWikiError> {
        let builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(5));
        mediawiki::api::Api::new_from_builder(&self.api_url, builder).await
    }

    pub async fn api_log_in(&mut self) -> Result<()> {
        if self.mw_api.is_none() {
            self.mw_api = Some(self.get_mw_api().await?);
        }
        let Some(mw_api) = self.mw_api.as_mut() else {
            return Err(anyhow!("API unreachable"));
        };
        if mw_api.user().logged_in() {
            // Already logged in
            return Ok(());
        }
        mw_api
            .login(self.bot_name.to_owned(), self.bot_password.to_owned())
            .await?;
        Ok(())
    }

    // Takes an ItemEntity and tries to create a new Wikidata item.
    // Returns the new item ID, or an error.
    pub async fn create_new_wikidata_item(
        &mut self,
        item: &wikimisc::wikibase::ItemEntity,
        comment: &str,
    ) -> Result<String> {
        self.api_log_in().await?;
        let mw_api = self
            .mw_api
            .as_mut()
            .ok_or_else(|| anyhow!("Failed to get mutable reference to MW API"))?;
        let json = item.to_json();
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("action".to_string(), "wbeditentity".to_string());
        params.insert("new".to_string(), "item".to_string());
        params.insert("data".to_string(), json.to_string());
        params.insert("token".to_string(), mw_api.get_edit_token().await?);
        params.insert("summary".to_string(), comment.to_string());
        let result = mw_api.post_query_api_json_mut(&params).await?;
        let new_id = result["entity"]["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Could not create new item"))?
            .to_string();
        Ok(new_id)
    }

    pub async fn perform_ac2wd(&mut self, q: &str) -> Result<String> {
        let url = format!("https://ac2wd.toolforge.org/extend/{q}");
        let new_data = wikimisc::wikidata::Wikidata::new()
            .reqwest_client()?
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;

        self.api_log_in().await?;
        let comment = "Import of Authority Control data via https://ac2wd.toolforge.org";
        let mw_api = self
            .mw_api
            .as_mut()
            .ok_or_else(|| anyhow!("Failed to get mutable reference to MW API"))?;
        let mut params: HashMap<String, String> = HashMap::new();
        params.insert("action".to_string(), "wbeditentity".to_string());
        params.insert("id".to_string(), q.to_string());
        params.insert("data".to_string(), new_data.to_string());
        params.insert("token".to_string(), mw_api.get_edit_token().await?);
        params.insert("summary".to_string(), comment.to_string());
        let result = mw_api.post_query_api_json_mut(&params).await?;
        let new_id = result["entity"]["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Could not create new item"))?
            .to_string();
        Ok(new_id)
    }

    /// Re-fetch `q` and collapse any duplicate claims that share the same
    /// property, main value, and qualifiers into a single statement, merging
    /// their references and dropping self-referential ref snaks. Intended to
    /// run right after `perform_ac2wd`, which lets duplicates slip in
    /// because `wbeditentity` does not deduplicate when merging
    /// caller-supplied claim arrays into existing items.
    pub async fn merge_duplicate_claims(&mut self, q: &str) -> Result<()> {
        self.api_log_in().await?;
        let mw_api = self
            .mw_api
            .as_mut()
            .ok_or_else(|| anyhow!("Failed to get mutable reference to MW API"))?;
        crate::claim_dedup::merge_duplicate_claims_on_wikidata(
            mw_api,
            q,
            "Mix'n'match: merging duplicate statements after item creation",
        )
        .await
    }

    /// Performs a Wikidata API search for the query string. Returns item IDs matching the query.
    pub async fn search_api(&self, query: &str) -> Result<Vec<String>> {
        self.search_with_limit(query, None).await
    }

    pub async fn search_with_limit(
        &self,
        query: &str,
        srlimit: Option<usize>,
    ) -> Result<Vec<String>> {
        Self::search_with_limit_against(WIKIDATA_API_URL, query, srlimit).await
    }

    /// Test seam for [`Self::search_with_limit`]: lets a unit test point the
    /// search call at a `wiremock::MockServer` instead of the live Wikidata
    /// API. Production callers go through `search_with_limit`, which fixes
    /// `base_url` to [`WIKIDATA_API_URL`].
    pub(crate) async fn search_with_limit_against(
        base_url: &str,
        query: &str,
        srlimit: Option<usize>,
    ) -> Result<Vec<String>> {
        if query.is_empty() {
            return Ok(vec![]);
        }
        let ret = Self::search_with_limit_run_query(base_url, query, srlimit)
            .await?
            .iter()
            .filter_map(|result| {
                let result = result.as_object()?;
                let result = result.get("title")?;
                let result = result.as_str()?;
                Some(result.to_string())
            })
            .collect();
        Ok(ret)
    }

    pub async fn set_wikipage_text(
        &mut self,
        title: &str,
        wikitext: &str,
        summary: &str,
    ) -> Result<()> {
        self.api_log_in().await?;
        if let Some(mw_api) = self.mw_api.as_mut() {
            let mut params: HashMap<String, String> = HashMap::new();
            params.insert("action".to_string(), "edit".to_string());
            params.insert("title".to_string(), title.to_string());
            params.insert("summary".to_string(), summary.to_string());
            params.insert("text".to_string(), wikitext.to_string());
            params.insert("token".to_string(), mw_api.get_edit_token().await?);
            if mw_api.post_query_api_json_mut(&params).await.is_err() {
                error!("set_wikipage_text failed for [[{}]]", &title);
            }
        }
        Ok(())
    }

    pub async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()> {
        if Self::testing() {
            error!("SKIPPING COMMANDS {commands:?}");
            return Ok(());
        }
        if commands.is_empty() {
            return Ok(());
        }
        let mut item2commands: HashMap<usize, Vec<WikidataCommand>> = HashMap::new();
        for (key, group) in &commands.into_iter().chunk_by(|command| command.item_id) {
            item2commands.insert(key, group.collect());
        }

        self.api_log_in().await?;
        for (item_id, subcommands) in &item2commands {
            self.execute_item_command(subcommands, item_id).await?;
        }

        Ok(())
    }

    async fn execute_item_command(
        &mut self,
        commands: &[WikidataCommand],
        item_id: &usize,
    ) -> Result<()> {
        let mut comments: HashSet<String> = HashSet::new();
        let mut json = json!({});
        for command in commands {
            if let Some(comment) = &command.comment {
                comments.insert(comment.to_owned());
            }
            command.edit_entity(&mut json);
        }
        let comment: String = comments.iter().join(";");
        if let Some(mw_api) = self.mw_api.as_mut() {
            let mut params: HashMap<String, String> = HashMap::new();
            params.insert("action".to_string(), "wbeditentity".to_string());
            params.insert("id".to_string(), format!("Q{item_id}"));
            params.insert("data".to_string(), json.to_string());
            params.insert("token".to_string(), mw_api.get_edit_token().await?);
            if !comment.is_empty() {
                params.insert("summary".to_string(), comment);
            }
            if mw_api.post_query_api_json_mut(&params).await.is_err() {
                error!("wbeditentiry failed for Q{item_id}: {commands:?}");
            }
        }
        Ok(())
    }

    /// Runs a Wikidata API text search, specifying a P31 value `type_q`.
    /// This value can be blank, in which case a normal search is performed.
    /// "Scholarly article" items are excluded from results, unless specifically asked for with Q13442814
    /// Common "meta items" such as disambiguation items are excluded as well
    pub async fn search_with_type_api(&self, name: &str, type_q: &str) -> Result<Vec<String>> {
        Self::search_with_type_against(WIKIDATA_API_URL, name, type_q).await
    }

    /// Test seam for [`Self::search_with_type_api`]. Production fixes
    /// `base_url` to [`WIKIDATA_API_URL`]; tests inject a `wiremock`
    /// server URI to stay hermetic.
    pub(crate) async fn search_with_type_against(
        base_url: &str,
        name: &str,
        type_q: &str,
    ) -> Result<Vec<String>> {
        if name.is_empty() {
            return Ok(vec![]);
        }
        if type_q.is_empty() {
            return Self::search_with_limit_against(base_url, name, None).await;
        }
        let mut query = format!("{name} haswbstatement:P31={type_q}");
        if type_q != "Q13442814" {
            // Exclude "scholarly article"
            query = format!("{query} -haswbstatement:P31=Q13442814");
        }
        let meta_items: Vec<String> = META_ITEMS
            .iter()
            .map(|q| format!(" -haswbstatement:P31={q}"))
            .collect();
        query += &meta_items.join("");
        Self::search_with_limit_against(base_url, &query, None).await
    }

    /// Queries SPARQL and returns a filename with the result as CSV.
    pub async fn load_sparql_csv(&self, sparql: &str) -> Result<csv::Reader<File>> {
        wikimisc::wikidata::Wikidata::new()
            .load_sparql_csv(sparql)
            .await
    }

    async fn search_with_limit_run_query(
        base_url: &str,
        query: &str,
        srlimit: Option<usize>,
    ) -> Result<Vec<Value>> {
        // TODO via mw_api?
        let query = encode(query);
        let srlimit = srlimit.unwrap_or(10);
        let url = format!(
            "{base_url}?action=query&list=search&format=json&srsearch={query}&srlimit={srlimit}"
        );
        let v = wikimisc::wikidata::Wikidata::new()
            .reqwest_client()?
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        let v = v
            .as_object()
            .ok_or(anyhow!("bad result"))?
            .get("query")
            .ok_or(anyhow!("no key 'query'"))?
            .as_object()
            .ok_or(anyhow!("not an object"))?
            .get("search")
            .ok_or(anyhow!("no key 'search'"))?
            .as_array()
            .ok_or(anyhow!("not an array"))?;
        Ok(v.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;

    #[test]
    fn test_sql_placeholders() {
        assert_eq!(Wikidata::sql_placeholders(0), "".to_string());
        assert_eq!(Wikidata::sql_placeholders(1), "?".to_string());
        assert_eq!(Wikidata::sql_placeholders(3), "?,?,?".to_string());
    }

    /// Live MediaWiki login smoke test. Mocking the full
    /// `mediawiki::api::Api` token+credentials dance with `wiremock` is
    /// fragile and offers no protection against the real failure mode this
    /// test guards against (bad credentials in `config.json`). Mark
    /// `#[ignore]` so it joins the rest of the live-services tests behind
    /// `cargo test -- --ignored`.
    #[tokio::test]
    #[ignore = "requires database / external services — run with `cargo test -- --ignored`"]
    async fn test_api_log_in() {
        let app = crate::app_state::get_test_app();
        let mut wd = app.wikidata().to_owned();
        wd.api_log_in().await.unwrap();
        let api = wd.mw_api.as_ref().unwrap();
        assert!(api.user().logged_in());
    }

    /// Hermetic version of the search test: a `wiremock` server stands in
    /// for `https://www.wikidata.org/w/api.php`. Asserts:
    ///
    /// * Empty input short-circuits to an empty `Vec` without hitting the API
    ///   (verified by the absence of any registered mock — wiremock would
    ///   return 404 for an unmatched request, which would bubble up as an
    ///   error and fail the test).
    /// * A normal `srsearch=...` query returns the `query.search[].title`
    ///   field as a `Vec<String>`.
    /// * The typed-search wrapper layers in the `-haswbstatement:P31=...`
    ///   meta-item exclusions and parses the same response shape.
    #[tokio::test]
    async fn test_wd_search() {
        use std::path::PathBuf;
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn fixture(name: &str) -> String {
            let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            p.push("test_data");
            p.push("wikidata");
            p.push(name);
            std::fs::read_to_string(&p)
                .unwrap_or_else(|e| panic!("missing fixture {}: {e}", p.display()))
        }

        let server = MockServer::start().await;

        // Plain search: srsearch="Magnus Manske haswbstatement:P31=Q5"
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("action", "query"))
            .and(query_param("list", "search"))
            .and(query_param(
                "srsearch",
                "Magnus Manske haswbstatement:P31=Q5",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(fixture("search_magnus_manske_p31_q5.json"))
                    .insert_header("content-type", "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;

        // Typed search adds " -haswbstatement:P31=Q13442814" plus six META_ITEMS
        // exclusions. Hard-code the exact srsearch the production code builds so
        // the test catches accidental re-ordering of those exclusions.
        let typed_query = "Magnus Manske haswbstatement:P31=Q5 \
             -haswbstatement:P31=Q13442814 \
             -haswbstatement:P31=Q4167410 \
             -haswbstatement:P31=Q11266439 \
             -haswbstatement:P31=Q4167836 \
             -haswbstatement:P31=Q13406463 \
             -haswbstatement:P31=Q22808320 \
             -haswbstatement:P31=Q17362920"
            .replace("             ", "");
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("action", "query"))
            .and(query_param("list", "search"))
            .and(query_param("srsearch", typed_query.as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(fixture("search_magnus_manske_q5_typed.json"))
                    .insert_header("content-type", "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let base = server.uri();

        // Empty input must NOT hit the network — production short-circuits.
        assert!(
            Wikidata::search_with_limit_against(&base, "", None)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            Wikidata::search_with_limit_against(
                &base,
                "Magnus Manske haswbstatement:P31=Q5",
                None,
            )
            .await
            .unwrap(),
            vec!["Q13520818".to_string()]
        );
        assert_eq!(
            Wikidata::search_with_type_against(&base, "Magnus Manske", "Q5")
                .await
                .unwrap(),
            vec!["Q13520818".to_string()]
        );
        // `expect(1)` on each mock guarantees both production code paths
        // were exercised exactly once and that the empty-string call did
        // NOT touch the wire.
    }

    #[tokio::test]
    async fn test_get_deleted_items_empty_input() {
        // Empty input short-circuits to an empty result (and skips the SQL,
        // which would build the syntactically-invalid `IN ()` if reached).
        let app = test_support::test_app().await;
        let out = app.wikidata().get_deleted_items(&[]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn test_get_deleted_items_bails_on_implausible_result() {
        // Regression for GitHub #6: when the page-table query returns far
        // fewer hits than expected (the failure mode that caused the
        // mass-unmatch incidents), `get_deleted_items` must return Err
        // rather than declare every "missing" Q as deleted. We feed it 100
        // QIDs in a range no other test seeds; the page-table will return
        // ~zero hits, which exceeds the 10%-or-50-items missing threshold.
        let app = test_support::test_app().await;
        let qs: Vec<String> = (9_990_000_001..=9_990_000_100_u64)
            .map(|n| format!("Q{n}"))
            .collect();
        let result = app.wikidata().get_deleted_items(&qs).await;
        assert!(result.is_err(), "must bail on implausibly short result");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("missing") && msg.contains("threshold"),
            "error should mention missing items and threshold; got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_remove_meta_items() {
        // get_meta_items now goes through the live wbgetentities API and
        // checks P31 directly, so the test mocks the API rather than
        // seeding the replica's pagelinks (which is what the old SQL
        // implementation read). Q3522 is set up with P31=Q4167410 (a
        // meta class), Q1 and Q2 with P31=Q188451 (a non-meta class).
        use wiremock::matchers::{method, query_param_contains};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        const SITEINFO_JSON: &str = include_str!("../test_data/wikidata/siteinfo.json");
        const META_FIXTURE: &str =
            include_str!("../test_data/wikidata/wbgetentities_meta_items.json");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SITEINFO_JSON))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(query_param_contains("action", "wbgetentities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(META_FIXTURE))
            .mount(&server)
            .await;
        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let mut items: Vec<String> = ["Q1", "Q3522", "Q2"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        app.wikidata().remove_meta_items(&mut items).await.unwrap();
        assert_eq!(items, ["Q1", "Q2"]);
    }

    #[tokio::test]
    async fn test_get_meta_items_empty_input() {
        // Empty input must short-circuit without hitting the API.
        let app = test_support::test_app().await;
        let out = app.wikidata().get_meta_items(&[]).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn test_get_meta_items_does_not_flag_non_meta_p31() {
        // Regression for GitHub #6: a real item whose P31 is *not* one of
        // the meta classes must not be flagged, even if its claims happen
        // to mention a meta-class Q-id elsewhere. Under the old SQL the
        // pagelinks check could surface such items as "meta"; the API
        // path checks P31 directly so they pass through cleanly.
        use wiremock::matchers::{method, query_param_contains};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        const SITEINFO_JSON: &str = include_str!("../test_data/wikidata/siteinfo.json");
        // Q13520818 has P31=Q5 (human) — not a meta class.
        const Q13520818_JSON: &str = include_str!("../test_data/wikidata/Q13520818.json");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(query_param_contains("action", "query"))
            .respond_with(ResponseTemplate::new(200).set_body_string(SITEINFO_JSON))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(query_param_contains("action", "wbgetentities"))
            .respond_with(ResponseTemplate::new(200).set_body_string(Q13520818_JSON))
            .mount(&server)
            .await;
        let api_url = format!("{}/w/api.php", server.uri());
        let app = test_support::test_app_with_wikidata_api_url(&api_url).await;
        let qs = vec!["Q13520818".to_string()];
        let meta = app.wikidata().get_meta_items(&qs).await.unwrap();
        assert!(meta.is_empty(), "non-meta item must not be flagged");
    }

    #[tokio::test]
    async fn test_search_db_with_type() {
        test_support::seed_wbt_label(13520818, "Magnus Manske")
            .await
            .unwrap();
        let app = test_support::test_app().await;
        let result = app
            .wdt()
            .search_db_with_type("Magnus Manske", "Q5")
            .await
            .unwrap();
        assert!(
            result.contains(&"Q13520818".to_string()),
            "expected Q13520818 in {:?}",
            result
        );
    }
}
