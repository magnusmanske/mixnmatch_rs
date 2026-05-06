//! Test harness for tests that need a real MySQL-compatible database.
//!
//! Boots one MariaDB container per test process via `testcontainers`,
//! preloaded with the production schema dumped to
//! `tests/fixtures/schema.sql`. Each test gets its own catalog id and
//! can seed its own entries, so tests run in parallel without
//! serialising on a shared mutex (no `TEST_MUTEX`, no shared
//! `TEST_ENTRY_ID`).
//!
//! Compared with `app_state::get_test_app()` this:
//!   - does not need `config.json`,
//!   - does not need an SSH tunnel to Toolforge,
//!   - never touches production data.
//!
//! Requires a running Docker daemon. The first call in a process pays
//! for container boot + schema load (~3–4 s, image is `mariadb:11.3`
//! which is cached after the first pull). Subsequent calls return the
//! same handle without additional overhead.
//!
//! Wikidata / WDRC pools in the returned `AppState` are wired to dummy
//! URLs. `mysql_async::Pool` is lazy, so unused pools never connect;
//! tests that exercise those code paths must mock them at the call site.

use crate::app_state::AppState;
use anyhow::Result;
use mysql_async::prelude::*;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use testcontainers::{ContainerAsync, core::IntoContainerPort, runners::AsyncRunner};
use testcontainers_modules::mariadb::Mariadb;
use tokio::sync::OnceCell;

const SCHEMA_SQL: &str = include_str!("../test_data/schema.sql");

struct TestDb {
    url: String,
    /// Held to keep the container alive for the duration of the
    /// process. Cleanup is automatic on drop.
    _container: ContainerAsync<Mariadb>,
}

static TEST_DB: OnceCell<TestDb> = OnceCell::const_new();

async fn start_container() -> Result<TestDb> {
    let container = Mariadb::default()
        .with_init_sql(SCHEMA_SQL.as_bytes().to_vec())
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let url = format!("mysql://root@{host}:{port}/test");
    Ok(TestDb {
        url,
        _container: container,
    })
}

async fn db() -> &'static TestDb {
    TEST_DB
        .get_or_init(|| async {
            start_container()
                .await
                .expect("failed to start MariaDB test container — is Docker running?")
        })
        .await
}

/// Create a short-lived pool on the caller's Tokio runtime.
/// Each seed call gets its own pool so we never share pool state
/// across test runtimes (each #[tokio::test] spawns its own runtime,
/// and mysql_async pool internals are bound to the runtime they were
/// created on — sharing them across runtimes causes "Pool was
/// disconnected" failures).
async fn seed_conn() -> Result<(mysql_async::Pool, mysql_async::Conn)> {
    let url = db().await.url.as_str().to_owned();
    let pool = mysql_async::Pool::new(url.as_str());
    let conn = pool.get_conn().await?;
    Ok((pool, conn))
}

fn dummy_pool_config() -> serde_json::Value {
    json!({ "url": "mysql://x:x@127.0.0.1:65535/x", "min_connections": 0, "max_connections": 1, "keep_sec": 1 })
}

/// Build an `AppState` wired to the per-process MariaDB container.
/// `import_file_path` defaults to `/tmp/mnm_test_imports`.
pub async fn test_app() -> AppState {
    test_app_with_import_path("/tmp/mnm_test_imports").await
}

/// Like [`test_app`] but with a custom `import_file_path`.
/// Use this for tests that read files via `DataSource`.
pub async fn test_app_with_import_path(import_file_path: &str) -> AppState {
    let url = &db().await.url;
    let dummy = dummy_pool_config();
    let cfg = json!({
        "wikidata": { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "wdt":      { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "wdrc":     dummy,
        "mixnmatch":    { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "mixnmatch_ro": { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "bot_name": "test_bot",
        "bot_password": "test_password",
        "import_file_path": import_file_path,
        "task_specific_usize": {},
        "max_concurrent_jobs": 1,
    });
    AppState::from_config(&cfg).expect("AppState::from_config failed for test container")
}

/// Like [`test_app`] but with a custom Wikidata MediaWiki API URL.
/// Use this for tests that exercise HTTP API calls via wiremock.
pub async fn test_app_with_wikidata_api_url(api_url: &str) -> AppState {
    let url = &db().await.url;
    let dummy = dummy_pool_config();
    let cfg = json!({
        "wikidata": { "url": url, "api_url": api_url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "wdt":      { "url": url, "api_url": api_url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "wdrc":     dummy,
        "mixnmatch":    { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "mixnmatch_ro": { "url": url, "min_connections": 0, "max_connections": 4, "keep_sec": 2 },
        "bot_name": "test_bot",
        "bot_password": "test_password",
        "import_file_path": "/tmp/mnm_test_imports",
        "task_specific_usize": {},
        "max_concurrent_jobs": 1,
    });
    AppState::from_config(&cfg).expect("AppState::from_config failed for test container")
}

/// A catalog id no other test in this process is using.
///
/// Starts at 1_000_000 to stay clear of any small ids a test may
/// hard-code or insert directly.
pub fn unique_catalog_id() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(1_000_000);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Insert a minimal `(catalog, entry)` row pair and return `(catalog_id, entry_id)`.
///
/// The entry's `ext_name` is `"Test Person"`.
pub async fn seed_minimal_entry(_app: &AppState) -> Result<(usize, usize)> {
    seed_entry_with_name("Test Person").await
}

/// Like [`seed_minimal_entry`] but with a custom `ext_name`.
pub async fn seed_entry_with_name(name: &str) -> Result<(usize, usize)> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();

    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run)
       VALUES (:id, :name, '', '', 'person', 'en', 1, 0, '', '', 0)"
        .with(params! {
            "id" => catalog_id,
            "name" => format!("test_catalog_{catalog_id}"),
        })
        .ignore(&mut conn)
        .await?;

    "INSERT INTO entry (catalog, ext_id, ext_url, ext_name, ext_desc, type, random) \
     VALUES (:catalog, :ext_id, '', :name, '', 'Q5', 0.5)"
        .with(params! {
            "catalog" => catalog_id,
            "ext_id"  => format!("ext_{catalog_id}"),
            "name"    => name,
        })
        .ignore(&mut conn)
        .await?;

    let entry_id: u64 = "SELECT LAST_INSERT_ID()".first(&mut conn).await?.unwrap();
    drop(conn);
    pool.disconnect().await.ok();
    Ok((catalog_id, entry_id as usize))
}

/// Insert a `(catalog, update_info)` pair and return `catalog_id`.
///
/// `json` is the raw update_info JSON stored in `update_info.json`.
pub async fn seed_catalog_with_update_info(user_id: usize, update_json: &str) -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();

    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run)
       VALUES (:id, :name, '', '', 'person', 'en', 1, :owner, '', '', 0)"
        .with(params! {
            "id"    => catalog_id,
            "name"  => format!("test_catalog_{catalog_id}"),
            "owner" => user_id,
        })
        .ignore(&mut conn)
        .await?;

    r"INSERT INTO update_info (catalog, json, note, user_id, is_current)
      VALUES (:catalog, :json, '', :user_id, 1)"
        .with(params! {
            "catalog" => catalog_id,
            "json"    => update_json,
            "user_id" => user_id,
        })
        .ignore(&mut conn)
        .await?;

    drop(conn);
    pool.disconnect().await.ok();
    Ok(catalog_id)
}

/// Insert `person_dates` for an existing entry.
///
/// `born` / `died` use the DB string format (`"1869"`, `"1869-04-03"`, etc.).
/// Pass an empty string to leave a field blank.
pub async fn seed_person_dates(entry_id: usize, born: &str, died: &str) -> Result<()> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT INTO person_dates (entry_id, born, died) VALUES (:entry_id, :born, :died)"
        .with(params! {
            "entry_id" => entry_id,
            "born"     => born,
            "died"     => died,
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Insert a `jobs` row and return `job_id`.
pub async fn seed_job(action: &str, catalog_id: usize) -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;

    r"INSERT INTO jobs (action, catalog, status, last_ts, next_ts, user_id)
      VALUES (:action, :catalog, 'TODO', '20220101000000', '', 0)"
        .with(params! {
            "action"  => action,
            "catalog" => catalog_id,
        })
        .ignore(&mut conn)
        .await?;

    let job_id: u64 = "SELECT LAST_INSERT_ID()".first(&mut conn).await?.unwrap();
    drop(conn);
    pool.disconnect().await.ok();
    Ok(job_id as usize)
}

/// Seed a wdt page row that is a redirect (page_is_redirect=1) and a matching
/// redirect row pointing to `to_q`. Used by test_fix_redirects.
/// Uses INSERT IGNORE so parallel tests seeding the same Q-numbers don't conflict.
pub async fn seed_wdt_redirect(from_q: &str, to_q: &str) -> Result<()> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT IGNORE INTO page (page_namespace, page_title, page_is_redirect) VALUES (0, :title, 1)"
        .with(params! { "title" => from_q })
        .ignore(&mut conn)
        .await?;
    let page_id: u64 = "SELECT page_id FROM page WHERE page_namespace=0 AND page_title=:title"
        .with(params! { "title" => from_q })
        .first(&mut conn)
        .await?
        .expect("page must exist after insert");
    "INSERT IGNORE INTO redirect (rd_from, rd_namespace, rd_title) VALUES (:from, 0, :to)"
        .with(params! { "from" => page_id, "to" => to_q })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Insert a catalog with `active=0` and return its `catalog_id`.
pub async fn seed_inactive_catalog() -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();
    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run)
       VALUES (:id, :name, '', '', 'person', 'en', 0, 0, '', '', 0)"
        .with(params! {
            "id" => catalog_id,
            "name" => format!("test_catalog_{catalog_id}"),
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(catalog_id)
}

/// Insert an active catalog with `wd_prop` and `source_item` set (no `wd_qual`)
/// and one minimal entry pointing at it. Returns `(catalog_id, entry_id)`.
/// Used by tests that need a catalog/entry pair where the catalog has a
/// primary external-id property — e.g. `prep_match_claim` reference building.
pub async fn seed_entry_with_catalog_wd_prop(
    wd_prop: usize,
    source_item: usize,
) -> Result<(usize, usize)> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();
    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run, wd_prop, source_item)
       VALUES (:id, :name, '', '', 'person', 'en', 1, 0, '', '', 0, :wd_prop, :source_item)"
        .with(params! {
            "id" => catalog_id,
            "name" => format!("test_catalog_{catalog_id}"),
            "wd_prop" => wd_prop,
            "source_item" => source_item,
        })
        .ignore(&mut conn)
        .await?;
    "INSERT INTO entry (catalog, ext_id, ext_url, ext_name, ext_desc, type, random) \
     VALUES (:catalog, :ext_id, '', :name, '', 'Q5', 0.5)"
        .with(params! {
            "catalog" => catalog_id,
            "ext_id"  => format!("ext_{catalog_id}"),
            "name"    => "Test Person",
        })
        .ignore(&mut conn)
        .await?;
    let entry_id: u64 = "SELECT LAST_INSERT_ID()".first(&mut conn).await?.unwrap();
    drop(conn);
    pool.disconnect().await.ok();
    Ok((catalog_id, entry_id as usize))
}

/// Insert a minimal entry into an existing catalog. Returns the new `entry_id`.
pub async fn seed_entry_in_catalog(catalog_id: usize, name: &str) -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT INTO entry (catalog, ext_id, ext_url, ext_name, ext_desc, type, random) \
     VALUES (:catalog, :ext_id, '', :name, '', 'Q5', 0.5)"
        .with(params! {
            "catalog" => catalog_id,
            "ext_id"  => format!("ext_{catalog_id}_{name}"),
            "name"    => name,
        })
        .ignore(&mut conn)
        .await?;
    let entry_id: u64 = "SELECT LAST_INSERT_ID()".first(&mut conn).await?.unwrap();
    drop(conn);
    pool.disconnect().await.ok();
    Ok(entry_id as usize)
}

/// Insert an active catalog with `wd_prop` and `wd_qual` both set; return its `catalog_id`.
pub async fn seed_catalog_with_wd_qual(wd_prop: usize, wd_qual: usize) -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();
    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run, wd_prop, wd_qual)
       VALUES (:id, :name, '', '', 'person', 'en', 1, 0, '', '', 0, :wd_prop, :wd_qual)"
        .with(params! {
            "id" => catalog_id,
            "name" => format!("test_catalog_{catalog_id}"),
            "wd_prop" => wd_prop,
            "wd_qual" => wd_qual,
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(catalog_id)
}

/// Insert a `multi_match` row for `entry_id` in `catalog`. The candidates
/// field is set to a placeholder string; the count to 2.
pub async fn seed_multi_match(entry_id: usize, catalog_id: usize) -> Result<()> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT INTO multi_match (entry_id, catalog, candidates, candidate_count) \
     VALUES (:entry_id, :catalog, 'Q1|Q2', 2)"
        .with(params! {
            "entry_id" => entry_id,
            "catalog" => catalog_id,
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Insert a `wd_matches` row for `entry_id` in `catalog`.
pub async fn seed_wd_match(entry_id: usize, catalog_id: usize) -> Result<()> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT INTO wd_matches (entry_id, catalog, status, timestamp) \
     VALUES (:entry_id, :catalog, 'UNKNOWN', '')"
        .with(params! {
            "entry_id" => entry_id,
            "catalog" => catalog_id,
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Return `true` if a `multi_match` row exists for `entry_id`.
pub async fn multi_match_entry_exists(entry_id: usize) -> Result<bool> {
    let (pool, mut conn) = seed_conn().await?;
    let count: Option<u64> =
        "SELECT COUNT(*) FROM multi_match WHERE entry_id = :entry_id"
            .with(params! { "entry_id" => entry_id })
            .first(&mut conn)
            .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(count.unwrap_or(0) > 0)
}

/// Return `true` if a `wd_matches` row exists for `entry_id`.
pub async fn wd_match_entry_exists(entry_id: usize) -> Result<bool> {
    let (pool, mut conn) = seed_conn().await?;
    let count: Option<u64> =
        "SELECT COUNT(*) FROM wd_matches WHERE entry_id = :entry_id"
            .with(params! { "entry_id" => entry_id })
            .first(&mut conn)
            .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(count.unwrap_or(0) > 0)
}

/// Seed the wbt_* chain for `item_id` with label `name`.
/// Used by test_search_db_with_type: seeds Magnus Manske → Q13520818.
pub async fn seed_wbt_label(item_id: u64, name: &str) -> Result<()> {
    let (pool, mut conn) = seed_conn().await?;
    "INSERT IGNORE INTO wbt_text (wbx_text) VALUES (:name)"
        .with(params! { "name" => name })
        .ignore(&mut conn)
        .await?;
    let wbx_id: u64 = "SELECT wbx_id FROM wbt_text WHERE wbx_text=:name"
        .with(params! { "name" => name })
        .first(&mut conn)
        .await?
        .expect("wbx_id must exist after insert");
    "INSERT IGNORE INTO wbt_text_in_lang (wbxl_language, wbxl_text_id) VALUES ('en', :id)"
        .with(params! { "id" => wbx_id })
        .ignore(&mut conn)
        .await?;
    let text_in_lang_id: u64 =
        "SELECT wbxl_id FROM wbt_text_in_lang WHERE wbxl_text_id=:id AND wbxl_language='en'"
            .with(params! { "id" => wbx_id })
            .first(&mut conn)
            .await?
            .expect("wbxl_id must exist");
    "INSERT IGNORE INTO wbt_term_in_lang (wbtl_text_in_lang_id) VALUES (:id)"
        .with(params! { "id" => text_in_lang_id })
        .ignore(&mut conn)
        .await?;
    let wbtl_id: u64 =
        "SELECT wbtl_id FROM wbt_term_in_lang WHERE wbtl_text_in_lang_id=:id"
            .with(params! { "id" => text_in_lang_id })
            .first(&mut conn)
            .await?
            .expect("wbtl_id must exist");
    "INSERT IGNORE INTO wbt_item_terms (wbit_item_id, wbit_term_in_lang_id) VALUES (:item, :term)"
        .with(params! { "item" => item_id, "term" => wbtl_id })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(())
}

/// Insert a catalog with a specific URL; return its `catalog_id`.
pub async fn seed_catalog_with_url(url: &str) -> Result<usize> {
    let (pool, mut conn) = seed_conn().await?;
    let catalog_id = unique_catalog_id();
    r"INSERT INTO catalog
       (id, name, url, `desc`, type, search_wp, active, owner, note, has_person_date, taxon_run)
       VALUES (:id, :name, :url, '', 'person', 'en', 1, 0, '', '', 0)"
        .with(params! {
            "id"   => catalog_id,
            "name" => format!("test_catalog_{catalog_id}"),
            "url"  => url,
        })
        .ignore(&mut conn)
        .await?;
    drop(conn);
    pool.disconnect().await.ok();
    Ok(catalog_id)
}
