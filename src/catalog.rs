use crate::app_state::{AppContext, ExternalServicesContext};
use crate::entry::EntryWriter;
use crate::auxiliary_data::AuxiliaryRow;
use crate::job::Job;
use crate::util::wikidata_props as wp;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use wikimisc::wikibase::Reference;
use wikimisc::wikibase::Snak;

pub type CatalogId = Option<usize>;

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    id: CatalogId,
    name: Option<String>,
    url: Option<String>,
    desc: String,
    type_name: String,
    wd_prop: Option<usize>,
    wd_qual: Option<usize>,
    search_wp: String,
    active: bool,
    owner: usize,
    note: String,
    source_item: Option<usize>,
    has_person_date: String,
    taxon_run: bool,
}

impl Catalog {
    /// Returns a Catalog object for a given ID.
    pub async fn from_id(catalog_id: usize, app: &dyn ExternalServicesContext) -> Result<Self> {
        app.storage().get_catalog_from_id(catalog_id).await
    }

    /// Returns a Catalog object for a given name.
    pub async fn from_name(name: &str, app: &dyn ExternalServicesContext) -> Result<Self> {
        app.storage().get_catalog_from_name(name).await
    }

    pub fn from_mysql_row(row: &mysql_async::Row) -> Option<Self> {
        Some(Self {
            id: row.get("id")?,
            name: row.get("name")?,
            url: row.get("url")?,
            desc: row.get("desc")?,
            type_name: row.get("type")?,
            wd_prop: row.get("wd_prop")?,
            wd_qual: row.get("wd_qual")?,
            search_wp: row.get("search_wp")?,
            active: row.get("active")?,
            owner: row.get("owner")?,
            note: row.get("note")?,
            source_item: row.get("source_item")?,
            has_person_date: row.get("has_person_date")?,
            taxon_run: row.get("taxon_run")?,
        })
    }

    pub async fn create_catalog(&mut self, app: &dyn ExternalServicesContext) -> Result<()> {
        self.id = Some(app.storage().create_catalog(self).await?);
        Ok(())
    }

    pub fn id(&self) -> CatalogId {
        self.id
    }

    pub fn get_valid_id(&self) -> Result<usize> {
        match self.id {
            Some(id) => Ok(id),
            None => Err(anyhow!("No catalog ID set")),
        }
    }

    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub fn set_name(&mut self, name: Option<String>) {
        self.name = name;
    }

    pub fn url(&self) -> Option<&String> {
        self.url.as_ref()
    }

    pub fn set_url(&mut self, url: Option<String>) {
        self.url = url;
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn wd_prop(&self) -> Option<usize> {
        self.wd_prop
    }

    pub fn set_wd_prop(&mut self, wd_prop: Option<usize>) {
        self.wd_prop = wd_prop;
    }

    pub fn wd_qual(&self) -> Option<usize> {
        self.wd_qual
    }

    pub fn search_wp(&self) -> &str {
        &self.search_wp
    }

    pub fn set_search_wp(&mut self, search_wp: &str) {
        self.search_wp = search_wp.to_string();
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    pub fn set_owner(&mut self, owner: usize) {
        self.owner = owner;
    }

    pub fn owner(&self) -> usize {
        self.owner
    }

    pub fn set_note(&mut self, note: &str) {
        self.note = note.to_string();
    }

    pub fn note(&self) -> &str {
        &self.note
    }

    pub fn source_item(&self) -> Option<usize> {
        self.source_item
    }

    pub fn has_person_date(&self) -> &str {
        &self.has_person_date
    }

    pub fn taxon_run(&self) -> bool {
        self.taxon_run
    }

    pub async fn delete(&mut self, app: &dyn ExternalServicesContext) -> Result<()> {
        app.storage()
            .delete_catalog(self.get_valid_id()?)
            .await?;
        self.id = None;
        Ok(())
    }

    /// Delete all entries (and their associated rows) from the catalog, but
    /// leave the `catalog` and `autoscrape` rows intact.
    pub async fn empty(&self, app: &dyn ExternalServicesContext) -> Result<()> {
        app.storage()
            .empty_catalog(self.get_valid_id()?)
            .await
    }

    /// Returns a `HashMap` of key-value pairs for the catalog.
    pub async fn get_key_value_pairs(&self, app: &dyn ExternalServicesContext) -> Result<HashMap<String, String>> {
        app.storage()
            .get_catalog_key_value_pairs(self.get_valid_id()?)
            .await
    }

    //TODO test
    pub async fn refresh_overview_table(&self, app: &dyn ExternalServicesContext) -> Result<()> {
        app.storage()
            .catalog_refresh_overview_table(self.get_valid_id()?)
            .await
    }

    /// Pull every `(item, value)` pair where the item carries the
    /// given Wikidata property, then auto-match any catalog entry
    /// whose `ext_id` equals one of those values *and* hasn't been
    /// manually matched yet. Returns the number of new matches set.
    ///
    /// Mirrors PHP `Catalog::syncFromSPARQL`. Used by the property-
    /// migration pipeline (`CatalogMerger::migrate_property`) to
    /// freshen the old catalog's matches against live Wikidata before
    /// porting them to a successor catalog. Attribution uses
    /// `USER_DATE_MATCH` (id 3) for parity with the PHP user — keeps
    /// the post-migration audit trail consistent with rows that have
    /// been around since the PHP era.
    pub async fn sync_from_sparql(&self, app: &dyn AppContext, property: usize) -> Result<usize> {
        if property == 0 {
            return Ok(0);
        }
        let id = self.get_valid_id()?;

        let already_matched = app
            .storage()
            .catalog_get_manually_matched_ext_ids(id)
            .await?;

        let client = crate::wdqs::build_client()?;
        let sparql = format!("SELECT ?q ?v {{ ?q wdt:P{property} ?v }}");
        let rows = crate::wdqs::run_tsv_query(&client, &sparql).await?;

        let mut count = 0_usize;
        for row in rows {
            let Some(q_uri) = row.first() else { continue };
            let value = row.get(1).map(|s| s.trim().to_string()).unwrap_or_default();
            if value.is_empty() || already_matched.contains(&value) {
                continue;
            }
            let Some(q_num) = crate::wdqs::entity_id_from_uri(q_uri, 'Q') else {
                continue;
            };
            // The entry must already exist in the catalog with this
            // ext_id — sync_from_sparql does not invent rows. Failure
            // is non-fatal: the Wikidata side might have catalogued an
            // ext_id we never imported.
            let mut entry = match crate::entry::Entry::from_ext_id(id, &value, app).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            let q_str = format!("Q{q_num}");
            if EntryWriter::new(app, &mut entry)
                .set_match(&q_str, crate::app_state::USER_DATE_MATCH)
                .await
                .is_ok()
            {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn references(&self, app: &dyn ExternalServicesContext, entry: &crate::entry::Entry) -> Vec<Reference> {
        let mut snaks = vec![];
        if let Some(source_item) = self.source_item {
            let value = format!("Q{source_item}");
            let snak = Snak::new_item(wp::P_STATED_IN, &value);
            snaks.push(snak);
        }
        match (self.wd_prop, self.wd_qual) {
            (Some(prop), None) => {
                let prop = format!("P{prop}");
                let value = AuxiliaryRow::fix_external_id(&prop, &entry.ext_id);
                let snak = Snak::new_external_id(&prop, &value);
                snaks.push(snak);
            }
            _ => {
                if !entry.ext_url.is_empty() {
                    let url = sanitize_reference_url(&entry.ext_url);
                    let snak = Snak::new_string(wp::P_REFERENCE_URL, &url);
                    snaks.push(snak);
                }
            }
        }

        if let Some(entry_id) = entry.id {
            if let Some(ts) = app.storage().entry_get_creation_time(entry_id).await {
                if let Some(date) = ts.split(' ').next() {
                    let time = format!("+{date}T00:00:00Z");
                    let snak = Snak::new_time(wp::P_RETRIEVED, &time, 11);
                    snaks.push(snak);
                }
            }
        }
        if snaks.is_empty() {
            return vec![];
        }
        let reference = Reference::new(snaks);
        vec![reference]
    }

    // TODO test
    pub async fn set_taxon_run(&mut self, app: &dyn ExternalServicesContext, new_taxon_run: bool) -> Result<()> {
        if self.taxon_run != new_taxon_run {
            app.storage()
                .set_catalog_taxon_run(self.get_valid_id()?, new_taxon_run)
                .await?;
            self.taxon_run = new_taxon_run;
        }
        Ok(())
    }

    /// Changes the `has_person_date` field of a catalog, in both struct and database.
    ///
    /// # Returns
    ///
    /// * `Result<bool>` - A result indicating whether the `has_person_date` field was changed to "yes".
    pub async fn check_and_set_person_date(&mut self, app: &dyn ExternalServicesContext) -> Result<bool> {
        let has_new_dates = if self.has_person_date != "yes"
            && app
                .storage()
                .do_catalog_entries_have_person_date(self.get_valid_id()?)
                .await?
        {
            self.set_has_person_date(app, "yes").await?;
            true
        } else {
            false
        };
        Ok(has_new_dates)
    }

    pub async fn set_has_person_date(
        &mut self,
        app: &dyn ExternalServicesContext,
        new_has_person_date: &str,
    ) -> Result<()> {
        app.storage()
            .set_has_person_date(self.get_valid_id()?, new_has_person_date)
            .await?;
        self.has_person_date = new_has_person_date.to_string();
        Ok(())
    }

    pub async fn number_of_entries(&self, app: &dyn ExternalServicesContext) -> Result<usize> {
        app.storage()
            .number_of_entries_in_catalog(self.get_valid_id()?)
            .await
    }

    /// Queue a `microsync` job for this catalog if it has a `wd_prop`.
    /// Called at the successful end of import / autoscrape / bespoke scraper.
    /// If a `(catalog, "microsync")` row already exists the underlying upsert
    /// resets its status back to `TODO`.
    pub async fn queue_microsync_if_applicable(
        &self,
        app: &dyn ExternalServicesContext,
    ) -> Result<()> {
        if self.wd_prop.is_none() {
            return Ok(());
        }
        let catalog_id = self.get_valid_id()?;
        Job::queue_simple_job(app, catalog_id, "microsync", None).await?;
        Ok(())
    }
}

/// Percent-encode characters Wikidata's URL validator rejects (spaces,
/// square brackets, braces, pipe, backslash, angle brackets, double quote,
/// caret, backtick, control chars). Other characters — including already
/// percent-encoded sequences — pass through untouched, so this is
/// idempotent on well-formed URLs.
fn sanitize_reference_url(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    for c in url.chars() {
        match c {
            ' ' => out.push_str("%20"),
            '"' => out.push_str("%22"),
            '<' => out.push_str("%3C"),
            '>' => out.push_str("%3E"),
            '[' => out.push_str("%5B"),
            '\\' => out.push_str("%5C"),
            ']' => out.push_str("%5D"),
            '^' => out.push_str("%5E"),
            '`' => out.push_str("%60"),
            '{' => out.push_str("%7B"),
            '|' => out.push_str("%7C"),
            '}' => out.push_str("%7D"),
            c if (c as u32) < 0x20 || c == '\x7f' => {
                out.push_str(&format!("%{:02X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use mysql_async::prelude::*;

    #[test]
    fn sanitize_reference_url_encodes_space() {
        let raw = "http://scan-bugs.org/portal/taxa/index.php?taxon=Oecophylla longiceps";
        assert_eq!(
            sanitize_reference_url(raw),
            "http://scan-bugs.org/portal/taxa/index.php?taxon=Oecophylla%20longiceps"
        );
    }

    #[test]
    fn sanitize_reference_url_encodes_brackets_and_pipes() {
        assert_eq!(sanitize_reference_url("http://x/a[1]"), "http://x/a%5B1%5D");
        assert_eq!(sanitize_reference_url("http://x/a|b"), "http://x/a%7Cb");
        assert_eq!(sanitize_reference_url("http://x/{q}"), "http://x/%7Bq%7D");
    }

    #[test]
    fn sanitize_reference_url_is_idempotent_on_clean_urls() {
        let clean = "https://example.com/foo/bar?baz=qux&n=1#frag";
        assert_eq!(sanitize_reference_url(clean), clean);
    }

    #[test]
    fn sanitize_reference_url_preserves_percent_encoded_sequences() {
        let already = "https://example.com/a%20b";
        assert_eq!(sanitize_reference_url(already), already);
    }

    #[test]
    fn sanitize_reference_url_encodes_control_chars() {
        assert_eq!(sanitize_reference_url("http://x/a\tb"), "http://x/a%09b");
        assert_eq!(sanitize_reference_url("http://x/a\nb"), "http://x/a%0Ab");
    }

    async fn fetch_microsync_status(catalog_id: usize) -> Option<String> {
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        let row: Option<(String,)> = conn
            .exec_first(
                "SELECT `status` FROM `jobs` WHERE `catalog`=:catalog AND `action`='microsync'",
                params! { "catalog" => catalog_id },
            )
            .await
            .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        row.map(|(s,)| s)
    }

    #[tokio::test]
    async fn test_catalog_from_id() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        assert_eq!(catalog.name.unwrap(), format!("test_catalog_{catalog_id}"));
    }

    #[tokio::test]
    async fn queue_microsync_skips_when_no_wd_prop() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        assert!(catalog.wd_prop().is_none());

        catalog.queue_microsync_if_applicable(&app).await.unwrap();

        assert_eq!(
            fetch_microsync_status(catalog_id).await,
            None,
            "no microsync job should be queued for a catalog without wd_prop"
        );
    }

    #[tokio::test]
    async fn queue_microsync_inserts_when_wd_prop_set() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_entry_with_catalog_wd_prop(31, 5).await.unwrap();
        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        assert_eq!(catalog.wd_prop(), Some(31));

        catalog.queue_microsync_if_applicable(&app).await.unwrap();

        assert_eq!(
            fetch_microsync_status(catalog_id).await.as_deref(),
            Some("TODO"),
        );
    }

    #[tokio::test]
    async fn queue_microsync_resets_existing_to_todo() {
        let app = test_support::test_app().await;
        let (catalog_id, _) = test_support::seed_entry_with_catalog_wd_prop(31, 5).await.unwrap();

        // Pre-seed an existing microsync row, then mark it DONE so we can
        // verify the helper resets it to TODO (ON DUPLICATE KEY UPDATE path).
        test_support::seed_job("microsync", catalog_id).await.unwrap();
        let (pool, mut conn) = test_support::raw_conn().await.unwrap();
        conn.exec_drop(
            "UPDATE `jobs` SET `status`='DONE' WHERE `catalog`=:catalog AND `action`='microsync'",
            params! { "catalog" => catalog_id },
        )
        .await
        .unwrap();
        drop(conn);
        pool.disconnect().await.ok();
        assert_eq!(fetch_microsync_status(catalog_id).await.as_deref(), Some("DONE"));

        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        catalog.queue_microsync_if_applicable(&app).await.unwrap();

        assert_eq!(
            fetch_microsync_status(catalog_id).await.as_deref(),
            Some("TODO"),
            "existing microsync job should be reset to TODO"
        );
    }

    #[tokio::test]
    async fn test_catalog_url_not_truncated() {
        // Issue #16: catalog.url was VARCHAR(128); URLs longer than 128 chars
        // were silently truncated. Column is now VARCHAR(512).
        let long_url = "https://example.org/".to_string() + &"a".repeat(200);
        assert_eq!(long_url.len(), 220);
        let app = test_support::test_app().await;
        let catalog_id = test_support::seed_catalog_with_url(&long_url)
            .await
            .unwrap();
        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        assert_eq!(catalog.url().map(|s| s.as_str()), Some(long_url.as_str()));
    }

    #[tokio::test]
    async fn test_empty_catalog_removes_entries_keeps_catalog_row() {
        let app = test_support::test_app().await;
        let (catalog_id, _entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();

        let catalog = Catalog::from_id(catalog_id, &app).await.unwrap();
        assert!(catalog.number_of_entries(&app).await.unwrap() > 0);

        catalog.empty(&app).await.unwrap();

        // Entries are gone.
        assert_eq!(catalog.number_of_entries(&app).await.unwrap(), 0);

        // Catalog row is still there.
        let still_there = Catalog::from_id(catalog_id, &app).await;
        assert!(still_there.is_ok(), "catalog row should survive empty()");
    }
}
