//! Cleanup jobs that prune or correct existing matches and rows.
//!
//! These jobs walk the catalog/entry tables looking for stale state
//! (redirected items, deleted items, meta items, mismatched human
//! flags, undifferentiated GND records, HTML-encoded names, multi-match
//! orphans, ext_url drift) and rewrite or remove the offending rows.

use super::Maintenance;
use crate::app_state::{AppState, USER_AUX_MATCH};
use crate::auxiliary_matcher::AuxiliaryMatcher;
use crate::catalog::Catalog;
use crate::entry::{Entry, EntryWriter};
use crate::job::Job;
use crate::match_state::MatchState;
use crate::util::wikidata_props as wp;
use anyhow::{Result, anyhow};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};

impl Maintenance {
    /// Iterates over blocks of (fully or partially) matched Wikidata items, and replaces redirects with their targets.
    pub async fn fix_redirects(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks meta items, such as disambiguation pages.
    pub async fn unlink_meta_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Iterates over blocks of (fully or partially) matched Wikidata items, and unlinks deleted pages
    pub async fn unlink_deleted_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
        }
    }

    /// Fixes redirected items, and unlinks deleted and meta items.
    /// This is more efficient than calling the functions individually, because it uses the same batching run.
    pub async fn fix_matched_items(&self, catalog_id: usize, state: &MatchState) -> Result<()> {
        let mut offset = 0;
        loop {
            let unique_qs = self
                .app
                .storage()
                .get_items(catalog_id, offset, state)
                .await?;
            if unique_qs.is_empty() {
                return Ok(());
            }
            offset += unique_qs.len();
            let _ = self.fix_redirected_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_deleted_items_batch(&unique_qs).await; // Ignore error
            let _ = self.unlink_meta_items_batch(&unique_qs).await; // Ignore errors
        }
    }

    /// Finds redirects in a batch of items, and changes app matches to their respective targets.
    async fn fix_redirected_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let page2rd = self.app.wikidata().get_redirected_items(unique_qs).await?;
        for (from, to) in &page2rd {
            if let (Some(from), Some(to)) =
                (AppState::item2numeric(from), AppState::item2numeric(to))
            {
                if from > 0 && to > 0 {
                    self.app
                        .storage()
                        .maintenance_fix_redirects(from, to)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Finds deleted items in a batch of items, and unlinks app matches to them.
    async fn unlink_deleted_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let not_found = self.app.wikidata().get_deleted_items(unique_qs).await?;
        self.unlink_item_matches(&not_found).await?;
        Ok(())
    }

    /// Finds meta items (disambig etc) in a batch of items, and unlinks app matches to them.
    async fn unlink_meta_items_batch(&self, unique_qs: &[String]) -> Result<()> {
        let meta_items = self.app.wikidata().get_meta_items(unique_qs).await?;
        self.unlink_item_matches(&meta_items).await?;
        Ok(())
    }

    /// Unlinks app matches to items in a list.
    pub async fn unlink_item_matches(&self, items: &[String]) -> Result<()> {
        let items: Vec<isize> = items
            .iter()
            .filter_map(|q| AppState::item2numeric(q))
            .collect();

        if !items.is_empty() {
            let items: Vec<String> = items.iter().map(|q| q.to_string()).collect();
            self.app
                .storage()
                .maintenance_unlink_item_matches(items)
                .await?;
        }
        Ok(())
    }

    /// Walk every Q5 entry whose match was set by the date matcher
    /// (`user=3`) or aux matcher (`user=4`) and verify the matched
    /// Wikidata item is actually `instance of` Q5. Items that aren't
    /// get unmatched — those are the false positives that the
    /// algorithmic matchers occasionally produce when a name+date
    /// pair coincidentally matches a non-human Wikidata item (a
    /// fictional character with year-of-creation = "1850", a
    /// disambiguation page, …).
    ///
    /// Items are fetched from Wikidata in batches via `wbgetentities`
    /// (one network round-trip per batch through the
    /// `EntityContainer`); the `P31=Q5` check is then a pure
    /// in-memory walk of the loaded claims. A single Q can show up
    /// against many entries (the same item is the "match" for many
    /// catalogs), so the check is implicitly cached by the
    /// container's dedup.
    ///
    /// Mirrors PHP `Maintenance::sanityCheckDateMatchesAreHuman`,
    /// trading PHP's per-row `pagelinks` SELECT against the
    /// Wikidata replica for an item-bulk-load — same result, fewer
    /// network/DB round-trips. Returns the number of matches it
    /// removed.
    pub async fn sanity_check_date_matches_are_human(&self) -> Result<usize> {
        // wbgetentities caps at 50 ids per request; the
        // EntityContainer chunks internally so any batch this size
        // or below maps to one round-trip. 200 keeps the per-batch
        // memory footprint bounded for very large match sets.
        const BATCH_SIZE: usize = 200;

        let candidates = self
            .app
            .storage()
            .entry_get_algorithmic_human_matches()
            .await?;
        if candidates.is_empty() {
            log::info!("sanity_check_date_matches_are_human: no algorithmic Q5 matches");
            return Ok(0);
        }
        log::info!(
            "sanity_check_date_matches_are_human: checking {} algorithmic match(es)",
            candidates.len()
        );

        // Group by Q so each item is loaded once regardless of how
        // many entries point at it.
        let mut q_to_entries: HashMap<isize, Vec<usize>> = HashMap::new();
        for (entry_id, q) in candidates {
            q_to_entries.entry(q).or_default().push(entry_id);
        }
        let qs: Vec<String> = q_to_entries.keys().map(|q| format!("Q{q}")).collect();

        let api = self.app.wikidata().get_mw_api().await?;
        let mut removed = 0_usize;
        for batch in qs.chunks(BATCH_SIZE) {
            removed += self
                .sanity_check_one_batch(&api, batch, &q_to_entries)
                .await;
        }
        log::info!("sanity_check_date_matches_are_human: removed {removed} non-human match(es)");
        Ok(removed)
    }

    /// Process one batch of QIDs: bulk-load via `wbgetentities`,
    /// unmatch the entries whose target item isn't an instance of
    /// Q5. Returns the number of matches removed in this batch.
    /// Pulled out of `sanity_check_date_matches_are_human` so the
    /// outer loop stays linear.
    async fn sanity_check_one_batch(
        &self,
        api: &mediawiki::api::Api,
        batch: &[String],
        q_to_entries: &HashMap<isize, Vec<usize>>,
    ) -> usize {
        let entities = wikimisc::wikibase::entity_container::EntityContainer::new();
        if let Err(e) = entities.load_entities(api, &batch.to_vec()).await {
            log::warn!("sanity_check_date_matches_are_human: batch load failed: {e} — continuing");
            return 0;
        }
        let mut removed = 0_usize;
        for q_label in batch {
            let Some(q_num) = q_label
                .strip_prefix('Q')
                .and_then(|s| s.parse::<isize>().ok())
            else {
                continue;
            };
            let Some(entry_ids) = q_to_entries.get(&q_num) else {
                continue;
            };
            if item_is_human(entities.get_entity(q_label.clone()).as_ref()) {
                continue;
            }
            // Item is not Q5 (or didn't load — treat as non-human;
            // the mismatch is real and the next run can re-check
            // after WD data updates).
            for &entry_id in entry_ids {
                if self.unmatch_one(entry_id).await {
                    removed += 1;
                }
            }
        }
        removed
    }

    async fn unmatch_one(&self, entry_id: usize) -> bool {
        let mut entry = match Entry::from_id(entry_id, &self.app).await {
            Ok(e) => e,
            Err(e) => {
                log::warn!(
                    "sanity_check_date_matches_are_human: cannot load entry {entry_id}: {e}"
                );
                return false;
            }
        };
        if let Err(e) = EntryWriter::new(&self.app, &mut entry).unmatch().await {
            log::warn!(
                "sanity_check_date_matches_are_human: unmatch failed for entry {entry_id}: {e}"
            );
            return false;
        }
        true
    }

    /// Walk every `auxiliary` row for P227 (GND ID) and delete the
    /// rows whose GND target is an "Undifferentiated Person" — the
    /// GND placeholder used when several real-world people share a
    /// name and DNB hasn't yet split them. Treating those as a single
    /// person poisons matches downstream, so the rule is to drop them
    /// from MnM rather than ever match against them.
    ///
    /// Each row triggers an HTTP fetch to `d-nb.info` for its
    /// `…/about/lds` RDF representation; presence of the
    /// `gndo:UndifferentiatedPerson` literal flags the row for
    /// deletion. 404s and other read errors are treated as "not
    /// undifferentiated" — same as PHP — to keep a transient DNB
    /// outage from sweeping live data.
    ///
    /// Mirrors PHP `Maintenance::fixGndUndifferentiatedPersons`.
    pub async fn fix_gnd_undifferentiated_persons(&self) -> Result<()> {
        let rows = self.app.storage().auxiliary_select_for_prop(227).await?;
        if rows.is_empty() {
            log::info!("fix_gnd_undifferentiated_persons: no GND aux rows");
            return Ok(());
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("mix-n-match (https://mix-n-match.toolforge.org)")
            .build()?;

        let mut checked = 0_usize;
        let mut removed = 0_usize;
        for (id, gnd) in rows {
            checked += 1;
            if !is_gnd_undifferentiated_person(&client, &gnd).await {
                continue;
            }
            if let Err(e) = self.app.storage().auxiliary_delete_row(id).await {
                log::warn!("fix_gnd_undifferentiated_persons: delete failed for aux row {id}: {e}");
                continue;
            }
            removed += 1;
        }
        log::info!("fix_gnd_undifferentiated_persons: checked {checked}, removed {removed}");
        Ok(())
    }

    /// Decode HTML entities (`&amp;`, `&eacute;`, …) in every
    /// `entry.ext_name` of the given catalog and rewrite the row when
    /// the decoded form differs. Imports occasionally pick names up
    /// pre-decoded by the source HTML; this is the post-hoc fix.
    /// Mirrors PHP `Maintenance::fixHTMLentitiesForNamesInCatalog`.
    pub async fn fix_html_entities_in_catalog(&self, catalog_id: usize) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let candidates = self
            .app
            .storage()
            .entry_select_with_html_entities_in_name(catalog_id)
            .await?;
        let mut rewritten = 0_usize;
        for (id, raw) in candidates {
            let decoded = html_escape::decode_html_entities(&raw).trim().to_string();
            if decoded == raw {
                // LIKE '%&%;%' is approximate: `&` and `;` can show
                // up in text that isn't an entity reference. Skipping
                // unchanged rows keeps a noisy false-positive set
                // from generating no-op writes.
                continue;
            }
            // Existing storage helper takes (name, id) — note the
            // arg order. Truncates to 127 chars at the SQL layer to
            // match the column width.
            if let Err(e) = self.app.storage().entry_set_ext_name(&decoded, id).await {
                log::warn!("fix_html_entities_in_catalog: update failed for entry {id}: {e}");
                continue;
            }
            rewritten += 1;
        }
        log::info!("fix_html_entities_in_catalog: catalog {catalog_id} rewrote {rewritten} name(s)");
        Ok(())
    }

    pub async fn fully_match_via_collection_inventory_number(&self) -> Result<()> {
        let catalog_ids: Vec<usize> = self
            .app
            .storage()
            .get_all_catalogs_key_value_pairs()
            .await?
            .iter()
            .filter(|(_catalog_id, key, _value)| key == "collection")
            .map(|(catalog_id, _key, _value)| *catalog_id)
            .collect();
        let mut futures = vec![];
        for catalog_id in catalog_ids {
            let future = self.fully_match_via_collection_inventory_number_for_catalog(catalog_id);
            futures.push(future);
        }
        let _ = join_all(futures).await;
        Ok(())
    }

    async fn fully_match_via_collection_inventory_number_for_catalog(
        &self,
        catalog_id: usize,
    ) -> Result<()> {
        let inventory_number2entry_id = self.get_inventory_numbers_to_entry_id(catalog_id).await?;
        if inventory_number2entry_id.is_empty() {
            return Ok(());
        }

        let mw_api = self.app.wikidata().get_mw_api().await?;
        let results = self
            .get_items_and_inventory_numbers_for_catalog(catalog_id, &mw_api)
            .await?;

        // Match via aux to inventory numbers
        for binding in results {
            self.fully_match_via_collection_inventory_number_for_catalog_process_binding(
                binding,
                &mw_api,
                &inventory_number2entry_id,
            )
            .await;
        }
        Ok(())
    }

    async fn fully_match_via_collection_inventory_number_for_catalog_process_binding(
        &self,
        binding: serde_json::Value,
        mw_api: &mediawiki::Api,
        inventory_number2entry_id: &HashMap<String, usize>,
    ) {
        let q = binding["q"]["value"].as_str();
        let id = binding["id"]["value"].as_str();
        let (q, id) = match (q, id) {
            (Some(q), Some(id)) => (q.to_string(), id.to_string()),
            _ => return,
        };
        let q = match mw_api.extract_entity_from_uri(&q) {
            Ok(q) => q,
            Err(_) => return,
        };
        if let Some(entry_id) = inventory_number2entry_id.get(&id) {
            if let Ok(mut entry) = Entry::from_id(*entry_id, &self.app).await {
                if !entry.is_fully_matched() {
                    let _ = EntryWriter::new(&self.app, &mut entry)
                        .set_match(&q, USER_AUX_MATCH)
                        .await;
                }
            }
        }
    }

    async fn get_items_and_inventory_numbers_for_catalog(
        &self,
        catalog_id: usize,
        mw_api: &mediawiki::Api,
    ) -> Result<Vec<serde_json::Value>> {
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let kv_catalog = catalog.get_key_value_pairs(&self.app).await?;
        let collection_q = kv_catalog
            .get("collection")
            .ok_or_else(|| anyhow!("Catalog {catalog_id} does not have a 'collection' key"))?;
        let sparql = format!(
            "SELECT ?q ?id {{ ?q p:P217 ?statement . ?statement pq:P195 wd:{collection_q}; ps:P217 ?id }}"
        );
        let results = mw_api.sparql_query(&sparql).await?;
        let results = results["results"]["bindings"]
            .as_array()
            .ok_or_else(|| anyhow!("SPARQL failed"))?;
        Ok(results.to_owned())
    }

    async fn get_inventory_numbers_to_entry_id(
        &self,
        catalog_id: usize,
    ) -> Result<HashMap<String, usize>> {
        let inventory_number2entry_id: HashMap<String, usize> = self
            .app
            .storage()
            .auxiliary_matcher_match_via_aux(
                catalog_id,
                0,
                usize::MAX,
                &["217".to_string()],
                &AuxiliaryMatcher::get_blacklisted_catalogs(),
            )
            .await?
            .iter()
            .map(|a| (a.value.to_owned(), a.entry_id))
            .collect();
        Ok(inventory_number2entry_id)
    }

    /// Process one `(aux_p, aux_name)` candidate group: load the
    /// entries, decide whether exactly one manual Q can be
    /// propagated, and write the resulting matches. Updates the
    /// caller's running counters in place. Pulled out of
    /// `crossmatch_via_aux` to keep that function's flow linear.
    async fn crossmatch_via_aux_one_group(
        &self,
        prop: usize,
        entry_ids: &[usize],
        active_catalogs: &HashSet<usize>,
        total_matched: &mut usize,
        catalogs_to_microsync: &mut HashSet<usize>,
    ) {
        let entries = match Entry::multiple_from_ids(entry_ids, &self.app).await {
            Ok(map) => map,
            Err(e) => {
                log::warn!("crossmatch_via_aux: cannot load entries for P{prop} group: {e}");
                return;
            }
        };

        // Bucket the loaded entries by match state. Inactive catalogs
        // are dropped here rather than at the SQL level — mirrors
        // PHP's `JOIN catalog ... WHERE catalog.active=1`.
        let mut manual_qs: HashSet<isize> = HashSet::new();
        let mut unmatched_entries: Vec<(usize, usize)> = Vec::new(); // (entry_id, catalog_id)
        for (_id, entry) in entries {
            if !active_catalogs.contains(&entry.catalog) {
                continue;
            }
            let user = entry.user.unwrap_or(0);
            let q = entry.q.unwrap_or(0);
            if user > 0 && q > 0 {
                manual_qs.insert(q);
            } else if let Some(entry_id) = entry.id {
                unmatched_entries.push((entry_id, entry.catalog));
            }
        }

        // Need exactly one human-confirmed Q to know what to
        // propagate. Zero → nothing to share. More than one →
        // ambiguous; PHP logs and skips, we do the same.
        let Some(&q) = single_value(&manual_qs) else {
            return;
        };
        if unmatched_entries.is_empty() {
            return;
        }
        let q_str = format!("Q{q}");
        for (entry_id, catalog_id) in unmatched_entries {
            let mut entry = match Entry::from_id(entry_id, &self.app).await {
                Ok(e) => e,
                Err(e) => {
                    log::warn!("crossmatch_via_aux: re-load failed for entry {entry_id}: {e}");
                    continue;
                }
            };
            if EntryWriter::new(&self.app, &mut entry)
                .set_match(&q_str, USER_AUX_MATCH)
                .await
                .is_ok()
            {
                *total_matched += 1;
                catalogs_to_microsync.insert(catalog_id);
            }
        }
    }

    /// Propagate a confirmed Wikidata match across entries that
    /// share a strong authority identifier — ISNI (P214) or GND
    /// (P227). When two or more catalog entries hold the same value
    /// for one of those properties and exactly one of them has a
    /// human-confirmed match, the others are auto-matched to the
    /// same Q with attribution `USER_AUX_MATCH`. After the pass,
    /// every catalog that received a new match gets a `microsync`
    /// job queued so the cross-walk between MnM and WD reflects
    /// the new state.
    ///
    /// Mirrors PHP `Maintenance::crossmatchViaAux` but builds the
    /// candidate group set in one storage call rather than walking
    /// per-row, and skips the optional `entry_is_matched=1`
    /// pre-flight UPDATE — the storage HAVING clause already does
    /// the right comparison without needing the column to be fresh.
    ///
    /// Returns total auto-matches set; useful both for cron logging
    /// and for callers that want to know whether to run a follow-up
    /// pass straight away.
    pub async fn crossmatch_via_aux(&self) -> Result<usize> {
        // Authority properties strong enough to imply same-person.
        // Matches PHP's hardcoded list — keeping them in lock-step
        // means cross-tool data quality stays consistent.
        const AUX_PROPS: &[usize] = &[214, 227];

        let groups = self
            .app
            .storage()
            .auxiliary_get_crossmatch_groups(AUX_PROPS)
            .await?;
        if groups.is_empty() {
            log::info!("crossmatch_via_aux: no candidate groups");
            return Ok(0);
        }

        // Cache active catalogs once — looking each catalog up
        // per-entry would dominate the runtime when groups are large
        // (P214/P227 alone routinely have tens of thousands of rows).
        let active_catalogs: HashSet<usize> = self
            .app
            .storage()
            .api_get_active_catalog_ids()
            .await?
            .into_iter()
            .collect();

        let mut total_matched = 0_usize;
        let mut catalogs_to_microsync: HashSet<usize> = HashSet::new();
        for (prop, _aux_name, entry_ids) in groups {
            self.crossmatch_via_aux_one_group(
                prop,
                &entry_ids,
                &active_catalogs,
                &mut total_matched,
                &mut catalogs_to_microsync,
            )
            .await;
        }

        // Queue follow-up microsync per touched catalog so the
        // wd_matches / external state reflects the new matches in
        // the next sweep. Failure to queue is logged but doesn't
        // unwind the matches — the wd_matches inline insert in
        // entry_set_match_cleanup already covers the most important
        // bookkeeping.
        for catalog_id in catalogs_to_microsync {
            if let Err(e) = Job::queue_simple_job(&self.app, catalog_id, "microsync", None).await
            {
                log::warn!(
                    "crossmatch_via_aux: failed to queue microsync for catalog {catalog_id}: {e}"
                );
            }
        }

        log::info!("crossmatch_via_aux: matched {total_matched} entry(ies)");
        Ok(total_matched)
    }
}

/// `Some(&v)` iff the set has exactly one element. Lets a caller
/// pattern-match on "exactly one value" without a separate
/// `len() == 1` + `iter().next().unwrap()` two-step that clippy
/// (correctly) flags as a potential panic source.
fn single_value<T>(set: &HashSet<T>) -> Option<&T> {
    if set.len() != 1 {
        return None;
    }
    set.iter().next()
}

/// True iff the loaded item carries at least one `P31 = Q5`
/// statement. Treats a missing item (None) as not-human — the
/// match either points at a deleted/redirected Q (which
/// `fix_redirected_items` and `unlink_deleted_items` handle
/// separately) or at something else entirely; either way, the
/// algorithmic match shouldn't keep claiming it's a person.
fn item_is_human(item: Option<&wikimisc::wikibase::Entity>) -> bool {
    let Some(item) = item else { return false };
    item.claims_with_property(wp::P_INSTANCE_OF.to_string())
        .iter()
        .filter_map(|s| s.main_snak().data_value().clone())
        .any(|dv| match dv.value() {
            wikimisc::wikibase::Value::Entity(e) => e.id() == "Q5",
            _ => false,
        })
}

/// Look up a GND identifier in DNB's RDF representation and return
/// `true` iff the resource is flagged `gndo:UndifferentiatedPerson`.
/// Network / parse failures count as "not undifferentiated" so a
/// transient DNB outage can't silently delete live aux rows. Mirrors
/// PHP `MixNMatch::isGNDundifferentiatedPerson`.
async fn is_gnd_undifferentiated_person(client: &reqwest::Client, gnd: &str) -> bool {
    // The `/about/lds` endpoint returns a small Turtle/RDF fragment;
    // a substring match is enough — the property literal is unique
    // to this exact concept in DNB's vocabulary.
    let url = format!("https://d-nb.info/gnd/{gnd}/about/lds");
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => body.contains("gndo:UndifferentiatedPerson"),
            Err(_) => false,
        },
        _ => false,
    }
}
