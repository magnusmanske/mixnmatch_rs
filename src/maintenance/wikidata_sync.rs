//! Maintenance jobs that read from or write to Wikidata.
//!
//! The methods here either pull authoritative state from Wikidata
//! (property cache, ISO codes, ExternalId lists, per-property item maps)
//! or push corrections back into Mix'n'match storage based on what
//! Wikidata says (overwrite manual matches, rebuild aux candidates).

use super::Maintenance;
use crate::entry::{Entry, EntryWriter};
use crate::app_state::USER_AUX_MATCH;
use crate::catalog::Catalog;
use crate::prop_todo::PropTodo;
use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, HashMap, HashSet};

impl Maintenance {
    /// Rebuild the `property_cache` table from Wikidata's authoritative
    /// view of which items can serve as values for a handful of
    /// "schema-pointing" properties (currently `P17` and `P31`). The
    /// cache backs property-aware UIs and is read on hot paths, so we
    /// can't afford a per-pageload SPARQL query.
    ///
    /// Mirrors PHP `Maintenance::updatePropertyCache`: pull the same
    /// SPARQL twice (once per "prop-group" property), accumulate, gate
    /// on the result count to defend against a partial outage wiping
    /// the table, then `TRUNCATE` + chunked-`INSERT`.
    pub async fn update_property_cache(&self) -> Result<()> {
        // Tracks which Wikidata properties act as our "what kind of
        // values does this property take?" pointers. P17 = country,
        // P31 = instance of — both are richly populated and the labels
        // they yield drive the property-cache-aware editors.
        const PROP_GROUPS: &[usize] = &[17, 31];
        // Sanity floor: a partial WDQS outage produces a small but
        // non-empty result, which would silently shrink the cache.
        // PHP picks 20 000; the same threshold has held up in
        // practice across both implementations.
        const MIN_EXPECTED_ROWS: usize = 20_000;

        let client = crate::wdqs::build_client()?;
        let mut rows: Vec<crate::storage::PropertyCacheRow> = Vec::new();
        for &group in PROP_GROUPS {
            let sparql = format!(
                "SELECT ?p ?v ?vLabel {{ \
                    ?p rdf:type wikibase:Property ; wdt:P{group} ?v . \
                    SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'en' }} \
                }}"
            );
            let tsv_rows = crate::wdqs::run_tsv_query(&client, &sparql).await?;
            log::info!(
                "update_property_cache: P{group} returned {} row(s)",
                tsv_rows.len()
            );
            for row in tsv_rows {
                if let Some(parsed) = parse_property_cache_row(group, &row) {
                    rows.push(parsed);
                }
            }
        }

        if rows.len() < MIN_EXPECTED_ROWS {
            return Err(anyhow!(
                "update_property_cache: only {} row(s) parsed (< {} expected); \
                 refusing to truncate property_cache — likely a partial WDQS outage",
                rows.len(),
                MIN_EXPECTED_ROWS,
            ));
        }

        self.app.storage().property_cache_replace(&rows).await?;
        log::info!(
            "update_property_cache: replaced cache with {} row(s)",
            rows.len()
        );
        Ok(())
    }

    pub async fn update_props_todo(&self) -> Result<()> {
        // We don't really care if one of these fails occasionally
        let _ = self.update_props_todo_add_new_properties().await;
        let _ = self.update_props_todo_update_items_using().await;
        let _ = self.app.storage().mark_props_todo_as_has_catalog().await;
        // TODO add default_type?
        Ok(())
    }

    async fn update_props_todo_update_items_using_get_props_todo(&self) -> Result<HashSet<u64>> {
        let props_todo: HashSet<u64> = self
            .app
            .storage()
            .get_props_todo()
            .await?
            .into_iter()
            .filter(|p| p.items_using.is_none()) // Only those without number; comment out to update everything
            .map(|p| p.prop_num)
            .collect();
        Ok(props_todo)
    }

    async fn update_props_todo_update_items_using_get_bindings(
        &self,
    ) -> Result<Vec<serde_json::Value>> {
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let sparql = r#"select ?p (count(?q) AS ?cnt) { ?q ?p [] } group by ?p"#;
        let results = match mw_api.sparql_query(sparql).await {
            Ok(results) => results,
            Err(_) => return Ok(vec![]),
        };
        let bindings = match results["results"]["bindings"].as_array() {
            Some(bindings) => bindings,
            None => return Ok(vec![]),
        };
        Ok(bindings.to_owned())
    }

    async fn update_props_todo_update_items_using(&self) -> Result<()> {
        let props_todo = self
            .update_props_todo_update_items_using_get_props_todo()
            .await?;
        let bindings = self
            .update_props_todo_update_items_using_get_bindings()
            .await?;
        for b in bindings {
            self.update_props_todo_update_items_using_process_binding(b, &props_todo)
                .await;
        }
        Ok(())
    }

    async fn update_props_todo_update_items_using_process_binding(
        &self,
        b: serde_json::Value,
        props_todo: &HashSet<u64>,
    ) {
        let (prop_url, cnt) = match (b["p"]["value"].as_str(), b["cnt"]["value"].as_str()) {
            (Some(prop_url), Some(cnt)) => (prop_url, cnt),
            _ => return,
        };
        let (url, prop) = match prop_url.rsplit_once('/') {
            Some((url, prop)) => (url, prop),
            _ => return,
        };
        if prop.chars().nth(0) != Some('P') || url != "http://www.wikidata.org/prop/direct" {
            return;
        }
        let prop_num = match prop[1..].parse::<u64>() {
            Ok(prop_num) => prop_num,
            Err(_) => return,
        };
        if !props_todo.contains(&prop_num) {
            return;
        }
        let cnt = match cnt.parse::<u64>() {
            Ok(cnt) => cnt,
            Err(_) => return,
        };
        let _ = self
            .app
            .storage()
            .set_props_todo_items_using(prop_num, cnt)
            .await;
    }

    async fn update_props_todo_add_new_properties_get_bindings(
        &self,
        mw_api: &mediawiki::Api,
    ) -> Result<Vec<serde_json::Value>> {
        let sparql = r#"SELECT ?p ?pLabel {
    	VALUES ?auth { wd:Q19595382 wd:Q62589316 wd:Q42396390 } .
     	?p rdf:type wikibase:Property ; wdt:P31/wdt:P279* ?auth .
      	MINUS { ?p wdt:P2264 [] } .
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
    }"#;
        let results = match mw_api.sparql_query(sparql).await {
            Ok(results) => results,
            Err(_) => return Ok(vec![]),
        };
        let bindings = match results["results"]["bindings"].as_array() {
            Some(bindings) => bindings,
            None => return Ok(vec![]),
        };
        Ok(bindings.to_owned())
    }

    async fn update_props_todo_add_new_properties(&self) -> Result<()> {
        let (properties, prop_names) = self
            .update_props_todo_add_new_properties_get_props()
            .await?;
        let extisting_props = self.app.storage().get_props_todo().await?;
        let existing_hash: HashSet<u64> = extisting_props.iter().map(|p| p.prop_num).collect();
        let new_props: Vec<PropTodo> = properties
            .iter()
            .filter(|prop_num| !existing_hash.contains(prop_num))
            .map(|prop_num| {
                let name = prop_names
                    .get(prop_num)
                    .map(|s| s.to_string())
                    .unwrap_or(format!("P{prop_num}"));
                PropTodo::new(*prop_num, name)
            })
            .collect();
        self.app.storage().add_props_todo(new_props).await?;
        Ok(())
    }

    async fn update_props_todo_add_new_properties_get_props(
        &self,
    ) -> Result<(Vec<u64>, HashMap<u64, String>)> {
        let mut properties = vec![];
        let mut prop_names = HashMap::new();
        let mw_api = self.app.wikidata().get_mw_api().await?;
        let bindings = self
            .update_props_todo_add_new_properties_get_bindings(&mw_api)
            .await?;
        for b in bindings {
            if let Some(entity_url) = b["p"]["value"].as_str() {
                if let Ok(entity) = mw_api.extract_entity_from_uri(entity_url) {
                    if let Ok(prop_num) = entity[1..].parse::<u64>() {
                        properties.push(prop_num);
                        if let Some(prop_name) = b["pLabel"]["value"].as_str() {
                            let prop_name = prop_name.to_string();
                            prop_names.insert(prop_num, prop_name);
                        }
                    }
                }
            }
        }
        Ok((properties, prop_names))
    }

    pub async fn fix_auxiliary_item_values(&self) -> Result<()> {
        self.update_auxiliary_fix_table().await?;
        self.app
            .storage()
            .maintenance_use_auxiliary_broken()
            .await?;
        Ok(())
    }

    async fn update_auxiliary_fix_table(&self) -> Result<()> {
        let prop2type = self.get_sparql_prop2type().await?;
        self.app
            .storage()
            .maintenance_update_auxiliary_props(&prop2type)
            .await?;
        Ok(())
    }

    pub(super) async fn get_sparql_prop2type(&self) -> Result<Vec<(String, String)>> {
        let sparql = "SELECT ?p ?type { ?p a wikibase:Property; wikibase:propertyType ?type }";
        let mut reader = self.app.wikidata().load_sparql_csv(sparql).await?;
        let api = self.app.wikidata().get_mw_api().await?;
        let mut prop2type = vec![];
        for row in reader.records().filter_map(|r| r.ok()) {
            let q = api.extract_entity_from_uri(&row[0])?;
            let property_type = row[1].to_string();
            if let Some(property_type) = property_type.split('#').next_back() {
                prop2type.push((q, property_type.to_string()));
            };
        }
        Ok(prop2type)
    }

    /// Sweep every `multi_match` row whose entry has since been fully
    /// matched. Per-row cleanup happens inline in `Entry::set_match`
    /// already, but legacy data paths and ad-hoc fixes leave behind
    /// rows that the inline cleanup never saw — this is the broom.
    /// Mirrors PHP `Maintenance::deleteMultimatchesForFullyMatchedEntries`.
    pub async fn delete_multi_match_for_fully_matched(&self) -> Result<()> {
        let n = self
            .app
            .storage()
            .maintenance_delete_multi_match_for_fully_matched()
            .await?;
        log::info!("delete_multi_match_for_fully_matched: removed {n} row(s)");
        Ok(())
    }

    /// Tidy up `wd_matches` against the catalogs they reference:
    /// orphan deletes, catalog back-fills, and N/A flips for catalogs
    /// `wd_match_sync` can't do anything with. Mirrors PHP
    /// `Maintenance::fixupWdMatches`.
    pub async fn fixup_wd_matches(&self) -> Result<()> {
        let (deleted, recatalogued, marked_na) =
            self.app.storage().maintenance_fixup_wd_matches().await?;
        log::info!(
            "fixup_wd_matches: deleted {deleted}, back-filled {recatalogued}, \
             marked {marked_na} N/A"
        );
        Ok(())
    }

    /// Forcibly bring a catalog's manual matches into agreement with
    /// the live Wikidata view of its property. Reads
    /// `?q wdt:Pn ?v` for every entry, then for any entry whose
    /// stored Q disagrees with Wikidata's, rewrites the match to the
    /// Wikidata-side Q with attribution `USER_AUX_MATCH`.
    ///
    /// **This overwrites manual matches.** Use it deliberately, as a
    /// per-catalog corrective when an entry's external IDs have been
    /// reissued or human matches have systematically drifted from
    /// Wikidata's authoritative state. The `wd_match_sync` classifier
    /// covers the routine cross-check; `overwrite_from_wikidata` is
    /// the heavier "trust Wikidata, rewrite ours" intervention.
    ///
    /// Mirrors PHP `Maintenance::overwriteFromWikidata`. Returns the
    /// number of rows it rewrote.
    pub async fn overwrite_from_wikidata(&self, catalog_id: usize) -> Result<usize> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let cat = Catalog::from_id(catalog_id, self.app.as_ref()).await?;
        if !cat.is_active() {
            return Err(anyhow!("catalog {catalog_id} is not active"));
        }
        let prop = cat
            .wd_prop()
            .ok_or_else(|| anyhow!("catalog {catalog_id} has no wd_prop set"))?;
        if cat.wd_qual().is_some() {
            return Err(anyhow!(
                "catalog {catalog_id} uses a wd_qual; overwrite_from_wikidata \
                 only supports primary-property catalogs"
            ));
        }

        // SPARQL: pull the live ext_id → Q map for the property.
        let client = crate::wdqs::build_client()?;
        let sparql = format!("SELECT ?q ?v {{ ?q wdt:P{prop} ?v }}");
        let rows = crate::wdqs::run_tsv_query(&client, &sparql).await?;
        let mut wd: HashMap<String, isize> = HashMap::with_capacity(rows.len());
        for row in rows {
            let Some(q_uri) = row.first() else { continue };
            let value = row.get(1).map(|s| s.trim().to_string()).unwrap_or_default();
            if value.is_empty() {
                continue;
            }
            if let Some(q) = crate::wdqs::entity_id_from_uri(q_uri, 'Q') {
                wd.insert(value, q as isize);
            }
        }
        if wd.is_empty() {
            return Err(anyhow!(
                "overwrite_from_wikidata: SPARQL for P{prop} returned no usable rows; \
                 refusing to proceed"
            ));
        }

        // Walk our manual matches; rewrite the disagreements.
        let manual = self
            .app
            .storage()
            .entry_get_manual_matches_for_catalog(catalog_id)
            .await?;
        let mut rewritten = 0_usize;
        for (entry_id, ext_id, our_q) in manual {
            let Some(&wd_q) = wd.get(&ext_id) else {
                continue;
            };
            if wd_q == our_q {
                continue;
            }
            let mut entry = match Entry::from_id(entry_id, self.app.as_ref()).await {
                Ok(e) => e,
                Err(e) => {
                    log::warn!("overwrite_from_wikidata: cannot load entry {entry_id}: {e}");
                    continue;
                }
            };
            let q_str = format!("Q{wd_q}");
            if let Err(e) = EntryWriter::new(self.app.as_ref(), &mut entry)
                .set_match(&q_str, USER_AUX_MATCH)
                .await
            {
                log::warn!("overwrite_from_wikidata: rewrite failed for entry {entry_id}: {e}");
                continue;
            }
            rewritten += 1;
        }
        log::info!("overwrite_from_wikidata: catalog {catalog_id} rewrote {rewritten} match(es)");
        Ok(rewritten)
    }

    /// Apply every regex rule from `description_aux` to one
    /// catalog's `entry.ext_desc` column, materialising matched
    /// `(property, value)` pairs as new `auxiliary` rows. Idempotent:
    /// the SQL already skips entries that have the corresponding
    /// auxiliary row, so re-runs are no-ops on stable data.
    /// Mirrors PHP `Maintenance::applyDescriptionAux`.
    pub async fn apply_description_aux(&self, catalog_id: usize) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let rules = self.app.storage().description_aux_get_all().await?;
        if rules.is_empty() {
            log::info!("apply_description_aux: description_aux table is empty");
            return Ok(());
        }
        let mut total = 0_usize;
        for rule in &rules {
            match self
                .app
                .storage()
                .apply_description_aux_to_catalog(catalog_id, rule)
                .await
            {
                Ok(n) => total += n,
                Err(e) => log::warn!(
                    "apply_description_aux: rule P{}={} failed for catalog {catalog_id}: {e}",
                    rule.property,
                    rule.value
                ),
            }
        }
        log::info!(
            "apply_description_aux: catalog {catalog_id} added {total} aux row(s) across {} rule(s)",
            rules.len()
        );
        Ok(())
    }

    /// Rewrite every `entry.ext_url` in the catalog by substituting
    /// `$1` in `url_pattern` with the row's `ext_id`. Useful when a
    /// catalog's source moves to a new URL scheme and the existing
    /// rows need their cached external URLs refreshed; also a one-
    /// off fix when an importer wrote the wrong URLs to begin with.
    /// Mirrors PHP `Maintenance::updateExternalUrlsFromPattern`.
    ///
    /// The bulk SQL lives in `storage::api_update_catalog_ext_urls`
    /// (also reached by the web `update_ext_urls` API endpoint); this
    /// is the matching Rust-callable wrapper that splits the pattern
    /// on `$1` and validates the input.
    pub async fn update_ext_urls_from_pattern(
        &self,
        catalog_id: usize,
        url_pattern: &str,
    ) -> Result<()> {
        if catalog_id == 0 {
            return Err(anyhow!("catalog id must be positive"));
        }
        let parts: Vec<&str> = url_pattern.splitn(2, "$1").collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "url_pattern '{url_pattern}' does not contain '$1'"
            ));
        }
        self.app
            .storage()
            .api_update_catalog_ext_urls(catalog_id, parts[0], parts[1])
            .await?;
        log::info!(
            "update_ext_urls_from_pattern: catalog {catalog_id} rewritten with '{url_pattern}'"
        );
        Ok(())
    }

    /// Rebuild `aux_candidates` — the cache the
    /// `creation_candidates?mode=random_prop` picker reads from.
    ///
    /// Three steps mirroring PHP `Maintenance::updateAuxCandidates`:
    ///
    /// 1. Pull every distinct `aux_p` we currently know about from
    ///    `auxiliary`.
    /// 2. Intersect that with Wikidata's list of external-ID
    ///    properties via SPARQL — only properties that actually take
    ///    string values can power the picker, and external-ID is the
    ///    canonical type for those.
    /// 3. Hand the filtered allowlist to the storage layer's
    ///    `aux_candidates` rebuild.
    ///
    /// Refuses to wipe the table when the allowlist is empty (a
    /// transient SPARQL outage shouldn't break the picker until the
    /// next successful run). The 3-row floor matches the PHP
    /// constant.
    pub async fn update_aux_candidates(&self) -> Result<()> {
        const MIN_COUNT: usize = 3;
        let props_in_aux = self.app.storage().auxiliary_distinct_props().await?;
        if props_in_aux.is_empty() {
            log::info!("update_aux_candidates: auxiliary is empty, nothing to do");
            return Ok(());
        }

        // Wikidata-side allowlist via SPARQL. The matcher uses the
        // same query, so it lives there as the single source of truth.
        let ext_id_props: HashSet<usize> =
            crate::auxiliary_matcher::AuxiliaryMatcher::get_properties_that_have_external_ids(
                self.app.as_ref(),
            )
            .await?
            .into_iter()
            .filter_map(|s| s.trim_start_matches('P').parse::<usize>().ok())
            .collect();
        let allowlist: Vec<usize> = props_in_aux
            .into_iter()
            .filter(|p| ext_id_props.contains(p))
            .collect();
        if allowlist.is_empty() {
            return Err(anyhow!(
                "update_aux_candidates: no aux property survived the external-ID \
                 filter — refusing to truncate aux_candidates"
            ));
        }

        let n = self
            .app
            .storage()
            .maintenance_update_aux_candidates(&allowlist, MIN_COUNT)
            .await?;
        log::info!(
            "update_aux_candidates: rebuilt with {n} row(s) across {} prop(s)",
            allowlist.len()
        );
        Ok(())
    }

    /// Fetch ISO 639-1 (2-letter) → ISO 639-3 (3-letter) code mappings from
    /// Wikidata and write the result to `html/iso.json` (or the path given by
    /// `html_dir_override` in the config). Existing entries not returned by
    /// Wikidata are preserved so that manually maintained special-cases
    /// (e.g. `"չկա"→"xcl"`) survive the update.
    pub async fn update_iso_codes(&self) -> Result<()> {
        let sparql = "SELECT ?iso1 ?iso3 WHERE { \
                          ?lang wdt:P218 ?iso1 ; \
                                wdt:P220 ?iso3 . \
                      }";
        let client = crate::wdqs::build_client()?;
        let rows = crate::wdqs::run_tsv_query(&client, sparql).await?;
        if rows.len() < 100 {
            return Err(anyhow!(
                "update_iso_codes: only {} row(s) from SPARQL, expected ≥100; aborting",
                rows.len()
            ));
        }

        let path = match self.app.html_dir_override() {
            Some(dir) => dir.join("iso.json"),
            None => std::path::PathBuf::from("html/iso.json"),
        };

        // Seed the map from the existing file so manually maintained entries survive.
        let mut map: BTreeMap<String, String> = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        for row in &rows {
            if row.len() < 2 {
                continue;
            }
            let iso1 = row[0].trim().to_string();
            let iso3 = row[1].trim().to_string();
            if iso1.is_empty() || iso3.is_empty() {
                continue;
            }
            map.insert(iso1, iso3);
        }

        let json = serde_json::to_string_pretty(&map)?;
        std::fs::write(&path, json + "\n")?;
        log::info!(
            "update_iso_codes: wrote {} entries to {}",
            map.len(),
            path.display()
        );
        Ok(())
    }
}

/// Decode one TSV row of `?p ?v ?vLabel` into the storage row.
/// Returns `None` if either entity URI doesn't end with the expected
/// numeric id — defensive against redirect IRIs and the occasional
/// nonsense row.
pub(super) fn parse_property_cache_row(
    prop_group: usize,
    row: &[String],
) -> Option<crate::storage::PropertyCacheRow> {
    let p_uri = row.first()?;
    let v_uri = row.get(1)?;
    let label = row.get(2).cloned().unwrap_or_default();
    let property = crate::wdqs::entity_id_from_uri(p_uri, 'P')?;
    let item = crate::wdqs::entity_id_from_uri(v_uri, 'Q')?;
    Some(crate::storage::PropertyCacheRow {
        prop_group,
        property,
        item,
        label,
    })
}
