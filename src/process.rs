use crate::{
    app_state::AppState, entry::Entry, entry_query::EntryQuery, item_creator::ItemCreator,
    match_state::MatchState,
};
use anyhow::Result;
use futures::prelude::*;
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct Process {
    app: AppState,
}

impl Process {
    pub fn new(app: AppState) -> Self {
        Self { app }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_unmatched(
        &mut self,
        catalog_id: &usize,
        min_dates: &Option<u8>,
        min_aux: &Option<usize>,
        entry_type: &Option<String>,
        no_search: &bool,
        desc_hint: &Option<String>,
    ) -> Result<()> {
        let do_search = !*no_search;
        let mut query = EntryQuery::default()
            .with_catalog_id(*catalog_id)
            .with_match_state(MatchState::unmatched());
        if let Some(min_dates) = min_dates {
            query = query.with_min_dates(*min_dates);
        }
        if let Some(min_aux) = min_aux {
            query = query.with_min_aux(*min_aux);
        }
        if let Some(entry_type) = entry_type {
            query = query.with_type(entry_type);
        }
        if let Some(desc_hint) = desc_hint {
            query = query.with_desc_hint(desc_hint);
        }
        let entries = self.app.storage().entry_query(&query).await?;
        info!("Running for {} entries", entries.len());

        // Run 5 in parallel, as to not overload the DB and search
        let futures = entries
            .into_iter()
            .map(|entry| self.generate_item_creator(do_search, entry))
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(futures).buffer_unordered(5);
        let ics = stream.collect::<Vec<_>>().await; // Collect ItemCreators
        let mut ics = ics.into_iter().flatten().collect::<Vec<_>>(); // Remove None

        // Run 3 in parallel, as to not get banned on WD
        info!("Starting item creation for {} ICs", ics.len());
        let futures2 = ics
            .iter_mut()
            .map(|ic| ic.create_and_match_item())
            .collect::<Vec<_>>();
        let stream2 = futures::stream::iter(futures2).buffer_unordered(3);
        let _ = stream2.collect::<Vec<_>>().await; // Wait for all to complete

        Ok(())
    }

    async fn generate_item_creator(&self, do_search: bool, entry: Entry) -> Option<ItemCreator> {
        if do_search {
            // Search (with type where applicable) and skip if there are results on Wikidata
            let type_name = entry.type_name.clone().unwrap_or_default();
            let results = match self
                .app
                .wikidata()
                .search_with_type_api(&entry.ext_name, &type_name)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    warn!("Error searching for entry: {err}");
                    return None;
                }
            };
            if !results.is_empty() {
                // These are supposed to be unmatched, use multimatch
                let _ = entry.set_multi_match(&results).await;
                return None;
            }
        }
        let mut ic = ItemCreator::new(&self.app);
        ic.add_entry(entry);
        let _ = ic.add_entries_by_aux().await;
        Some(ic)
    }
}
