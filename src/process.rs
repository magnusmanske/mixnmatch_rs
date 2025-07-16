use crate::{
    app_state::AppState, entry_query::EntryQuery, item_creator::ItemCreator,
    match_state::MatchState,
};
use anyhow::Result;

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
        try_search: &bool,
        desc_hint: &Option<String>,
    ) -> Result<()> {
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

        // self.app.wikidata_mut().api_log_in().await?;
        for mut entry in entries {
            if *try_search {
                // Search (with type where applicable) and skip if there are results on Wikidata
                let type_name = entry.type_name.clone().unwrap_or_default();
                let results = match self
                    .app
                    .wikidata()
                    .search_with_type_api(&entry.ext_name, &type_name)
                    .await
                {
                    Ok(results) => results,
                    Err(_err) => {
                        // eprintln!("Error searching for entry: {}", err);
                        continue;
                    }
                };
                if !results.is_empty() {
                    // These are supposed to be unmatched, use multimatch
                    let _ = entry.set_multi_match(&results).await;
                    continue;
                }
            }
            // println!("No search result for entry '{}'", entry.ext_name);
            let mut ic = ItemCreator::new(&self.app);
            let comment = format!("Created from Mix'n'Match entry {}", entry.id);
            ic.add_entry(entry.to_owned());

            let item = ic.generate_item().await?;
            let new_id = self
                .app
                .wikidata_mut()
                .create_new_wikidata_item(&item, &comment)
                .await?;
            let _ = entry.set_match(&new_id, 4).await;
        }

        Ok(())
    }
}
