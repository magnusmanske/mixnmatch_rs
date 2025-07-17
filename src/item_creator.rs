use crate::{app_state::AppState, entry::Entry};
use anyhow::{anyhow, Result};
use futures::future::join_all;
use std::collections::HashMap;
use wikimisc::wikibase::ItemEntity;

#[derive(Debug)]
pub struct ItemCreator {
    app: AppState,
    entries: HashMap<usize, Entry>,
}

impl ItemCreator {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            entries: HashMap::new(),
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.values()
    }

    pub fn entries_mut(&mut self) -> impl Iterator<Item = &mut Entry> {
        self.entries.values_mut()
    }

    pub fn add_entry(&mut self, entry: Entry) {
        self.add_entries(&[entry]);
    }

    pub fn add_entries(&mut self, entries: &[Entry]) {
        self.entries
            .extend(entries.iter().map(|entry| (entry.id, entry.clone())));
    }

    pub async fn add_entries_by_id(&mut self, entry_ids: &[usize]) -> Result<()> {
        let entry_ids = entry_ids
            .iter()
            .filter(|entry_id| !self.has_entry(**entry_id))
            .cloned()
            .collect::<Vec<usize>>();
        if entry_ids.is_empty() {
            return Ok(());
        }
        let entries = Entry::multiple_from_ids(&entry_ids, &self.app).await?;
        self.entries.extend(entries);
        Ok(())
    }

    /// Extends the current entries list with entries with the same aux values, eg VIAF.
    pub async fn add_entries_by_aux(&mut self) -> Result<()> {
        const EXT_PROPS: &[usize] = &[214, 227]; // TODO more, dynamically?
        let mut entries2process = self.entries.keys().cloned().collect::<Vec<_>>();
        let mut entries_done = vec![];
        while let Some(entry_id) = entries2process.pop() {
            if entries_done.contains(&entry_id) {
                continue;
            }
            entries_done.push(entry_id);
            self.add_entries_by_id(&[entry_id]).await?;
            let entry = match self.entries.get(&entry_id) {
                Some(entry) => entry,
                None => continue, // This should never happen, but...
            };

            // Check out this entries aux values,
            // and find other entries that have the same aux values,
            // for specific properties (eg VIAF, GND)
            let mut aux_vec = entry.get_aux().await.unwrap_or_default();
            aux_vec.retain(|a| EXT_PROPS.contains(&a.prop_numeric()));
            for aux in aux_vec {
                if let Ok(mut other_entries) = self
                    .app
                    .storage()
                    .get_entry_ids_by_aux(aux.prop_numeric(), aux.value())
                    .await
                {
                    other_entries.retain(|id| !entries_done.contains(id));
                    let _ = self.add_entries_by_id(&other_entries).await;
                    entries2process.extend(other_entries);
                }
            }
        }
        Ok(())
    }

    fn has_entry(&self, entry_id: usize) -> bool {
        self.entries.contains_key(&entry_id)
    }

    pub async fn generate_item(&mut self) -> Result<ItemEntity> {
        self.assert_at_least_one_entry()?;
        let mut item = ItemEntity::new_empty();
        for entry in self.entries.values_mut() {
            entry.set_app(&self.app);
            entry.add_to_item(&mut item).await?;
        }
        Ok(item)
    }

    pub async fn create_and_match_item(&mut self) -> Result<ItemEntity> {
        self.assert_at_least_one_entry()?;
        let comment = self.generate_item_creation_comment();
        let item = self.generate_item().await?;
        let new_id = self
            .app
            .wikidata_mut()
            .create_new_wikidata_item(&item, &comment)
            .await?;
        let _ = self.app.wikidata_mut().perform_ac2wd(&new_id).await; // Ignore error
        let futures = self
            .entries_mut()
            .map(|e| e.set_match(&new_id, 4))
            .collect::<Vec<_>>();
        let _ = join_all(futures).await; // Ignore errors
        Ok(item)
    }

    fn assert_at_least_one_entry(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Err(anyhow!("No entries to create item from"));
        }
        Ok(())
    }

    fn generate_item_creation_comment(&mut self) -> String {
        let entry_links = self
            .entries
            .keys()
            .map(|entry_id| format!("https://mix-n-match.toolforge.org/#/entry/{entry_id}"))
            .collect::<Vec<String>>();
        let etext = if self.entries.len() == 1 {
            "entry"
        } else {
            "entries"
        };
        let comment = format!("Created from Mix'n'Match {etext} {}", entry_links.join(" "));
        comment
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;
    use wikimisc::wikibase::{EntityTrait, LocaleString};

    #[tokio::test]
    async fn test_generate_item() {
        let app = get_test_app();
        let mut ic = ItemCreator::new(&app);
        ic.add_entries_by_id(&[170955005, 195316400]).await.unwrap();
        let item = ic.generate_item().await.unwrap();
        let claims = item.claims();
        assert_eq!(claims.iter().filter(|c| c.property() == "P31").count(), 1);
        assert_eq!(claims.iter().filter(|c| c.property() == "P569").count(), 1);
        assert_eq!(claims.iter().filter(|c| c.property() == "P570").count(), 1);
        assert_eq!(claims.iter().filter(|c| c.property() == "P227").count(), 1);
        assert_eq!(
            claims.iter().filter(|c| c.property() == "P13049").count(),
            1
        );
        assert_eq!(*item.labels(), [LocaleString::new("mul", "Fritz Koch")]);
    }
}
