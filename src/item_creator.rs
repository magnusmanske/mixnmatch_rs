use crate::{
    app_state::{AppContext, AppState},
    entry::{Entry, EntryWriter},
    wikidata_writer::WikidataWriter,
};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use wikimisc::wikibase::ItemEntity;

#[derive(Debug)]
pub struct ItemCreator {
    app: Arc<dyn AppContext>,
    /// Wikidata write session. Production code holds a real `Wikidata`
    /// (boxed); tests substitute `MockWikidataWriter` via `new_with_writer`.
    wikidata: Box<dyn WikidataWriter>,
    entries: HashMap<usize, Entry>,
}

impl ItemCreator {
    pub fn new(app: &AppState) -> Self {
        Self::new_with_writer(app, Box::new(app.wikidata().clone()))
    }

    pub(crate) fn new_with_writer(app: &AppState, wikidata: Box<dyn WikidataWriter>) -> Self {
        let app: Arc<dyn AppContext> = Arc::new(app.clone());
        Self {
            app,
            wikidata,
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
        self.entries.extend(
            entries
                .iter()
                .filter_map(|entry| Some((entry.id?, entry.clone()))),
        );
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
        let entries = Entry::multiple_from_ids(&entry_ids, self.app.as_ref()).await?;
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
            let entry = match self.entries.get_mut(&entry_id) {
                Some(entry) => entry,
                None => continue, // This should never happen, but...
            };

            // Check out this entries aux values,
            // and find other entries that have the same aux values,
            // for specific properties (eg VIAF, GND)
            let mut aux_vec = EntryWriter::new(self.app.as_ref(), entry).get_aux().await.unwrap_or_default();
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
            EntryWriter::new(self.app.as_ref(), entry).add_to_item(&mut item).await?;
        }
        Ok(item)
    }

    pub async fn create_and_match_item(&mut self) -> Result<ItemEntity> {
        self.assert_at_least_one_entry()?;
        let comment = self.generate_item_creation_comment();
        let item = self.generate_item().await?;
        let new_id = self
            .wikidata
            .create_new_wikidata_item(&item, &comment)
            .await?;
        let _ = self.wikidata.perform_ac2wd(&new_id).await; // Ignore error
        for e in self.entries.values_mut() {
            let _ = EntryWriter::new(self.app.as_ref(), e).set_match(&new_id, 4).await;
        }
        Ok(item)
    }

    fn assert_at_least_one_entry(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Err(anyhow!("No entries to create item from"));
        }
        Ok(())
    }

    fn generate_item_creation_comment(&self) -> String {
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
        format!("Created from Mix'n'Match {etext} {}", entry_links.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use crate::wikidata_writer::MockWikidataWriter;
    use wikimisc::wikibase::{EntityTrait, LocaleString};

    /// Verifies that `generate_item` assembles the correct Wikidata claims from
    /// an entry's type, person dates, and name.
    ///
    /// Aux-based claims (P227/GND etc.) are intentionally omitted: resolving
    /// the property data-type requires a live Wikidata API call, and that path
    /// is already exercised by the auxiliary-matcher tests.
    #[tokio::test]
    async fn test_generate_item() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_entry_with_name("Fritz Koch").await.unwrap();
        test_support::seed_person_dates(entry_id, "1869", "1941").await.unwrap();
        let mut ic = ItemCreator::new(&app);
        ic.add_entries_by_id(&[entry_id]).await.unwrap();
        let item = ic.generate_item().await.unwrap();
        let claims = item.claims();
        assert_eq!(claims.iter().filter(|c| c.property() == "P31").count(), 1);
        assert_eq!(claims.iter().filter(|c| c.property() == "P569").count(), 1);
        assert_eq!(claims.iter().filter(|c| c.property() == "P570").count(), 1);
        assert_eq!(*item.labels(), [LocaleString::new("mul", "Fritz Koch")]);
    }

    #[tokio::test]
    async fn test_new_with_writer_stores_mock_and_as_any_works() {
        let app = test_support::test_app().await;
        let mut ic = ItemCreator::new_with_writer(&app, Box::new(MockWikidataWriter::new()));
        // No entries → errors before any writer calls.
        let err = ic.create_and_match_item().await.unwrap_err();
        assert!(err.to_string().contains("No entries"));
        // Downcast must succeed and no calls should have been recorded.
        let mock = ic.wikidata
            .as_any()
            .downcast_ref::<MockWikidataWriter>()
            .expect("downcast to MockWikidataWriter should succeed");
        assert_eq!(mock.create_calls.len(), 0);
        assert_eq!(mock.ac2wd_calls.len(), 0);
    }

    #[tokio::test]
    async fn test_create_and_match_item_calls_writer() {
        let app = test_support::test_app().await;
        let (_, entry_id) = test_support::seed_minimal_entry(&app).await.unwrap();
        let mut mock = MockWikidataWriter::new();
        mock.next_qid = Some("Q-MOCK-ITEM".to_string());
        let mut ic = ItemCreator::new_with_writer(&app, Box::new(mock));
        ic.add_entries_by_id(&[entry_id]).await.unwrap();
        ic.create_and_match_item().await.unwrap();
        let mock = ic.wikidata
            .as_any()
            .downcast_ref::<MockWikidataWriter>()
            .expect("should be MockWikidataWriter");
        assert_eq!(mock.create_calls.len(), 1, "create_new_wikidata_item called once");
        assert_eq!(mock.ac2wd_calls, vec!["Q-MOCK-ITEM"], "perform_ac2wd called with the new QID");
    }
}
