use std::collections::HashMap;

use crate::{app_state::AppState, entry::Entry};
use anyhow::Result;
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

    pub fn add_entry(&mut self, entry: Entry) {
        self.add_entries(&[entry]);
    }

    pub fn add_entries(&mut self, entries: &[Entry]) {
        self.entries
            .extend(entries.iter().map(|entry| (entry.id, entry.clone())));
    }

    pub async fn add_entries_by_id(&mut self, entry_ids: &[usize]) -> Result<()> {
        let entries = Entry::multiple_from_ids(entry_ids, &self.app).await?;
        self.entries.extend(entries);
        Ok(())
    }

    pub async fn generate_item(&self) -> Result<ItemEntity> {
        let mut item = ItemEntity::new_empty();
        for entry in self.entries.values() {
            entry.add_to_item(&mut item).await?;
        }
        Ok(item)
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
