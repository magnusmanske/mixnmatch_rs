use crate::app_state::AppState;
use crate::match_state::MatchState;
use anyhow::Result;

pub struct Maintenance {
    app: AppState,
}

impl Maintenance {
    pub fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

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

    /// Removes P17 auxiliary values for entryies of type Q5 (human)
    pub async fn remove_p17_for_humans(&self) -> Result<()> {
        self.app.storage().remove_p17_for_humans().await
    }

    pub async fn cleanup_mnm_relations(&self) -> Result<()> {
        self.app.storage().cleanup_mnm_relations().await
    }

    /// Finds redirects in a batch of items, and changes app matches to their respective targets.
    async fn fix_redirected_items_batch(&self, unique_qs: &Vec<String>) -> Result<()> {
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
    async fn unlink_meta_items_batch(&self, unique_qs: &Vec<String>) -> Result<()> {
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
            let items: Vec<String> = items.iter().map(|q| format!("{}", q)).collect();
            self.app
                .storage()
                .maintenance_unlink_item_matches(items)
                .await?;
        }
        Ok(())
    }

    /// Finds some unmatched (Q5) entries where there is a (unique) full match for that name,
    /// and uses it as an auto-match
    pub async fn maintenance_automatch(&self) -> Result<()> {
        self.app.storage().maintenance_automatch().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_state::{get_test_app, TEST_MUTEX},
        entry::Entry,
    };

    const TEST_CATALOG_ID: usize = 5526;
    const TEST_ENTRY_ID: usize = 143962196;

    #[tokio::test]
    async fn test_unlink_meta_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Set a match to a disambiguation item
        let mut entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        entry.set_match("Q16456", 2).await.unwrap();

        // Remove matches to disambiguation items
        let maintenance = Maintenance::new(&app);
        maintenance
            .unlink_meta_items(TEST_CATALOG_ID, &MatchState::any_matched())
            .await
            .unwrap();

        // Check that removal was successful
        assert_eq!(Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap().q, None);
    }

    #[tokio::test]
    async fn test_fix_redirects() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .set_match("Q100000067", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.fix_redirects(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, Some(91013264));
    }

    #[tokio::test]
    async fn test_unlink_deleted_items() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();
        Entry::from_id(TEST_ENTRY_ID, &app)
            .await
            .unwrap()
            .set_match("Q115205673", 2)
            .await
            .unwrap();
        let ms = Maintenance::new(&app);
        ms.unlink_deleted_items(TEST_CATALOG_ID, &MatchState::fully_matched())
            .await
            .unwrap();
        let entry = Entry::from_id(TEST_ENTRY_ID, &app).await.unwrap();
        assert_eq!(entry.q, None);
    }
}
