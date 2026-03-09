use crate::{
    app_state::AppState, entry::Entry, extended_entry::ExtendedEntry, php_wrapper::PhpWrapper,
};
use anyhow::Result;
use async_trait::async_trait;
use log::info;
use regex::Regex;
use std::collections::HashMap;

pub mod scraper_121;
pub mod scraper_6479;
pub mod scraper_6794;
pub mod scraper_6975;
pub mod scraper_6976;
pub mod scraper_7043;
pub mod scraper_7433;
pub mod scraper_7696;
pub mod scraper_7697;
pub mod scraper_7700;

pub use scraper_121::BespokeScraper121;
pub use scraper_6479::BespokeScraper6479;
pub use scraper_6794::BespokeScraper6794;
pub use scraper_6975::BespokeScraper6975;
pub use scraper_6976::BespokeScraper6976;
pub use scraper_7043::BespokeScraper7043;
pub use scraper_7433::BespokeScraper7433;
pub use scraper_7696::BespokeScraper7696;
pub use scraper_7697::BespokeScraper7697;
pub use scraper_7700::BespokeScraper7700;

/** WHEN YOU CREATE A NEW `BespokeScraper`, ALSO ADD IT HERE TO BE CALLED! **/
pub async fn run_bespoke_scraper(catalog_id: usize, app: &AppState) -> Result<()> {
    match catalog_id {
        121 => BespokeScraper121::new(app).run().await,
        6479 => BespokeScraper6479::new(app).run().await,
        6794 => BespokeScraper6794::new(app).run().await,
        6975 => BespokeScraper6975::new(app).run().await,
        6976 => BespokeScraper6976::new(app).run().await,
        7043 => BespokeScraper7043::new(app).run().await,
        7433 => BespokeScraper7433::new(app).run().await,
        7696 => BespokeScraper7696::new(app).run().await,
        7697 => BespokeScraper7697::new(app).run().await,
        7700 => BespokeScraper7700::new(app).run().await,
        other => PhpWrapper::bespoke_scraper(other).await, // PHP fallback
    }
}

#[async_trait]
pub trait BespokeScraper {
    fn new(app: &AppState) -> Self;
    fn catalog_id(&self) -> usize;
    fn app(&self) -> &AppState;
    async fn run(&self) -> Result<()>;

    fn testing(&self) -> bool {
        false
    }

    fn keep_existing_names(&self) -> bool {
        false
    }

    fn log(&self, msg: String) {
        if self.testing() {
            info!("{msg}");
        }
    }

    fn http_client(&self) -> reqwest::Client {
        reqwest::Client::new()
    }

    async fn load_single_line_text_from_url(&self, url: &str) -> Result<String> {
        let text = self
            .http_client()
            .get(url.to_owned())
            .send()
            .await?
            .text()
            .await?
            .replace("\n", ""); // Single line
        Ok(text)
    }

    async fn add_missing_aux(&self, entry_id: usize, prop_re: &[(usize, Regex)]) -> Result<()> {
        let entry = Entry::from_id(entry_id, self.app()).await?;
        let html = self.load_single_line_text_from_url(&entry.ext_url).await?;
        let mut new_aux: Vec<(usize, String)> = vec![];

        for (property, re) in prop_re.iter() {
            if let Some(caps) = re.captures(&html) {
                if let Some(id) = caps.get(1) {
                    new_aux.push((*property, id.as_str().to_string()));
                }
            }
        }

        if !new_aux.is_empty() {
            let existing_aux = entry.get_aux().await?;
            for (aux_p, aux_name) in new_aux {
                if !existing_aux
                    .iter()
                    .any(|a| a.prop_numeric() == aux_p && a.value() == aux_name)
                {
                    let _ = entry.set_auxiliary(aux_p, Some(aux_name)).await;
                }
            }
        }
        Ok(())
    }

    async fn process_cache(&self, entry_cache: &mut Vec<ExtendedEntry>) -> Result<()> {
        if entry_cache.is_empty() {
            return Ok(());
        }
        let ext_ids: Vec<String> = entry_cache.iter().map(|e| e.entry.ext_id.clone()).collect();
        let ext_id2id: HashMap<String, usize> = self
            .app()
            .storage()
            .get_entry_ids_for_ext_ids(self.catalog_id(), &ext_ids)
            .await?
            .into_iter()
            .collect();
        let entry_ids: Vec<usize> = ext_id2id.values().copied().collect();
        let existing_entries = Entry::multiple_from_ids(&entry_ids, self.app()).await?;
        for ext_entry in entry_cache {
            let ext_id = &ext_entry.entry.ext_id;
            let existing_entry = ext_id2id
                .get(ext_id)
                .map_or_else(|| None, |id| existing_entries.get(id).cloned());
            match existing_entry {
                Some(mut entry) => {
                    if self.keep_existing_names() {
                        ext_entry.entry.ext_name = entry.ext_name.to_string();
                    }
                    if self.testing() {
                        info!("EXISTS: {ext_entry:?}");
                    } else {
                        ext_entry.update_existing(&mut entry, self.app()).await?;
                    }
                }
                None => {
                    if self.testing() {
                        info!("CREATE: {ext_entry:?}");
                    } else {
                        ext_entry.insert_new(self.app()).await?;
                    }
                }
            };
        }
        Ok(())
    }
}
