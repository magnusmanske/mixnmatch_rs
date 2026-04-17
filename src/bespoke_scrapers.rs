use crate::{
    app_state::AppState, entry::Entry, extended_entry::ExtendedEntry,
};
use anyhow::Result;
use async_trait::async_trait;
use log::info;
use regex::Regex;
use std::collections::HashMap;

pub mod scraper_53;
pub mod scraper_85;
pub mod scraper_121;
pub mod scraper_122;
pub mod scraper_722;
pub mod scraper_1178;
pub mod scraper_1223;
pub mod scraper_1379;
pub mod scraper_1619;
pub mod scraper_2670;
pub mod scraper_4825;
pub mod scraper_5100;
pub mod scraper_5103;
pub mod scraper_5311;
pub mod scraper_6479;
pub mod scraper_6794;
pub mod scraper_6975;
pub mod scraper_6976;
pub mod scraper_7043;
pub mod scraper_7433;
pub mod scraper_7696;
pub mod scraper_7697;
pub mod scraper_7700;

pub use scraper_53::BespokeScraper53;
pub use scraper_85::BespokeScraper85;
pub use scraper_121::BespokeScraper121;
pub use scraper_122::BespokeScraper122;
pub use scraper_722::BespokeScraper722;
pub use scraper_1178::BespokeScraper1178;
pub use scraper_1223::BespokeScraper1223;
pub use scraper_1379::BespokeScraper1379;
pub use scraper_1619::BespokeScraper1619;
pub use scraper_2670::BespokeScraper2670;
pub use scraper_4825::BespokeScraper4825;
pub use scraper_5100::BespokeScraper5100;
pub use scraper_5103::BespokeScraper5103;
pub use scraper_5311::BespokeScraper5311;
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
#[allow(clippy::cognitive_complexity)]
pub async fn run_bespoke_scraper(catalog_id: usize, app: &AppState) -> Result<()> {
    match catalog_id {
        53 => BespokeScraper53::new(app).run().await,
        85 => BespokeScraper85::new(app).run().await,
        121 => BespokeScraper121::new(app).run().await,
        122 => BespokeScraper122::new(app).run().await,
        722 => BespokeScraper722::new(app).run().await,
        1178 => BespokeScraper1178::new(app).run().await,
        1223 => BespokeScraper1223::new(app).run().await,
        1379 => BespokeScraper1379::new(app).run().await,
        1619 => BespokeScraper1619::new(app).run().await,
        2670 => BespokeScraper2670::new(app).run().await,
        4825 => BespokeScraper4825::new(app).run().await,
        5100 => BespokeScraper5100::new(app).run().await,
        5103 => BespokeScraper5103::new(app).run().await,
        5311 => BespokeScraper5311::new(app).run().await,
        6479 => BespokeScraper6479::new(app).run().await,
        6794 => BespokeScraper6794::new(app).run().await,
        6975 => BespokeScraper6975::new(app).run().await,
        6976 => BespokeScraper6976::new(app).run().await,
        7043 => BespokeScraper7043::new(app).run().await,
        7433 => BespokeScraper7433::new(app).run().await,
        7696 => BespokeScraper7696::new(app).run().await,
        7697 => BespokeScraper7697::new(app).run().await,
        7700 => BespokeScraper7700::new(app).run().await,
        other => Err(anyhow::anyhow!("No bespoke scraper for catalog {other}")),
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
