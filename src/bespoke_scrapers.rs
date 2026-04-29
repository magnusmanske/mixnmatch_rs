use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use log::info;
use regex::Regex;
use std::collections::HashMap;

/// Generates the three identical boilerplate methods required by every `BespokeScraper` impl:
/// `new`, `catalog_id`, and `app`. Place this inside the `impl BespokeScraper for …` block,
/// followed by the scraper's unique `async fn run` implementation.
///
/// ```ignore
/// impl BespokeScraper for BespokeScraper53 {
///     scraper_boilerplate!(53);
///     async fn run(&self) -> Result<()> { … }
/// }
/// ```
macro_rules! scraper_boilerplate {
    ($catalog_id:expr) => {
        fn new(app: &$crate::app_state::AppState) -> Self {
            Self { app: app.clone() }
        }
        fn catalog_id(&self) -> usize {
            $catalog_id
        }
        fn app(&self) -> &$crate::app_state::AppState {
            &self.app
        }
    };
}

pub mod scraper_1178;
pub mod scraper_121;
pub mod scraper_122;
pub mod scraper_1223;
pub mod scraper_1379;
pub mod scraper_1619;
pub mod scraper_2670;
pub mod scraper_2849;
pub mod scraper_2964;
pub mod scraper_3386;
pub mod scraper_3387;
pub mod scraper_3862;
pub mod scraper_4097;
pub mod scraper_4825;
pub mod scraper_5100;
pub mod scraper_5103;
pub mod scraper_53;
pub mod scraper_5311;
pub mod scraper_6479;
pub mod scraper_6794;
pub mod scraper_6975;
pub mod scraper_6976;
pub mod scraper_7043;
pub mod scraper_722;
pub mod scraper_7433;
pub mod scraper_7696;
pub mod scraper_7697;
pub mod scraper_7700;
pub mod scraper_85;

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
pub use scraper_2849::BespokeScraper2849;
pub use scraper_2964::BespokeScraper2964;
pub use scraper_3386::BespokeScraper3386;
pub use scraper_3387::BespokeScraper3387;
pub use scraper_3862::BespokeScraper3862;
pub use scraper_4097::BespokeScraper4097;
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

/// Erased async-fn signature: `(catalog_id, factory)`.
///
/// Each `factory` constructs the scraper struct and runs it; the
/// boxed future is what lets us hold heterogeneous scrapers behind
/// the same fn-pointer type. The trait can't be made object-safe
/// directly because of `fn new(app: &AppState) -> Self` (returns
/// `Self`).
type ScraperRunFn = for<'a> fn(&'a AppState) -> BoxFuture<'a, Result<()>>;

/// Build a `(catalog_id, ScraperRunFn)` entry for the registry.
/// Factor of three less per-scraper boilerplate vs the old match arm.
macro_rules! scraper_entry {
    ($id:literal, $ty:ty) => {
        (
            $id,
            (|app| Box::pin(async move { <$ty>::new(app).run().await })) as ScraperRunFn,
        )
    };
}

/// Dispatch table: ordered by catalog id for grep/diff-friendliness.
/// Add a new scraper by adding one line here and a `pub mod / pub use`
/// pair above; nothing else in this file changes.
const SCRAPER_REGISTRY: &[(usize, ScraperRunFn)] = &[
    scraper_entry!(53, BespokeScraper53),
    scraper_entry!(85, BespokeScraper85),
    scraper_entry!(121, BespokeScraper121),
    scraper_entry!(122, BespokeScraper122),
    scraper_entry!(722, BespokeScraper722),
    scraper_entry!(1178, BespokeScraper1178),
    scraper_entry!(1223, BespokeScraper1223),
    scraper_entry!(1379, BespokeScraper1379),
    scraper_entry!(1619, BespokeScraper1619),
    scraper_entry!(2670, BespokeScraper2670),
    scraper_entry!(2849, BespokeScraper2849),
    scraper_entry!(2964, BespokeScraper2964),
    scraper_entry!(3386, BespokeScraper3386),
    scraper_entry!(3387, BespokeScraper3387),
    scraper_entry!(3862, BespokeScraper3862),
    scraper_entry!(4097, BespokeScraper4097),
    scraper_entry!(4825, BespokeScraper4825),
    scraper_entry!(5100, BespokeScraper5100),
    scraper_entry!(5103, BespokeScraper5103),
    scraper_entry!(5311, BespokeScraper5311),
    scraper_entry!(6479, BespokeScraper6479),
    scraper_entry!(6794, BespokeScraper6794),
    scraper_entry!(6975, BespokeScraper6975),
    scraper_entry!(6976, BespokeScraper6976),
    scraper_entry!(7043, BespokeScraper7043),
    scraper_entry!(7433, BespokeScraper7433),
    scraper_entry!(7696, BespokeScraper7696),
    scraper_entry!(7697, BespokeScraper7697),
    scraper_entry!(7700, BespokeScraper7700),
];

pub async fn run_bespoke_scraper(catalog_id: usize, app: &AppState) -> Result<()> {
    SCRAPER_REGISTRY
        .iter()
        .find(|(id, _)| *id == catalog_id)
        .map(|(_, run)| *run)
        .ok_or_else(|| anyhow::anyhow!("No bespoke scraper for catalog {catalog_id}"))?(app)
    .await
}

/// Number of buffered entries that triggers an intermediate `process_cache` flush.
const CACHE_FLUSH_THRESHOLD: usize = 100;

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

    /// Default HTTP client for scrapers — re-uses the AppState's shared
    /// connection pool. Override per-scraper if a specialised client
    /// (custom UA, longer timeout, no proxy) is needed.
    fn http_client(&self) -> reqwest::Client {
        self.app().http_client().clone()
    }

    /// Push-and-flush helper: if `cache` has reached `CACHE_FLUSH_THRESHOLD`
    /// entries, call `process_cache` and clear it. Call once per loop iteration,
    /// then call `process_cache` unconditionally after the loop for the remainder.
    async fn maybe_flush_cache(&self, cache: &mut Vec<ExtendedEntry>) -> Result<()> {
        if cache.len() >= CACHE_FLUSH_THRESHOLD {
            self.process_cache(cache).await?;
            cache.clear();
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Registry must be ordered ascending and must not contain duplicates —
    /// both invariants are easy to break with a copy-paste edit, and the
    /// match-based predecessor would silently keep the first arm. Pin them.
    #[test]
    fn scraper_registry_is_sorted_and_unique() {
        for window in SCRAPER_REGISTRY.windows(2) {
            let (a, _) = window[0];
            let (b, _) = window[1];
            assert!(a < b, "scraper registry not ascending: {a} >= {b}");
        }
    }

    #[test]
    fn scraper_registry_contains_known_ids() {
        // Spot-check: every scraper module declared via `pub mod scraper_X`
        // up top must be findable by id. If a `pub mod` line is added but
        // the registry entry is forgotten, this fails.
        let ids: Vec<usize> = SCRAPER_REGISTRY.iter().map(|(id, _)| *id).collect();
        for expected in &[
            53_usize, 85, 121, 122, 722, 1178, 1223, 1379, 1619, 2670, 2849, 2964, 3386, 3387,
            3862, 4097, 4825, 5100, 5103, 5311, 6479, 6794, 6975, 6976, 7043, 7433, 7696, 7697,
            7700,
        ] {
            assert!(
                ids.contains(expected),
                "scraper_{expected} declared above but missing from SCRAPER_REGISTRY"
            );
        }
    }
}
