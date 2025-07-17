// DO NOT USE, NOT READY!
use anyhow::{anyhow, Result};
use chrono::Utc;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use crate::app_state::AppState;
use crate::catalog::Catalog;
use crate::entry::Entry;
use crate::extended_entry::ExtendedEntry;

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiScraper {
    pub id: usize,
    pub name: String,
    pub url: String,
    pub property: Option<usize>,
    pub language: String,
    pub status: String,
    pub url_pattern: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiScrapersResponse {
    pub scrapers: Vec<CerseiScraper>,
}

#[derive(Debug)]
pub struct CurrentScraper {
    pub cersei_scraper_id: usize,
    pub catalog_id: usize,
    pub last_sync: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiEntry {
    pub source_id: String,
    pub label: Option<String>,
    pub original_label: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub q: Option<String>,
    pub p569: Option<String>, // birth date
    pub p570: Option<String>, // death date
    pub p31: Option<usize>,   // instance of
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiEntriesResponse {
    pub entries: Vec<CerseiEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiRelationRow {
    pub e1_source_id: String,
    pub e2_scraper_id: usize,
    pub e2_source_id: String,
    pub property: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CerseiRelationsResponse {
    pub rows: Vec<CerseiRelationRow>,
}

#[derive(Debug)]
pub struct CerseiSync {
    app: AppState,
    http_client: reqwest::Client,
}

impl CerseiSync {
    pub fn new(app: &AppState) -> Result<Self> {
        Ok(Self {
            app: app.clone(),
            http_client: crate::autoscrape::Autoscrape::reqwest_client_external()?,
        })
    }

    /// Fetch scrapers from CERSEI API
    async fn get_cersei_scrapers(&self) -> Result<Vec<CerseiScraper>> {
        let url = "https://cersei.toolforge.org/api/scrapers";
        let response = self.http_client.get(url).send().await?;
        let scrapers_response: CerseiScrapersResponse = response.json().await?;
        Ok(scrapers_response.scrapers)
    }

    /// Update empty external URLs based on URL pattern
    async fn update_empty_ext_urls(
        &self,
        _scraper: &CerseiScraper,
        _catalog_id: usize,
    ) -> Result<()> {
        // DEACTIVATED inefficient; updates all external URLs every time
        // if let Some(url_pattern) = &scraper.url_pattern {
        //     if !url_pattern.is_empty() {
        //         let mut conn = self.pool.get_conn().await?;
        //         let sql = "UPDATE `entry` SET `ext_url`=replace('{$url_safe}','$1',ext_id) WHERE `catalog`={$catalog_id} AND `ext_url`=''";
        //         conn.exec_drop(sql, (url_pattern, catalog_id)).await?;
        //     }
        // }
        Ok(())
    }

    /// Create new catalogs for active scrapers
    pub async fn create_new_catalogs(&self) -> Result<()> {
        let scrapers_current = self.app.storage().get_cersei_scrapers().await?;
        let scrapers_cersei = self.get_cersei_scrapers().await?;

        for scraper in scrapers_cersei {
            if scraper.status != "active" {
                continue;
            }

            if let Some(current_scraper) = scrapers_current.get(&scraper.id) {
                // We have that scraper, just update empty URLs
                self.update_empty_ext_urls(&scraper, current_scraper.catalog_id)
                    .await?;
                continue;
            }

            let mut name = scraper.name.clone();
            let note = format!("CERSEI scraper #{}", scraper.id);

            // Check if catalog with that name exists
            loop {
                let catalog_with_name_exists = Catalog::from_name(&name, &self.app).await.is_ok();
                if catalog_with_name_exists {
                    name = format!("{name} [CERSEI]");
                } else {
                    break;
                }
            }

            // Create new catalog
            let mut catalog = Catalog::new(&self.app);
            catalog.set_name(Some(name.clone()));
            catalog.set_url(Some(scraper.url.clone()));
            catalog.set_wd_prop(scraper.property.to_owned());
            catalog.set_search_wp(&scraper.language);
            catalog.set_note(&note);
            catalog.set_owner(6);
            catalog.set_active(true);
            catalog.create_catalog().await?;

            // Link cersei and catalog
            self.app
                .storage()
                .add_cersei_catalog(catalog.get_valid_id()?, scraper.id)
                .await?;
        }

        Ok(())
    }

    /// Parse time precision from CERSEI format
    fn parse_time(time_precision: Option<&String>) -> Option<String> {
        if let Some(time_str) = time_precision {
            let parts: Vec<&str> = time_str.split('/').collect();
            if parts.len() == 2 {
                match parts[1] {
                    "9" => {
                        // Year precision
                        let re = Regex::new(r"^\+(\d+).*$").unwrap();
                        if let Some(caps) = re.captures(parts[0]) {
                            return Some(caps[1].to_string());
                        }
                    }
                    "10" => {
                        // Month precision
                        let re = Regex::new(r"^\+(\d+-\d{1,2}).*$").unwrap();
                        if let Some(caps) = re.captures(parts[0]) {
                            return Some(caps[1].to_string());
                        }
                    }
                    "11" => {
                        // Day precision
                        let re = Regex::new(r"^\+(\d+-\d{1,2}-\d{1,2}).*$").unwrap();
                        if let Some(caps) = re.captures(parts[0]) {
                            return Some(caps[1].to_string());
                        }
                    }
                    _ => return None,
                }
            }
        }
        None
    }

    /// Set human dates flag for catalog
    async fn set_human_dates_flag(&self, catalog_id: usize) -> Result<bool> {
        let mut catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let has_new_dates = catalog.check_and_set_person_date().await?;
        Ok(has_new_dates)
    }

    /// Queue a job (simplified - you may need to adapt based on your job queue system)
    async fn queue_job(&self, catalog_id: usize, job_type: &str) -> Result<()> {
        crate::job::Job::queue_simple_job(&self.app, catalog_id, job_type, None).await?;
        Ok(())
    }

    /// Add a new entry to the database
    async fn add_new_entry(&self, entry: &mut Entry) -> Result<usize> {
        entry.insert_as_new().await?;
        entry.get_valid_id()
    }

    /// Sync a single scraper
    #[allow(clippy::cognitive_complexity)]
    pub async fn sync_scraper(
        &self,
        scraper_id: usize,
        catalog_id: usize,
        last_sync: Option<&str>,
    ) -> Result<()> {
        // println!("Syncing scraper {} for catalog {}", scraper_id, catalog_id);

        let mut existing_ext_ids = self.app.storage().get_all_external_ids(catalog_id).await?;
        let start_time = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let mut offset = 0;
        let limit = 500;
        let mut added_new_entries = false;

        loop {
            let mut url = format!(
                "https://cersei.toolforge.org/api/get_entries/{scraper_id}?offset={offset}&limit={limit}&no_json=1&wide=1"
            );

            if let Some(sync_time) = last_sync {
                if !sync_time.is_empty() {
                    url = format!("{url}&revision_since={sync_time}");
                }
            }

            // Retry logic for API calls
            let mut response_data = None;
            for attempt in 0..=5 {
                match self.http_client.get(&url).send().await {
                    Ok(response) => {
                        match response.json::<CerseiEntriesResponse>().await {
                            Ok(data) => {
                                response_data = Some(data);
                                break;
                            }
                            Err(_e) => {
                                // eprintln!("Failed to parse JSON on attempt {}: {}", attempt + 1, e);
                                if attempt < 5 {
                                    thread::sleep(Duration::from_secs((attempt + 1) * 5));
                                    // Add cache-busting parameter
                                    url = format!(
                                        "{}&blah{}={}",
                                        url,
                                        rand::random::<u32>(),
                                        rand::random::<u32>()
                                    );
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        // eprintln!("HTTP request failed on attempt {}: {}", attempt + 1, e);
                        if attempt < 5 {
                            thread::sleep(Duration::from_secs((attempt + 1) * 5));
                        }
                    }
                }
            }

            let response_data =
                response_data.ok_or_else(|| anyhow!("CERSEI API failed after retries"))?;

            // Process entries
            for ce in &response_data.entries {
                let name = ce
                    .label
                    .as_ref()
                    .or(ce.original_label.as_ref())
                    .unwrap_or(&"".to_string())
                    .clone();

                if name.is_empty() || ce.source_id.is_empty() {
                    continue;
                }

                let ne_entry = Entry {
                    catalog: catalog_id,
                    ext_id: ce.source_id.clone(),
                    ext_name: name.chars().take(127).collect(),
                    ext_desc: ce.description.clone().unwrap_or_default(),
                    ext_url: ce.url.clone().unwrap_or_default(),
                    q: ce.q.as_ref().and_then(|q| q.parse::<isize>().ok()),
                    app: Some(self.app.clone()),
                    ..Default::default()
                };
                let mut ne = ExtendedEntry {
                    entry: ne_entry,
                    born: Self::parse_time(ce.p569.as_ref()),
                    died: Self::parse_time(ce.p570.as_ref()),
                    ..Default::default()
                };

                if let Some(p31) = ce.p31 {
                    ne.entry.type_name = Some(format!("Q{p31}"));
                }

                if let Some(&_entry_id) = existing_ext_ids.get(&ne.entry.ext_id) {
                    // TODO
                    // Update existing entry
                    // let mut conn = self.pool.get_conn().await?;
                    // let sql = "UPDATE `entry` SET `ext_name`=?, `ext_desc`=?, `type`=?, `ext_url`=? WHERE `id`=? AND (`ext_name`!=? OR `ext_desc`!=? OR `type`!=? OR `ext_url`!=?)";

                    // match conn
                    //     .exec_drop(
                    //         sql,
                    //         (
                    //             &ne.name,
                    //             &ne.desc,
                    //             &ne.entry_type,
                    //             &ne.url,
                    //             entry_id,
                    //             &ne.name,
                    //             &ne.desc,
                    //             &ne.entry_type,
                    //             &ne.url,
                    //         ),
                    //     )
                    //     .await
                    // {
                    //     Ok(_) => {}
                    //     Err(e) => {
                    //         eprintln!("Error updating entry {}: {}", entry_id, e);
                    //         continue;
                    //     }
                    // }

                    // Set person dates if available
                    if ne.born.is_some() || ne.died.is_some() {
                        ne.entry.set_person_dates(&ne.born, &ne.died).await?;
                    }
                } else {
                    // Create new entry
                    match self.add_new_entry(&mut ne.entry).await {
                        Ok(entry_id) => {
                            // Update our local cache
                            existing_ext_ids.insert(ne.entry.id.unwrap_or(0).to_string(), entry_id);
                            added_new_entries = true;

                            // Set person dates if available
                            if ne.born.is_some() || ne.died.is_some() {
                                ne.entry.set_person_dates(&ne.born, &ne.died).await?;
                            }
                        }
                        Err(_e) => {
                            // eprintln!("Error creating entry: {}", e);
                            continue;
                        }
                    }
                }
            }

            if response_data.entries.len() < limit {
                break; // No more data
            }
            offset += limit;
        }

        // Sync internal relations
        self.sync_internal_relations(scraper_id, last_sync).await?;

        // Update last sync time
        self.app
            .storage()
            .update_cersei_last_update(scraper_id, &start_time)
            .await?;

        self.app
            .storage()
            .catalog_refresh_overview_table(catalog_id)
            .await?;

        // Check for human dates and queue appropriate jobs
        if self.set_human_dates_flag(catalog_id).await? {
            self.queue_job(catalog_id, "match_person_dates").await?;
            self.queue_job(catalog_id, "match_on_birthdate").await?;
        }

        if added_new_entries {
            // Queue automatch jobs
            self.queue_job(catalog_id, "automatch_by_sitelink").await?;
            self.queue_job(catalog_id, "automatch_from_other_catalogs")
                .await?;
            self.queue_job(catalog_id, "automatch_by_search").await?;
        }

        // println!(
        //     "Completed syncing scraper {} for catalog {}",
        //     scraper_id, catalog_id
        // );
        Ok(())
    }

    /// Sync all scrapers
    pub async fn sync_all_scrapers(&self) -> Result<()> {
        let scrapers = self.app.storage().get_cersei_scrapers().await?;

        for (scraper_id, scraper_data) in scrapers {
            self.sync_scraper(
                scraper_id,
                scraper_data.catalog_id,
                scraper_data.last_sync.as_deref(),
            )
            .await?;
        }

        Ok(())
    }

    /// Sync internal relations between scrapers
    pub async fn sync_internal_relations(
        &self,
        scraper_id: usize,
        earliest: Option<&str>,
    ) -> Result<()> {
        let batch_size = 5000;
        let scrapers = self.app.storage().get_cersei_scrapers().await?;

        let mut scraper2catalog = HashMap::new();
        for (id, scraper) in &scrapers {
            scraper2catalog.insert(*id, scraper.catalog_id);
        }

        let _catalog_id = match scraper2catalog.get(&scraper_id) {
            Some(&id) => id,
            None => return Ok(()), // Scraper not found
        };

        let mut offset = 0;
        let earliest_param = earliest.unwrap_or("");

        loop {
            let url = format!(
                "https://cersei.toolforge.org/api/relations/{scraper_id}/{offset}?limit={batch_size}&earliest={earliest_param}"
            );

            let response = self.http_client.get(&url).send().await?;
            let relations: CerseiRelationsResponse = response.json().await?;

            let mut candidates: HashMap<usize, Vec<(String, usize, String)>> = HashMap::new();

            for row in &relations.rows {
                if let Some(&catalog_id2) = scraper2catalog.get(&row.e2_scraper_id) {
                    candidates.entry(row.property).or_default().push((
                        row.e1_source_id.clone(),
                        catalog_id2,
                        row.e2_source_id.clone(),
                    ));
                }
            }

            // Remove existing relations and add new ones
            for (_property, list) in candidates {
                if list.is_empty() {
                    continue;
                }

                // TODO
                // let mut conn = self.pool.get_conn().await?;

                // // Build query to find existing relations
                // let mut where_parts = Vec::new();
                // for (ext_id1, catalog2, ext_id2) in &list {
                //     where_parts.push(format!(
                //         "(e1.ext_id='{}' AND e2.catalog={} AND e2.ext_id='{}')",
                //         ext_id1.replace("'", "''"),
                //         catalog2,
                //         ext_id2.replace("'", "''")
                //     ));
                // }

                // let sql = format!(
                //     "SELECT e1.ext_id AS ext_id1, e2.catalog, e2.ext_id AS ext_id2 FROM mnm_relation, entry e1, entry e2 WHERE entry_id=e1.id AND target_entry_id=e2.id AND e1.catalog={} AND property={} AND ({})",
                //     catalog_id, property, where_parts.join(" OR ")
                // );

                // let existing_rows: Vec<Row> = conn.query(&sql).await?;
                // let mut existing_set = std::collections::HashSet::new();
                // for row in existing_rows {
                //     let ext_id1: String = row.get("ext_id1").unwrap();
                //     let catalog2: usize = row.get("catalog").unwrap();
                //     let ext_id2: String = row.get("ext_id2").unwrap();
                //     existing_set.insert((ext_id1, catalog2, ext_id2));
                // }

                // // Add new relations
                // for (ext_id1, catalog2, ext_id2) in list {
                //     if !existing_set.contains(&(ext_id1.clone(), catalog2, ext_id2.clone())) {
                //         let sql = "INSERT IGNORE INTO mnm_relation (entry_id, property, target_entry_id) SELECT (SELECT id FROM entry WHERE catalog=? AND ext_id=?) AS entry_id, ? AS property, (SELECT id FROM entry WHERE catalog=? AND ext_id=?) AS target_entry_id HAVING entry_id IS NOT NULL AND target_entry_id IS NOT NULL";

                //         match conn
                //             .exec_drop(sql, (catalog_id, &ext_id1, property, catalog2, &ext_id2))
                //             .await
                //         {
                //             Ok(_) => {}
                //             Err(e) => eprintln!("Error inserting relation: {}", e),
                //         }
                //     }
                // }
            }

            if relations.rows.len() < batch_size {
                break;
            }
            offset += batch_size;
        }

        Ok(())
    }
}

// Command-line interface
// pub async fn run_cersei_sync(pool: Pool, args: Vec<String>) -> Result<()> {
//     let cs = CerseiSync::new(pool);

//     if args.len() < 2 {
//         // Default: create new catalogs and sync all scrapers
//         cs.create_new_catalogs().await?;
//         cs.sync_all_scrapers().await?;
//         cs.app.storage().import_relations_into_aux().await?;
//     } else {
//         match args[1].as_str() {
//             "relations" => {
//                 if args.len() < 3 {
//                     eprintln!("Usage: relations <scraper_id>");
//                     return Ok(());
//                 }
//                 let scraper_id: usize = args[2].parse()?;
//                 cs.sync_internal_relations(scraper_id, None).await?;
//             }
//             "update_aux" => {
//                 cs.app.storage().import_relations_into_aux().await?;
//             }
//             _ => {
//                 eprintln!("Unknown command: {}", args[1]);
//                 eprintln!("Available commands: relations, update_aux");
//             }
//         }
//     }

//     Ok(())
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::get_test_app;

    #[tokio::test]
    async fn test_get_cersei_scrapers() {
        let app = get_test_app();
        let cs = CerseiSync::new(&app).unwrap();
        let scrapers = cs.get_cersei_scrapers().await.unwrap();
        assert!(scrapers.len() > 10);
    }
}
