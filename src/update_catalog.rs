use crate::app_state::AppState;
use crate::catalog::Catalog;
use crate::datasource::DataSource;
use crate::entry::*;
use crate::extended_entry::ExtendedEntry;
use crate::job::*;
use anyhow::Result;
use csv::StringRecord;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum UpdateCatalogError {
    NoUpdateInfoForCatalog,
    MissingColumn,
    MissingDataSourceLocation,
    MissingDataSourceType,
    NotEnoughColumns(usize),
    UnknownColumnLabel(String),
    BadPattern,
}

impl Error for UpdateCatalogError {}

impl fmt::Display for UpdateCatalogError {
    //TODO test
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UpdateCatalogError::NoUpdateInfoForCatalog => {
                write!(f, "UpdateCatalogError::NoUpdateInfoForCatalog")
            }
            UpdateCatalogError::MissingColumn => write!(f, "UpdateCatalogError::MissingColumn"),
            UpdateCatalogError::MissingDataSourceLocation => {
                write!(f, "UpdateCatalogError::MissingDataSourceLocation")
            }
            UpdateCatalogError::MissingDataSourceType => {
                write!(f, "UpdateCatalogError::MissingDataSourceType")
            }
            UpdateCatalogError::NotEnoughColumns(v) => write!(f, "NotEnoughColumns {v}"),
            UpdateCatalogError::UnknownColumnLabel(s) => write!(f, "UnknownColumnLabel {s}"),
            UpdateCatalogError::BadPattern => write!(f, "UpdateCatalogError::BadPattern"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct UpdateInfo {
    pub id: usize,
    pub catalog: usize,
    pub json: String,
    pub note: String,
    pub user_id: usize,
    pub is_current: u8,
}

impl UpdateInfo {
    //TODO test
    pub fn json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.json)
    }
}

impl Jobbable for UpdateCatalog {
    //TODO test
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    //TODO test
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }

    fn get_current_job_mut(&mut self) -> Option<&mut Job> {
        self.job.as_mut()
    }
}

#[derive(Debug, Clone)]
pub struct UpdateCatalog {
    app: AppState,
    job: Option<Job>,
}

impl UpdateCatalog {
    pub fn new(app: &AppState) -> Self {
        Self {
            app: app.clone(),
            job: None,
        }
    }

    fn update_from_tabbed_file_check_result(
        &self,
        result: Result<StringRecord, csv::Error>,
        datasource: &mut DataSource,
    ) -> Result<Option<StringRecord>> {
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                if datasource.fail_on_error {
                    return Err(e.into());
                } else {
                    return Ok(None);
                }
            }
        };
        if result.is_empty() {
            // Skip blank lines
            return Ok(None);
        }
        datasource.line_counter.all += 1;
        // TODO? read_max_rows but it's only used from the API so...
        if datasource.rows_to_skip > 0 {
            datasource.rows_to_skip -= 1;
            return Ok(None);
        }
        if result.len() < datasource.min_cols {
            if datasource.fail_on_error {
                return Err(
                    UpdateCatalogError::NotEnoughColumns(datasource.line_counter.all).into(),
                );
            }
            return Ok(None);
        }

        datasource.line_counter.offset += 1;
        if datasource.line_counter.offset < datasource.offset {
            return Ok(None);
        }
        Ok(Some(result))
    }

    /// Updates a catalog by reading a tabbed file.
    pub async fn update_from_tabbed_file(&mut self, catalog_id: usize) -> Result<()> {
        let update_info = self.get_update_info(catalog_id).await?;
        let json = update_info.json()?;
        let catalog = Catalog::from_id(catalog_id, &self.app).await?;
        let entries_already_in_catalog = catalog.number_of_entries().await?;
        let batch_size = 5000;
        let mut datasource = DataSource::new(catalog_id, &json)?;
        datasource.offset = self.get_last_job_offset().await;
        datasource.just_add = entries_already_in_catalog == 0 || datasource.just_add;
        let mut reader = datasource.get_reader(&self.app).await?;
        let mut row_cache = vec![];
        while let Some(result) = reader.records().next() {
            let result = match self.update_from_tabbed_file_check_result(result, &mut datasource)? {
                Some(result) => result,
                None => continue,
            };
            row_cache.push(result);
            if row_cache.len() >= batch_size {
                self.update_from_tabbed_file_process_row_cache(&mut datasource, &mut row_cache)
                    .await?;
            }
        }
        if let Err(e) = self.process_rows(&mut row_cache, &mut datasource).await {
            if datasource.fail_on_error {
                return Err(e);
            }
        }

        datasource.clear_tmp_file();
        let _ = self.clear_offset().await;

        /*
               $this->app->queue_job($this->catalog_id(),'microsync');
               $this->app->queue_job($this->catalog_id(),'automatch_by_search');
               if ( $this->has_born_died ) $this->app->queue_job($this->catalog_id(),'match_person_dates');
        */
        Ok(())
    }

    async fn update_from_tabbed_file_process_row_cache(
        &mut self,
        datasource: &mut DataSource,
        row_cache: &mut Vec<StringRecord>,
    ) -> Result<()> {
        if datasource.fail_on_error {
            self.process_rows(row_cache, datasource).await?
        } else {
            // Ignore error
            let _ = self.process_rows(row_cache, datasource).await;
        }
        let _ = self.remember_offset(datasource.line_counter.offset).await;
        Ok(())
    }

    //TODO test
    async fn process_rows(
        &self,
        rows: &mut Vec<csv::StringRecord>,
        datasource: &mut DataSource,
    ) -> Result<()> {
        let mut existing_ext_ids = HashSet::new();
        if datasource.just_add {
            let ext_ids: Vec<String> = rows
                .iter()
                .filter_map(|row| row.get(datasource.ext_id_column))
                .map(|s| s.to_string())
                .collect();
            existing_ext_ids = match self
                .get_existing_ext_ids(datasource.catalog_id, &ext_ids)
                .await
            {
                Ok(x) => x,
                Err(_e) => return Ok(()), // TODO is this the correct thing to do?
            }
        }
        for row in rows.iter() {
            let ext_id = match row.get(datasource.ext_id_column) {
                Some(ext_id) => ext_id,
                None => continue,
            };
            if existing_ext_ids.contains(ext_id) {
                // An entry with this ext_id already exists, and we only know that because just_add==true, so skip this
            } else if let Err(e) = self.process_row(row, datasource).await {
                if datasource.fail_on_error {
                    return Err(e);
                }
            }
        }
        rows.clear();
        Ok(())
    }

    //TODO test
    async fn process_row(
        &self,
        row: &csv::StringRecord,
        datasource: &mut DataSource,
    ) -> Result<()> {
        let ext_id = match row.get(datasource.ext_id_column) {
            Some(ext_id) => ext_id,
            None => return Ok(()), // TODO ???
        };
        match Entry::from_ext_id(datasource.catalog_id, ext_id, &self.app).await {
            Ok(mut entry) => {
                if !datasource.just_add {
                    let mut extended_entry = ExtendedEntry::from_row(row, datasource)?;
                    extended_entry
                        .update_existing(&mut entry, &self.app)
                        .await?;
                }
            }
            _ => {
                let mut extended_entry = ExtendedEntry::from_row(row, datasource)?;
                extended_entry.insert_new(&self.app).await?;
            }
        }
        Ok(())
    }

    //TODO test
    async fn get_existing_ext_ids(
        &self,
        catalog_id: usize,
        ext_ids: &[String],
    ) -> Result<HashSet<String>> {
        let mut ret = HashSet::new();
        if ext_ids.is_empty() {
            return Ok(ret);
        }
        let existing_ext_ids = self
            .app
            .storage()
            .get_existing_ext_ids(catalog_id, ext_ids)
            .await?;
        existing_ext_ids.iter().for_each(|ext_id| {
            ret.insert(ext_id.to_owned());
        });
        Ok(ret)
    }

    async fn get_update_info(&self, catalog_id: usize) -> Result<UpdateInfo> {
        let mut results = self
            .app
            .storage()
            .update_catalog_get_update_info(catalog_id)
            .await?;
        results
            .pop()
            .ok_or(UpdateCatalogError::NoUpdateInfoForCatalog.into())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        app_state::{get_test_app, TEST_MUTEX},
        datasource::DataSourceLocation,
        extended_entry::ExtendedEntry,
    };

    const TEST_CATALOG_ID: usize = 5526; // was 4175

    #[tokio::test]
    async fn test_get_source_location() {
        let app = get_test_app();

        let url = "http://www.example.org".to_string();
        let ds = DataSource::new(
            TEST_CATALOG_ID,
            &json!({"source_url":&url,"columns":["id","name"]}),
        )
        .unwrap();
        assert_eq!(
            ds.get_source_location(&app).unwrap(),
            DataSourceLocation::Url(url)
        );

        let uuid = "4b115b29-2ad9-4f43-90ed-7023b51a6337";
        let ds = DataSource::new(
            TEST_CATALOG_ID,
            &json!({"file_uuid":&uuid,"columns":["id","name"]}),
        )
        .unwrap();
        assert_eq!(
            ds.get_source_location(&app).unwrap(),
            DataSourceLocation::FilePath(format!("{}/{}", app.import_file_path(), uuid))
        );
    }

    #[tokio::test]
    async fn test_get_update_info() {
        let app = get_test_app();
        let uc = UpdateCatalog::new(&app);
        let info = uc.get_update_info(TEST_CATALOG_ID).await.unwrap();
        let json = info.json().unwrap();
        let type_name = json.get("default_type").unwrap().as_str().unwrap();
        assert_eq!(info.user_id, 2);
        assert_eq!(type_name, "Q5");
    }

    #[test]
    fn test_extended_entry() {
        assert_eq!(
            ExtendedEntry::parse_type("Q12345"),
            Some("Q12345".to_string())
        );
        assert_eq!(ExtendedEntry::parse_type("12345"), None);
        assert_eq!(ExtendedEntry::parse_type("foobar"), None);
        assert_eq!(ExtendedEntry::parse_type(""), None);

        assert_eq!(
            ExtendedEntry::parse_date("2022-11-03"),
            Some("2022-11-03".to_string())
        );
        assert_eq!(
            ExtendedEntry::parse_date("2022-11"),
            Some("2022-11".to_string())
        );
        assert_eq!(ExtendedEntry::parse_date("2022"), Some("2022".to_string()));
        assert_eq!(ExtendedEntry::parse_date("2"), None);
        assert_eq!(ExtendedEntry::parse_date("foobar"), None);
        assert_eq!(ExtendedEntry::parse_date(""), None);
    }

    #[tokio::test]
    async fn test_update_from_tabbed_file() {
        let _test_lock = TEST_MUTEX.lock();
        let app = get_test_app();

        // Delete the entry if it exists
        if let Ok(mut entry) = Entry::from_ext_id(TEST_CATALOG_ID, "n2014191777", &app).await {
            entry.delete().await.unwrap();
        }

        // Import single entry
        let mut uc = UpdateCatalog::new(&app);
        uc.update_from_tabbed_file(TEST_CATALOG_ID).await.unwrap();

        // Get new entry
        let mut entry = Entry::from_ext_id(TEST_CATALOG_ID, "n2014191777", &app)
            .await
            .unwrap();

        // Check base values
        assert_eq!(entry.ext_name, "Hauk Aabel");
        assert_eq!(
            entry.ext_url,
            "https://www.aspi.unimib.it/collections/entity/detail/n2014191777/"
        );
        assert_eq!(entry.type_name, Some("Q5".to_string()));

        // Check aux values
        let aux = entry.get_aux().await.unwrap();
        assert_eq!(aux.len(), 2);
        assert_eq!(
            aux.iter()
                .find(|row| row.prop_numeric == 213)
                .unwrap()
                .value,
            "0000 0000 6555 4670"
        );
        assert_eq!(
            aux.iter()
                .find(|row| row.prop_numeric == 214)
                .unwrap()
                .value,
            "91113950"
        );

        // Check person dates
        let (born, died) = entry.get_person_dates().await.unwrap();
        assert_eq!(born.unwrap(), "1869");
        assert_eq!(died.unwrap(), "1961");

        // Cleanup
        entry.delete().await.unwrap();
    }
}
