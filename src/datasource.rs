use crate::app_state::AppState;
use crate::autoscrape::Autoscrape;
use crate::update_catalog::UpdateCatalogError;
use anyhow::Result;
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::json;
use std::collections::HashMap;
use std::env::temp_dir;
use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use uuid::Uuid;

lazy_static! {
    static ref RE_PATTERN_WRAP_REMOVAL: Regex =
        Regex::new(r"^\|(.+)\|$").expect("Regexp construction");
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct LineCounter {
    pub all: usize,
    pub added: usize,
    pub updates: usize,
    pub offset: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum DataSourceType {
    Unknown,
    Csv,
    Tsv,
}

impl DataSourceType {
    //TODO test
    fn from_str(s: &str) -> DataSourceType {
        match s.to_string().trim().to_uppercase().as_str() {
            "CSV" => Self::Csv,
            "TSV" => Self::Tsv,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DataSourceLocation {
    Url(String),
    FilePath(String),
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub use_column_label: String,
    pub column_number: usize,
    pub pattern: Regex,
}

impl Pattern {
    //TODO test
    fn from_json(use_column_label: &str, data: &serde_json::Value) -> Result<Self> {
        let pattern = match data.get("pattern") {
            Some(col) => col.as_str().ok_or(UpdateCatalogError::BadPattern)?,
            None => return Err(UpdateCatalogError::BadPattern.into()),
        };
        let patterns = match RE_PATTERN_WRAP_REMOVAL.captures(pattern) {
            Some(patterns) => patterns,
            None => return Err(UpdateCatalogError::BadPattern.into()),
        };
        let pattern = match patterns.get(1).map(|s| s.as_str()) {
            Some(s) => s,
            None => pattern,
        };
        Ok(Self {
            use_column_label: use_column_label.to_string(),
            column_number: match data.get("col") {
                Some(col) => col.as_u64().ok_or(UpdateCatalogError::BadPattern)? as usize,
                None => return Err(UpdateCatalogError::BadPattern.into()),
            },
            pattern: Regex::new(pattern)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DataSource {
    pub catalog_id: usize,
    pub json: serde_json::Value,
    _columns: Vec<String>,
    pub just_add: bool,
    pub min_cols: usize,
    pub num_header_rows: u64,
    pub skip_first_rows: u64,
    pub ext_id_column: usize,
    pub patterns: Vec<Pattern>,
    pub tmp_file: Option<OsString>,
    pub colmap: HashMap<String, usize>,
    pub default_type: Option<String>,
    pub url_pattern: Option<String>,
    _update_existing_description: Option<bool>,
    _update_all_descriptions: Option<bool>,
    pub fail_on_error: bool,
    pub line_counter: LineCounter,
    pub rows_to_skip: u64, // Modified at runtime
    pub offset: usize,     // Set at runtime
}

impl DataSource {
    //TODO test
    pub fn new(catalog_id: usize, json: &serde_json::Value) -> Result<Self> {
        let columns = Self::extract_columns(json);
        let patterns = Self::extract_patterns(json);
        let colmap = Self::get_colmap(&columns);
        let ext_id_column = Self::get_ext_id_column(&colmap)?;
        let min_cols = Self::extract_min_cols(json, &columns);
        let mut ret = Self {
            catalog_id,
            json: json.clone(),
            _columns: columns,
            just_add: Self::extract_bool("just_add", json),
            min_cols: min_cols as usize,
            num_header_rows: Self::extract_u64("num_header_rows", json),
            skip_first_rows: Self::extract_u64("skip_first_rows", json),
            ext_id_column,
            patterns,
            colmap,
            default_type: json
                .get("default_type")
                .and_then(|v| v.as_str().map(|s| s.to_string())),
            url_pattern: json
                .get("url_pattern")
                .and_then(|v| v.as_str().map(|s| s.to_string())),
            _update_existing_description: json
                .get("update_existing_description")
                .and_then(|v| v.as_bool()),
            _update_all_descriptions: json
                .get("update_all_descriptions")
                .map(|v| v.as_bool())
                .unwrap_or(None),
            line_counter: LineCounter::default(),
            fail_on_error: false, // TODO?
            tmp_file: None,
            rows_to_skip: 0,
            offset: 0,
        };
        ret.rows_to_skip = ret.num_header_rows + ret.skip_first_rows;
        Ok(ret)
    }

    //TODO test
    async fn fetch_url(&self, url: &String, file_name: &Path) -> Result<()> {
        let response = Autoscrape::reqwest_client_external()?
            .get(url)
            .send()
            .await?;
        let mut file = std::fs::File::create(file_name)?;
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut file)?;
        Ok(())
    }

    //TODO test
    pub fn clear_tmp_file(&self) {
        if let Some(path) = &self.tmp_file {
            let _ = fs::remove_file(path);
        };
    }

    //TODO test
    pub async fn get_reader(&mut self, app: &AppState) -> Result<csv::Reader<File>> {
        let mut builder = csv::ReaderBuilder::new();
        let builder = builder.flexible(true).has_headers(false);
        let builder = match self.get_source_type(app).await? {
            DataSourceType::Csv => builder.delimiter(b','),
            DataSourceType::Tsv => builder.delimiter(b'\t'),
            DataSourceType::Unknown => return Err(UpdateCatalogError::MissingDataSourceType.into()),
        };
        match self.get_source_location(app)? {
            DataSourceLocation::Url(url) => {
                let mut full_path = temp_dir();
                let file_name = format!("{}.tmp", Uuid::new_v4());
                full_path.push(file_name);
                let full_path = full_path.as_path();
                let full_path_string = OsString::from(full_path);
                self.tmp_file = Some(full_path_string);
                self.fetch_url(&url, full_path).await?;
                let builder = builder.from_path(full_path)?;
                Ok(builder)
            }
            DataSourceLocation::FilePath(path) => Ok(builder.from_path(path)?),
        }
    }

    pub fn get_source_location(&self, app: &AppState) -> Result<DataSourceLocation> {
        if let Some(url) = self.json.get("source_url") {
            if let Some(url) = url.as_str() {
                return Ok(DataSourceLocation::Url(url.to_string()));
            }
        };
        if let Some(uuid) = self.json.get("file_uuid") {
            if let Some(uuid) = uuid.as_str() {
                let path = format!("{}/{}", app.import_file_path(), uuid);
                return Ok(DataSourceLocation::FilePath(path));
            }
        };
        Err(UpdateCatalogError::MissingDataSourceLocation.into())
    }

    //TODO test
    async fn get_source_type(&self, app: &AppState) -> Result<DataSourceType> {
        if let Some(s) = self.json.get("data_format") {
            return Ok(DataSourceType::from_str(s.as_str().unwrap_or("")));
        };
        if let Some(file_uuid_value) = self.json.get("file_uuid") {
            if let Some(uuid) = file_uuid_value.as_str() {
                let mut results = app.storage().get_data_source_type_for_uuid(uuid).await?;
                if let Some(type_name) = results.pop() {
                    return Ok(DataSourceType::from_str(&type_name));
                }
            }
        }
        Ok(DataSourceType::Unknown)
    }

    fn extract_u64(key: &str, json: &serde_json::Value) -> u64 {
        json.get(key).unwrap_or(&json! {0}).as_u64().unwrap_or(0)
    }

    fn extract_bool(key: &str, json: &serde_json::Value) -> bool {
        json.get(key)
            .map(|v| v.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    fn extract_min_cols(json: &serde_json::Value, columns: &[String]) -> u64 {
        let min_cols = json
            .get("min_cols")
            .map(|v| v.as_u64().unwrap_or(columns.len() as u64))
            .unwrap_or_else(|| columns.len() as u64);
        min_cols
    }

    fn get_ext_id_column(colmap: &HashMap<String, usize>) -> Result<usize> {
        let ext_id_column = colmap
            .get("id")
            .ok_or(Box::new(UpdateCatalogError::MissingColumn))?;
        let _ = colmap
            .get("name")
            .ok_or(Box::new(UpdateCatalogError::MissingColumn))?;
        Ok(*ext_id_column)
    }

    fn get_colmap(columns: &[String]) -> HashMap<String, usize> {
        let colmap: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .filter_map(|(num, col)| {
                let col = col.trim();
                if col.is_empty() {
                    None
                } else {
                    Some((col.to_string(), num))
                }
            })
            .collect();
        colmap
    }

    fn extract_patterns(json: &serde_json::Value) -> Vec<Pattern> {
        let patterns = json
            .get("patterns")
            .cloned()
            .unwrap_or_else(|| json!({}))
            .as_object()
            .cloned()
            .unwrap_or_default();
        let patterns = patterns
            .iter()
            .filter_map(|(k, v)| Pattern::from_json(k, v).ok())
            .collect();
        patterns
    }

    fn extract_columns(json: &serde_json::Value) -> Vec<String> {
        let columns: Vec<String> = match json.get("columns") {
            Some(c) => c.as_array().unwrap_or(&vec![]).to_owned(),
            None => vec![],
        }
        .iter()
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
        columns
    }
}
