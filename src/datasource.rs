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

#[derive(Debug, PartialEq, Eq, Clone, Default, Copy)]
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
    Ssv,
}

impl DataSourceType {
    //TODO test
    fn from_str(s: &str) -> DataSourceType {
        match s.trim().to_uppercase().as_str() {
            "CSV" => Self::Csv,
            "TSV" => Self::Tsv,
            "SSV" => Self::Ssv,
            _ => Self::Unknown,
        }
    }

    fn delimiter(&self) -> Option<u8> {
        match self {
            Self::Csv => Some(b','),
            Self::Tsv => Some(b'\t'),
            Self::Ssv => Some(b';'),
            Self::Unknown => None,
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
        let pattern = patterns.get(1).map(|s| s.as_str()).map_or(pattern, |s| s);
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
        let delim = self
            .get_source_type(app)
            .await?
            .delimiter()
            .ok_or(UpdateCatalogError::MissingDataSourceType)?;
        let builder = builder.delimiter(delim);
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

    /// Read the first row (treated as a header row) and up to `max_rows`
    /// data rows. Bypasses the `DataSource::new` invariant that `id`/`name`
    /// columns be present, because this is called *before* the user has
    /// mapped them.
    pub async fn read_headers_and_preview(
        app: &AppState,
        update_info: &serde_json::Value,
        max_rows: usize,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let (delim, location) = Self::location_and_delimiter(app, update_info).await?;

        // Download URL to a temp file if needed so csv::Reader can iterate
        // by path (simplest; CSV reader doesn't accept &str bytes directly
        // without an async->sync bridge).
        let path_buf;
        let path: &std::path::Path = match &location {
            DataSourceLocation::Url(url) => {
                let mut p = temp_dir();
                p.push(format!("{}.tmp", Uuid::new_v4()));
                Self::fetch_url_static(url, &p).await?;
                path_buf = p;
                path_buf.as_path()
            }
            DataSourceLocation::FilePath(s) => {
                path_buf = std::path::PathBuf::from(s);
                path_buf.as_path()
            }
        };

        let tmp_to_remove = matches!(location, DataSourceLocation::Url(_))
            .then(|| path_buf.clone());

        let mut builder = csv::ReaderBuilder::new();
        let mut reader = builder
            .flexible(true)
            .has_headers(false)
            .delimiter(delim)
            .from_path(path)?;
        let mut records = reader.records();

        let headers: Vec<String> = match records.next() {
            Some(Ok(rec)) => rec.iter().map(|s| s.to_string()).collect(),
            Some(Err(e)) => {
                if let Some(p) = tmp_to_remove {
                    let _ = fs::remove_file(p);
                }
                return Err(e.into());
            }
            None => vec![],
        };

        let mut preview: Vec<Vec<String>> = Vec::new();
        for _ in 0..max_rows {
            match records.next() {
                Some(Ok(rec)) => preview.push(rec.iter().map(|s| s.to_string()).collect()),
                Some(Err(_)) => continue,
                None => break,
            }
        }

        if let Some(p) = tmp_to_remove {
            let _ = fs::remove_file(p);
        }
        Ok((headers, preview))
    }

    /// Count rows (after the first header row) up to `max_rows`, plus how
    /// many of them yield a non-empty ext_id in the column mapped to `id`.
    /// Cheap dry-run for the Review step.
    pub async fn count_rows(
        app: &AppState,
        update_info: &serde_json::Value,
        max_rows: usize,
    ) -> Result<(usize, usize, usize)> {
        let (delim, location) = Self::location_and_delimiter(app, update_info).await?;

        let path_buf;
        let path: &std::path::Path = match &location {
            DataSourceLocation::Url(url) => {
                let mut p = temp_dir();
                p.push(format!("{}.tmp", Uuid::new_v4()));
                Self::fetch_url_static(url, &p).await?;
                path_buf = p;
                path_buf.as_path()
            }
            DataSourceLocation::FilePath(s) => {
                path_buf = std::path::PathBuf::from(s);
                path_buf.as_path()
            }
        };
        let tmp_to_remove = matches!(location, DataSourceLocation::Url(_))
            .then(|| path_buf.clone());

        // Which column is `id`?  columns is parallel to headers; value is
        // the label the user mapped to that column ("id", "name", "?" …).
        let id_col_idx = update_info
            .get("columns")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                arr.iter()
                    .position(|c| c.as_str().map(|s| s == "id").unwrap_or(false))
            });

        let mut builder = csv::ReaderBuilder::new();
        let mut reader = builder
            .flexible(true)
            .has_headers(false)
            .delimiter(delim)
            .from_path(path)?;
        let mut records = reader.records();

        // Consume headers row
        let _ = records.next();

        let mut total = 0usize;
        let mut with_id = 0usize;
        let mut errors = 0usize;
        for _ in 0..max_rows {
            match records.next() {
                Some(Ok(rec)) => {
                    total += 1;
                    if let Some(idx) = id_col_idx {
                        if rec.get(idx).map(|s| !s.trim().is_empty()).unwrap_or(false) {
                            with_id += 1;
                        }
                    }
                }
                Some(Err(_)) => errors += 1,
                None => break,
            }
        }

        if let Some(p) = tmp_to_remove {
            let _ = fs::remove_file(p);
        }
        Ok((total, with_id, errors))
    }

    async fn location_and_delimiter(
        app: &AppState,
        update_info: &serde_json::Value,
    ) -> Result<(u8, DataSourceLocation)> {
        // data_format in the update_info wins; fall back to import_file.type for
        // uploaded files.
        let data_format = update_info
            .get("data_format")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let ds_type = if let Some(df) = &data_format {
            DataSourceType::from_str(df)
        } else if let Some(uuid) = update_info.get("file_uuid").and_then(|v| v.as_str()) {
            let results = app.storage().get_data_source_type_for_uuid(uuid).await?;
            results
                .into_iter()
                .next()
                .map(|s| DataSourceType::from_str(&s))
                .unwrap_or(DataSourceType::Unknown)
        } else {
            DataSourceType::Unknown
        };
        let delim = ds_type
            .delimiter()
            .ok_or(UpdateCatalogError::MissingDataSourceType)?;
        let location = if let Some(url) = update_info.get("source_url").and_then(|v| v.as_str()) {
            if url.is_empty() {
                if let Some(uuid) = update_info.get("file_uuid").and_then(|v| v.as_str()) {
                    DataSourceLocation::FilePath(format!("{}/{}", app.import_file_path(), uuid))
                } else {
                    return Err(UpdateCatalogError::MissingDataSourceLocation.into());
                }
            } else {
                DataSourceLocation::Url(url.to_string())
            }
        } else if let Some(uuid) = update_info.get("file_uuid").and_then(|v| v.as_str()) {
            DataSourceLocation::FilePath(format!("{}/{}", app.import_file_path(), uuid))
        } else {
            return Err(UpdateCatalogError::MissingDataSourceLocation.into());
        };
        Ok((delim, location))
    }

    async fn fetch_url_static(url: &str, file_name: &Path) -> Result<()> {
        let response = Autoscrape::reqwest_client_external()?
            .get(url)
            .send()
            .await?;
        let mut file = std::fs::File::create(file_name)?;
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut file)?;
        Ok(())
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
        json.get("min_cols")
            .map(|v| v.as_u64().unwrap_or(columns.len() as u64))
            .unwrap_or_else(|| columns.len() as u64)
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
        patterns
            .iter()
            .filter_map(|(k, v)| Pattern::from_json(k, v).ok())
            .collect()
    }

    fn extract_columns(json: &serde_json::Value) -> Vec<String> {
        let columns: Vec<String> = json
            .get("columns")
            .map_or_else(Vec::new, |c| c.as_array().unwrap_or(&vec![]).to_owned())
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect();
        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_source_type_from_str() {
        assert_eq!(DataSourceType::from_str("CSV"), DataSourceType::Csv);
        assert_eq!(DataSourceType::from_str("csv"), DataSourceType::Csv);
        assert_eq!(DataSourceType::from_str("  csv  "), DataSourceType::Csv);
        assert_eq!(DataSourceType::from_str("TSV"), DataSourceType::Tsv);
        assert_eq!(DataSourceType::from_str("tsv"), DataSourceType::Tsv);
        assert_eq!(DataSourceType::from_str("  tsv  "), DataSourceType::Tsv);
        assert_eq!(DataSourceType::from_str(""), DataSourceType::Unknown);
        assert_eq!(DataSourceType::from_str("json"), DataSourceType::Unknown);
        assert_eq!(DataSourceType::from_str("XML"), DataSourceType::Unknown);
    }

    #[test]
    fn test_line_counter_default() {
        let lc = LineCounter::default();
        assert_eq!(lc.all, 0);
        assert_eq!(lc.added, 0);
        assert_eq!(lc.updates, 0);
        assert_eq!(lc.offset, 0);
    }

    #[test]
    fn test_data_source_location_equality() {
        let url1 = DataSourceLocation::Url("http://example.com".to_string());
        let url2 = DataSourceLocation::Url("http://example.com".to_string());
        let url3 = DataSourceLocation::Url("http://other.com".to_string());
        let file1 = DataSourceLocation::FilePath("/tmp/test".to_string());
        assert_eq!(url1, url2);
        assert_ne!(url1, url3);
        assert_ne!(url1, file1);
    }

    #[test]
    fn test_extract_columns() {
        let json = serde_json::json!({"columns": ["id", "name", "desc"]});
        let cols = DataSource::extract_columns(&json);
        assert_eq!(cols, vec!["id", "name", "desc"]);
    }

    #[test]
    fn test_extract_columns_empty() {
        let json = serde_json::json!({});
        let cols = DataSource::extract_columns(&json);
        assert!(cols.is_empty());
    }

    #[test]
    fn test_extract_bool() {
        let json = serde_json::json!({"flag": true, "other": "not_bool"});
        assert!(DataSource::extract_bool("flag", &json));
        assert!(!DataSource::extract_bool("other", &json));
        assert!(!DataSource::extract_bool("missing", &json));
    }

    #[test]
    fn test_extract_u64() {
        let json = serde_json::json!({"num": 42});
        assert_eq!(DataSource::extract_u64("num", &json), 42);
        assert_eq!(DataSource::extract_u64("missing", &json), 0);
    }

    #[test]
    fn test_get_colmap() {
        let columns = vec![
            "id".to_string(),
            "name".to_string(),
            "".to_string(),
            "desc".to_string(),
        ];
        let colmap = DataSource::get_colmap(&columns);
        assert_eq!(colmap.get("id"), Some(&0));
        assert_eq!(colmap.get("name"), Some(&1));
        assert_eq!(colmap.get("desc"), Some(&3));
        assert!(!colmap.contains_key("")); // empty column names are skipped
        assert_eq!(colmap.len(), 3);
    }

    #[test]
    fn test_get_ext_id_column() {
        let mut colmap = std::collections::HashMap::new();
        colmap.insert("id".to_string(), 0_usize);
        colmap.insert("name".to_string(), 1_usize);
        assert_eq!(DataSource::get_ext_id_column(&colmap).unwrap(), 0);
    }

    #[test]
    fn test_get_ext_id_column_missing_id() {
        let mut colmap = std::collections::HashMap::new();
        colmap.insert("name".to_string(), 1_usize);
        assert!(DataSource::get_ext_id_column(&colmap).is_err());
    }

    #[test]
    fn test_get_ext_id_column_missing_name() {
        let mut colmap = std::collections::HashMap::new();
        colmap.insert("id".to_string(), 0_usize);
        assert!(DataSource::get_ext_id_column(&colmap).is_err());
    }
}
