use lazy_static::lazy_static;
use regex::Regex;
use std::path::Path;
use std::ffi::OsString;
use std::env::temp_dir;
use uuid::Uuid;
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use serde_json::json;
use mysql_async::prelude::*;
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::entry::*;
use crate::job::*;

lazy_static!{
    static ref RE_TYPE : Regex = Regex::new(r"^(Q\d+)$").unwrap();
    static ref RE_DATE : Regex = Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").unwrap();
    static ref RE_PROPERTY : Regex = Regex::new(r"^P(\d+)$").unwrap();
    static ref RE_ALIAS : Regex = Regex::new(r"^A([a-z]+)$").unwrap();
    static ref RE_DESCRIPTION : Regex = Regex::new(r"^D([a-z]+)$").unwrap();
    static ref RE_POINT : Regex = Regex::new(r"^\s*POINT\s*\(\s*(\S+?)[, ](\S+?)\s*\)\s*$").unwrap();
    static ref RE_LAT_LON : Regex = Regex::new(r"^(\S+)/(\S+)$").unwrap();
}

#[derive(Debug)]
enum UpdateCatalogError {
    NoUpdateInfoForCatalog,
    MissingColumn,
    MissingDataSourceLocation,
    MissingDataSourceType,
    NotEnoughColumns(usize),
    UnknownColumnLabel(String),
    RegexpCaptureError,
    BadCoordinates
}

impl Error for UpdateCatalogError {}

impl fmt::Display for UpdateCatalogError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self) // user-facing output
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct UpdateInfo {
    id: usize,
    catalog: usize,
    json: String,
    note: String,
    user_id: usize,
    is_current: u8
}

impl UpdateInfo {
    pub fn json(&self) -> Result<serde_json::Value,serde_json::Error> {
        serde_json::from_str(&self.json)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct LineCounter {
    pub all: usize,
    pub added: usize,
    pub updates: usize
}

impl LineCounter {
    pub fn new() -> Self {
        Self { all: 0, added: 0, updates: 0 }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum DataSourceType {
    Unknown,
    CSV,
    TSV
}

impl DataSourceType {
    fn from_str(s: &str) ->DataSourceType {
        match s.to_string().to_uppercase().as_str() {
            "CSV" => Self::CSV,
            "TSV" => Self::TSV,
            _ => Self::Unknown
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum DataSourceLocation {
    Url(String),
    FilePath(String)
}

#[derive(Debug, Clone)]
struct ExtendedEntry{
    pub entry: Entry,
    pub aux: HashMap<usize,String>,
    pub born: Option<String>,
    pub died: Option<String>,
    pub aliases: HashMap<String,String>,
    pub descriptions: HashMap<String,String>,
    pub location: Option<(f64,f64)> // lat,lon
}

impl ExtendedEntry {
    pub fn from_row(row: &csv::StringRecord, datasource: &mut DataSource) -> Result<Self,GenericError> {
        let ext_id = row.get(datasource.ext_id_column).ok_or(format!("No external ID for entry"))?;
        let mut ret = Self {
            entry:  Entry::from_catalog_and_ext_id(datasource.catalog_id, &ext_id),
            aux: HashMap::new(),
            born: None,
            died: None,
            aliases: HashMap::new(),
            descriptions: HashMap::new(),
            location: None
        };

        for (label,col_num) in datasource.colmap.iter() {
            let cell = match row.get(*col_num) {
                Some(cell) => cell,
                None => continue
            } ;
            if ret.parse_alias(&label,cell) || ret.parse_description(&label,cell) || ret.parse_property(&label,cell)? {
                continue;
            }

            match label.as_str() {
                "id" => { /* Already have that in entry */ }
                "name" => { ret.entry.ext_name = cell.to_owned() }
                "desc" => { ret.entry.ext_desc = cell.to_owned() }
                "url" => { ret.entry.ext_url = cell.to_owned() }
                "type" => { ret.entry.type_name = Self::parse_type(cell) }
                "born" => { ret.born = Self::parse_date(cell) }
                "died" => { ret.died = Self::parse_date(cell) }
                other => { return Err(Box::new(UpdateCatalogError::UnknownColumnLabel(format!("Don't understand label '{}'",other)))); }
            }
        }

        Ok(ret)
    }

    fn parse_type(type_name: &str) -> Option<String> {
        Self::get_capture(&RE_TYPE, type_name)
    }

    fn parse_date(date: &str) -> Option<String> {
        Self::get_capture(&RE_DATE, date)
    }

    fn parse_alias(&mut self, label: &str, cell: &str) -> bool {
        if let Some(s) = Self::get_capture(&RE_ALIAS, label) {
            self.aliases.insert(s, cell.to_string());
            true
        } else {
            false
        }
    }

    fn parse_description(&mut self, label: &str, cell: &str) -> bool {
        if let Some(s) = Self::get_capture(&RE_DESCRIPTION, label) {
            self.descriptions.insert(s, cell.to_string());
            true
        } else {
            false
        }
    }

    fn parse_property(&mut self, label: &str, cell: &str) -> Result<bool,GenericError> {
        let property_num = match Self::get_capture(&RE_PROPERTY, label) {
            Some(s) => s.parse::<usize>()?,
            None => return Ok(false)
        };

        // Convert from POINT
        let captures = RE_POINT.captures(cell).ok_or(UpdateCatalogError::RegexpCaptureError)?;
        let value= match captures.len() {
            3 => {
                match (captures.get(1),captures.get(2)) {
                    (Some(lon),Some(lat)) => format!("{},{}",lat.as_str(),lon.as_str()),
                    _ => cell.to_string()
                }
            }
            _ => cell.to_string()
        };

        // Do location if necessary
        // TODO get all location properties, not only P625 hardcoded
        if property_num == 625 {
            let captures = RE_LAT_LON.captures(&value).ok_or(UpdateCatalogError::RegexpCaptureError)?;
            if captures.len() == 3 {
                match (captures.get(1),captures.get(2)) {
                    (Some(lat),Some(lon)) => {
                        let lat = lat.as_str().to_string().parse::<f64>()?;
                        let lon = lon.as_str().to_string().parse::<f64>()?;
                        self.location = Some((lat,lon));
                    },
                    _ => return Err(Box::new(UpdateCatalogError::BadCoordinates))
                }
            }
        } else {
            self.aux.insert(property_num,value);
        }

        Ok(true)
    }

    fn get_capture(regexp: &Regex, text: &str) -> Option<String> {
        regexp.captures(text)?.get(1).map(|s|s.as_str().to_string())
    }
}

struct DataSource {
    catalog_id: usize,
    json: serde_json::Value,
    _columns: Vec<String>,
    just_add: bool,
    min_cols: usize,
    num_header_rows: u64,
    skip_first_rows: u64,
    ext_id_column: usize,
    patterns: serde_json::Map<String,serde_json::Value>,
    tmp_file: Option<OsString>,
    colmap: HashMap<String,usize>,
    line_counter: LineCounter
}

impl DataSource {
    fn new(catalog_id: usize, json: &serde_json::Value) -> Result<Self,GenericError> {
        let columns: Vec<String> = match json.get("columns") {
            Some(c) => c.as_array().unwrap_or(&vec!()).to_owned(),
            None => vec!()
        }.iter().filter_map(|v|v.as_str()).map(|s|s.to_string()).collect();
        let patterns = json
            .get("patterns")
            .map(|v|v.clone())
            .unwrap_or_else(|| json!({}))
            .as_object()
            .map(|v|v.clone())
            .unwrap_or(serde_json::Map::new());
        let colmap : HashMap<String,usize> = columns.iter().enumerate().filter_map(|(num,col)|{
            let col = col.trim();
            if col.is_empty() {
                None
            } else {
                Some((col.to_string(),num))
            }
        }).collect();

        // Paranoia
        let ext_id_column = *colmap.get("id").ok_or(Box::new(UpdateCatalogError::MissingColumn))?;
        let _ = colmap.get("name").ok_or(Box::new(UpdateCatalogError::MissingColumn))?;

        let min_cols = json.get("min_cols").map(|v|v.as_u64().unwrap_or(columns.len() as u64)).unwrap_or_else(||columns.len() as u64);
        Ok(Self {
            catalog_id,
            json: json.clone(),
            _columns: columns,
            just_add: json.get("just_add").map(|v|v.as_bool().unwrap_or(false)).unwrap_or(false),
            min_cols: min_cols as usize,
            num_header_rows: json.get("num_header_rows").map(|v|v.as_u64().unwrap_or(0)).unwrap_or(0),
            skip_first_rows: json.get("skip_first_rows").unwrap_or(&json!{0}).as_u64().unwrap_or(0),
            ext_id_column,
            patterns,
            colmap,
            line_counter: LineCounter::new(),
            tmp_file: None
        })
    }

    async fn fetch_url(&self, url: &String, file_name: &Path) -> Result<(),GenericError> {
        let response = reqwest::get(url).await?;
        let mut file = std::fs::File::create(file_name)?;
        let mut content =  Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut file)?;
        Ok(())
    }

    fn clear_tmp_file(&self) {
        if let Some(path) = &self.tmp_file {
            println!("Removing tmp file at {:?}",path);
            let _ = fs::remove_file(path);
        };
    }

    async fn get_reader(&mut self, mnm: &MixNMatch) -> Result<csv::Reader<File>,GenericError> {
        let mut builder = csv::ReaderBuilder::new();
        let builder = builder.flexible(true).has_headers(false);
        let builder = match self.get_source_type(mnm).await? {
            DataSourceType::CSV => builder.delimiter(b','),
            DataSourceType::TSV => builder.delimiter(b'\t'),
            DataSourceType::Unknown => return Err(Box::new(UpdateCatalogError::MissingDataSourceType))
        };
        match self.get_source_location(mnm)? {
            DataSourceLocation::Url(url) => {
                let mut full_path = temp_dir();
                let file_name = format!("{}.tmp", Uuid::new_v4());
                full_path.push(file_name);
                let full_path = full_path.as_path();
                let full_path_string = OsString::from(full_path);
                println!("Storing tmp file at {:?}", full_path);
                self.tmp_file = Some(full_path_string);
                self.fetch_url(&url,full_path).await?;
                let builder = builder.from_path(&full_path)?;
                Ok(builder)
            }
            DataSourceLocation::FilePath(path) => {
                println!("Local file: {}",&path);
                Ok(builder.from_path(path)?)
            }
        }
    }

    fn get_source_location(&self, mnm: &MixNMatch) -> Result<DataSourceLocation,GenericError> {
        if let Some(url) = self.json.get("source_url") {
            if let Some(url) = url.as_str() {
                return Ok(DataSourceLocation::Url(url.to_string()));
            }
        };
        if let Some(uuid) = self.json.get("file_uuid") {
            if let Some(uuid) = uuid.as_str() {
                let path = format!("{}/{}",mnm.import_file_path(),uuid);
                return Ok(DataSourceLocation::FilePath(path));
            }
        };
        Err(Box::new(UpdateCatalogError::MissingDataSourceLocation))
    }

    async fn get_source_type(&self, mnm: &MixNMatch) -> Result<DataSourceType,GenericError> {
        if let Some(s) = self.json.get("data_format") {
            return Ok(DataSourceType::from_str(s.as_str().unwrap_or("")));
        };
        if let Some(file_uuid_value) = self.json.get("file_uuid") {
            if let Some(uuid) = file_uuid_value.as_str() {
                let mut results: Vec<String> = "SELECT `type` FROM `import_file` WHERE `uuid`=:uuid"
                    .with(params!{uuid})
                    .map(mnm.app.get_mnm_conn()
                    .await?,
                |type_name|type_name)
                    .await?;
                if let Some(type_name) = results.pop() {return Ok(DataSourceType::from_str(&type_name));}
            }
        }
        Ok(DataSourceType::Unknown)
    }
}


impl Jobbable for UpdateCatalog {
    fn set_current_job(&mut self, job: &Job) {
        self.job = Some(job.clone());
    }
    fn get_current_job(&self) -> Option<&Job> {
        self.job.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct UpdateCatalog {
    mnm: MixNMatch,
    job: Option<Job>
}

impl UpdateCatalog {
    pub fn new(mnm: &MixNMatch) -> Self {
        Self {
            mnm: mnm.clone(),
            job: None
        }
    }

    pub async fn update_from_tabbed_file(&self, catalog_id: usize) -> Result<(),GenericError> {
        let update_info = self.get_update_info(catalog_id).await?;
        let json = update_info.json()?;
        let mut datasource = DataSource::new(catalog_id, &json)?;
        let batch_size = 5000;
        let entries_already_in_catalog = self.number_of_entries_in_catalog(catalog_id).await?;

        let mut rows_to_skip = datasource.num_header_rows + datasource.skip_first_rows;// TODO +  self.get_last_job_offset() ;
        datasource.just_add = entries_already_in_catalog==0 || datasource.just_add ;
        let mut reader = datasource.get_reader(&self.mnm).await?;

        let mut row_cache = vec![];
        while let Some(result) = reader.records().next() {
            let result = result?; // TODO parsingerror
            if result.is_empty() { // Skip blank lines
                continue ;
            }
            datasource.line_counter.all += 1;
            // TODO? read_max_rows
            if rows_to_skip>0 {
                rows_to_skip = rows_to_skip-1 ;
                continue;
            }
            if result.len() < datasource.min_cols {
                // TODO? ignore_errors
                return Err(Box::new(UpdateCatalogError::NotEnoughColumns(datasource.line_counter.all)))
            }
            row_cache.push(result);
            if row_cache.len()>= batch_size {
                self.process_rows(&mut row_cache, &mut datasource).await?;
                // let _ = self.remember_offset(offset).await; // TODO
            }
        }
        self.process_rows(&mut row_cache, &mut datasource).await?;

        datasource.clear_tmp_file();
        let _ = self.clear_offset().await;

/*
		$this->mnm->queue_job($this->catalog_id(),'microsync');
		$this->mnm->queue_job($this->catalog_id(),'automatch_by_search');
		if ( $this->has_born_died ) $this->mnm->queue_job($this->catalog_id(),'match_person_dates');
 */
        Ok(())
    }

    async fn process_rows(&self, rows: &mut Vec<csv::StringRecord>, datasource: &mut DataSource) -> Result<(),GenericError> {
        let mut existing_ext_ids = HashSet::new();
        if datasource.just_add {
            let ext_ids: Vec<String> = rows.iter().filter_map(|row|row.get(datasource.ext_id_column)).map(|s|s.to_string()).collect();
            existing_ext_ids = self.get_existing_ext_ids(datasource.catalog_id, &ext_ids).await?;
        }
        println!("Existing: {} {:?}",datasource.just_add,&existing_ext_ids);
        for row in rows.iter() {
            let ext_id = row.get(datasource.ext_id_column).unwrap();
            if existing_ext_ids.contains(ext_id) {
                println!("Entry with external ID {} already exists, skipping",&ext_id);
            } else {
                self.process_row(&row,datasource).await?;
            }
        }
        rows.clear();
        Ok(())
    }
    
    async fn process_row(&self, row: &csv::StringRecord, datasource: &mut DataSource) -> Result<(),GenericError> {
        let ext_id = row.get(datasource.ext_id_column).unwrap();
        match  Entry::from_ext_id(datasource.catalog_id,ext_id, &self.mnm).await {
            Ok(entry) => {
                if !datasource.just_add {
                    // TODO modify entry
                    let extended_entry = ExtendedEntry::from_row(row, datasource);
                    println!("Modifying {:?}",&extended_entry);
                    }
            }
            _ => {
                let extended_entry = ExtendedEntry::from_row(row, datasource);
                println!("Creating {:?}",&extended_entry);
            }
        }
        Ok(())
    }

    async fn get_existing_ext_ids(&self, catalog_id: usize, ext_ids: &Vec<String>) -> Result<HashSet<String>,GenericError> {
        let mut ret = HashSet::new();
        if ext_ids.is_empty() {
            return Ok(ret);
        }
        let mut placeholders: Vec<String> = Vec::new();
        placeholders.resize(ext_ids.len(),"?".to_string());
        let sql = format!("SELECT `ext_id` FROM entry WHERE `ext_id` IN ({}) AND `catalog`={}",&placeholders.join(","),catalog_id);
        let existing_ext_ids: Vec<String> = sql.with(ext_ids.clone())
        .map(self.mnm.app.get_mnm_conn().await?, |ext_id|ext_id)
        .await?;
        existing_ext_ids.iter().for_each(|ext_id|{ret.insert(ext_id.to_owned());});
        Ok(ret)
    }

    async fn get_update_info(&self, catalog_id: usize) -> Result<UpdateInfo,GenericError> {
        let mut results: Vec<UpdateInfo> = "SELECT id, catalog, json, note, user_id, is_current FROM `update_info` WHERE `catalog`=:catalog_id AND `is_current`=1 LIMIT 1"
            .with(params!{catalog_id})
            .map(self.mnm.app.get_mnm_conn().await?,
                |(id, catalog, json, note, user_id, is_current)|{
                UpdateInfo{id, catalog, json, note, user_id, is_current}
            })
            .await?;
        results.pop().ok_or(Box::new(UpdateCatalogError::NoUpdateInfoForCatalog))
    }

    async fn number_of_entries_in_catalog(&self, catalog_id: usize) -> Result<usize,GenericError> {
        let mut results: Vec<usize> = "SELECT count(*) AS cnt FROM `entry` WHERE `catalog`=:catalog_id"
            .with(params!{catalog_id})
            .map(self.mnm.app.get_mnm_conn().await?,|num|{num})
            .await?;
        Ok(results.pop().unwrap_or(0))
    }

}


#[cfg(test)]
mod tests {

    use super::*;

    const TEST_CATALOG_ID: usize = 4175 ;
    //const TEST_ENTRY_ID: usize = 143962196 ;
    //const TEST_ENTRY_ID2: usize = 144000954 ;

    #[tokio::test]
    async fn test_get_source_location() {
        let mnm = get_test_mnm();

        let url = "http://www.example.org".to_string();
        let ds = DataSource::new(TEST_CATALOG_ID, &json!({"source_url":&url})).unwrap();
        assert_eq!(ds.get_source_location(&mnm).unwrap(),DataSourceLocation::Url(url));

        let uuid = "4b115b29-2ad9-4f43-90ed-7023b51a6337";
        let ds = DataSource::new(TEST_CATALOG_ID, &json!({"file_uuid":&uuid})).unwrap();
        assert_eq!(ds.get_source_location(&mnm).unwrap(),DataSourceLocation::FilePath(format!("{}/{}",mnm.import_file_path(),uuid)));
    }

    #[tokio::test]
    async fn test_get_update_info() {
        let mnm = get_test_mnm();
        let uc = UpdateCatalog::new(&mnm);
        let info = uc.get_update_info(TEST_CATALOG_ID).await.unwrap();
        let json = info.json().unwrap();
        let type_name = json.get("default_type").unwrap().as_str().unwrap();
        assert_eq!(info.user_id,5271664);
        assert_eq!(type_name,"Q5");
    }

    #[test]
    fn test_extended_entry() {
        assert_eq!(ExtendedEntry::parse_type("Q12345"),Some("Q12345".to_string()));
        assert_eq!(ExtendedEntry::parse_type("12345"),None);
        assert_eq!(ExtendedEntry::parse_type("foobar"),None);
        assert_eq!(ExtendedEntry::parse_type(""),None);

        assert_eq!(ExtendedEntry::parse_date("2022-11-03"),Some("2022-11-03".to_string()));
        assert_eq!(ExtendedEntry::parse_date("2022-11"),Some("2022-11".to_string()));
        assert_eq!(ExtendedEntry::parse_date("2022"),Some("2022".to_string()));
        assert_eq!(ExtendedEntry::parse_date("2"),None);
        assert_eq!(ExtendedEntry::parse_date("foobar"),None);
        assert_eq!(ExtendedEntry::parse_date(""),None);
    }
    

    #[tokio::test]
    async fn test_update_from_tabbed_file() {
        let mnm = get_test_mnm();
        let uc = UpdateCatalog::new(&mnm);

        // catalog 3849: file_uuid, patterns
        // catalog 4175: url_pattern
        uc.update_from_tabbed_file(3849).await.unwrap();
    }
}
