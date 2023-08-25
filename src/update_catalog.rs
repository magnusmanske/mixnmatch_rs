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
use wikibase::locale_string::LocaleString;
use crate::app_state::*;
use crate::autoscrape::Autoscrape;
use crate::mixnmatch::*;
use crate::entry::*;
use crate::job::*;

lazy_static!{
    static ref RE_PATTERN_WRAP_REMOVAL : Regex = Regex::new(r"^\|(.+)\|$").expect("Regexp construction");
    static ref RE_TYPE : Regex = Regex::new(r"^(Q\d+)$").expect("Regexp construction");
    static ref RE_DATE : Regex = Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").expect("Regexp construction");
    static ref RE_PROPERTY : Regex = Regex::new(r"^P(\d+)$").expect("Regexp construction");
    static ref RE_ALIAS : Regex = Regex::new(r"^A([a-z]+)$").expect("Regexp construction");
    static ref RE_DESCRIPTION : Regex = Regex::new(r"^D([a-z]+)$").expect("Regexp construction");
    static ref RE_POINT : Regex = Regex::new(r"^\s*POINT\s*\(\s*(\S+?)[, ](\S+?)\s*\)\s*$").expect("Regexp construction");
    static ref RE_LAT_LON : Regex = Regex::new(r"^(\S+)/(\S+)$").expect("Regexp construction");
}

#[derive(Debug)]
enum UpdateCatalogError {
    NoUpdateInfoForCatalog,
    MissingColumn,
    MissingDataSourceLocation,
    MissingDataSourceType,
    NotEnoughColumns(usize),
    UnknownColumnLabel(String),
    BadPattern
}

impl Error for UpdateCatalogError {}

impl fmt::Display for UpdateCatalogError {
    //TODO test
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
    //TODO test
    pub fn json(&self) -> Result<serde_json::Value,serde_json::Error> {
        serde_json::from_str(&self.json)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct LineCounter {
    pub all: usize,
    pub added: usize,
    pub updates: usize,
    pub offset: usize
}

impl LineCounter {
    //TODO test
    pub fn new() -> Self {
        Self { all: 0, added: 0, updates: 0 , offset: 0 }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum DataSourceType {
    Unknown,
    CSV,
    TSV
}

impl DataSourceType {
    //TODO test
    fn from_str(s: &str) ->DataSourceType {
        match s.to_string().trim().to_uppercase().as_str() {
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
pub struct ExtendedEntry {
    pub entry: Entry,
    pub aux: HashMap<usize,String>,
    pub born: Option<String>,
    pub died: Option<String>,
    pub aliases: Vec<LocaleString>,
    pub descriptions: HashMap<String,String>,
    pub location: Option<CoordinateLocation>
}

impl ExtendedEntry {
    //TODO test
    pub fn from_row(row: &csv::StringRecord, datasource: &mut DataSource) -> Result<Self,GenericError> {
        let ext_id = row.get(datasource.ext_id_column).ok_or(format!("No external ID for entry"))?;
        let mut ret = Self {
            entry:  Entry::new_from_catalog_and_ext_id(datasource.catalog_id, &ext_id),
            aux: HashMap::new(),
            born: None,
            died: None,
            aliases: Vec::new(),
            descriptions: HashMap::new(),
            location: None
        };

        println!("from_row: labels {:?}",datasource.colmap);
        for (label,col_num) in &datasource.colmap {
            let cell = match row.get(*col_num) {
                Some(cell) => cell,
                None => continue
            } ;
            ret.process_cell(label, cell)?;
        }

        println!("from_row: patterns");
        for pattern in &datasource.patterns {
            let cell = match row.get(pattern.column_number) {
                Some(cell) => cell,
                None => continue
            } ;
            if let Some(new_cell) = Self::get_capture(&pattern.pattern, cell) {
                ret.process_cell(&pattern.use_column_label, &new_cell)?;
            }
        }
        println!("from_row: DONE");

        if ret.entry.type_name.is_none() {
            ret.entry.type_name = datasource.default_type.to_owned();
        }

        if ret.entry.ext_url.is_empty() {
            if let Some(pattern) = &datasource.url_pattern {
                ret.entry.ext_url = pattern.replace("$1",&ret.entry.ext_id);
            }
        }

        Ok(ret)
    }

    //TODO test
    pub async fn update_existing(&mut self, entry: &mut Entry, mnm: &MixNMatch) -> Result<(),GenericError> {
        entry.set_mnm(mnm);

        // TODO use update_existing_description
        // TODO use update_all_descriptions

        // We add, we do not remove from the existing data!
        if !self.entry.ext_name.is_empty() {
            entry.set_ext_name(&self.entry.ext_name).await?;
        }
        if !self.entry.ext_desc.is_empty() {
            entry.set_ext_desc(&self.entry.ext_desc).await?;
        }
        if !self.entry.type_name.is_none() {
            entry.set_type_name(self.entry.type_name.clone()).await?;
        }
        if !self.entry.ext_url.is_empty() {
            entry.set_ext_url(&self.entry.ext_url).await?;
        }
        // Ignore ID, this is the key anyway
        // q, user, and timetamp would not change
        // Ignore random

        if !self.born.is_none() || !self.died.is_none() {
            entry.set_person_dates(&self.born,&self.died).await?;
        }
        if !self.location.is_none() {
            entry.set_coordinate_location(&self.location).await?;
        }
        self.sync_aliases(entry).await?;
        self.sync_descriptions(entry).await?;
        self.sync_auxiliary(entry).await?;

        Ok(())
    }

    // Adds new aliases.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_aliases(&self, entry: &Entry)  -> Result<(),GenericError> {
        let existing = entry.get_aliases().await?;
        for alias in &self.aliases {
            if !existing.contains(alias) {
                self.entry.add_alias(&alias).await?;
            }
        }
        Ok(())
    }

    // Adds/replaces new aux values.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_auxiliary(&self, entry: &Entry)  -> Result<(),GenericError> {
        let existing: HashMap<usize,String> = entry.get_aux().await?.iter().map(|a|(a.prop_numeric,a.value.to_owned())).collect();
        for (prop,value) in &self.aux {
            if existing.get(&prop)!=Some(&value) {
                entry.set_auxiliary(*prop,Some(value.to_owned())).await?;
            }
        }
        Ok(())
    }

    // Adds/replaces new language descriptions.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_descriptions(&self, entry: &Entry)  -> Result<(),GenericError> {
        let existing = entry.get_language_descriptions().await?;
        for (language,value) in &self.descriptions {
            if existing.get(language)!=Some(&value) {
                entry.set_language_description(language,Some(value.to_owned())).await?;
            }
        }
        Ok(())
    }

    /// Inserts a new entry and its associated data into the database
    //TODO test
    pub async fn insert_new(&mut self, mnm: &MixNMatch) -> Result<(),GenericError> {
        self.entry.set_mnm(mnm);
        self.entry.insert_as_new().await?;

        // TODO use update_existing_description
        // TODO use update_all_descriptions
    
        if !self.born.is_none() || !self.died.is_none() {
            self.entry.set_person_dates(&self.born,&self.died).await?;
        }
        if !self.location.is_none() {
            self.entry.set_coordinate_location(&self.location).await?;
        }

        for alias in &self.aliases {
            self.entry.add_alias(&alias).await?;
        }
        for (prop,value) in &self.aux {
            self.entry.set_auxiliary(*prop,Some(value.to_owned())).await?;
        }
        for (language,text) in &self.descriptions {
            self.entry.set_language_description(language,Some(text.to_owned())).await?;
        }

        Ok(())
    }

    /// Processes a key-value pair, with keys from table columns, or matched patterns
    //TODO test
    fn process_cell(&mut self, label: &str, cell: &str) -> Result<(),GenericError> {
        if !self.parse_alias(&label,cell) && !self.parse_description(&label,cell) && !self.parse_property(&label,cell)? {
            match label {
                "id" => { /* Already have that in entry */ }
                "name" => { self.entry.ext_name = cell.to_owned() }
                "desc" => { self.entry.ext_desc = cell.to_owned() }
                "url" => { self.entry.ext_url = cell.to_owned() }
                "autoq" => {
                    self.entry.q = cell.to_string().replace('Q',"").parse::<isize>().ok();
                    if let Some(i) = self.entry.q { // Don't accept invalid or N/A item IDs
                        if i<=0 {
                            self.entry.q = None;
                        }
                    }
                    if self.entry.q.is_some() { // q is set, also set user and timestamp
                        self.entry.user = Some(4); // Auxiliary data matcher
                        self.entry.timestamp = Some(MixNMatch::get_timestamp());
                    }
                }
                "type" => { self.entry.type_name = Self::parse_type(cell) }
                "born" => { self.born = Self::parse_date(cell) }
                "died" => { self.died = Self::parse_date(cell) }
                other => { return Err(Box::new(UpdateCatalogError::UnknownColumnLabel(format!("Don't understand label '{}'",other)))); }
            }
        }
        Ok(())
    }

    //TODO test
    fn parse_type(type_name: &str) -> Option<String> {
        Self::get_capture(&RE_TYPE, type_name)
    }

    //TODO test
    fn parse_date(date: &str) -> Option<String> {
        Self::get_capture(&RE_DATE, date)
    }

    //TODO test
    fn parse_alias(&mut self, label: &str, cell: &str) -> bool {
        if let Some(s) = Self::get_capture(&RE_ALIAS, label) {
            self.aliases.push(LocaleString::new(s, cell.to_string()));
            true
        } else {
            false
        }
    }

    //TODO test
    fn parse_description(&mut self, label: &str, cell: &str) -> bool {
        if let Some(s) = Self::get_capture(&RE_DESCRIPTION, label) {
            self.descriptions.insert(s, cell.to_string());
            true
        } else {
            false
        }
    }

    //TODO test
    fn parse_property(&mut self, label: &str, cell: &str) -> Result<bool,GenericError> {
        let property_num = match Self::get_capture(&RE_PROPERTY, label) {
            Some(s) => s.parse::<usize>()?,
            None => return Ok(false)
        };

        // Convert from POINT
        let value = match RE_POINT.captures(cell) {
            Some(captures) => {
                if let (Some(lat),Some(lon)) = (captures.get(1),captures.get(2)) {
                    format!("{},{}",lat.as_str(),lon.as_str())
                } else {
                    cell.to_string()
                }
            }
            None => cell.to_string()
        };

        // Do location if necessary
        // TODO for all location properties, not only P625 hardcoded
        if property_num == 625 {
            match RE_LAT_LON.captures(&value) {
                Some(captures) => {
                    match (captures.get(1),captures.get(2)) {
                        (Some(lat),Some(lon)) => {
                            let lat = lat.as_str().to_string().parse::<f64>()?;
                            let lon = lon.as_str().to_string().parse::<f64>()?;
                            self.location = Some(CoordinateLocation{lat,lon});
                        },
                        _ => {}
                    }
                }
                None => {}
            }
        } else {
            self.aux.insert(property_num,value);
        }

        Ok(true)
    }

    //TODO test
    fn get_capture(regexp: &Regex, text: &str) -> Option<String> {
        regexp.captures(text)?.get(1).map(|s|s.as_str().to_string())
    }
}

#[derive(Debug, Clone)]
struct Pattern {
    use_column_label: String,
    column_number: usize,
    pattern: Regex
}

impl Pattern {
    //TODO test
    fn from_json(use_column_label: &str, data: &serde_json::Value) -> Result<Self,GenericError> {
        let pattern = match data.get("pattern") {
            Some(col) => col.as_str().ok_or(UpdateCatalogError::BadPattern)?,
            None => return Err(Box::new(UpdateCatalogError::BadPattern))
        };
        let patterns = match RE_PATTERN_WRAP_REMOVAL.captures(pattern) {
            Some(patterns) => patterns,
            None => return Err(Box::new(UpdateCatalogError::BadPattern))
        };
        let pattern = match patterns.get(1).map(|s|s.as_str()) {
            Some(s) => s,
            None => pattern
        } ;
        Ok(Self {
            use_column_label: use_column_label.to_string(),
            column_number:  match data.get("col") {
                Some(col) => col.as_u64().ok_or(UpdateCatalogError::BadPattern)? as usize,
                None => return Err(Box::new(UpdateCatalogError::BadPattern))
            },
            pattern:  Regex::new(pattern)?
        })
    }
}

#[derive(Debug, Clone)]
pub struct DataSource {
    catalog_id: usize,
    json: serde_json::Value,
    _columns: Vec<String>,
    just_add: bool,
    min_cols: usize,
    num_header_rows: u64,
    skip_first_rows: u64,
    ext_id_column: usize,
    patterns: Vec<Pattern>,
    tmp_file: Option<OsString>,
    colmap: HashMap<String,usize>,
    default_type: Option<String>,
    url_pattern: Option<String>,
    _update_existing_description: Option<bool>,
    _update_all_descriptions: Option<bool>,
    fail_on_error: bool,
    line_counter: LineCounter
}

impl DataSource {
    //TODO test
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
        let patterns = patterns
            .iter()
            .filter_map(|(k,v)| Pattern::from_json(k, v).ok() )
            .collect();

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
            default_type: json.get("default_type").map(|v|v.as_str().map(|s|s.to_string())).unwrap_or(None),
            url_pattern: json.get("url_pattern").map(|v|v.as_str().map(|s|s.to_string())).unwrap_or(None),
            _update_existing_description: json.get("update_existing_description").map(|v|v.as_bool()).unwrap_or(None),
            _update_all_descriptions: json.get("update_all_descriptions").map(|v|v.as_bool()).unwrap_or(None),
            line_counter: LineCounter::new(),
            fail_on_error: false, // TODO?
            tmp_file: None
        })
    }

    //TODO test
    async fn fetch_url(&self, url: &String, file_name: &Path) -> Result<(),GenericError> {
        let response = Autoscrape::reqwest_client_external()?.get(url).send().await?;
        let mut file = std::fs::File::create(file_name)?;
        let mut content =  Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut file)?;
        Ok(())
    }

    //TODO test
    fn clear_tmp_file(&self) {
        if let Some(path) = &self.tmp_file {
            let _ = fs::remove_file(path);
        };
    }

    //TODO test
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
                self.tmp_file = Some(full_path_string);
                self.fetch_url(&url,full_path).await?;
                let builder = builder.from_path(&full_path)?;
                Ok(builder)
            }
            DataSourceLocation::FilePath(path) => {
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

    //TODO test
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

    /// Updates a catalog by reading a tabbed file.
    pub async fn update_from_tabbed_file(&mut self, catalog_id: usize) -> Result<(),GenericError> {
        let update_info = self.get_update_info(catalog_id).await?;
        let json = update_info.json()?;
        let mut datasource = DataSource::new(catalog_id, &json)?;
        let offset = self.get_last_job_offset().await ;
        let batch_size = 5000;
        let entries_already_in_catalog = self.number_of_entries_in_catalog(catalog_id).await?;

        let mut rows_to_skip = datasource.num_header_rows + datasource.skip_first_rows;
        datasource.just_add = entries_already_in_catalog==0 || datasource.just_add ;
        let mut reader = datasource.get_reader(&self.mnm).await?;

        let mut row_cache = vec![];
        while let Some(result) = reader.records().next() {
            let result = match result {
                Ok(result) => result,
                Err(e) => {
                    if datasource.fail_on_error { return Err(Box::new(e)) } else {continue}
                }
            };
            if result.is_empty() { // Skip blank lines
                continue ;
            }
            datasource.line_counter.all += 1;
            // TODO? read_max_rows but it's only used from the API so...
            if rows_to_skip>0 {
                rows_to_skip = rows_to_skip-1 ;
                continue;
            }
            if result.len() < datasource.min_cols {
                if datasource.fail_on_error {
                    return Err(Box::new(UpdateCatalogError::NotEnoughColumns(datasource.line_counter.all)))
                }
                continue
            }

            datasource.line_counter.offset += 1;
            if datasource.line_counter.offset < offset {
                continue;
            }

            row_cache.push(result);
            if row_cache.len()>= batch_size {
                if let Err(e) = self.process_rows(&mut row_cache, &mut datasource).await {
                    if datasource.fail_on_error { return Err(e) }
                }
                let _ = self.remember_offset(datasource.line_counter.offset).await;
            }
        }
        if let Err(e) = self.process_rows(&mut row_cache, &mut datasource).await {
            if datasource.fail_on_error { return Err(e) }
        }

        datasource.clear_tmp_file();
        let _ = self.clear_offset().await;

/*
		$this->mnm->queue_job($this->catalog_id(),'microsync');
		$this->mnm->queue_job($this->catalog_id(),'automatch_by_search');
		if ( $this->has_born_died ) $this->mnm->queue_job($this->catalog_id(),'match_person_dates');
 */
        Ok(())
    }

    //TODO test
    async fn process_rows(&self, rows: &mut Vec<csv::StringRecord>, datasource: &mut DataSource) -> Result<(),GenericError> {
        let mut existing_ext_ids = HashSet::new();
        if datasource.just_add {
            let ext_ids: Vec<String> = rows.iter().filter_map(|row|row.get(datasource.ext_id_column)).map(|s|s.to_string()).collect();
            existing_ext_ids = match self.get_existing_ext_ids(datasource.catalog_id, &ext_ids).await {
                Ok(x) => x,
                Err(_e) => { return Ok(()) } // TODO is this the correct thing to do?
            }
        }
        for row in rows.iter() {
            let ext_id = match row.get(datasource.ext_id_column) {
                Some(ext_id) => ext_id,
                None => continue
            };
            if existing_ext_ids.contains(ext_id) {
                // An entry with this ext_id already exists, and we only know that because just_add==true, so skip this
            } else {
                if let Err(e) = self.process_row(&row,datasource).await {
                    if datasource.fail_on_error {
                        return Err(e)
                    }
                }
            }
        }
        rows.clear();
        Ok(())
    }
    
    //TODO test
    async fn process_row(&self, row: &csv::StringRecord, datasource: &mut DataSource) -> Result<(),GenericError> {
        println!("{row:?}");
        let ext_id = match row.get(datasource.ext_id_column) {
            Some(ext_id) => ext_id,
            None => return Ok(()) // TODO ???
        };
        println!("Extid: {ext_id}");
        match Entry::from_ext_id(datasource.catalog_id,ext_id, &self.mnm).await {
            Ok(mut entry) => {
                println!("Already exists");
                if !datasource.just_add {
                    let mut extended_entry = ExtendedEntry::from_row(row, datasource)?;
                    extended_entry.update_existing(&mut entry, &self.mnm).await?;
                }
            }
            _ => {
                println!("Does not exits yet: #{} : {:?}",datasource.ext_id_column,row.get(0));
                let mut extended_entry = ExtendedEntry::from_row(row, datasource)?;
                println!("Extended entry generated");
                extended_entry.insert_new(&self.mnm).await?;
                println!("Extended entry added");
            }
        }
        Ok(())
    }

    //TODO test
    async fn get_existing_ext_ids(&self, catalog_id: usize, ext_ids: &Vec<String>) -> Result<HashSet<String>,GenericError> {
        let mut ret = HashSet::new();
        if ext_ids.is_empty() {
            return Ok(ret);
        }
        let placeholders = MixNMatch::sql_placeholders(ext_ids.len());
        let sql = format!("SELECT `ext_id` FROM entry WHERE `ext_id` IN ({}) AND `catalog`={}",&placeholders,catalog_id);
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

    //TODO test
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

    const TEST_CATALOG_ID: usize = 5526 ; // was 4175

    #[tokio::test]
    async fn test_get_source_location() {
        let mnm = get_test_mnm();

        let url = "http://www.example.org".to_string();
        let ds = DataSource::new(TEST_CATALOG_ID, &json!({"source_url":&url,"columns":["id","name"]})).unwrap();
        assert_eq!(ds.get_source_location(&mnm).unwrap(),DataSourceLocation::Url(url));

        let uuid = "4b115b29-2ad9-4f43-90ed-7023b51a6337";
        let ds = DataSource::new(TEST_CATALOG_ID, &json!({"file_uuid":&uuid,"columns":["id","name"]})).unwrap();
        assert_eq!(ds.get_source_location(&mnm).unwrap(),DataSourceLocation::FilePath(format!("{}/{}",mnm.import_file_path(),uuid)));
    }

    #[tokio::test]
    async fn test_get_update_info() {
        let mnm = get_test_mnm();
        let uc = UpdateCatalog::new(&mnm);
        let info = uc.get_update_info(TEST_CATALOG_ID).await.unwrap();
        let json = info.json().unwrap();
        let type_name = json.get("default_type").unwrap().as_str().unwrap();
        assert_eq!(info.user_id,2);
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
        let _test_lock = TEST_MUTEX.lock();
        let mnm = get_test_mnm();

        // Delete the entry if it exists
        if let Ok(mut entry) = Entry::from_ext_id(TEST_CATALOG_ID,"n2014191777",&mnm).await { entry.delete().await.unwrap(); }

        // Import single entry
        let mut uc = UpdateCatalog::new(&mnm);
        uc.update_from_tabbed_file(TEST_CATALOG_ID).await.unwrap();

        // Get new entry
        let mut entry = Entry::from_ext_id(TEST_CATALOG_ID,"n2014191777",&mnm).await.unwrap();

        // Check base values
        assert_eq!(entry.ext_name,"Hauk Aabel");
        assert_eq!(entry.ext_url,"https://www.aspi.unimib.it/collections/entity/detail/n2014191777/");
        assert_eq!(entry.type_name,Some("Q5".to_string()));

        // Check aux values
        let aux = entry.get_aux().await.unwrap();
        assert_eq!(aux.len(),2);
        assert_eq!(aux.iter().filter(|row|row.prop_numeric==213).next().unwrap().value,"0000 0000 6555 4670");
        assert_eq!(aux.iter().filter(|row|row.prop_numeric==214).next().unwrap().value,"91113950");

        // Check person dates
        let (born,died) = entry.get_person_dates().await.unwrap();
        assert_eq!(born.unwrap(),"1869");
        assert_eq!(died.unwrap(),"1961");

        // Cleanup
        entry.delete().await.unwrap();
    }
}
