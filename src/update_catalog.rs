use std::path::Path;
use std::ffi::OsString;
use std::env::temp_dir;
use uuid::Uuid;
use std::fs;
use std::fs::File;
use std::io::Cursor;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use serde_json::json;
use mysql_async::prelude::*;
use crate::app_state::*;
use crate::mixnmatch::*;
use crate::job::*;

#[derive(Debug)]
enum UpdateCatalogError {
    NoUpdateInfoForCatalog,
    MissingColumn,
    MissingDataSourceLocation,
    MissingDataSourceType
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

struct DataSource {
    json: serde_json::Value,
    columns: Vec<String>,
    just_add: bool,
    min_cols: u64,
    num_header_rows: u64,
    skip_first_rows: u64,
    patterns: serde_json::Map<String,serde_json::Value>,
    tmp_file: Option<OsString>,
    colmap: HashMap<String,usize>
}

impl DataSource {
    fn new(json: &serde_json::Value) -> Result<Self,GenericError> {
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
        let _ = colmap.get("id").ok_or(Box::new(UpdateCatalogError::MissingColumn));
        let _ = colmap.get("name").ok_or(Box::new(UpdateCatalogError::MissingColumn));

        let min_cols = json.get("min_cols").map(|v|v.as_u64().unwrap_or(columns.len() as u64)).unwrap_or_else(||columns.len() as u64);
        Ok(Self {
            json: json.clone(),
            columns,
            just_add: json.get("just_add").map(|v|v.as_bool().unwrap_or(false)).unwrap_or(false),
            min_cols: min_cols,
            num_header_rows: json.get("num_header_rows").map(|v|v.as_u64().unwrap_or(0)).unwrap_or(0),
            skip_first_rows: json.get("skip_first_rows").unwrap_or(&json!{0}).as_u64().unwrap_or(0),
            patterns,
            colmap,
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
        let builder = builder.flexible(true);
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
        let mut datasource = DataSource::new(&json)?;
        
        let entries_already_in_catalog = self.number_of_entries_in_catalog(catalog_id).await?;
/*
        println!("min_cols {}",datasource.min_cols);
        println!("num_header_rows {}",datasource.num_header_rows);
        println!("skip_first_rows {}",datasource.skip_first_rows);
        println!("columns\n{:?}",&datasource.columns);
        println!("colmap\n{:?}",&datasource.colmap);
        println!("patterns\n{:?}",&datasource.patterns);
*/

        let mut line_counter = LineCounter::new() ;
        let mut rows_to_skip = datasource.num_header_rows + datasource.skip_first_rows;
        let just_add = entries_already_in_catalog==0 || datasource.just_add ;
        let mut reader = datasource.get_reader(&self.mnm).await?;

        while let Some(result) = reader.records().next() {
            if rows_to_skip>0 {
                rows_to_skip = rows_to_skip-1 ;
                //continue;
            }
            println!("{:?}",&result);
        }

        datasource.clear_tmp_file();

/*
		$this->mnm->queue_job($this->catalog_id(),'microsync');
		$this->mnm->queue_job($this->catalog_id(),'automatch_by_search');
		if ( $this->has_born_died ) $this->mnm->queue_job($this->catalog_id(),'match_person_dates');
 */
        Ok(())
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
        let ds = DataSource::new(&json!({"source_url":&url})).unwrap();
        assert_eq!(ds.get_source_location(&mnm).unwrap(),DataSourceLocation::Url(url));

        let uuid = "4b115b29-2ad9-4f43-90ed-7023b51a6337";
        let ds = DataSource::new(&json!({"file_uuid":&uuid})).unwrap();
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

    #[tokio::test]
    async fn test_update_from_tabbed_file() {
        let mnm = get_test_mnm();
        let uc = UpdateCatalog::new(&mnm);

        // catalog 3849: file_uuid, patterns
        // catalog 4175: url_pattern
        uc.update_from_tabbed_file(3849).await.unwrap();
    }
}
