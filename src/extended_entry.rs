use crate::app_state::AppState;
use crate::datasource::DataSource;
use crate::entry::*;
use crate::update_catalog::UpdateCatalogError;
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use wikimisc::timestamp::TimeStamp;
use wikimisc::wikibase::LocaleString;

lazy_static! {
    static ref RE_TYPE: Regex = Regex::new(r"^(Q\d+)$").expect("Regexp construction");
    static ref RE_DATE: Regex =
        Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").expect("Regexp construction");
    static ref RE_PROPERTY: Regex = Regex::new(r"^P(\d+)$").expect("Regexp construction");
    static ref RE_ALIAS: Regex = Regex::new(r"^A([a-z]+)$").expect("Regexp construction");
    static ref RE_DESCRIPTION: Regex = Regex::new(r"^D([a-z]+)$").expect("Regexp construction");
    static ref RE_POINT: Regex =
        Regex::new(r"^\s*POINT\s*\(\s*(\S+?)[, ](\S+?)\s*\)\s*$").expect("Regexp construction");
    static ref RE_LAT_LON: Regex = Regex::new(r"^(\S+)/(\S+)$").expect("Regexp construction");
}

#[derive(Debug, Clone, Default)]
pub struct ExtendedEntry {
    pub entry: Entry,
    pub aux: HashMap<usize, String>,
    pub born: Option<String>,
    pub died: Option<String>,
    pub aliases: Vec<LocaleString>,
    pub descriptions: HashMap<String, String>,
    pub location: Option<CoordinateLocation>,
}

impl ExtendedEntry {
    //TODO test
    pub fn from_row(row: &csv::StringRecord, datasource: &mut DataSource) -> Result<Self> {
        let ext_id = row
            .get(datasource.ext_id_column)
            .ok_or(anyhow!("No external ID for entry"))?;
        let mut ret = Self {
            entry: Entry::new_from_catalog_and_ext_id(datasource.catalog_id, ext_id),
            ..Default::default()
        };

        Self::from_row_colmap(datasource, row, &mut ret)?;
        Self::from_row_patterns(datasource, row, &mut ret)?;

        if ret.entry.type_name.is_none() {
            ret.entry.type_name.clone_from(&datasource.default_type);
        }

        if ret.entry.ext_url.is_empty() {
            if let Some(pattern) = &datasource.url_pattern {
                ret.entry.ext_url = pattern.replace("$1", &ret.entry.ext_id);
            }
        }

        Ok(ret)
    }

    fn from_row_patterns(
        datasource: &mut DataSource,
        row: &csv::StringRecord,
        ret: &mut ExtendedEntry,
    ) -> Result<()> {
        for pattern in &datasource.patterns {
            let cell = match row.get(pattern.column_number) {
                Some(cell) => cell,
                None => continue,
            };
            if let Some(new_cell) = Self::get_capture(&pattern.pattern, cell) {
                ret.process_cell(&pattern.use_column_label, &new_cell)?;
            }
        }
        Ok(())
    }

    fn from_row_colmap(
        datasource: &mut DataSource,
        row: &csv::StringRecord,
        ret: &mut ExtendedEntry,
    ) -> Result<()> {
        for (label, col_num) in &datasource.colmap {
            let cell = match row.get(*col_num) {
                Some(cell) => cell,
                None => continue,
            };
            ret.process_cell(label, cell)?;
        }
        Ok(())
    }

    //TODO test
    pub async fn update_existing(&mut self, entry: &mut Entry, app: &AppState) -> Result<()> {
        entry.set_app(app);
        self.update_existing_basic_values(entry).await?;
        if self.born.is_some() || self.died.is_some() {
            entry.set_person_dates(&self.born, &self.died).await?;
        }
        if self.location.is_some() {
            entry.set_coordinate_location(&self.location).await?;
        }
        self.sync_aliases(entry).await?;
        self.sync_descriptions(entry).await?;
        self.sync_auxiliary(entry).await?;
        Ok(())
    }

    async fn update_existing_basic_values(&mut self, entry: &mut Entry) -> Result<()> {
        if !self.entry.ext_name.is_empty() {
            entry.set_ext_name(&self.entry.ext_name).await?;
        }
        if !self.entry.ext_desc.is_empty() {
            entry.set_ext_desc(&self.entry.ext_desc).await?;
        }
        if self.entry.type_name.is_some() {
            entry.set_type_name(self.entry.type_name.clone()).await?;
        }
        if !self.entry.ext_url.is_empty() {
            entry.set_ext_url(&self.entry.ext_url).await?;
        }
        Ok(())
    }

    // Adds new aliases.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_aliases(&self, entry: &Entry) -> Result<()> {
        let existing = entry.get_aliases().await?;
        for alias in &self.aliases {
            if !existing.contains(alias) {
                self.entry.add_alias(alias).await?;
            }
        }
        Ok(())
    }

    // Adds/replaces new aux values.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_auxiliary(&self, entry: &Entry) -> Result<()> {
        let existing: HashMap<usize, String> = entry
            .get_aux()
            .await?
            .iter()
            .map(|a| (a.prop_numeric, a.value.to_owned()))
            .collect();
        for (prop, value) in &self.aux {
            if existing.get(prop) != Some(value) {
                entry.set_auxiliary(*prop, Some(value.to_owned())).await?;
            }
        }
        Ok(())
    }

    // Adds/replaces new language descriptions.
    // Does NOT remove ones that don't exist anymore. Who knows how they got into the database.
    //TODO test
    pub async fn sync_descriptions(&self, entry: &Entry) -> Result<()> {
        let existing = entry.get_language_descriptions().await?;
        for (language, value) in &self.descriptions {
            if existing.get(language) != Some(value) {
                entry
                    .set_language_description(language, Some(value.to_owned()))
                    .await?;
            }
        }
        Ok(())
    }

    /// Inserts a new entry and its associated data into the database
    // #lizard forgives
    //TODO test
    pub async fn insert_new(&mut self, app: &AppState) -> Result<()> {
        self.entry.set_app(app);
        self.entry.insert_as_new().await?;

        // TODO use update_existing_description
        // TODO use update_all_descriptions

        if self.born.is_some() || self.died.is_some() {
            self.entry.set_person_dates(&self.born, &self.died).await?;
        }
        if self.location.is_some() {
            self.entry.set_coordinate_location(&self.location).await?;
        }

        for alias in &self.aliases {
            self.entry.add_alias(alias).await?;
        }
        for (prop, value) in &self.aux {
            self.entry
                .set_auxiliary(*prop, Some(value.to_owned()))
                .await?;
        }
        for (language, text) in &self.descriptions {
            self.entry
                .set_language_description(language, Some(text.to_owned()))
                .await?;
        }

        Ok(())
    }

    /// Processes a key-value pair, with keys from table columns, or matched patterns
    //TODO test
    fn process_cell(&mut self, label: &str, cell: &str) -> Result<()> {
        if !self.parse_alias(label, cell)
            && !self.parse_description(label, cell)
            && !self.parse_property(label, cell)?
        {
            match label {
                "id" => { /* Already have that in entry */ }
                "name" => self.entry.ext_name = cell.to_string(),
                "desc" => self.entry.ext_desc = cell.to_string(),
                "url" => self.entry.ext_url = cell.to_string(),
                "q" | "autoq" => {
                    self.entry.q = cell.to_string().replace('Q', "").parse::<isize>().ok();
                    if let Some(i) = self.entry.q {
                        // Don't accept invalid or N/A item IDs
                        if i <= 0 {
                            self.entry.q = None;
                        }
                    }
                    if self.entry.q.is_some() {
                        // q is set, also set user and timestamp
                        self.entry.user = Some(4); // Auxiliary data matcher
                        self.entry.timestamp = Some(TimeStamp::now());
                    }
                }
                "type" => self.entry.type_name = Self::parse_type(cell),
                "born" => self.born = Self::parse_date(cell),
                "died" => self.died = Self::parse_date(cell),
                other => {
                    return Err(UpdateCatalogError::UnknownColumnLabel(format!(
                        "Don't understand label '{}'",
                        other
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    //TODO test
    pub fn parse_type(type_name: &str) -> Option<String> {
        Self::get_capture(&RE_TYPE, type_name)
    }

    //TODO test
    pub fn parse_date(date: &str) -> Option<String> {
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
    fn parse_property(&mut self, label: &str, cell: &str) -> Result<bool> {
        let property_num = match Self::get_capture(&RE_PROPERTY, label) {
            Some(s) => s.parse::<usize>()?,
            None => return Ok(false),
        };

        // Convert from POINT
        let value = match RE_POINT.captures(cell) {
            Some(captures) => {
                if let (Some(lat), Some(lon)) = (captures.get(1), captures.get(2)) {
                    format!("{},{}", lat.as_str(), lon.as_str())
                } else {
                    cell.to_string()
                }
            }
            None => cell.to_string(),
        };

        // Do location if necessary
        // TODO for all location properties, not only P625 hardcoded
        if property_num == 625 {
            if let Some(captures) = RE_LAT_LON.captures(&value) {
                if let (Some(lat), Some(lon)) = (captures.get(1), captures.get(2)) {
                    let lat = lat.as_str().to_string().parse::<f64>()?;
                    let lon = lon.as_str().to_string().parse::<f64>()?;
                    self.location = Some(CoordinateLocation { lat, lon });
                }
            }
        } else {
            self.aux.insert(property_num, value);
        }

        Ok(true)
    }

    //TODO test
    fn get_capture(regexp: &Regex, text: &str) -> Option<String> {
        regexp
            .captures(text)?
            .get(1)
            .map(|s| s.as_str().to_string())
    }
}
