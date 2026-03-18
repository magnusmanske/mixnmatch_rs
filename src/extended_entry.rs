use crate::app_state::AppState;
use crate::coordinates::CoordinateLocation;
use crate::datasource::DataSource;
use crate::entry::Entry;
use crate::person_date::PersonDate;
use crate::update_catalog::UpdateCatalogError;
use anyhow::{Result, anyhow};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use wikimisc::timestamp::TimeStamp;
use wikimisc::wikibase::LocaleString;

lazy_static! {
    static ref RE_TYPE: Regex = Regex::new(r"^(Q\d+)$").expect("Regexp construction");
    static ref RE_DATE: Regex =
        Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").expect("Regexp construction");
    static ref RE_PROPERTY: Regex = Regex::new(r"^P(\d+)$").expect("Regexp construction");
    static ref RE_ALIAS: Regex = Regex::new(r"^A([a-z]+)$").expect("Regexp construction");
    static ref RE_DESCRIPTION: Regex = Regex::new(r"^D([a-z]+)$").expect("Regexp construction");
}

#[derive(Debug, Clone, Default)]
pub struct ExtendedEntry {
    pub entry: Entry,
    pub aux: HashSet<(usize, String)>,
    pub born: Option<PersonDate>,
    pub died: Option<PersonDate>,
    pub aliases: Vec<LocaleString>,
    pub descriptions: HashMap<String, String>,
    pub location: Option<CoordinateLocation>,
}

impl ExtendedEntry {
    pub async fn load_extended_data(&mut self) -> Result<()> {
        self.aux = self
            .entry
            .get_aux()
            .await?
            .into_iter()
            .map(|aux| (aux.prop_numeric(), aux.value().to_string()))
            .collect();
        self.location = self.entry.get_coordinate_location().await?;
        (self.born, self.died) = self.entry.get_person_dates().await?;
        self.aliases = self.entry.get_aliases().await?;
        self.descriptions = self.entry.get_language_descriptions().await?;
        // TODO mnm
        // TODO kv_entry
        Ok(())
    }

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
        if entry.q.is_none() {
            if let Some(q) = self.entry.q {
                // println!("UPDATING Q{q} for {}", entry.id);
                entry.set_match(&format!("Q{q}"), 4).await?;
            }
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
        let existing: Vec<(usize, String)> = entry
            .get_aux()
            .await?
            .iter()
            .map(|a| (a.prop_numeric(), a.value().to_owned()))
            .collect();
        for prop_value in &self.aux {
            if !existing.contains(prop_value) {
                entry
                    .set_auxiliary(prop_value.0, Some(prop_value.1.to_owned()))
                    .await?;
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
                        "Don't understand label '{other}'"
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
    pub fn parse_date(date: &str) -> Option<PersonDate> {
        let captured = Self::get_capture(&RE_DATE, date)?;
        PersonDate::from_db_string(&captured)
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

        // Do location if necessary
        // TODO for all location properties, not only P625 hardcoded
        // also dates (P569/P570)?
        if property_num == 625 {
            if let Some(coord) = CoordinateLocation::parse(cell) {
                self.location = Some(coord);
            }
        } else {
            for part in cell.split('|') {
                let part = part.trim();
                if !part.is_empty() {
                    self.aux.insert((property_num, part.to_string()));
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_type_valid() {
        assert_eq!(ExtendedEntry::parse_type("Q5"), Some("Q5".to_string()));
        assert_eq!(
            ExtendedEntry::parse_type("Q12345"),
            Some("Q12345".to_string())
        );
    }

    #[test]
    fn test_parse_type_invalid() {
        assert_eq!(ExtendedEntry::parse_type(""), None);
        assert_eq!(ExtendedEntry::parse_type("12345"), None);
        assert_eq!(ExtendedEntry::parse_type("foobar"), None);
        assert_eq!(ExtendedEntry::parse_type("P123"), None);
    }

    #[test]
    fn test_parse_date_valid() {
        assert_eq!(
            ExtendedEntry::parse_date("2022-11-03"),
            Some(PersonDate::year_month_day(2022, 11, 3))
        );
        assert_eq!(
            ExtendedEntry::parse_date("2022-11"),
            Some(PersonDate::year_month(2022, 11))
        );
        assert_eq!(ExtendedEntry::parse_date("2022"), Some(PersonDate::year_only(2022)));
        assert_eq!(ExtendedEntry::parse_date("800"), Some(PersonDate::year_only(800)));
    }

    #[test]
    fn test_parse_date_invalid() {
        assert_eq!(ExtendedEntry::parse_date(""), None);
        assert_eq!(ExtendedEntry::parse_date("22"), None);
        assert_eq!(ExtendedEntry::parse_date("foobar"), None);
    }

    #[test]
    fn test_parse_alias() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_alias("Aen", "John"));
        assert_eq!(ee.aliases.len(), 1);
        assert_eq!(ee.aliases[0], LocaleString::new("en", "John"));

        assert!(ee.parse_alias("Ade", "Johann"));
        assert_eq!(ee.aliases.len(), 2);
    }

    #[test]
    fn test_parse_alias_non_alias_label() {
        let mut ee = ExtendedEntry::default();
        assert!(!ee.parse_alias("name", "John"));
        assert!(!ee.parse_alias("P123", "value"));
        assert!(ee.aliases.is_empty());
    }

    #[test]
    fn test_parse_description() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_description("Den", "A painter"));
        assert_eq!(ee.descriptions.get("en"), Some(&"A painter".to_string()));

        assert!(ee.parse_description("Dfr", "Un peintre"));
        assert_eq!(ee.descriptions.get("fr"), Some(&"Un peintre".to_string()));
    }

    #[test]
    fn test_parse_description_non_description_label() {
        let mut ee = ExtendedEntry::default();
        assert!(!ee.parse_description("name", "John"));
        assert!(!ee.parse_description("P123", "value"));
        assert!(ee.descriptions.is_empty());
    }

    #[test]
    fn test_parse_property() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P214", "12345").unwrap());
        assert!(ee.aux.contains(&(214, "12345".to_string())));
    }

    #[test]
    fn test_parse_property_non_property_label() {
        let mut ee = ExtendedEntry::default();
        assert!(!ee.parse_property("name", "value").unwrap());
        assert!(!ee.parse_property("Q5", "value").unwrap());
        assert!(ee.aux.is_empty());
    }

    #[test]
    fn test_parse_property_p625_location() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P625", "1.5/-2.5").unwrap());
        assert!(ee.aux.is_empty()); // P625 goes to location, not aux
        let loc = ee.location.unwrap();
        assert!((loc.lat() - 1.5).abs() < f64::EPSILON);
        assert!((loc.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_property_point_format() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P625", "POINT(1.5 -2.5)").unwrap());
        let loc = ee.location.unwrap();
        assert!((loc.lat() - 1.5).abs() < f64::EPSILON);
        assert!((loc.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_process_cell_name() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("name", "John Doe").unwrap();
        assert_eq!(ee.entry.ext_name, "John Doe");
    }

    #[test]
    fn test_process_cell_desc() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("desc", "A painter").unwrap();
        assert_eq!(ee.entry.ext_desc, "A painter");
    }

    #[test]
    fn test_process_cell_url() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("url", "http://example.com").unwrap();
        assert_eq!(ee.entry.ext_url, "http://example.com");
    }

    #[test]
    fn test_process_cell_type() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("type", "Q5").unwrap();
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
    }

    #[test]
    fn test_process_cell_born() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("born", "1900").unwrap();
        assert_eq!(ee.born, Some(PersonDate::year_only(1900)));
    }

    #[test]
    fn test_process_cell_died() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("died", "2000-01-15").unwrap();
        assert_eq!(ee.died, Some(PersonDate::year_month_day(2000, 1, 15)));
    }

    #[test]
    fn test_process_cell_q_valid() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("q", "Q42").unwrap();
        assert_eq!(ee.entry.q, Some(42));
        assert_eq!(ee.entry.user, Some(4));
        assert!(ee.entry.timestamp.is_some());
    }

    #[test]
    fn test_process_cell_q_invalid() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("q", "Q0").unwrap();
        assert_eq!(ee.entry.q, None);
        assert!(ee.entry.user.is_none());
    }

    #[test]
    fn test_process_cell_q_negative() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("q", "Q-1").unwrap();
        assert_eq!(ee.entry.q, None);
    }

    #[test]
    fn test_process_cell_unknown_label() {
        let mut ee = ExtendedEntry::default();
        let result = ee.process_cell("foobar", "value");
        assert!(result.is_err());
    }

    #[test]
    fn test_process_cell_id_ignored() {
        let mut ee = ExtendedEntry::default();
        // "id" label should be silently ignored
        ee.process_cell("id", "12345").unwrap();
    }

    #[test]
    fn test_get_capture() {
        let re = Regex::new(r"^Q(\d+)$").unwrap();
        assert_eq!(
            ExtendedEntry::get_capture(&re, "Q123"),
            Some("123".to_string())
        );
        assert_eq!(ExtendedEntry::get_capture(&re, "P123"), None);
        assert_eq!(ExtendedEntry::get_capture(&re, ""), None);
    }

    #[test]
    fn test_parse_property_multiple_values() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5|Q515").unwrap());
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&(31, "Q5".to_string())));
        assert!(ee.aux.contains(&(31, "Q515".to_string())));
    }

    #[test]
    fn test_parse_property_multiple_values_with_spaces() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5 | Q515 | Q123").unwrap());
        assert_eq!(ee.aux.len(), 3);
        assert!(ee.aux.contains(&(31, "Q5".to_string())));
        assert!(ee.aux.contains(&(31, "Q515".to_string())));
        assert!(ee.aux.contains(&(31, "Q123".to_string())));
    }

    #[test]
    fn test_parse_property_multiple_values_empty_parts_ignored() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5||Q515|").unwrap());
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&(31, "Q5".to_string())));
        assert!(ee.aux.contains(&(31, "Q515".to_string())));
    }

    #[test]
    fn test_parse_property_single_value_unchanged() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P214", "12345").unwrap());
        assert_eq!(ee.aux.len(), 1);
        assert!(ee.aux.contains(&(214, "12345".to_string())));
    }

    #[test]
    fn test_parse_property_multiple_string_values() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P214", "12345|67890").unwrap());
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&(214, "12345".to_string())));
        assert!(ee.aux.contains(&(214, "67890".to_string())));
    }

    #[test]
    fn test_parse_property_duplicate_values() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5|Q5").unwrap());
        // HashSet deduplicates
        assert_eq!(ee.aux.len(), 1);
        assert!(ee.aux.contains(&(31, "Q5".to_string())));
    }

    #[test]
    fn test_process_cell_property_multiple_values() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("P31", "Q5|Q515").unwrap();
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&(31, "Q5".to_string())));
        assert!(ee.aux.contains(&(31, "Q515".to_string())));
    }

    #[test]
    fn test_process_cell_alias() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("Aen", "Johnny").unwrap();
        assert_eq!(ee.aliases.len(), 1);
        assert_eq!(ee.aliases[0], LocaleString::new("en", "Johnny"));
    }

    #[test]
    fn test_process_cell_description() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("Den", "A scientist").unwrap();
        assert_eq!(ee.descriptions.get("en"), Some(&"A scientist".to_string()));
    }
}
