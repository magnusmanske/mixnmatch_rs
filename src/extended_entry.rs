use crate::app_state::AppContext;
use crate::auxiliary_data::AuxiliaryRow;
use crate::coordinates::CoordinateLocation;
use crate::datasource::DataSource;
use crate::entry::{Entry, EntryWriter};
use crate::meta_entry::{MetaEntry, MetaPersonDates};
use crate::person_date::PersonDate;
use crate::update_catalog::UpdateCatalogError;
use anyhow::{Result, anyhow};
use std::sync::LazyLock;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use wikimisc::timestamp::TimeStamp;
use wikimisc::wikibase::LocaleString;

static RE_TYPE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(Q\d+)$").expect("Regexp construction"));
static RE_DATE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d{3,}|\d{3,4}-\d{2}|\d{3,4}-\d{2}-\d{2})$").expect("Regexp construction"));
static RE_PROPERTY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^P(\d+)$").expect("Regexp construction"));
static RE_ALIAS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^A([a-z]+)$").expect("Regexp construction"));
static RE_DESCRIPTION: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^D([a-z]+)$").expect("Regexp construction"));

#[derive(Debug, Clone, Default)]
pub struct ExtendedEntry {
    pub entry: Entry,
    pub aux: HashSet<AuxiliaryRow>,
    pub born: Option<PersonDate>,
    pub died: Option<PersonDate>,
    pub aliases: Vec<LocaleString>,
    pub descriptions: HashMap<String, String>,
    pub location: Option<CoordinateLocation>,
}

/// Bridge for the in-progress retirement of ExtendedEntry. Callers
/// being migrated to MetaEntry can take `From<ExtendedEntry>` at the
/// boundary; once every caller produces MetaEntry directly, both this
/// impl and the struct it converts can be deleted.
impl From<&ExtendedEntry> for MetaEntry {
    fn from(ee: &ExtendedEntry) -> Self {
        let person_dates = if ee.born.is_some() || ee.died.is_some() {
            Some(MetaPersonDates { born: ee.born, died: ee.died })
        } else {
            None
        };
        MetaEntry {
            entry: ee.entry.clone(),
            auxiliary: ee.aux.iter().cloned().collect(),
            coordinate: ee.location,
            person_dates,
            descriptions: ee.descriptions.clone(),
            aliases: ee.aliases.clone(),
            ..MetaEntry::default()
        }
    }
}

impl From<ExtendedEntry> for MetaEntry {
    fn from(ee: ExtendedEntry) -> Self {
        let person_dates = if ee.born.is_some() || ee.died.is_some() {
            Some(MetaPersonDates { born: ee.born, died: ee.died })
        } else {
            None
        };
        MetaEntry {
            entry: ee.entry,
            auxiliary: ee.aux.into_iter().collect(),
            coordinate: ee.location,
            person_dates,
            descriptions: ee.descriptions,
            aliases: ee.aliases,
            ..MetaEntry::default()
        }
    }
}

impl ExtendedEntry {
    pub async fn load_extended_data(&mut self, app: &dyn AppContext) -> Result<()> {
        let ew = EntryWriter::new(app, &mut self.entry);
        self.aux = ew.get_aux().await?.into_iter().collect();
        self.location = ew.get_coordinate_location().await?;
        (self.born, self.died) = ew.get_person_dates().await?;
        self.aliases = ew.get_aliases().await?;
        self.descriptions = ew.get_language_descriptions().await?;
        // TODO mnm
        // TODO kv_entry
        Ok(())
    }

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

    /// View this ExtendedEntry as a MetaEntry. Thin wrapper around
    /// `From<&ExtendedEntry> for MetaEntry`; kept as a method so the
    /// existing internal callers (`insert_new`, `update_existing`) read
    /// naturally.
    fn to_meta(&self) -> MetaEntry {
        self.into()
    }

    /// Update an existing entry. Delegates entirely to
    /// [`MetaEntry::update_merge_in_storage`], which is now the
    /// canonical home for the scraper-style merge contract (empty
    /// scalars are skipped, matches only assigned to unmatched entries,
    /// aliases/aux/descriptions are add-only).
    pub async fn update_existing(&mut self, entry: &mut Entry, app: &dyn AppContext) -> Result<()> {
        self.to_meta().update_merge_in_storage(entry, app).await
    }

    /// Insert a new entry. Delegates to `MetaEntry::create_in_storage`.
    pub async fn insert_new(&mut self, app: &dyn AppContext) -> Result<()> {
        let meta = self.to_meta();
        let new_id = meta.create_in_storage(app).await?;
        self.entry.id = Some(new_id);
        Ok(())
    }

    /// Processes a key-value pair, with keys from table columns, or matched patterns
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
                    self.entry.q = cell.replace('Q', "").parse::<isize>().ok().filter(|&i| i > 0);
                    if self.entry.q.is_some() {
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

    pub fn parse_type(type_name: &str) -> Option<String> {
        Self::get_capture(&RE_TYPE, type_name)
    }

    pub fn parse_date(date: &str) -> Option<PersonDate> {
        let captured = Self::get_capture(&RE_DATE, date)?;
        PersonDate::from_db_string(&captured)
    }

    fn parse_alias(&mut self, label: &str, cell: &str) -> bool {
        if let Some(s) = Self::get_capture(&RE_ALIAS, label) {
            self.aliases.push(LocaleString::new(s, cell.to_string()));
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
                    self.aux
                        .insert(AuxiliaryRow::new(property_num, part.to_string()));
                }
            }
        }

        Ok(true)
    }

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
        assert_eq!(
            ExtendedEntry::parse_date("2022"),
            Some(PersonDate::year_only(2022))
        );
        assert_eq!(
            ExtendedEntry::parse_date("800"),
            Some(PersonDate::year_only(800))
        );
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
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(214, "12345".to_string()))
        );
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
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q515".to_string())));
    }

    #[test]
    fn test_parse_property_multiple_values_with_spaces() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5 | Q515 | Q123").unwrap());
        assert_eq!(ee.aux.len(), 3);
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q515".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q123".to_string())));
    }

    #[test]
    fn test_parse_property_multiple_values_empty_parts_ignored() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5||Q515|").unwrap());
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q515".to_string())));
    }

    #[test]
    fn test_parse_property_single_value_unchanged() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P214", "12345").unwrap());
        assert_eq!(ee.aux.len(), 1);
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(214, "12345".to_string()))
        );
    }

    #[test]
    fn test_parse_property_multiple_string_values() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P214", "12345|67890").unwrap());
        assert_eq!(ee.aux.len(), 2);
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(214, "12345".to_string()))
        );
        assert!(
            ee.aux
                .contains(&AuxiliaryRow::new(214, "67890".to_string()))
        );
    }

    #[test]
    fn test_parse_property_duplicate_values() {
        let mut ee = ExtendedEntry::default();
        assert!(ee.parse_property("P31", "Q5|Q5").unwrap());
        // HashSet deduplicates
        assert_eq!(ee.aux.len(), 1);
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
    }

    #[test]
    fn test_process_cell_property_multiple_values() {
        let mut ee = ExtendedEntry::default();
        ee.process_cell("P31", "Q5|Q515").unwrap();
        assert_eq!(ee.aux.len(), 2);
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q5".to_string())));
        assert!(ee.aux.contains(&AuxiliaryRow::new(31, "Q515".to_string())));
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
