use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::Rng;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// bg-rock-archives.com - Bulgarian Rock Archives

#[derive(Debug)]
pub struct BespokeScraper7700 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7700 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        7700
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for letter in Self::ALL_LETTERS {
            let url = format!(
                "http://bg-rock-archives.com/alphabetical.php?letter={}",
                letter
            );
            let html = client.get(&url).send().await?.text().await?;
            let entries = Self::parse_entries(self.catalog_id(), &html);
            entry_cache.extend(entries);
            if entry_cache.len() >= 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper7700 {
    /// All letter/digit pages available in the navigation menu.
    pub(crate) const ALL_LETTERS: &'static [&'static str] = &[
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G",
        "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X",
        "Y", "Z", "А", "Б", "В", "Г", "Д", "Е", "Ж", "З", "И", "Й", "К", "Л", "М", "Н", "О",
        "П", "Р", "С", "Т", "У", "Ф", "Х", "Ц", "Ч", "Ш", "Щ", "Ъ", "Ь", "Ю", "Я",
    ];

    /// Parse all band entries from an alphabetical listing page.
    pub(crate) fn parse_entries(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        lazy_static! {
            static ref RE_ROW: Regex = Regex::new(
                r#"href="bio\.php\?band_id=(\d+)">([^<]+)</a></td>\s*<td>([^<]*)</td>\s*<td>([^<]*)</td>\s*<td>(\d*)</td>"#
            )
            .unwrap();
        }
        RE_ROW
            .captures_iter(html)
            .filter_map(|caps| {
                let band_id = caps.get(1)?.as_str().to_string();
                let name = caps.get(2)?.as_str().trim().to_string();
                if name.is_empty() || band_id.is_empty() {
                    return None;
                }
                let genre = caps.get(3)?.as_str().trim().to_string();
                let city = caps.get(4)?.as_str().trim().to_string();
                let year = caps.get(5)?.as_str().trim().to_string();
                let ext_desc = Self::build_desc(&genre, &city, &year);
                let ext_url = format!(
                    "http://bg-rock-archives.com/bio.php?band_id={}",
                    band_id
                );
                let entry = Entry {
                    catalog: catalog_id,
                    ext_id: band_id,
                    ext_name: name,
                    ext_desc,
                    ext_url,
                    random: rand::rng().random(),
                    type_name: Some("Q215380".to_string()), // musical ensemble
                    ..Default::default()
                };
                Some(ExtendedEntry {
                    entry,
                    ..Default::default()
                })
            })
            .collect()
    }

    /// Build a short description from genre, city, and year.
    pub(crate) fn build_desc(genre: &str, city: &str, year: &str) -> String {
        let mut parts = vec![];
        if !genre.is_empty() {
            parts.push(genre.to_string());
        }
        if !city.is_empty() {
            parts.push(city.to_string());
        }
        if !year.is_empty() {
            parts.push(year.to_string());
        }
        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_HTML: &str = r#"
<table id='l1'>
    <tr>
        <td><a href="bio.php?band_id=2233">Абелит</a></td>
        <td>Hard Rock</td>
        <td>София</td>
        <td>1990</td>
    </tr>
    <tr>
        <td><a href="bio.php?band_id=218">Аборт</a></td>
        <td>Punk Rock</td>
        <td>Варна</td>
        <td>1989</td>
    </tr>
    <tr>
        <td><a href="bio.php?band_id=96">Абсолют</a></td>
        <td>Thrash Metal/Heavy Metal</td>
        <td>Свищов/Плевен</td>
        <td>1993</td>
    </tr>
    <tr>
        <td><a href="bio.php?band_id=59">Айсберг </a></td>
        <td>Hard Rock/Heavy Metal</td>
        <td>Ихтиман</td>
        <td>1987</td>
    </tr>
</table>"#;

    #[test]
    fn test_7700_parse_entries_count() {
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn test_7700_parse_entries_first_entry() {
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        let e = &entries[0].entry;
        assert_eq!(e.ext_id, "2233");
        assert_eq!(e.ext_name, "Абелит");
        assert_eq!(e.ext_desc, "Hard Rock, София, 1990");
        assert_eq!(
            e.ext_url,
            "http://bg-rock-archives.com/bio.php?band_id=2233"
        );
        assert_eq!(e.catalog, 7700);
        assert_eq!(e.type_name, Some("Q215380".to_string()));
    }

    #[test]
    fn test_7700_parse_entries_name_trimmed() {
        // "Айсберг " has a trailing space in the HTML — should be trimmed
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        assert_eq!(entries[3].entry.ext_name, "Айсберг");
    }

    #[test]
    fn test_7700_parse_entries_genre_with_slash() {
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        let e = &entries[2].entry;
        assert_eq!(e.ext_id, "96");
        assert_eq!(e.ext_desc, "Thrash Metal/Heavy Metal, Свищов/Плевен, 1993");
    }

    #[test]
    fn test_7700_parse_entries_empty_html() {
        let entries = BespokeScraper7700::parse_entries(7700, "");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_7700_build_desc_all_fields() {
        assert_eq!(
            BespokeScraper7700::build_desc("Hard Rock", "София", "1990"),
            "Hard Rock, София, 1990"
        );
    }

    #[test]
    fn test_7700_build_desc_missing_year() {
        assert_eq!(
            BespokeScraper7700::build_desc("Punk Rock", "Варна", ""),
            "Punk Rock, Варна"
        );
    }

    #[test]
    fn test_7700_build_desc_only_genre() {
        assert_eq!(
            BespokeScraper7700::build_desc("Heavy Metal", "", ""),
            "Heavy Metal"
        );
    }

    #[test]
    fn test_7700_build_desc_all_empty() {
        assert_eq!(BespokeScraper7700::build_desc("", "", ""), "");
    }

    #[test]
    fn test_7700_all_letters_count() {
        // 10 digits + 26 Latin + 30 Cyrillic = 66
        assert_eq!(BespokeScraper7700::ALL_LETTERS.len(), 66);
    }

    #[test]
    fn test_7700_all_letters_contains_cyrillic() {
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"А"));
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"Я"));
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"Щ"));
    }

    #[test]
    fn test_7700_all_letters_contains_latin() {
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"A"));
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"Z"));
    }

    #[test]
    fn test_7700_all_letters_contains_digits() {
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"0"));
        assert!(BespokeScraper7700::ALL_LETTERS.contains(&"9"));
    }

    #[test]
    fn test_7700_ext_url_format() {
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        for e in &entries {
            assert!(e.entry.ext_url.starts_with("http://bg-rock-archives.com/bio.php?band_id="));
        }
    }

    #[test]
    fn test_7700_no_duplicate_ext_ids() {
        let entries = BespokeScraper7700::parse_entries(7700, SAMPLE_HTML);
        let ids: Vec<&str> = entries.iter().map(|e| e.entry.ext_id.as_str()).collect();
        let mut seen = std::collections::HashSet::new();
        for id in &ids {
            assert!(seen.insert(*id), "Duplicate ext_id: {}", id);
        }
    }
}
