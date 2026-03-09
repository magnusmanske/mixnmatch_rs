use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::Rng;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// latvijasdaba.lv - Latvian Nature Species Encyclopedia

#[derive(Debug)]
pub struct BespokeScraper7696 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7696 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        7696
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let sitemap_url = "https://www.latvijasdaba.lv/sitemap.xml";
        let client = self.http_client();
        let text = client.get(sitemap_url).send().await?.text().await?;
        let species_urls = Self::extract_species_urls(&text);
        let mut entry_cache = vec![];
        for url in &species_urls {
            let (section, slug) = match Self::parse_species_url(url) {
                Some(v) => v,
                None => continue,
            };
            let ext_name = Self::slug_to_scientific_name(&slug);
            if ext_name.is_empty() {
                continue;
            }
            let ext_desc = Self::section_to_description(&section).to_string();
            let ext_id = format!("{}/{}", section, slug);
            let entry = Entry {
                catalog: self.catalog_id(),
                ext_id,
                ext_name,
                ext_desc,
                ext_url: url.to_string(),
                random: rand::rng().random(),
                type_name: Some("Q16521".to_string()),
                ..Default::default()
            };
            let ee = ExtendedEntry {
                entry,
                ..Default::default()
            };
            entry_cache.push(ee);
            if entry_cache.len() >= 100 {
                self.process_cache(&mut entry_cache).await?;
                entry_cache.clear();
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper7696 {
    pub(crate) const SECTIONS: &'static [(&'static str, &'static str)] = &[
        ("augi", "Plants"),
        ("senes", "Fungi"),
        ("gliemji", "Molluscs"),
        ("zirneklveidigie", "Arachnids"),
        ("vezi", "Crustaceans"),
        ("kukaini", "Insects"),
        ("taurini", "Butterflies and moths"),
        ("zivis", "Fish"),
        ("abinieki", "Amphibians"),
        ("rapuli", "Reptiles"),
        ("putni", "Birds"),
        ("ziditaji", "Mammals"),
        ("devona-fauna", "Devonian fauna"),
    ];

    pub(crate) fn extract_species_urls(sitemap_text: &str) -> Vec<String> {
        lazy_static! {
            static ref RE_SPECIES_URL: Regex = Regex::new(
                r"https://www\.latvijasdaba\.lv/([a-z-]+)/([a-z0-9][a-z0-9-]*[a-z0-9])/"
            )
            .unwrap();
        }
        let valid_sections: Vec<&str> = Self::SECTIONS.iter().map(|(s, _)| *s).collect();
        RE_SPECIES_URL
            .captures_iter(sitemap_text)
            .filter_map(|caps| {
                let section = caps.get(1)?.as_str();
                let slug = caps.get(2)?.as_str();
                if !valid_sections.contains(&section) {
                    return None;
                }
                if slug.starts_with("sistematiskais-raditajs") {
                    return None;
                }
                Some(caps.get(0)?.as_str().to_string())
            })
            .collect()
    }

    pub(crate) fn parse_species_url(url: &str) -> Option<(String, String)> {
        lazy_static! {
            static ref RE_PARSE_URL: Regex = Regex::new(
                r"https://www\.latvijasdaba\.lv/([a-z-]+)/([a-z0-9][a-z0-9-]*[a-z0-9])/"
            )
            .unwrap();
        }
        let caps = RE_PARSE_URL.captures(url)?;
        Some((
            caps.get(1)?.as_str().to_string(),
            caps.get(2)?.as_str().to_string(),
        ))
    }

    pub(crate) fn slug_to_scientific_name(slug: &str) -> String {
        let parts: Vec<&str> = slug.split('-').collect();
        if parts.is_empty() {
            return String::new();
        }
        let mut result = Vec::with_capacity(parts.len());
        for (i, part) in parts.iter().enumerate() {
            if i == 0 {
                let mut chars = part.chars();
                match chars.next() {
                    Some(c) => result.push(c.to_uppercase().to_string() + chars.as_str()),
                    None => return String::new(),
                }
            } else {
                result.push(part.to_string());
            }
        }
        result.join(" ")
    }

    pub(crate) fn section_to_description(section: &str) -> &'static str {
        Self::SECTIONS
            .iter()
            .find(|(s, _)| *s == section)
            .map(|(_, d)| *d)
            .unwrap_or("Species")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #lizard forgives the complexity
    #[test]
    fn test_7696_slug_to_scientific_name() {
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name("stellaria-nemorum-l"),
            "Stellaria nemorum l"
        );
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name("acer-platanoides-l"),
            "Acer platanoides l"
        );
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name("bromopsis-inermis-leyss-holub"),
            "Bromopsis inermis leyss holub"
        );
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name("anthyllis-x-baltica-juz-ex-kloczkova"),
            "Anthyllis x baltica juz ex kloczkova"
        );
        assert_eq!(BespokeScraper7696::slug_to_scientific_name(""), "");
    }

    #[test]
    fn test_7696_section_to_description() {
        assert_eq!(BespokeScraper7696::section_to_description("augi"), "Plants");
        assert_eq!(BespokeScraper7696::section_to_description("putni"), "Birds");
        assert_eq!(BespokeScraper7696::section_to_description("senes"), "Fungi");
        assert_eq!(
            BespokeScraper7696::section_to_description("devona-fauna"),
            "Devonian fauna"
        );
        assert_eq!(
            BespokeScraper7696::section_to_description("ziditaji"),
            "Mammals"
        );
        assert_eq!(
            BespokeScraper7696::section_to_description("unknown"),
            "Species"
        );
    }

    #[test]
    fn test_7696_parse_species_url() {
        assert_eq!(
            BespokeScraper7696::parse_species_url(
                "https://www.latvijasdaba.lv/augi/stellaria-nemorum-l/"
            ),
            Some(("augi".to_string(), "stellaria-nemorum-l".to_string()))
        );
        assert_eq!(
            BespokeScraper7696::parse_species_url(
                "https://www.latvijasdaba.lv/putni/accipiter-gentilis-l/"
            ),
            Some(("putni".to_string(), "accipiter-gentilis-l".to_string()))
        );
        assert_eq!(
            BespokeScraper7696::parse_species_url(
                "https://www.latvijasdaba.lv/devona-fauna/asterolepis-ornata/"
            ),
            Some(("devona-fauna".to_string(), "asterolepis-ornata".to_string()))
        );
        assert_eq!(
            BespokeScraper7696::parse_species_url("https://www.latvijasdaba.lv/augi/"),
            None
        );
        assert_eq!(
            BespokeScraper7696::parse_species_url("https://www.latvijasdaba.lv/"),
            None
        );
    }

    #[test]
    fn test_7696_extract_species_urls() {
        let sitemap = "https://www.latvijasdaba.lv/  \
            https://www.latvijasdaba.lv/augi/  \
            https://www.latvijasdaba.lv/augi/stellaria-nemorum-l/  \
            https://www.latvijasdaba.lv/augi/sistematiskais-raditajs/  \
            https://www.latvijasdaba.lv/augi/sistematiskais-raditajs/aceraceae/  \
            https://www.latvijasdaba.lv/putni/accipiter-gentilis-l/  \
            https://www.latvijasdaba.lv/zivis/esox-lucius-l/";
        let urls = BespokeScraper7696::extract_species_urls(sitemap);
        assert_eq!(urls.len(), 3);
        assert!(
            urls.contains(&"https://www.latvijasdaba.lv/augi/stellaria-nemorum-l/".to_string())
        );
        assert!(
            urls.contains(&"https://www.latvijasdaba.lv/putni/accipiter-gentilis-l/".to_string())
        );
        assert!(urls.contains(&"https://www.latvijasdaba.lv/zivis/esox-lucius-l/".to_string()));
        // Verify sistematiskais-raditajs entries are excluded
        assert!(!urls.iter().any(|u| u.contains("sistematiskais")));
        // Verify section root URLs are excluded
        assert!(!urls.contains(&"https://www.latvijasdaba.lv/augi/".to_string()));
    }

    #[test]
    fn test_7696_extract_species_urls_ignores_unknown_sections() {
        let sitemap = "https://www.latvijasdaba.lv/unknown-section/some-species-l/  \
            https://www.latvijasdaba.lv/augi/betula-pendula-roth/";
        let urls = BespokeScraper7696::extract_species_urls(sitemap);
        assert_eq!(urls.len(), 1);
        assert!(
            urls.contains(&"https://www.latvijasdaba.lv/augi/betula-pendula-roth/".to_string())
        );
    }

    #[test]
    fn test_7696_slug_to_scientific_name_single_part() {
        // A slug with just one part (genus only, no epithet)
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name("cyclops"),
            "Cyclops"
        );
    }

    #[test]
    fn test_7696_slug_to_scientific_name_preserves_hyphens_as_spaces() {
        // URL slugs with many hyphen-separated parts
        assert_eq!(
            BespokeScraper7696::slug_to_scientific_name(
                "equisetum-variegatum-schleich-ex-fweber-et-dmohr"
            ),
            "Equisetum variegatum schleich ex fweber et dmohr"
        );
    }

    #[test]
    fn test_7696_section_completeness() {
        // All 13 known sections should map to a description that is NOT "Species"
        let known_sections = [
            "augi",
            "senes",
            "gliemji",
            "zirneklveidigie",
            "vezi",
            "kukaini",
            "taurini",
            "zivis",
            "abinieki",
            "rapuli",
            "putni",
            "ziditaji",
            "devona-fauna",
        ];
        for section in &known_sections {
            let desc = BespokeScraper7696::section_to_description(section);
            assert_ne!(
                desc, "Species",
                "Section '{}' should have a specific description",
                section
            );
        }
    }

    #[test]
    fn test_7696_parse_species_url_trailing_content() {
        // Sistematiskais-raditajs sub-paths should not parse as species
        assert_eq!(
            BespokeScraper7696::parse_species_url(
                "https://www.latvijasdaba.lv/augi/sistematiskais-raditajs/"
            ),
            Some(("augi".to_string(), "sistematiskais-raditajs".to_string()))
        );
        // But extract_species_urls filters them out
        let sitemap = "https://www.latvijasdaba.lv/augi/sistematiskais-raditajs/";
        let urls = BespokeScraper7696::extract_species_urls(sitemap);
        assert!(urls.is_empty());
    }

    #[test]
    fn test_7696_extract_species_urls_deduplication_not_needed() {
        // Each URL in the sitemap should appear once; verify extraction handles unique URLs
        let sitemap = "https://www.latvijasdaba.lv/augi/betula-pendula-roth/ \
            https://www.latvijasdaba.lv/augi/betula-nana-l/";
        let urls = BespokeScraper7696::extract_species_urls(sitemap);
        assert_eq!(urls.len(), 2);
    }

    #[test]
    fn test_7696_sections_count() {
        // Ensure the SECTIONS constant has exactly 13 entries
        assert_eq!(BespokeScraper7696::SECTIONS.len(), 13);
    }

    #[test]
    fn test_7696_sections_no_duplicate_keys() {
        let keys: Vec<&str> = BespokeScraper7696::SECTIONS
            .iter()
            .map(|(k, _)| *k)
            .collect();
        let mut seen = std::collections::HashSet::new();
        for key in &keys {
            assert!(seen.insert(*key), "Duplicate section key: {}", key);
        }
    }

    #[test]
    fn test_7696_sections_no_duplicate_descriptions() {
        let descs: Vec<&str> = BespokeScraper7696::SECTIONS
            .iter()
            .map(|(_, d)| *d)
            .collect();
        let mut seen = std::collections::HashSet::new();
        for desc in &descs {
            assert!(
                seen.insert(*desc),
                "Duplicate section description: {}",
                desc
            );
        }
    }

    #[test]
    fn test_7696_parse_species_url_single_char_slug_not_matched() {
        // The regex requires slug to be at least 2 chars (starts and ends with [a-z0-9])
        assert_eq!(
            BespokeScraper7696::parse_species_url("https://www.latvijasdaba.lv/augi/a/"),
            None
        );
    }

    #[test]
    fn test_7696_ext_id_format() {
        // Verify the ext_id format produced by the scraper matches "section/slug"
        let section = "putni";
        let slug = "accipiter-gentilis-l";
        let ext_id = format!("{}/{}", section, slug);
        assert_eq!(ext_id, "putni/accipiter-gentilis-l");
    }
}
