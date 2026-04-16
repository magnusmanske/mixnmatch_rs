use crate::{app_state::AppState, entry::Entry, extended_entry::ExtendedEntry};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// MyMovies.it

#[derive(Debug)]
pub struct BespokeScraper4825 {
    pub(super) app: AppState,
}

#[async_trait]
impl BespokeScraper for BespokeScraper4825 {
    fn new(app: &AppState) -> Self {
        Self { app: app.clone() }
    }

    fn catalog_id(&self) -> usize {
        4825
    }

    fn app(&self) -> &AppState {
        &self.app
    }

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut entry_cache = vec![];
        for id in 300000..408000 {
            let url = format!("https://www.mymovies.it/persone/-/{}", id);
            let html = match client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_client_error() || status.is_server_error() {
                        continue;
                    }
                    match resp.text().await {
                        Ok(text) => text,
                        Err(_) => continue,
                    }
                }
                Err(_) => continue,
            };
            if let Some(ee) = Self::parse_person(self.catalog_id(), id, &url, &html) {
                entry_cache.push(ee);
                if entry_cache.len() >= 500 {
                    self.process_cache(&mut entry_cache).await?;
                    entry_cache.clear();
                }
            }
        }
        self.process_cache(&mut entry_cache).await?;
        Ok(())
    }
}

impl BespokeScraper4825 {
    /// Parse a person page from MyMovies.it.
    ///
    /// Extracts the name from `<h1>` and description from `<p class="sottotitolo">`.
    /// The regex uses `(?s)` to make `.` match newlines (equivalent to PHP `/s` flag).
    pub(crate) fn parse_person(
        catalog_id: usize,
        id: u64,
        url: &str,
        html: &str,
    ) -> Option<ExtendedEntry> {
        lazy_static! {
            static ref RE_PERSON: Regex = Regex::new(
                r#"(?s)<h1>(.*?)</h1>\s*</div>\s*</div>\s*<p class="sottotitolo">\s*(.*?)\s*</p>"#
            )
            .unwrap();
        }

        let caps = RE_PERSON.captures(html)?;
        let name = caps.get(1)?.as_str().trim().to_string();
        let raw_desc = caps.get(2)?.as_str().trim().to_string();

        if name.is_empty() {
            return None;
        }

        let desc = Self::clean_description(&raw_desc, &name);
        let id_str = id.to_string();

        let entry = Entry {
            catalog: catalog_id,
            ext_id: id_str,
            ext_url: url.to_string(),
            ext_name: name,
            ext_desc: desc,
            random: rand::rng().random(),
            type_name: Some("Q5".to_string()),
            ..Default::default()
        };
        Some(ExtendedEntry {
            entry,
            ..Default::default()
        })
    }

    /// Clean the description by removing the person's name and Italian
    /// copula fragments ("è un'" and "è un ").
    pub(crate) fn clean_description(desc: &str, name: &str) -> String {
        let s = desc.replace(name, "");
        let s = s.replace("è un'", "");
        let s = s.replace("è un ", "");
        s.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_4825_parse_person_basic() {
        let html = r#"
        <div><div>
        <h1>Marco Rossi</h1>
        </div>
        </div>
        <p class="sottotitolo">
            Marco Rossi è un attore italiano
        </p>
        "#;
        let ee = BespokeScraper4825::parse_person(4825, 300001, "https://www.mymovies.it/persone/-/300001", html).unwrap();
        assert_eq!(ee.entry.ext_id, "300001");
        assert_eq!(ee.entry.ext_name, "Marco Rossi");
        assert_eq!(ee.entry.ext_url, "https://www.mymovies.it/persone/-/300001");
        assert_eq!(ee.entry.catalog, 4825);
        assert_eq!(ee.entry.type_name, Some("Q5".to_string()));
        // Description should have name removed
        assert!(!ee.entry.ext_desc.contains("Marco Rossi"));
        assert!(ee.entry.ext_desc.contains("attore italiano"));
    }

    #[test]
    fn test_4825_parse_person_with_e_un() {
        let html = r#"
        <div><div>
        <h1>Giulia Bianchi</h1>
        </div>
        </div>
        <p class="sottotitolo">
            Giulia Bianchi è un'attrice italiana
        </p>
        "#;
        let ee = BespokeScraper4825::parse_person(4825, 350000, "https://www.mymovies.it/persone/-/350000", html).unwrap();
        assert_eq!(ee.entry.ext_name, "Giulia Bianchi");
        // Both name and "è un'" should be removed
        assert!(!ee.entry.ext_desc.contains("Giulia Bianchi"));
        assert!(!ee.entry.ext_desc.contains("è un'"));
        assert!(ee.entry.ext_desc.contains("attrice italiana"));
    }

    #[test]
    fn test_4825_parse_person_with_e_un_masc() {
        let html = r#"
        <div><div>
        <h1>Luca Verdi</h1>
        </div>
        </div>
        <p class="sottotitolo">
            Luca Verdi è un regista italiano
        </p>
        "#;
        let ee = BespokeScraper4825::parse_person(4825, 400000, "https://www.mymovies.it/persone/-/400000", html).unwrap();
        assert!(!ee.entry.ext_desc.contains("è un "));
        assert!(ee.entry.ext_desc.contains("regista italiano"));
    }

    #[test]
    fn test_4825_parse_person_no_match() {
        let html = "<html><body><h2>Not a person page</h2></body></html>";
        let result = BespokeScraper4825::parse_person(4825, 300000, "https://www.mymovies.it/persone/-/300000", html);
        assert!(result.is_none());
    }

    #[test]
    fn test_4825_parse_person_empty_html() {
        let result = BespokeScraper4825::parse_person(4825, 300000, "https://www.mymovies.it/persone/-/300000", "");
        assert!(result.is_none());
    }

    #[test]
    fn test_4825_parse_person_multiline_h1() {
        // The (?s) flag makes . match newlines
        let html = "<div><div>\n<h1>Test\nPerson</h1>\n</div>\n</div>\n<p class=\"sottotitolo\">\ndescription\n</p>";
        let ee = BespokeScraper4825::parse_person(4825, 300002, "https://www.mymovies.it/persone/-/300002", html).unwrap();
        assert_eq!(ee.entry.ext_name, "Test\nPerson");
    }

    #[test]
    fn test_4825_clean_description_basic() {
        assert_eq!(
            BespokeScraper4825::clean_description("Marco Rossi è un attore", "Marco Rossi"),
            "attore"
        );
    }

    #[test]
    fn test_4825_clean_description_feminine() {
        assert_eq!(
            BespokeScraper4825::clean_description("Anna Neri è un'attrice", "Anna Neri"),
            "attrice"
        );
    }

    #[test]
    fn test_4825_clean_description_no_name() {
        assert_eq!(
            BespokeScraper4825::clean_description("è un regista", "Other Name"),
            "regista"
        );
    }

    #[test]
    fn test_4825_clean_description_trims() {
        assert_eq!(
            BespokeScraper4825::clean_description("  Name è un attore  ", "Name"),
            "attore"
        );
    }

    #[test]
    fn test_4825_clean_description_empty() {
        assert_eq!(
            BespokeScraper4825::clean_description("Name", "Name"),
            ""
        );
    }

    #[test]
    fn test_4825_parse_person_desc_trimmed() {
        let html = r#"
        <div><div>
        <h1>Test Actor</h1>
        </div>
        </div>
        <p class="sottotitolo">
            Test Actor è un attore e regista
        </p>
        "#;
        let ee = BespokeScraper4825::parse_person(4825, 300003, "https://www.mymovies.it/persone/-/300003", html).unwrap();
        assert_eq!(ee.entry.ext_desc, "attore e regista");
    }

    #[test]
    fn test_4825_id_to_string() {
        let html = r#"
        <div><div>
        <h1>Someone</h1>
        </div>
        </div>
        <p class="sottotitolo">
            description
        </p>
        "#;
        let ee = BespokeScraper4825::parse_person(4825, 407999, "https://www.mymovies.it/persone/-/407999", html).unwrap();
        assert_eq!(ee.entry.ext_id, "407999");
    }
}
