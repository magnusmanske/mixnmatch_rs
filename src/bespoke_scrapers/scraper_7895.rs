use std::sync::Arc;
use std::time::Duration;
use crate::{
    app_state::AppContext,
    entry::Entry,
    extended_entry::ExtendedEntry,
    person_date::PersonDate,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use rand::RngExt;
use regex::Regex;
use std::sync::LazyLock;

use super::BespokeScraper;

// ______________________________________________________
// Norwegian historical register of persons / histreg.no (catalog 7895, P4574)
//
// The full database holds ~6.5M persons, but `robots.txt` disallows
// `/index.php/person/`. We therefore seed the catalog from the two
// browseable curated index pages that *are* allowed:
//
//   /index.php/celebrities — ~252 "known persons" with name + ID
//   /index.php/registered  — ~300 user-submitted persons; carries a
//       hidden <input name="tablejson"> with structured rows
//       (name parts, gender, born/diedsort, place, source).
//
// `robots.txt` also asks for a 5-second crawl delay; with only two
// requests we sleep 5s between them and that's it.
//
// Names on /celebrities are stored "Lastname, Firstnames" (the project's
// own canonical form); we flip them to "Firstnames Lastname" so they
// align with Wikidata labels.

const BASE: &str = "https://histreg.no";
const CRAWL_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct BespokeScraper7895 {
    pub(super) app: Arc<dyn AppContext>,
}

#[async_trait]
impl BespokeScraper for BespokeScraper7895 {
    scraper_boilerplate!(7895);

    async fn run(&self) -> Result<()> {
        let client = self.http_client();
        let mut cache: Vec<ExtendedEntry> = vec![];

        let celeb_html = client
            .get(format!("{BASE}/index.php/celebrities"))
            .send().await?.text().await?;
        cache.extend(Self::parse_celebrities(self.catalog_id(), &celeb_html));
        self.maybe_flush_cache(&mut cache).await?;

        tokio::time::sleep(CRAWL_DELAY).await;

        let reg_html = client
            .get(format!("{BASE}/index.php/registered"))
            .send().await?.text().await?;
        let tablejson = Self::extract_tablejson(&reg_html)
            .ok_or_else(|| anyhow!("histreg.no /registered: tablejson input not found"))?;
        cache.extend(Self::parse_registered(self.catalog_id(), &tablejson)?);

        self.process_cache(&mut cache).await?;
        Ok(())
    }
}

impl BespokeScraper7895 {
    /// Parse `<a href=".../person/hbrid/{id}"> Lastname, Firstnames</a>`
    /// rows from the /celebrities page. The same person URL is also
    /// reachable as `/person/{id}` (the Wikidata P4574 formatter form),
    /// which we use for `ext_url`.
    pub(crate) fn parse_celebrities(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"person/hbrid/([A-Za-z0-9]+)"[^>]*>\s*([^<]+?)\s*</a>"#).unwrap()
        });
        RE.captures_iter(html)
            .filter_map(|c| {
                let id = c.get(1)?.as_str().to_string();
                let raw = c.get(2)?.as_str();
                let decoded = html_escape::decode_html_entities(raw).trim().to_string();
                if decoded.is_empty() { return None; }
                let name = flip_lastname_first(&decoded);
                Some(make_entry(catalog_id, id, name, String::new(), None, None))
            })
            .collect()
    }

    /// Pull the JSON payload out of the hidden
    /// `<input id="tablejson" type="hidden" name="tablejson" value="…">`
    /// element used by the /registered page to bootstrap its DataTable.
    pub(crate) fn extract_tablejson(html: &str) -> Option<String> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"name="tablejson"\s+value="([^"]+)""#).unwrap()
        });
        let raw = RE.captures(html)?.get(1)?.as_str();
        Some(html_escape::decode_html_entities(raw).into_owned())
    }

    pub(crate) fn parse_registered(catalog_id: usize, tablejson: &str) -> Result<Vec<ExtendedEntry>> {
        let rows: Vec<serde_json::Value> = serde_json::from_str(tablejson)?;
        Ok(rows.iter().filter_map(|r| Self::parse_registered_row(catalog_id, r)).collect())
    }

    pub(crate) fn parse_registered_row(catalog_id: usize, row: &serde_json::Value) -> Option<ExtendedEntry> {
        let id = row.get("pfid")?.as_str()?.trim();
        if id.is_empty() { return None; }
        let name = compose_registered_name(row);
        if name.is_empty() { return None; }
        let desc = build_registered_desc(row);
        let born = row.get("bornsort").and_then(|v| v.as_str()).and_then(parse_sort_date);
        let died = row.get("diedsort").and_then(|v| v.as_str()).and_then(parse_sort_date);
        Some(make_entry(catalog_id, id.to_string(), name, desc, born, died))
    }
}

fn make_entry(
    catalog_id: usize,
    id: String,
    name: String,
    desc: String,
    born: Option<PersonDate>,
    died: Option<PersonDate>,
) -> ExtendedEntry {
    let ext_url = format!("{BASE}/index.php/person/{id}");
    let entry = Entry {
        catalog: catalog_id,
        ext_id: id,
        ext_name: name,
        ext_desc: desc,
        ext_url,
        random: rand::rng().random(),
        type_name: Some("Q5".to_string()),
        ..Default::default()
    };
    ExtendedEntry { entry, born, died, ..Default::default() }
}

/// `"Aabel, Peder Per Pavels"` → `"Peder Per Pavels Aabel"`. Used for
/// the /celebrities list, where names are stored in surname-first form
/// for alphabetical browsing — we flip them so Mix'n'match's
/// label-matching aligns with Wikidata's `Firstname Lastname` form.
pub(crate) fn flip_lastname_first(s: &str) -> String {
    match s.split_once(',') {
        Some((last, rest)) => {
            let last = last.trim();
            let rest = rest.trim();
            if last.is_empty() { rest.to_string() }
            else if rest.is_empty() { last.to_string() }
            else { format!("{rest} {last}") }
        }
        None => s.trim().to_string(),
    }
}

/// Compose a display name from the /registered tablejson row.
/// Order: `name` + `patronymic` + (`lastname` or `maidenname`).
pub(crate) fn compose_registered_name(row: &serde_json::Value) -> String {
    let pick = |k: &str| -> String {
        row.get(k)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("")
            .to_string()
    };
    let mut parts: Vec<String> = vec![pick("name"), pick("patronymic")]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    let surname = {
        let s = pick("lastname");
        if s.is_empty() { pick("maidenname") } else { s }
    };
    if !surname.is_empty() { parts.push(surname); }
    parts.join(" ")
}

/// Build a short description joining the small set of free-text fields
/// available on a /registered row. Skips fields that are missing or
/// blank so we never emit `", , "`-style noise.
pub(crate) fn build_registered_desc(row: &serde_json::Value) -> String {
    let mut parts: Vec<String> = vec![];
    for k in ["profession", "bornp", "deadp", "source"] {
        if let Some(s) = row.get(k).and_then(|v| v.as_str()).map(str::trim) {
            if !s.is_empty() { parts.push(s.to_string()); }
        }
    }
    parts.join("; ")
}

/// Parse histreg.no's `bornsort` / `diedsort` field, which is YYYYMMDD
/// with zero parts for unknown precision (e.g. `"17460000"` → year 1746;
/// `"18470129"` → 1847-01-29).
pub(crate) fn parse_sort_date(s: &str) -> Option<PersonDate> {
    if s.len() != 8 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let y: i32 = s[0..4].parse().ok()?;
    let m: u8 = s[4..6].parse().ok()?;
    let d: u8 = s[6..8].parse().ok()?;
    if y < 1 { return None; }
    Some(match (m, d) {
        (0, _) => PersonDate::year_only(y),
        (_, 0) => PersonDate::year_month(y, m),
        _ => PersonDate::year_month_day(y, m, d),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── /celebrities ─────────────────────────────────────────────────

    const CELEB_HTML: &str = r#"
<ul>
    <li><a href="https://histreg.no/index.php/person/hbrid/pf01036372009613"> Aabel, Peder Per Pavels</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pd00000011502568"> Abel, Niels Henrich</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pf01053257046634"> Amundsen, Roald</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pc00000003676954"> Benkow, &quot;Jo&quot; Josef Elias</a></li>
</ul>"#;

    #[test]
    fn test_7895_parse_celebrities_count() {
        let entries = BespokeScraper7895::parse_celebrities(7895, CELEB_HTML);
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn test_7895_parse_celebrities_id_and_flipped_name() {
        let entries = BespokeScraper7895::parse_celebrities(7895, CELEB_HTML);
        let e = &entries[1].entry;
        assert_eq!(e.ext_id, "pd00000011502568");
        assert_eq!(e.ext_name, "Niels Henrich Abel");
        assert_eq!(e.type_name.as_deref(), Some("Q5"));
        assert_eq!(e.catalog, 7895);
    }

    #[test]
    fn test_7895_parse_celebrities_ext_url_uses_canonical_form() {
        // The /celebrities links use /person/hbrid/{id}, but Wikidata's P4574
        // formatter is /person/{id} — store the latter so existing links match.
        let entries = BespokeScraper7895::parse_celebrities(7895, CELEB_HTML);
        assert_eq!(
            entries[2].entry.ext_url,
            "https://histreg.no/index.php/person/pf01053257046634"
        );
    }

    #[test]
    fn test_7895_parse_celebrities_decodes_html_entities() {
        let entries = BespokeScraper7895::parse_celebrities(7895, CELEB_HTML);
        let benkow = entries.iter().find(|e| e.entry.ext_id == "pc00000003676954").unwrap();
        // After flip + entity decode: name should contain literal quote chars, not &quot;
        assert!(benkow.entry.ext_name.contains("\"Jo\""), "got {:?}", benkow.entry.ext_name);
        assert!(!benkow.entry.ext_name.contains("&quot;"));
        assert!(benkow.entry.ext_name.ends_with("Benkow"));
    }

    #[test]
    fn test_7895_flip_lastname_first_basic() {
        assert_eq!(flip_lastname_first("Bjørnson, Bjørnstjerne"), "Bjørnstjerne Bjørnson");
        assert_eq!(flip_lastname_first("Abel, Niels Henrich"), "Niels Henrich Abel");
    }

    #[test]
    fn test_7895_flip_lastname_first_no_comma_passthrough() {
        assert_eq!(flip_lastname_first("Roald Amundsen"), "Roald Amundsen");
    }

    #[test]
    fn test_7895_flip_lastname_first_strips_whitespace() {
        assert_eq!(flip_lastname_first("  Smith ,  John  "), "John Smith");
    }

    // ── /registered ──────────────────────────────────────────────────

    const REG_TABLEJSON: &str = r#"[
      {"lastname":null,"name":"Abelona","patronymic":null,"othername":null,"maidenname":"Josephsdatter","pfid":"PFU0000008","gender":"Kvinne","born":1746,"bornsort":"17460000","bornp":null,"deadp":null,"died":"","diedsort":"","profession":null,"source":"Tromsø Sokneprestkontor"},
      {"lastname":null,"name":"Anders Olai","patronymic":null,"othername":null,"maidenname":null,"pfid":"PFU0000010","gender":"Mann","born":"13.09.1867","bornsort":"18670913","bornp":"Kinn","deadp":null,"died":"","diedsort":"","profession":"sokneprest","source":"Ministerialbok for Kinn"},
      {"lastname":"Halvorsen","name":"Ingeborg","patronymic":null,"othername":null,"maidenname":null,"pfid":"PFU0000036","gender":"Kvinne","born":"29.01.1847","bornsort":"18470129","bornp":"Vevelstad, Nordland","deadp":null,"died":"","diedsort":"","profession":null,"source":"Statsarkivet i Trondheim"}
    ]"#;

    #[test]
    fn test_7895_parse_registered_count() {
        let entries = BespokeScraper7895::parse_registered(7895, REG_TABLEJSON).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_7895_parse_registered_fields() {
        let entries = BespokeScraper7895::parse_registered(7895, REG_TABLEJSON).unwrap();
        let e = &entries[0];
        assert_eq!(e.entry.ext_id, "PFU0000008");
        // lastname is null → falls back to maidenname
        assert_eq!(e.entry.ext_name, "Abelona Josephsdatter");
        assert_eq!(e.entry.type_name.as_deref(), Some("Q5"));
        assert_eq!(e.born, Some(PersonDate::year_only(1746)));
        assert_eq!(e.died, None);
    }

    #[test]
    fn test_7895_parse_registered_full_date() {
        let entries = BespokeScraper7895::parse_registered(7895, REG_TABLEJSON).unwrap();
        let e = &entries[1];
        assert_eq!(e.entry.ext_id, "PFU0000010");
        assert_eq!(e.entry.ext_name, "Anders Olai");
        assert_eq!(e.born, Some(PersonDate::year_month_day(1867, 9, 13)));
        // Description should include the place and profession
        assert!(e.entry.ext_desc.contains("Kinn"));
        assert!(e.entry.ext_desc.contains("sokneprest"));
    }

    #[test]
    fn test_7895_parse_registered_prefers_lastname_over_maidenname() {
        let entries = BespokeScraper7895::parse_registered(7895, REG_TABLEJSON).unwrap();
        let e = &entries[2];
        assert_eq!(e.entry.ext_name, "Ingeborg Halvorsen");
    }

    #[test]
    fn test_7895_parse_sort_date_year_only() {
        assert_eq!(parse_sort_date("17460000"), Some(PersonDate::year_only(1746)));
    }

    #[test]
    fn test_7895_parse_sort_date_full() {
        assert_eq!(parse_sort_date("18470129"), Some(PersonDate::year_month_day(1847, 1, 29)));
    }

    #[test]
    fn test_7895_parse_sort_date_year_month_only() {
        assert_eq!(parse_sort_date("18470100"), Some(PersonDate::year_month(1847, 1)));
    }

    #[test]
    fn test_7895_parse_sort_date_rejects_empty() {
        assert_eq!(parse_sort_date(""), None);
        assert_eq!(parse_sort_date("1847"), None);
        assert_eq!(parse_sort_date("nonsense"), None);
        assert_eq!(parse_sort_date("00000000"), None);
    }

    #[test]
    fn test_7895_compose_registered_name_drops_blanks() {
        let row = serde_json::json!({
            "name": "  Olav  ", "patronymic": null, "lastname": "  Tryggvason  ", "maidenname": null
        });
        assert_eq!(compose_registered_name(&row), "Olav Tryggvason");
    }

    #[test]
    fn test_7895_compose_registered_name_with_patronymic() {
        let row = serde_json::json!({
            "name": "Sigrid", "patronymic": "Olafsdatter", "lastname": null, "maidenname": null
        });
        assert_eq!(compose_registered_name(&row), "Sigrid Olafsdatter");
    }

    #[test]
    fn test_7895_extract_tablejson_unescapes_entities() {
        let html = r#"<input id="tablejson" type="hidden" name="tablejson" value="[{&quot;pfid&quot;:&quot;PFU0000001&quot;,&quot;name&quot;:&quot;Test&quot;}]" />"#;
        let payload = BespokeScraper7895::extract_tablejson(html).unwrap();
        assert!(payload.starts_with("[{\"pfid\""));
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["pfid"], "PFU0000001");
    }

    #[test]
    fn test_7895_parse_registered_skips_row_missing_pfid() {
        let bad = r#"[{"name":"Nobody","pfid":""}]"#;
        let entries = BespokeScraper7895::parse_registered(7895, bad).unwrap();
        assert!(entries.is_empty());
    }
}
