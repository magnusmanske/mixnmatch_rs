use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use crate::{
    app_state::AppContext,
    entry::Entry,
    extended_entry::ExtendedEntry,
    person_date::PersonDate,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use log::info;
use rand::RngExt;
use regex::Regex;

use super::BespokeScraper;

// ______________________________________________________
// Norwegian historical register of persons / histreg.no (catalog 7895, P4574)
//
// The full database holds ~6.5M persons, but `robots.txt` disallows
// `/index.php/person/`, so we can't crawl person pages directly. The
// search interface is gated by Cloudflare Turnstile. We therefore
// seed the catalog from every *bulk* endpoint the site exposes:
//
//   /index.php/celebrities         ~252 curated "known persons" (HTML list)
//   /index.php/examples            ~2,070 example persons (HTML list, same shape)
//   /index.php/registered          ~300 user-submitted persons; tablejson
//                                  with name parts + bornsort/diedsort
//   /index.php/themareg/showbio    index page listing ~80 thematic CSVs
//   /index.php/themareg/downloadbio/{slug}
//                                  one CSV per slug (Statskalender-1869…1960,
//                                  Ingeniorer, Lensmenn, …) — semicolon-
//                                  separated, header row, ~600k rows in total
//
// `robots.txt` also asks for a 5-second crawl delay, which we honour
// with `tokio::time::sleep` between every fetch.
//
// Names on /celebrities and /examples are stored "Lastname, Firstnames"
// (the project's own canonical form); we flip them to "Firstnames
// Lastname" so Mix'n'match's label-matcher aligns with Wikidata.

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
        // Within a single run several sources can mention the same pfid
        // (Statskalender years overlap; a celebrity may also appear in
        // /examples). `process_cache` deduplicates against the DB but
        // its check is a single batched query taken *before* any insert,
        // so two new rows with the same ext_id within one batch would
        // both go to `insert_new` and trip the unique constraint. Dedup
        // here.
        let mut seen: HashSet<String> = HashSet::new();
        let cid = self.catalog_id();

        Self::extend_unique(&mut cache, &mut seen, Self::fetch_hbrid_listing(
            &client, cid, "/index.php/celebrities").await?);
        self.maybe_flush_cache(&mut cache).await?;
        tokio::time::sleep(CRAWL_DELAY).await;

        Self::extend_unique(&mut cache, &mut seen, Self::fetch_hbrid_listing(
            &client, cid, "/index.php/examples").await?);
        self.maybe_flush_cache(&mut cache).await?;
        tokio::time::sleep(CRAWL_DELAY).await;

        Self::extend_unique(&mut cache, &mut seen, Self::fetch_registered(&client, cid).await?);
        self.maybe_flush_cache(&mut cache).await?;
        tokio::time::sleep(CRAWL_DELAY).await;

        // Bulk thematic CSVs — the bulk of the catalog (~600k rows).
        let showbio_html = client
            .get(format!("{BASE}/index.php/themareg/showbio"))
            .send().await?.text().await?;
        let slugs = Self::parse_themareg_index(&showbio_html);
        for slug in slugs {
            tokio::time::sleep(CRAWL_DELAY).await;
            let url = format!("{BASE}/index.php/themareg/downloadbio/{slug}");
            let csv_text = match client.get(&url).send().await.and_then(|r| r.error_for_status()) {
                Ok(resp) => resp.text().await?,
                Err(e) => { self.log(format!("histreg.no: CSV fetch failed for {slug}: {e}")); continue; }
            };
            let rows = match Self::parse_themareg_csv(cid, &slug, &csv_text) {
                Ok(rs) => rs,
                Err(e) => { self.log(format!("histreg.no: CSV parse failed for {slug}: {e}")); continue; }
            };
            let count = rows.len();
            Self::extend_unique(&mut cache, &mut seen, rows);
            info!("histreg.no: {slug}: {count} rows");
            self.maybe_flush_cache(&mut cache).await?;
        }

        self.process_cache(&mut cache).await?;
        Ok(())
    }
}

impl BespokeScraper7895 {
    /// Push every entry whose `ext_id` hasn't been seen yet in this run.
    fn extend_unique(
        cache: &mut Vec<ExtendedEntry>,
        seen: &mut HashSet<String>,
        items: Vec<ExtendedEntry>,
    ) {
        for ee in items {
            if seen.insert(ee.entry.ext_id.clone()) {
                cache.push(ee);
            }
        }
    }

    async fn fetch_hbrid_listing(
        client: &reqwest::Client,
        catalog_id: usize,
        path: &str,
    ) -> Result<Vec<ExtendedEntry>> {
        let html = client.get(format!("{BASE}{path}")).send().await?.text().await?;
        Ok(Self::parse_hbrid_listing(catalog_id, &html))
    }

    async fn fetch_registered(
        client: &reqwest::Client,
        catalog_id: usize,
    ) -> Result<Vec<ExtendedEntry>> {
        let html = client.get(format!("{BASE}/index.php/registered")).send().await?.text().await?;
        let tablejson = Self::extract_tablejson(&html)
            .ok_or_else(|| anyhow!("histreg.no /registered: tablejson input not found"))?;
        Self::parse_registered(catalog_id, &tablejson)
    }

    /// Parse `<a href=".../person/hbrid/{id}"> Lastname, Firstnames</a>`
    /// anchors from /celebrities, /examples, and any other HTML page
    /// that uses the same link shape. The same person URL is also
    /// reachable as `/person/{id}` (the Wikidata P4574 formatter form),
    /// which we use for `ext_url`.
    pub(crate) fn parse_hbrid_listing(catalog_id: usize, html: &str) -> Vec<ExtendedEntry> {
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
    /// element used by /registered to bootstrap its DataTable.
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

    /// Extract the list of CSV slugs from /themareg/showbio. Each slug
    /// names a thematic biographical source (`Statskalender-1869-v3`,
    /// `Ingeniorer-data2`, …) and is appended to
    /// `/index.php/themareg/downloadbio/` to fetch the CSV.
    pub(crate) fn parse_themareg_index(html: &str) -> Vec<String> {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"/index\.php/themareg/downloadbio/([A-Za-z0-9_-]+)"#).unwrap()
        });
        let mut seen = HashSet::new();
        RE.captures_iter(html)
            .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
            .filter(|s| seen.insert(s.clone()))
            .collect()
    }

    /// Parse a semicolon-separated CSV from /themareg/downloadbio/.
    /// Header schemas vary between slugs but always share at least
    /// `pfid;fornavn;etternavn`. Optional columns we use when present:
    /// `fodselsdato`, `fodselsaar`, `tittel`, `yrke`, `bosted`, `aar`.
    pub(crate) fn parse_themareg_csv(
        catalog_id: usize,
        slug: &str,
        csv_text: &str,
    ) -> Result<Vec<ExtendedEntry>> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(true)
            .flexible(true)
            .from_reader(csv_text.as_bytes());
        let mut out = Vec::new();
        for rec in rdr.deserialize::<HashMap<String, String>>() {
            match rec {
                Ok(row) => {
                    if let Some(ee) = Self::parse_themareg_row(catalog_id, slug, &row) {
                        out.push(ee);
                    }
                }
                Err(_) => continue, // skip malformed rows; common at file end
            }
        }
        Ok(out)
    }

    pub(crate) fn parse_themareg_row(
        catalog_id: usize,
        slug: &str,
        row: &HashMap<String, String>,
    ) -> Option<ExtendedEntry> {
        let pfid = row.get("pfid").map(|s| s.trim()).unwrap_or("");
        if pfid.is_empty() { return None; }
        let fornavn = row.get("fornavn").map(|s| s.trim()).unwrap_or("");
        let etternavn = row.get("etternavn").map(|s| s.trim()).unwrap_or("");
        let name = match (fornavn.is_empty(), etternavn.is_empty()) {
            (true, true) => return None,
            (false, true) => fornavn.to_string(),
            (true, false) => etternavn.to_string(),
            (false, false) => format!("{fornavn} {etternavn}"),
        };
        let born = row.get("fodselsdato")
            .and_then(|s| parse_csv_date(s))
            .or_else(|| row.get("fodselsaar").and_then(|s| parse_year(s)));
        let desc = build_themareg_desc(row, slug);
        Some(make_entry(catalog_id, pfid.to_string(), name, desc, born, None))
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
/// /celebrities and /examples, where names are stored in surname-first
/// form for alphabetical browsing — we flip them so Mix'n'match's
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

/// Build a short description from a themareg CSV row. Combines the
/// rank/title/profession/place columns when present, and always
/// suffixes the source slug so the entry's provenance is visible.
pub(crate) fn build_themareg_desc(row: &HashMap<String, String>, slug: &str) -> String {
    let mut parts: Vec<String> = vec![];
    for k in ["tittel", "yrke", "organ", "bosted"] {
        if let Some(v) = row.get(k).map(|s| s.trim()) {
            if !v.is_empty() { parts.push(v.to_string()); }
        }
    }
    parts.push(format!("[{slug}]"));
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

/// Parse a CSV date cell. Accepts `YYYY`, `YYYY-MM-DD`, `DD.MM.YYYY`
/// (the latter is the Norwegian display form used in several
/// `fodselsdato` columns). Returns `None` if the cell is empty or
/// shaped like something else.
pub(crate) fn parse_csv_date(s: &str) -> Option<PersonDate> {
    let s = s.trim();
    if s.is_empty() { return None; }
    static RE_DMY: LazyLock<Regex> = LazyLock::new(||
        Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{3,4})$").unwrap()
    );
    static RE_YMD: LazyLock<Regex> = LazyLock::new(||
        Regex::new(r"^(\d{3,4})-(\d{1,2})-(\d{1,2})$").unwrap()
    );
    if let Some(c) = RE_DMY.captures(s) {
        let d: u8 = c[1].parse().ok()?;
        let m: u8 = c[2].parse().ok()?;
        let y: i32 = c[3].parse().ok()?;
        return Some(PersonDate::year_month_day(y, m, d));
    }
    if let Some(c) = RE_YMD.captures(s) {
        let y: i32 = c[1].parse().ok()?;
        let m: u8 = c[2].parse().ok()?;
        let d: u8 = c[3].parse().ok()?;
        return Some(PersonDate::year_month_day(y, m, d));
    }
    parse_year(s)
}

/// Parse a year-only cell. Some `fodselsdato` columns contain only the
/// year (e.g. `"1872"`), and `fodselsaar` always does.
pub(crate) fn parse_year(s: &str) -> Option<PersonDate> {
    let s = s.trim();
    if s.is_empty() { return None; }
    let y: i32 = s.parse().ok()?;
    if y < 1 { return None; }
    Some(PersonDate::year_only(y))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── /celebrities + /examples (parse_hbrid_listing) ─────────────────

    const CELEB_HTML: &str = r#"
<ul>
    <li><a href="https://histreg.no/index.php/person/hbrid/pf01036372009613"> Aabel, Peder Per Pavels</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pd00000011502568"> Abel, Niels Henrich</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pf01053257046634"> Amundsen, Roald</a></li>
    <li><a href="https://histreg.no/index.php/person/hbrid/pc00000003676954"> Benkow, &quot;Jo&quot; Josef Elias</a></li>
</ul>"#;

    #[test]
    fn test_7895_parse_hbrid_listing_count() {
        let entries = BespokeScraper7895::parse_hbrid_listing(7895, CELEB_HTML);
        assert_eq!(entries.len(), 4);
    }

    #[test]
    fn test_7895_parse_hbrid_listing_id_and_flipped_name() {
        let entries = BespokeScraper7895::parse_hbrid_listing(7895, CELEB_HTML);
        let e = &entries[1].entry;
        assert_eq!(e.ext_id, "pd00000011502568");
        assert_eq!(e.ext_name, "Niels Henrich Abel");
        assert_eq!(e.type_name.as_deref(), Some("Q5"));
        assert_eq!(e.catalog, 7895);
    }

    #[test]
    fn test_7895_parse_hbrid_listing_ext_url_uses_canonical_form() {
        // The page links use /person/hbrid/{id}, but Wikidata's P4574
        // formatter is /person/{id} — store the latter so existing links match.
        let entries = BespokeScraper7895::parse_hbrid_listing(7895, CELEB_HTML);
        assert_eq!(
            entries[2].entry.ext_url,
            "https://histreg.no/index.php/person/pf01053257046634"
        );
    }

    #[test]
    fn test_7895_parse_hbrid_listing_decodes_html_entities() {
        let entries = BespokeScraper7895::parse_hbrid_listing(7895, CELEB_HTML);
        let benkow = entries.iter().find(|e| e.entry.ext_id == "pc00000003676954").unwrap();
        assert!(benkow.entry.ext_name.contains("\"Jo\""), "got {:?}", benkow.entry.ext_name);
        assert!(!benkow.entry.ext_name.contains("&quot;"));
        assert!(benkow.entry.ext_name.ends_with("Benkow"));
    }

    #[test]
    fn test_7895_parse_hbrid_listing_handles_examples_format() {
        // /examples uses the same anchor shape but with FNG-prefixed IDs.
        let html = r#"<a href="https://histreg.no/index.php/person/hbrid/FNG0000000004133"> Aase, Andris</a>"#;
        let entries = BespokeScraper7895::parse_hbrid_listing(7895, html);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry.ext_id, "FNG0000000004133");
        assert_eq!(entries[0].entry.ext_name, "Andris Aase");
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
        assert!(e.entry.ext_desc.contains("Kinn"));
        assert!(e.entry.ext_desc.contains("sokneprest"));
    }

    #[test]
    fn test_7895_parse_registered_prefers_lastname_over_maidenname() {
        let entries = BespokeScraper7895::parse_registered(7895, REG_TABLEJSON).unwrap();
        assert_eq!(entries[2].entry.ext_name, "Ingeborg Halvorsen");
    }

    // ── parse_sort_date / parse_csv_date / parse_year ────────────────

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
    fn test_7895_parse_csv_date_dmy() {
        assert_eq!(parse_csv_date("13.09.1867"), Some(PersonDate::year_month_day(1867, 9, 13)));
        assert_eq!(parse_csv_date("01.01.1900"), Some(PersonDate::year_month_day(1900, 1, 1)));
    }

    #[test]
    fn test_7895_parse_csv_date_ymd() {
        assert_eq!(parse_csv_date("1867-09-13"), Some(PersonDate::year_month_day(1867, 9, 13)));
    }

    #[test]
    fn test_7895_parse_csv_date_year_only() {
        assert_eq!(parse_csv_date("1872"), Some(PersonDate::year_only(1872)));
    }

    #[test]
    fn test_7895_parse_csv_date_blank_and_garbage() {
        assert_eq!(parse_csv_date(""), None);
        assert_eq!(parse_csv_date("   "), None);
        assert_eq!(parse_csv_date("foo"), None);
        assert_eq!(parse_csv_date("13.13"), None);
    }

    #[test]
    fn test_7895_parse_year_rejects_zero() {
        assert_eq!(parse_year("0"), None);
        assert_eq!(parse_year("1872"), Some(PersonDate::year_only(1872)));
    }

    // ── compose_registered_name / desc helpers ───────────────────────

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

    // ── /themareg/showbio index + downloadbio CSV ────────────────────

    #[test]
    fn test_7895_parse_themareg_index_collects_slugs() {
        let html = r#"
            <a href="https://histreg.no/index.php/themareg/downloadbio/Ingeniorer-data2">Ingeniører</a>
            <a href="https://histreg.no/index.php/themareg/downloadbio/Statskalender-1885-v3">Statskalender 1885</a>
            <a href="https://histreg.no/index.php/themareg/downloadbio/Statskalender-1930--vv2">Statskalender 1930</a>
        "#;
        let slugs = BespokeScraper7895::parse_themareg_index(html);
        assert_eq!(slugs, vec![
            "Ingeniorer-data2".to_string(),
            "Statskalender-1885-v3".to_string(),
            "Statskalender-1930--vv2".to_string(),
        ]);
    }

    #[test]
    fn test_7895_parse_themareg_index_dedups() {
        let html = r#"
            <a href="https://histreg.no/index.php/themareg/downloadbio/Lensmenn-data2">A</a>
            <a href="https://histreg.no/index.php/themareg/downloadbio/Lensmenn-data2">B</a>
        "#;
        let slugs = BespokeScraper7895::parse_themareg_index(html);
        assert_eq!(slugs, vec!["Lensmenn-data2".to_string()]);
    }

    const CSV_INGENIORER: &str = "pfid;fornavn;etternavn;arkiv_navn;v1;url;sidenr;fodselsdato;bosted;kommunenummer;yrke;fars_navn;mors_navn;partners_navn;histreg;fodselsaar\n\
        Bio0002000000001;\"Olav A. V.\";Aabel;Ingeniorer-data2;1;https://www.nb.no/items/x;21;1872;Trondhjem;1601;jernbaneingeniør;;;;;1872\n\
        Bio0002000000002;\"Holger O. J.\";Aarhus;Ingeniorer-data2;2;https://www.nb.no/items/y;21;1890;Kristiania;301;ingeniør;;;;;1890\n";

    #[test]
    fn test_7895_parse_themareg_csv_count() {
        let entries = BespokeScraper7895::parse_themareg_csv(7895, "Ingeniorer-data2", CSV_INGENIORER).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_7895_parse_themareg_csv_extracts_fields() {
        let entries = BespokeScraper7895::parse_themareg_csv(7895, "Ingeniorer-data2", CSV_INGENIORER).unwrap();
        let e = &entries[0];
        assert_eq!(e.entry.ext_id, "Bio0002000000001");
        assert_eq!(e.entry.ext_name, "Olav A. V. Aabel");
        assert_eq!(e.entry.type_name.as_deref(), Some("Q5"));
        assert_eq!(e.born, Some(PersonDate::year_only(1872)));
        assert!(e.entry.ext_desc.contains("jernbaneingeniør"));
        assert!(e.entry.ext_desc.contains("Trondhjem"));
        assert!(e.entry.ext_desc.contains("[Ingeniorer-data2]"));
        assert_eq!(e.entry.ext_url, "https://histreg.no/index.php/person/Bio0002000000001");
    }

    const CSV_STATSKALENDER: &str = "pfid;fornavn;etternavn;arkiv_navn;url;sidenr;linjenr;fodselsaar;utdannelse;utdannelse_aar;utnevnt_aar;organ;tittel;kommunenummer;bosted;lonn;aar\n\
        BioSK18850000001;\"Ferdinand Nicolai\";Roll;Statskalender-1885-v3;http://x;59;7039;1831;;;;Høyesterett;Boghandler;1501;Aalesund;;1885\n";

    #[test]
    fn test_7895_parse_themareg_csv_statskalender_uses_fodselsaar() {
        let entries = BespokeScraper7895::parse_themareg_csv(7895, "Statskalender-1885-v3", CSV_STATSKALENDER).unwrap();
        let e = &entries[0];
        assert_eq!(e.entry.ext_id, "BioSK18850000001");
        assert_eq!(e.entry.ext_name, "Ferdinand Nicolai Roll");
        assert_eq!(e.born, Some(PersonDate::year_only(1831)));
        // tittel + organ + bosted should all surface
        assert!(e.entry.ext_desc.contains("Boghandler"));
        assert!(e.entry.ext_desc.contains("Høyesterett"));
        assert!(e.entry.ext_desc.contains("Aalesund"));
    }

    #[test]
    fn test_7895_parse_themareg_row_missing_pfid_returns_none() {
        let row: HashMap<String, String> = [("fornavn", "X"), ("etternavn", "Y")]
            .iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        assert!(BespokeScraper7895::parse_themareg_row(7895, "slug", &row).is_none());
    }

    #[test]
    fn test_7895_parse_themareg_row_missing_names_returns_none() {
        let row: HashMap<String, String> = [("pfid", "Bio1"), ("fornavn", ""), ("etternavn", "")]
            .iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        assert!(BespokeScraper7895::parse_themareg_row(7895, "slug", &row).is_none());
    }

    #[test]
    fn test_7895_parse_themareg_row_only_firstname() {
        let row: HashMap<String, String> = [("pfid", "Bio1"), ("fornavn", "Ola")]
            .iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let ee = BespokeScraper7895::parse_themareg_row(7895, "slug", &row).unwrap();
        assert_eq!(ee.entry.ext_name, "Ola");
    }

    // ── extend_unique ────────────────────────────────────────────────

    #[test]
    fn test_7895_extend_unique_dedupes_within_run() {
        let mut cache = Vec::new();
        let mut seen = HashSet::new();
        let make = |id: &str| ExtendedEntry {
            entry: Entry { ext_id: id.to_string(), ..Default::default() }, ..Default::default()
        };
        BespokeScraper7895::extend_unique(&mut cache, &mut seen, vec![make("a"), make("b")]);
        BespokeScraper7895::extend_unique(&mut cache, &mut seen, vec![make("b"), make("c")]);
        let ids: Vec<&str> = cache.iter().map(|e| e.entry.ext_id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }
}
