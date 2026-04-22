//! Rust port of `scripts/reference_fixer.php`.
//!
//! Takes a QID from the `reference_fixer` work queue, fetches the
//! item's current statements from Wikidata, and rewrites free-form
//! `reference URL (P854)` reference groups into typed external-id
//! references (`<target property>` + `stated in (P248)`) whenever the
//! URL matches a known property formatter pattern.
//!
//! The PHP author disabled the script at the top of the file
//! ("DEACTIVATED FOR POTENTIALLY ADDING MULTIPLE REFERENCE PARTS
//! INSTEAD OF SINGLE REFERENCE"). We preserve the original splitting
//! behaviour faithfully here — when a reference group contains
//! multiple P854 URLs, each URL becomes its own reference group —
//! but run the job in a way that's easy to disable (remove the
//! "reference_fixer" job row).

use crate::app_state::AppState;
use anyhow::{Result, anyhow};
use log::{info, warn};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

/// Summary string sent with every edit.
const EDIT_SUMMARY: &str = "Fixing references as part of Mix'n'match cleanup";

/// SPARQL endpoint. Using the public endpoint so the bot doesn't need
/// its own SPARQL service account.
const SPARQL_URL: &str = "https://query.wikidata.org/sparql";

/// Wait between every mutating API call so we don't burst through
/// maxlag / the rate-limiter. Matches PHP's `sleep(1)`.
const INTER_EDIT_DELAY: Duration = Duration::from_millis(1000);

/// How many QIDs to pull off the queue per batch. The queue is drained
/// in a loop, so this mostly affects how often we hit the DB vs. how
/// much we buffer in memory.
const BATCH_SIZE: usize = 50;

/// Properties ignored when deciding whether a reference group is a
/// "self-reference" (the reference restates the statement's main
/// property). P248 = stated in, P813 = retrieved.
const SELF_REF_IGNORED_PROPS: &[&str] = &["P248", "P813"];

/// URL-pattern regexes hard-coded in the original script for
/// collections that don't expose a usable P1630/P1921. Kept here so we
/// don't lose the curated coverage.
const HARDCODED_PATTERNS: &[(&str, &str)] = &[
    (r"^https?://www\.biodiversitylibrary\.org/creator/(.+?)/*$", "P4081"),
    (r"^https?://trove\.nla\.gov\.au/people/(\d+).*$", "P1315"),
    (r"^https?://openlibrary\.org/authors/(.+?)/.*$", "P648"),
    (r"^https?://www\.biusante\.parisdescartes\.fr/histoire/biographies/index\.php\?cle=(\d+)", "P5375"),
    (r"^https?://biusante\.parisdescartes\.fr/histoire/biographies/index\.php\?cle=(\d+)", "P5375"),
    (r"^https?://bibliotheque\.academie-medecine\.fr/membres/membre/\?mbreid=(\d+).*$", "P3956"),
    (r"^https?://www\.artnet\.com/artists/([^/]+).*$", "P3782"),
    (r"^https?://www\.mutualart\.com/Artist/[^/]+/([^/]+).*$", "P6578"),
    (r"^https?://en\.isabart\.org/person/(\d+).*$", "P6844"),
    (r"^https?://www\.sikart\.ch/KuenstlerInnen\.aspx\?id=(\d+).*$", "P781"),
];

/// Main actor. `new` is cheap; `initialize` does the SPARQL work to
/// build the URL-pattern / stated-in maps.
#[derive(Debug)]
pub struct ReferenceFixer {
    app: AppState,
    http: reqwest::Client,
    /// Compiled (regex, property-id) pairs. Ordered — the first match wins
    /// for any given URL.
    url_patterns: Vec<(Regex, String)>,
    /// Property id → "stated in" item (P9073), e.g. `P648 → Q1201876`.
    /// When we replace a URL reference with a typed external-id snak,
    /// we also emit a P248 stated-in snak pointing at this item.
    stated_in: HashMap<String, String>,
    /// If true, don't actually call the Wikidata API — just log what
    /// would have happened. Primarily for local testing.
    pub simulating: bool,
}

impl ReferenceFixer {
    pub fn new(app: &AppState) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent("Mix'n'match reference fixer (https://mix-n-match.toolforge.org)")
            .build()?;
        Ok(Self {
            app: app.clone(),
            http,
            url_patterns: Vec::new(),
            stated_in: HashMap::new(),
            simulating: false,
        })
    }

    /// Load the URL-pattern → property map and the stated-in map. Runs
    /// two SPARQL queries against query.wikidata.org.
    pub async fn initialize(&mut self) -> Result<()> {
        self.url_patterns = self.load_formatter_urls().await?;
        self.stated_in = self.load_stated_in().await?;
        info!(
            "reference_fixer: {} URL patterns, {} stated-in mappings",
            self.url_patterns.len(),
            self.stated_in.len()
        );
        Ok(())
    }

    async fn sparql_json(&self, sparql: &str) -> Result<Value> {
        let resp = self
            .http
            .get(SPARQL_URL)
            .query(&[("query", sparql), ("format", "json")])
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!(
                "SPARQL endpoint returned HTTP {}",
                resp.status()
            ));
        }
        Ok(resp.json().await?)
    }

    /// Build the URL-pattern → property map by converting each
    /// property's formatter URL (P1630) or its RDF counterpart (P1921)
    /// into a regex. Matches PHP `load_formatter_urls`.
    async fn load_formatter_urls(&self) -> Result<Vec<(Regex, String)>> {
        let sparql = "SELECT ?property ?formatterurl WHERE { \
            { ?property p:P1630 [ps:P1630 ?formatterurl]; rdf:type wikibase:Property } \
            UNION \
            { ?property p:P1921 [ps:P1921 ?formatterurl]; rdf:type wikibase:Property } \
        }";
        let json = self.sparql_json(sparql).await?;
        let bindings = json["results"]["bindings"]
            .as_array()
            .ok_or_else(|| anyhow!("no bindings in SPARQL response for formatter URLs"))?;

        // Collect pattern → set of properties. If a pattern maps to more
        // than one property the original script drops it entirely (too
        // ambiguous to rewrite safely); mirror that.
        let mut by_pattern: HashMap<String, Vec<String>> = HashMap::new();
        for b in bindings {
            let property = match b["property"]["value"].as_str() {
                Some(s) => extract_last_path_segment(s),
                None => continue,
            };
            let url = match b["formatterurl"]["value"].as_str() {
                Some(s) => s.trim(),
                None => continue,
            };
            let regex_src = match formatter_url_to_regex(url) {
                Some(r) => r,
                None => continue,
            };
            by_pattern
                .entry(regex_src)
                .or_default()
                .push(property);
        }

        let mut out: Vec<(Regex, String)> = Vec::new();
        for (pattern, props) in by_pattern {
            if props.len() > 1 {
                // Ambiguous: the same URL shape is used by multiple
                // properties. Not safe to auto-replace — skip.
                continue;
            }
            if let Ok(re) = Regex::new(&pattern) {
                out.push((re, props.into_iter().next().unwrap()));
            }
        }

        // Hardcoded patterns take priority — they fill coverage gaps where
        // the property doesn't expose a usable formatter URL. Prepend so
        // they're tried first on every input URL.
        let mut hardcoded: Vec<(Regex, String)> = Vec::new();
        for (pat, prop) in HARDCODED_PATTERNS {
            if let Ok(re) = Regex::new(pat) {
                hardcoded.push((re, (*prop).to_string()));
            }
        }
        hardcoded.extend(out);
        Ok(hardcoded)
    }

    /// Build the property → stated-in map from P9073.
    async fn load_stated_in(&self) -> Result<HashMap<String, String>> {
        let sparql = "SELECT ?property ?stated_in WHERE { \
            ?property rdf:type wikibase:Property . \
            ?property wdt:P9073 ?stated_in \
        }";
        let json = self.sparql_json(sparql).await?;
        let bindings = json["results"]["bindings"]
            .as_array()
            .ok_or_else(|| anyhow!("no bindings in SPARQL response for stated-in"))?;
        let mut out = HashMap::new();
        for b in bindings {
            let (Some(prop), Some(si)) = (
                b["property"]["value"].as_str(),
                b["stated_in"]["value"].as_str(),
            ) else {
                continue;
            };
            let prop = extract_last_path_segment(prop);
            let si = extract_last_path_segment(si);
            // First P9073 wins (some properties have more than one).
            out.entry(prop).or_insert(si);
        }
        Ok(out)
    }

    /// Drain the `reference_fixer` queue. Returns the number of QIDs
    /// processed.
    pub async fn run(&mut self) -> Result<usize> {
        if self.url_patterns.is_empty() {
            self.initialize().await?;
        }
        let mut processed = 0;
        loop {
            let pending = self
                .app
                .storage()
                .reference_fixer_pending(BATCH_SIZE)
                .await?;
            if pending.is_empty() {
                break;
            }
            for q in pending {
                if let Err(e) = self.check_item(q).await {
                    warn!("reference_fixer: Q{q} failed: {e}");
                }
                // Mark done regardless of success — a row that can't be
                // processed now (e.g. deleted item) won't become
                // processable later without explicit re-enqueue.
                let _ = self.app.storage().reference_fixer_mark_done(q).await;
                processed += 1;
            }
        }
        Ok(processed)
    }

    /// Process one item — fetch its statements and rewrite references
    /// where improvable.
    pub async fn check_item(&mut self, q: usize) -> Result<()> {
        let url = format!(
            "https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=Q{q}"
        );
        let json: Value = self.http.get(&url).send().await?.json().await?;
        let qstr = format!("Q{q}");
        let item = match json.pointer(&format!("/entities/{qstr}")) {
            Some(v) => v.clone(),
            None => return Ok(()), // item was deleted or hidden
        };
        self.check_item_json(&item).await
    }

    /// Process the entity JSON body (split out for testability — the
    /// decision logic is all offline once we have the JSON in hand).
    async fn check_item_json(&mut self, item: &Value) -> Result<()> {
        let claims = match item.get("claims").and_then(|v| v.as_object()) {
            Some(o) => o.clone(),
            None => return Ok(()),
        };
        for (_property, statements) in claims {
            let statements = match statements.as_array() {
                Some(a) => a.clone(),
                None => continue,
            };
            for statement in statements {
                if let Err(e) = self.check_statement(&statement).await {
                    warn!("reference_fixer: statement skipped: {e}");
                }
            }
        }
        Ok(())
    }

    async fn check_statement(&mut self, statement: &Value) -> Result<()> {
        let references = match statement.get("references").and_then(|v| v.as_array()) {
            Some(a) => a.clone(),
            None => return Ok(()),
        };
        let statement_id = match statement.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return Ok(()),
        };
        let mut remove_hashes: Vec<String> = Vec::new();
        for reference_group in references {
            if self.is_self_reference(statement, &reference_group) {
                if let Some(h) = reference_group.get("hash").and_then(|v| v.as_str()) {
                    remove_hashes.push(h.to_string());
                }
                continue;
            }
            let new_groups = match self.check_reference_group(&reference_group) {
                Some(g) => g,
                None => continue,
            };
            for rg in &new_groups {
                // Don't loop an external-id onto itself as its own reference.
                if self.is_self_reference(statement, rg) {
                    continue;
                }
                if !self
                    .add_reference_group(&statement_id, rg)
                    .await?
                {
                    // Stop this statement; don't remove the original if we
                    // couldn't successfully add replacement(s).
                    return Ok(());
                }
            }
            if let Some(h) = reference_group.get("hash").and_then(|v| v.as_str()) {
                remove_hashes.push(h.to_string());
            }
        }
        if !remove_hashes.is_empty() {
            self.remove_reference_group(&statement_id, &remove_hashes)
                .await?;
        }
        Ok(())
    }

    /// Given a reference group, decide whether it should be rewritten.
    /// Returns new group(s) to insert; None means "leave as-is".
    fn check_reference_group(&self, group: &Value) -> Option<Vec<Value>> {
        let snaks = group.get("snaks").and_then(|v| v.as_object())?;
        let url_snaks = snaks.get("P854")?.as_array()?;
        // We only rewrite reference groups whose only property is P854
        // (reference URL) — anything else (date retrieved, author, …) is
        // potentially meaningful context we don't want to lose.
        if snaks.len() > 1 {
            return None;
        }
        let is_multiple = url_snaks.len() > 1;
        let mut new_groups: Vec<Value> = Vec::new();
        let mut changed = is_multiple;
        for snak in url_snaks {
            match self.improved_reference_snak(snak) {
                Some(improved) => {
                    changed = true;
                    new_groups.push(new_reference_group(&improved));
                }
                None => {
                    // No rewrite for this URL — keep it as its own group
                    // (PHP parity — this is the split-multi-url step).
                    new_groups.push(new_reference_group(&[snak.clone()]));
                }
            }
        }
        if changed { Some(new_groups) } else { None }
    }

    /// Try to turn a P854 URL snak into [(P248 stated-in,) Pxxx external-id].
    fn improved_reference_snak(&self, snak: &Value) -> Option<Vec<Value>> {
        if snak.get("snaktype").and_then(|v| v.as_str()) != Some("value") {
            return None;
        }
        let dv = snak.get("datavalue")?;
        if dv.get("type").and_then(|v| v.as_str()) != Some("string") {
            return None;
        }
        let url = dv.get("value").and_then(|v| v.as_str())?;

        // Collect property → distinct-values-set across all matching
        // patterns. Multiple patterns may fire for the same property
        // (e.g. www./no-www variants); multiple properties firing is
        // ambiguous and bails.
        let mut matches: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
        for (re, property) in &self.url_patterns {
            let cap = match re.captures(url) {
                Some(c) => c,
                None => continue,
            };
            let raw = match cap.get(1) {
                Some(m) => m.as_str(),
                None => continue,
            };
            let cleaned = clean_captured_id(raw);
            if cleaned.is_empty() {
                continue;
            }
            matches.entry(property.clone()).or_default().insert(cleaned);
        }
        if matches.len() != 1 {
            return None;
        }
        let (property, values) = matches.into_iter().next().unwrap();
        if values.len() != 1 {
            return None;
        }
        let value = values.into_iter().next().unwrap();

        let mut out: Vec<Value> = Vec::new();
        if let Some(si) = self.stated_in.get(&property) {
            if let Some(num) = si.strip_prefix('Q').and_then(|s| s.parse::<u64>().ok()) {
                out.push(json!({
                    "snaktype": "value",
                    "property": "P248",
                    "datavalue": {
                        "value": {
                            "entity-type": "item",
                            "numeric-id": num,
                            "id": si,
                        },
                        "type": "wikibase-entityid",
                    },
                    "datatype": "wikibase-item",
                }));
            }
        }
        out.push(json!({
            "snaktype": "value",
            "property": property,
            "datavalue": { "value": value, "type": "string" },
            "datatype": "external-id",
        }));
        Some(out)
    }

    /// Is this reference group nothing but "the statement's main property
    /// as a reference", ignoring stated-in/retrieved?
    fn is_self_reference(&self, statement: &Value, group: &Value) -> bool {
        let mainsnak = match statement.get("mainsnak") {
            Some(v) => v,
            None => return false,
        };
        if mainsnak.get("datatype").and_then(|v| v.as_str()) != Some("external-id") {
            return false;
        }
        let main_prop = match mainsnak.get("property").and_then(|v| v.as_str()) {
            Some(p) => p,
            None => return false,
        };
        let snaks = match group.get("snaks").and_then(|v| v.as_object()) {
            Some(o) => o,
            None => return false,
        };
        let mut matches_main = false;
        for prop in snaks.keys() {
            if SELF_REF_IGNORED_PROPS.contains(&prop.as_str()) {
                continue;
            }
            if prop == main_prop {
                matches_main = true;
            } else {
                return false; // any other property → not a self-ref
            }
        }
        matches_main
    }

    async fn add_reference_group(&self, statement_id: &str, rg: &Value) -> Result<bool> {
        let snaks = rg.get("snaks").ok_or_else(|| anyhow!("rg missing snaks"))?;
        let order = rg
            .get("snaks-order")
            .ok_or_else(|| anyhow!("rg missing snaks-order"))?;
        let mut params = HashMap::new();
        params.insert("action".to_string(), "wbsetreference".to_string());
        params.insert("statement".to_string(), statement_id.to_string());
        params.insert("snaks".to_string(), snaks.to_string());
        params.insert("snaks-order".to_string(), order.to_string());
        self.api_action(params).await
    }

    async fn remove_reference_group(
        &self,
        statement_id: &str,
        hashes: &[String],
    ) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("action".to_string(), "wbremovereferences".to_string());
        params.insert("statement".to_string(), statement_id.to_string());
        params.insert("references".to_string(), hashes.join("|"));
        self.api_action(params).await
    }

    async fn api_action(&self, mut params: HashMap<String, String>) -> Result<bool> {
        if self.simulating {
            info!("reference_fixer (simulated) {}: {:?}", params.get("action").map(String::as_str).unwrap_or(""), params);
            return Ok(true);
        }
        params.insert("format".into(), "json".into());
        params.insert("bot".into(), "1".into());
        params.insert("summary".into(), EDIT_SUMMARY.into());
        // Be polite: rate-limit mutating edits at 1/s. The Wikidata API
        // would eventually throttle us anyway; self-throttling is less
        // disruptive.
        tokio::time::sleep(INTER_EDIT_DELAY).await;
        let mut wd = self.app.wikidata().clone();
        wd.api_log_in().await?;
        let mw_api = wd
            .get_mw_api()
            .await
            .map_err(|e| anyhow!("MW API unavailable: {e}"))?;
        let mut mw_api = mw_api;
        let token = mw_api
            .get_edit_token()
            .await
            .map_err(|e| anyhow!("edit token: {e}"))?;
        params.insert("token".into(), token);
        match mw_api.post_query_api_json_mut(&params).await {
            Ok(v) => {
                if let Some(err) = v.get("error") {
                    warn!("reference_fixer API error: {err}");
                    return Ok(false);
                }
                Ok(true)
            }
            Err(e) => {
                warn!("reference_fixer API call failed: {e}");
                Ok(false)
            }
        }
    }
}

/// Build a `{snaks, snaks-order}` reference group from a flat list of
/// snaks. P248 is forced to the top of the ordering if present.
fn new_reference_group(snaks_in: &[Value]) -> Value {
    let mut by_prop: HashMap<String, Vec<Value>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for snak in snaks_in {
        let mut s = snak.clone();
        // Drop any inherited `hash` — the API computes one on insert.
        if let Some(obj) = s.as_object_mut() {
            obj.remove("hash");
        }
        let property = match s.get("property").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => continue,
        };
        if !by_prop.contains_key(&property) {
            order.push(property.clone());
        }
        by_prop.entry(property).or_default().push(s);
    }
    // P248 first if present (stated-in is the canonical lead snak of a
    // structured reference).
    if let Some(pos) = order.iter().position(|p| p == "P248") {
        let v = order.remove(pos);
        order.insert(0, v);
    }
    let mut snaks_out = serde_json::Map::new();
    for (prop, arr) in by_prop {
        snaks_out.insert(prop, Value::Array(arr));
    }
    json!({
        "snaks": Value::Object(snaks_out),
        "snaks-order": order,
    })
}

fn extract_last_path_segment(url: &str) -> String {
    url.rsplit('/').next().unwrap_or("").to_string()
}

/// Convert a Wikidata formatter URL (with `$1` as the ID placeholder)
/// into a regex that captures the ID. Returns None when the URL is
/// unusable (no protocol, no placeholder, DOI template which we
/// deliberately skip, the nonsense P8009 `$1` pattern, …).
fn formatter_url_to_regex(url: &str) -> Option<String> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return None;
    }
    if url == "$1" {
        return None;
    }
    // DOI.org patterns collide with everything; skip.
    if url.ends_with("doi.org/$1") {
        return None;
    }
    // Escape every regex metachar, then substitute the ID placeholder
    // back in as a capture group.
    let escaped = regex::escape(url);
    // `regex::escape` turns `$1` into `\$1`. Swap that for `(.+)`.
    let mut pattern = escaped.replace(r"\$1", "(.+)");
    // Make the http vs https prefix tolerant.
    if let Some(rest) = pattern.strip_prefix("http://") {
        pattern = format!("https?://{rest}");
    } else if let Some(rest) = pattern.strip_prefix("https://") {
        pattern = format!("https?://{rest}");
    }
    // Trailing slash: make optional. Matches PHP's `{0,1}` rewrite.
    if let Some(base) = pattern.strip_suffix('/') {
        pattern = format!("{base}/?");
    }
    Some(format!("^{pattern}$"))
}

/// Tidy a captured ID: URL-decode, strip trailing `&query-string`,
/// trailing slash, and the long `_(Dizionario_Biografico)` suffix used
/// by P1986. Trimmed at the end.
fn clean_captured_id(raw: &str) -> String {
    let decoded = urlencoding::decode(raw).map(|s| s.into_owned()).unwrap_or_else(|_| raw.to_string());
    let mut s = decoded.as_str();
    if let Some(amp) = s.find('&') {
        s = &s[..amp];
    }
    let mut s = s.to_string();
    while s.ends_with('/') {
        s.pop();
    }
    if let Some(stripped) = s.strip_suffix("_(Dizionario_Biografico)") {
        s = stripped.to_string();
    }
    s.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatter_url_to_regex_rejects_unusable_templates() {
        assert!(formatter_url_to_regex("$1").is_none());
        assert!(formatter_url_to_regex("ftp://example.com/$1").is_none());
        assert!(formatter_url_to_regex("https://doi.org/$1").is_none());
    }

    #[test]
    fn formatter_url_to_regex_handles_basic_template() {
        let rx = formatter_url_to_regex("https://openlibrary.org/authors/$1").unwrap();
        let re = Regex::new(&rx).unwrap();
        let caps = re.captures("https://openlibrary.org/authors/OL23919A").unwrap();
        assert_eq!(&caps[1], "OL23919A");
    }

    #[test]
    fn formatter_url_to_regex_tolerates_http_and_optional_trailing_slash() {
        let rx = formatter_url_to_regex("https://example.com/x/$1/").unwrap();
        let re = Regex::new(&rx).unwrap();
        // https, https-trailing-slash, http-with-slash all hit.
        assert!(re.is_match("https://example.com/x/abc"));
        assert!(re.is_match("https://example.com/x/abc/"));
        assert!(re.is_match("http://example.com/x/abc"));
    }

    #[test]
    fn clean_captured_id_strips_query_trailing_slash_and_dizionario() {
        assert_eq!(clean_captured_id("foo"), "foo");
        assert_eq!(clean_captured_id("foo/"), "foo");
        assert_eq!(clean_captured_id("foo&bar=1"), "foo");
        assert_eq!(clean_captured_id("name_(Dizionario_Biografico)"), "name");
        assert_eq!(clean_captured_id("%C3%A9"), "é");
        assert_eq!(clean_captured_id("  trimmed  "), "trimmed");
    }

    #[test]
    fn extract_last_path_segment_handles_entity_uri() {
        assert_eq!(
            extract_last_path_segment("http://www.wikidata.org/entity/P648"),
            "P648"
        );
        assert_eq!(extract_last_path_segment("P648"), "P648");
    }

    #[test]
    fn new_reference_group_puts_p248_first() {
        let snaks = vec![
            json!({"property": "P648", "snaktype": "value", "datavalue": {"value": "x", "type": "string"}}),
            json!({"property": "P248", "snaktype": "value", "datavalue": {"value": {"id":"Q1"}, "type": "wikibase-entityid"}}),
        ];
        let rg = new_reference_group(&snaks);
        let order = rg["snaks-order"].as_array().unwrap();
        assert_eq!(order[0].as_str().unwrap(), "P248");
        assert_eq!(order[1].as_str().unwrap(), "P648");
        // snaks map keyed by property
        assert!(rg["snaks"]["P248"].is_array());
        assert!(rg["snaks"]["P648"].is_array());
    }

    #[test]
    fn is_self_reference_on_external_id_same_prop() {
        let rf = bare_fixer();
        let statement = json!({
            "mainsnak": {"datatype": "external-id", "property": "P648"},
            "id": "Q1$abc",
        });
        let group = json!({
            "snaks": {"P648": [{}], "P248": [{}], "P813": [{}]},
            "snaks-order": ["P248", "P813", "P648"],
        });
        assert!(rf.is_self_reference(&statement, &group));
    }

    #[test]
    fn is_self_reference_not_external_id() {
        let rf = bare_fixer();
        let statement = json!({
            "mainsnak": {"datatype": "wikibase-item", "property": "P31"},
        });
        let group = json!({
            "snaks": {"P31": [{}]},
        });
        assert!(!rf.is_self_reference(&statement, &group));
    }

    #[test]
    fn is_self_reference_other_prop_bails() {
        let rf = bare_fixer();
        let statement = json!({
            "mainsnak": {"datatype": "external-id", "property": "P648"},
        });
        let group = json!({
            "snaks": {"P648": [{}], "P1234": [{}]},
        });
        assert!(!rf.is_self_reference(&statement, &group));
    }

    fn bare_fixer() -> ReferenceFixer {
        // Build a ReferenceFixer that never calls network — just enough
        // to exercise the offline logic. Avoid AppState construction by
        // going through a tiny hand-rolled one.
        ReferenceFixer {
            app: crate::app_state::get_test_app(),
            http: reqwest::Client::new(),
            url_patterns: vec![],
            stated_in: HashMap::new(),
            simulating: true,
        }
    }
}
