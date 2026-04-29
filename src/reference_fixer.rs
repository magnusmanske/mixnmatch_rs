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
use crate::util::wikidata_props as wp;
use crate::wdqs::WDQS_URL;
use anyhow::{Result, anyhow};
use log::{info, warn};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

/// Summary string sent with every edit.
const EDIT_SUMMARY: &str = "Fixing references as part of Mix'n'match cleanup";


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
const SELF_REF_IGNORED_PROPS: &[&str] = &[wp::P_STATED_IN, wp::P_RETRIEVED];

/// URL-pattern regexes hard-coded in the original script for
/// collections that don't expose a usable P1630/P1921. Kept here so we
/// don't lose the curated coverage.
const HARDCODED_PATTERNS: &[(&str, &str)] = &[
    (
        r"^https?://www\.biodiversitylibrary\.org/creator/(.+?)/*$",
        "P4081",
    ),
    (r"^https?://trove\.nla\.gov\.au/people/(\d+).*$", "P1315"),
    (r"^https?://openlibrary\.org/authors/(.+?)/.*$", "P648"),
    (
        r"^https?://www\.biusante\.parisdescartes\.fr/histoire/biographies/index\.php\?cle=(\d+)",
        "P5375",
    ),
    (
        r"^https?://biusante\.parisdescartes\.fr/histoire/biographies/index\.php\?cle=(\d+)",
        "P5375",
    ),
    (
        r"^https?://bibliotheque\.academie-medecine\.fr/membres/membre/\?mbreid=(\d+).*$",
        "P3956",
    ),
    (r"^https?://www\.artnet\.com/artists/([^/]+).*$", "P3782"),
    (
        r"^https?://www\.mutualart\.com/Artist/[^/]+/([^/]+).*$",
        "P6578",
    ),
    (r"^https?://en\.isabart\.org/person/(\d+).*$", "P6844"),
    (
        r"^https?://www\.sikart\.ch/KuenstlerInnen\.aspx\?id=(\d+).*$",
        "P781",
    ),
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
    /// Logged-in bot API session, reused across every edit in a single
    /// run. Lazy-initialised on the first mutating call so initialize()
    /// / read-only flows don't pay for a login they never use. The
    /// previous implementation cloned AppState::wikidata() per call,
    /// which meant a fresh login for every wbsetreference / wbremove
    /// request — tens of thousands of round-trips on a large queue.
    mw_api: Option<mediawiki::api::Api>,
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
            mw_api: None,
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
            .get(WDQS_URL)
            .query(&[("query", sparql), ("format", "json")])
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("SPARQL endpoint returned HTTP {}", resp.status()));
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
            by_pattern.entry(regex_src).or_default().push(property);
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
                // Mark done regardless of per-item success — a row that
                // can't be processed now (deleted item, permission
                // issues, …) wouldn't become processable later without
                // an explicit re-enqueue, and leaving it pending would
                // spin on it forever. But we do log DB failures on the
                // mark-done itself: if we can't update the queue, we
                // need to know because the next loop iteration would
                // see the same row and retry in a tight loop.
                if let Err(e) = self.app.storage().reference_fixer_mark_done(q).await {
                    warn!("reference_fixer: failed to mark Q{q} done (queue will retry): {e}");
                    // Bail out — marking-done is supposed to be cheap;
                    // if it's failing, the whole run is in trouble.
                    return Err(e);
                }
                processed += 1;
            }
        }
        Ok(processed)
    }

    /// Process one item — fetch its statements and rewrite references
    /// where improvable.
    pub async fn check_item(&mut self, q: usize) -> Result<()> {
        let url =
            format!("https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&ids=Q{q}");
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow!(
                "wbgetentities Q{q} returned HTTP {}",
                resp.status()
            ));
        }
        let json: Value = resp.json().await?;
        // A missing / redirected / deleted item shows up as either an
        // `error` block at the top level, or an `entities.Q<id>` object
        // with `missing: ""`. Either way there's nothing for us to
        // rewrite — bail silently, don't blow up the run.
        if json.get("error").is_some() {
            return Ok(());
        }
        let qstr = format!("Q{q}");
        let item = match json.pointer(&format!("/entities/{qstr}")) {
            Some(v) if v.get("missing").is_none() => v.clone(),
            _ => return Ok(()),
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
            if Self::is_self_reference(statement, &reference_group) {
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
                if Self::is_self_reference(statement, rg) {
                    continue;
                }
                if !self.add_reference_group(&statement_id, rg).await? {
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
    ///
    /// We only rewrite the simplest, unambiguous case: a reference group
    /// whose only property is P854 (reference URL) and whose P854 has a
    /// single value. Splitting a multi-URL group into N single-URL
    /// groups — which the PHP original did — silently changes the
    /// semantic meaning (one combined citation becomes several
    /// independent ones), which is exactly the "POTENTIALLY ADDING
    /// MULTIPLE REFERENCE PARTS INSTEAD OF SINGLE REFERENCE" bug the
    /// PHP author flagged by disabling the script. We refuse to rewrite
    /// in that shape at all; the row still gets marked done so we don't
    /// spin on it forever.
    fn check_reference_group(&self, group: &Value) -> Option<Vec<Value>> {
        let snaks = group.get("snaks").and_then(|v| v.as_object())?;
        let url_snaks = snaks.get(wp::P_REFERENCE_URL)?.as_array()?;
        if snaks.len() > 1 {
            // Other properties present (retrieval date, author, …) —
            // can't tell which are load-bearing, so don't touch.
            return None;
        }
        if url_snaks.len() != 1 {
            // Zero URLs shouldn't happen (the array exists), multiple
            // URLs → see docstring. Either way, skip.
            return None;
        }
        let improved = self.improved_reference_snak(&url_snaks[0])?;
        Some(vec![new_reference_group(&improved)])
    }

    /// Try to turn a P854 URL snak into [(P248 stated-in,) Pxxx external-id].
    fn improved_reference_snak(&self, snak: &Value) -> Option<Vec<Value>> {
        let parsed = StringUrlSnak::from_json(snak)?;
        let (property, value) = unique_property_match(parsed.url, &self.url_patterns)?;
        let mut out = Vec::new();
        if let Some(qid) = self.stated_in.get(&property) {
            if let Some(snak) = build_stated_in_snak(qid) {
                out.push(snak);
            }
        }
        out.push(build_external_id_snak(&property, &value));
        Some(out)
    }

    /// Is this reference group nothing but "the statement's main property
    /// as a reference", ignoring stated-in/retrieved?
    fn is_self_reference(statement: &Value, group: &Value) -> bool {
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

    async fn add_reference_group(&mut self, statement_id: &str, rg: &Value) -> Result<bool> {
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
        &mut self,
        statement_id: &str,
        hashes: &[String],
    ) -> Result<bool> {
        let mut params = HashMap::new();
        params.insert("action".to_string(), "wbremovereferences".to_string());
        params.insert("statement".to_string(), statement_id.to_string());
        params.insert("references".to_string(), hashes.join("|"));
        self.api_action(params).await
    }

    /// Execute a bot API call. Reuses the cached logged-in session if
    /// available (one login per job run, not per edit).
    async fn api_action(&mut self, mut params: HashMap<String, String>) -> Result<bool> {
        if self.simulating {
            info!(
                "reference_fixer (simulated) {}: {:?}",
                params.get("action").map(String::as_str).unwrap_or(""),
                params
            );
            return Ok(true);
        }
        self.ensure_logged_in().await?;
        let api = self
            .mw_api
            .as_mut()
            .ok_or_else(|| anyhow!("bot API not available after login"))?;
        params.insert("format".into(), "json".into());
        params.insert("bot".into(), "1".into());
        params.insert("summary".into(), EDIT_SUMMARY.into());
        let token = api
            .get_edit_token()
            .await
            .map_err(|e| anyhow!("edit token: {e}"))?;
        params.insert("token".into(), token);
        // Self-throttle: the Wikidata API would eventually rate-limit
        // us anyway; doing it up-front is less disruptive than hitting
        // maxlag mid-pass.
        tokio::time::sleep(INTER_EDIT_DELAY).await;
        match api.post_query_api_json_mut(&params).await {
            Ok(v) => {
                if let Some(err) = v.get("error") {
                    warn!("reference_fixer API error: {err}");
                    return Ok(false);
                }
                Ok(true)
            }
            Err(e) => {
                warn!("reference_fixer API call failed: {e}");
                // Drop the cached session on transport errors — a stale
                // cookie / expired token would otherwise keep failing.
                self.mw_api = None;
                Ok(false)
            }
        }
    }

    /// Build a logged-in bot session on first use, and re-login if the
    /// cached session lost its auth (e.g. the server dropped our cookie).
    async fn ensure_logged_in(&mut self) -> Result<()> {
        if let Some(api) = self.mw_api.as_ref() {
            if api.user().logged_in() {
                return Ok(());
            }
        }
        let wd = self.app.wikidata();
        let mut api = wd
            .get_mw_api()
            .await
            .map_err(|e| anyhow!("MW API unavailable: {e}"))?;
        api.login(wd.bot_name().to_string(), wd.bot_password().to_string())
            .await
            .map_err(|e| anyhow!("bot login failed: {e}"))?;
        self.mw_api = Some(api);
        Ok(())
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
    if let Some(pos) = order.iter().position(|p| p == wp::P_STATED_IN) {
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

/// A `snaktype=value`, `datavalue.type=string` snak whose value is a URL —
/// the only shape `improved_reference_snak` can act on. Borrowing parser:
/// the URL string is referenced from the input `Value` so we don't pay
/// for a clone on every queue item.
struct StringUrlSnak<'a> {
    url: &'a str,
}

impl<'a> StringUrlSnak<'a> {
    /// Validate the snak's shape and return its URL, or `None` for any
    /// snak that's `novalue` / `somevalue`, has a non-string datavalue,
    /// or is missing a field. Each rejection is a documented case the
    /// caller doesn't need to handle.
    fn from_json(snak: &'a Value) -> Option<Self> {
        if snak.get("snaktype").and_then(Value::as_str)? != "value" {
            return None;
        }
        let dv = snak.get("datavalue")?;
        if dv.get("type").and_then(Value::as_str)? != "string" {
            return None;
        }
        let url = dv.get("value").and_then(Value::as_str)?;
        Some(Self { url })
    }
}

/// Run `url` against every `(regex, property)` pair, collecting cleaned
/// capture-1 values into a property→set map. Returns `Some((property, value))`
/// only when exactly one property matched and its set has exactly one
/// element — i.e., the URL maps unambiguously to a single external id.
/// Multiple patterns may fire for the same property (www / no-www
/// variants); multiple *properties* firing is ambiguous and bails.
fn unique_property_match(
    url: &str,
    url_patterns: &[(Regex, String)],
) -> Option<(String, String)> {
    let mut matches: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    for (re, property) in url_patterns {
        let Some(cap) = re.captures(url) else { continue };
        let Some(raw) = cap.get(1).map(|m| m.as_str()) else { continue };
        let cleaned = clean_captured_id(raw);
        if cleaned.is_empty() {
            continue;
        }
        matches.entry(property.clone()).or_default().insert(cleaned);
    }
    if matches.len() != 1 {
        return None;
    }
    let (property, values) = matches.into_iter().next()?;
    if values.len() != 1 {
        return None;
    }
    Some((property, values.into_iter().next()?))
}

/// Build a P248 stated-in snak pointing at `qid`. Returns `None` if `qid`
/// isn't `Q\d+` — every value in `stated_in` is supposed to be, but the
/// SPARQL query that populates it could in principle return junk.
fn build_stated_in_snak(qid: &str) -> Option<Value> {
    let num = qid.strip_prefix('Q')?.parse::<u64>().ok()?;
    Some(json!({
        "snaktype": "value",
        "property": wp::P_STATED_IN,
        "datavalue": {
            "value": {
                "entity-type": "item",
                "numeric-id": num,
                "id": qid,
            },
            "type": "wikibase-entityid",
        },
        "datatype": "wikibase-item",
    }))
}

/// Build the typed external-id snak for `(property, value)`.
fn build_external_id_snak(property: &str, value: &str) -> Value {
    json!({
        "snaktype": "value",
        "property": property,
        "datavalue": { "value": value, "type": "string" },
        "datatype": "external-id",
    })
}

/// Tidy a captured ID: URL-decode, strip trailing `&query-string`,
/// trailing slash, and the long `_(Dizionario_Biografico)` suffix used
/// by P1986. Trimmed at the end.
fn clean_captured_id(raw: &str) -> String {
    let decoded = urlencoding::decode(raw)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| raw.to_string());
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
    fn string_url_snak_accepts_value_snak_with_string_datavalue() {
        let snak = json!({
            "snaktype": "value",
            "property": "P854",
            "datavalue": {"type": "string", "value": "https://example.com/x"},
        });
        let parsed = StringUrlSnak::from_json(&snak).unwrap();
        assert_eq!(parsed.url, "https://example.com/x");
    }

    #[test]
    fn string_url_snak_rejects_novalue_and_wrong_datatype() {
        // novalue snak — no URL to act on.
        let snak = json!({"snaktype": "novalue", "property": "P854"});
        assert!(StringUrlSnak::from_json(&snak).is_none());
        // value snak with non-string datavalue — also unactionable.
        let snak = json!({
            "snaktype": "value",
            "property": "P854",
            "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q1"}},
        });
        assert!(StringUrlSnak::from_json(&snak).is_none());
        // missing datavalue — malformed.
        let snak = json!({"snaktype": "value", "property": "P854"});
        assert!(StringUrlSnak::from_json(&snak).is_none());
    }

    #[test]
    fn unique_property_match_bails_on_multiple_properties() {
        let patterns = vec![
            (
                Regex::new(r"^https?://a\.test/(\w+)$").unwrap(),
                "P100".into(),
            ),
            (
                Regex::new(r"^https?://a\.test/(\w+)$").unwrap(),
                "P200".into(),
            ),
        ];
        assert!(unique_property_match("https://a.test/foo", &patterns).is_none());
    }

    #[test]
    fn unique_property_match_collapses_duplicate_pattern_for_same_property() {
        // www / no-www variant of the same property must NOT cause
        // ambiguity bail — the BTreeSet collapses identical capture values.
        let patterns = vec![
            (
                Regex::new(r"^https?://(?:www\.)?a\.test/(\w+)$").unwrap(),
                "P100".into(),
            ),
            (
                Regex::new(r"^https?://a\.test/(\w+)$").unwrap(),
                "P100".into(),
            ),
        ];
        let got = unique_property_match("https://a.test/foo", &patterns);
        assert_eq!(got, Some(("P100".into(), "foo".into())));
    }

    #[test]
    fn build_stated_in_snak_rejects_non_q_id() {
        assert!(build_stated_in_snak("garbage").is_none());
        assert!(build_stated_in_snak("P648").is_none());
        let snak = build_stated_in_snak("Q1201876").unwrap();
        assert_eq!(snak["datavalue"]["value"]["numeric-id"], 1201876);
        assert_eq!(snak["datavalue"]["value"]["id"], "Q1201876");
    }

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
        let caps = re
            .captures("https://openlibrary.org/authors/OL23919A")
            .unwrap();
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
        let statement = json!({
            "mainsnak": {"datatype": "external-id", "property": "P648"},
            "id": "Q1$abc",
        });
        let group = json!({
            "snaks": {"P648": [{}], "P248": [{}], "P813": [{}]},
            "snaks-order": ["P248", "P813", "P648"],
        });
        assert!(ReferenceFixer::is_self_reference(&statement, &group));
    }

    #[test]
    fn is_self_reference_not_external_id() {
        let statement = json!({
            "mainsnak": {"datatype": "wikibase-item", "property": "P31"},
        });
        let group = json!({
            "snaks": {"P31": [{}]},
        });
        assert!(!ReferenceFixer::is_self_reference(&statement, &group));
    }

    #[test]
    fn is_self_reference_other_prop_bails() {
        let statement = json!({
            "mainsnak": {"datatype": "external-id", "property": "P648"},
        });
        let group = json!({
            "snaks": {"P648": [{}], "P1234": [{}]},
        });
        assert!(!ReferenceFixer::is_self_reference(&statement, &group));
    }

    #[test]
    fn check_reference_group_skips_multi_url_groups() {
        // The PHP script's splitting behaviour (N URLs → N separate
        // single-URL reference groups) flips semantics from AND to OR
        // and was explicitly flagged by the original author. Make
        // damn sure we don't do that: any group with != 1 P854 URL
        // must be left untouched.
        let mut rf = bare_fixer();
        rf.url_patterns.push((
            Regex::new(r"^https?://openlibrary\.org/authors/(.+?)/.*$").unwrap(),
            "P648".to_string(),
        ));
        let group = json!({
            "snaks": {
                "P854": [
                    {"snaktype":"value","property":"P854",
                     "datavalue":{"type":"string","value":"https://openlibrary.org/authors/OL1A/foo"}},
                    {"snaktype":"value","property":"P854",
                     "datavalue":{"type":"string","value":"https://openlibrary.org/authors/OL2A/bar"}},
                ],
            },
        });
        assert!(rf.check_reference_group(&group).is_none());
    }

    #[test]
    fn check_reference_group_skips_groups_with_other_properties() {
        let rf = bare_fixer();
        let group = json!({
            "snaks": {
                "P854": [{"snaktype":"value","property":"P854",
                          "datavalue":{"type":"string","value":"https://example.org/$1/x"}}],
                "P813": [{"snaktype":"value","property":"P813",
                          "datavalue":{"type":"time","value":{"time":"+2020-01-01T00:00:00Z"}}}],
            },
        });
        assert!(rf.check_reference_group(&group).is_none());
    }

    #[test]
    fn check_reference_group_rewrites_single_url_match() {
        let mut rf = bare_fixer();
        rf.url_patterns.push((
            Regex::new(r"^https?://openlibrary\.org/authors/(.+?)/.*$").unwrap(),
            "P648".to_string(),
        ));
        rf.stated_in.insert("P648".into(), "Q1201876".into());
        let group = json!({
            "snaks": {
                "P854": [{
                    "snaktype": "value",
                    "property": "P854",
                    "datavalue": {"type": "string", "value": "https://openlibrary.org/authors/OL23919A/foo"},
                }],
            },
        });
        let new_groups = rf.check_reference_group(&group).expect("should rewrite");
        assert_eq!(new_groups.len(), 1);
        let g = &new_groups[0];
        // P248 first in snaks-order, followed by the external-id property.
        let order: Vec<&str> = g["snaks-order"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(order, vec!["P248", "P648"]);
        // The typed external-id snak carries the cleaned capture group.
        assert_eq!(
            g["snaks"]["P648"][0]["datavalue"]["value"]
                .as_str()
                .unwrap(),
            "OL23919A"
        );
        assert_eq!(
            g["snaks"]["P248"][0]["datavalue"]["value"]["id"]
                .as_str()
                .unwrap(),
            "Q1201876"
        );
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
            mw_api: None,
        }
    }
}
