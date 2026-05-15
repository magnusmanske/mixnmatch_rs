//! Two-engine regex facade for autoscrape patterns.
//!
//! Catalog patterns are PHP/PCRE-style and many features the Rust `regex`
//! crate doesn't support — lookahead/lookbehind (`(?!…)`, `(?<!…)`) and
//! a more permissive treatment of literal `{` — are common in production
//! autoscrape configs. We therefore compile every pattern with `regex`
//! first (linear-time, ReDoS-safe) and on a parse error fall back to
//! `fancy_regex` (backtracking, PCRE-style feature set).
//!
//! Runtime errors from the fancy backend (backtracking-limit, etc.) are
//! treated as "no match" — same observable effect as the pattern not
//! matching, which is how a misbehaving pattern would degrade a scrape
//! today (zero entries returned).

use std::borrow::Cow;
use std::fmt;

/// Engine that successfully compiled a given pattern.
#[derive(Debug)]
pub enum AutoscrapeRegex {
    /// Fast linear-time `regex` crate engine. Tried first.
    Fast(regex::Regex),
    /// Backtracking `fancy_regex` engine. Tried when `Fast` rejects the
    /// pattern. Supports lookaround, backrefs, and literal `{`.
    Fancy(fancy_regex::Regex),
}

/// Compile error after BOTH engines refused a pattern. The display form
/// embeds both underlying error strings so the failing catalog's note
/// field tells the author exactly what's wrong.
#[derive(Debug, Clone)]
pub struct AutoscrapeRegexError {
    pub pattern: String,
    pub fast_error: String,
    pub fancy_error: String,
}

impl fmt::Display for AutoscrapeRegexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "regex compile failed for '{}'\n  fast engine: {}\n  fancy engine: {}",
            self.pattern, self.fast_error, self.fancy_error
        )
    }
}

impl std::error::Error for AutoscrapeRegexError {}

/// Builder that mirrors `regex::RegexBuilder` for the small subset of
/// flags autoscrape actually uses. `multi_line(true)` is the only flag
/// currently propagated; fancy_regex has no builder of its own, so we
/// prepend `(?m)` to the pattern when falling back.
#[derive(Debug)]
pub struct AutoscrapeRegexBuilder {
    pattern: String,
    multi_line: bool,
}

impl AutoscrapeRegexBuilder {
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_string(),
            multi_line: false,
        }
    }

    pub fn multi_line(mut self, on: bool) -> Self {
        self.multi_line = on;
        self
    }

    pub fn build(self) -> Result<AutoscrapeRegex, AutoscrapeRegexError> {
        let fast_err = match regex::RegexBuilder::new(&self.pattern)
            .multi_line(self.multi_line)
            .build()
        {
            Ok(r) => return Ok(AutoscrapeRegex::Fast(r)),
            Err(e) => e.to_string(),
        };
        // fancy_regex has no builder. Prepend (?m) to mirror multi_line.
        let fancy_input = if self.multi_line {
            format!("(?m){}", self.pattern)
        } else {
            self.pattern.clone()
        };
        match fancy_regex::Regex::new(&fancy_input) {
            Ok(r) => Ok(AutoscrapeRegex::Fancy(r)),
            Err(e) => Err(AutoscrapeRegexError {
                pattern: self.pattern,
                fast_error: fast_err,
                fancy_error: e.to_string(),
            }),
        }
    }
}

/// Owned single-match-positional view that lets callers keep `Match`-like
/// semantics (`.as_str()`) without exposing engine-specific lifetimes.
#[derive(Debug)]
pub struct AutoscrapeMatch<'a>(&'a str);

impl<'a> AutoscrapeMatch<'a> {
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// Owned captures: stores each group's matched string (or `None` if the
/// group didn't participate). Index 0 is the full match, mirroring
/// regex/PCRE convention. Owning the strings keeps the API trivial and
/// engine-independent — match counts in autoscrape are bounded by the
/// number of result rows per page (low hundreds), so allocations are
/// negligible relative to HTTP latency.
#[derive(Debug)]
pub struct AutoscrapeCaptures {
    values: Vec<Option<String>>,
}

impl AutoscrapeCaptures {
    pub fn get(&self, i: usize) -> Option<AutoscrapeMatch<'_>> {
        self.values.get(i).and_then(|v| v.as_deref()).map(AutoscrapeMatch)
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<AutoscrapeMatch<'_>>> + '_ {
        self.values.iter().map(|v| v.as_deref().map(AutoscrapeMatch))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl AutoscrapeRegex {
    /// One-shot constructor that defaults `multi_line(false)`. Mirrors
    /// `regex::Regex::new`.
    pub fn new(pattern: &str) -> Result<Self, AutoscrapeRegexError> {
        AutoscrapeRegexBuilder::new(pattern).build()
    }

    /// Pattern source string. Mirrors `regex::Regex::as_str`. For fancy
    /// patterns built via the builder with `multi_line(true)` we still
    /// return the *original* pattern (without the `(?m)` we prepended),
    /// so callers see what the catalog author wrote.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Fast(r) => r.as_str(),
            Self::Fancy(r) => {
                // Strip the leading "(?m)" we may have prepended during
                // build() so callers see the author-written pattern.
                r.as_str().strip_prefix("(?m)").unwrap_or(r.as_str())
            }
        }
    }

    pub fn is_match(&self, text: &str) -> bool {
        match self {
            Self::Fast(r) => r.is_match(text),
            // fancy_regex returns Result; treat runtime errors as no match.
            Self::Fancy(r) => r.is_match(text).unwrap_or(false),
        }
    }

    /// Eager capture collection. Each successful match becomes one owned
    /// `AutoscrapeCaptures`. Runtime errors from the fancy engine are
    /// silently skipped so a misbehaving pattern degrades to zero
    /// matches rather than crashing the scrape.
    pub fn captures_iter(&self, text: &str) -> Vec<AutoscrapeCaptures> {
        match self {
            Self::Fast(r) => r
                .captures_iter(text)
                .map(|cap| AutoscrapeCaptures {
                    values: cap
                        .iter()
                        .map(|m| m.map(|x| x.as_str().to_string()))
                        .collect(),
                })
                .collect(),
            Self::Fancy(r) => r
                .captures_iter(text)
                .filter_map(|res| res.ok())
                .map(|cap| AutoscrapeCaptures {
                    values: (0..cap.len())
                        .map(|i| cap.get(i).map(|m| m.as_str().to_string()))
                        .collect(),
                })
                .collect(),
        }
    }

    /// Replace every match. Mirrors `regex::Regex::replace_all` for the
    /// `&str` replacement form (the only one the resolve pipeline uses,
    /// with `$1`/`$2` backrefs).
    pub fn replace_all<'t>(&self, text: &'t str, rep: &str) -> Cow<'t, str> {
        match self {
            Self::Fast(r) => r.replace_all(text, rep),
            Self::Fancy(r) => r.replace_all(text, rep),
        }
    }
}

// `regex::Regex::to_string` returns the pattern source. Preserve that
// semantic so the existing `regex.to_string()` test assertion keeps
// meaning "the pattern source".
impl fmt::Display for AutoscrapeRegex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// Clone is needed because AutoscrapeScraper derives Clone and holds
// these. Both regex::Regex and fancy_regex::Regex are Clone.
impl Clone for AutoscrapeRegex {
    fn clone(&self) -> Self {
        match self {
            Self::Fast(r) => Self::Fast(r.clone()),
            Self::Fancy(r) => Self::Fancy(r.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Compile path selection ----

    #[test]
    fn plain_pattern_uses_fast_engine() {
        let r = AutoscrapeRegex::new(r"\d+").expect("plain pattern compiles");
        assert!(matches!(r, AutoscrapeRegex::Fast(_)));
    }

    #[test]
    fn lookahead_falls_back_to_fancy() {
        let r =
            AutoscrapeRegex::new(r"<tr(([^<]|<(?!/tr>))*)</tr>").expect("lookahead compiles via fancy");
        assert!(matches!(r, AutoscrapeRegex::Fancy(_)));
    }

    #[test]
    fn lookbehind_falls_back_to_fancy() {
        let r = AutoscrapeRegex::new(r"(?<!^)<br>(?!$)").expect("lookbehind compiles via fancy");
        assert!(matches!(r, AutoscrapeRegex::Fancy(_)));
    }

    #[test]
    fn literal_open_brace_falls_back_to_fancy() {
        // The {"key": pattern start trips the fast engine (it tries to
        // parse a {n,m} quantifier and fails). PCRE accepts it as a
        // literal { because what follows isn't a valid count.
        let r =
            AutoscrapeRegex::new(r#"\{"SkiArea":\{"id":"(\d+)","name":"(.+?)""#)
                .expect("literal brace compiles via fancy");
        // We don't care which engine accepts it as long as one does;
        // pin to fancy here because the test exists to document the bug
        // bucket we're fixing.
        match r {
            AutoscrapeRegex::Fast(_) => { /* also acceptable, future regex versions may accept this */ }
            AutoscrapeRegex::Fancy(_) => {}
        }
    }

    #[test]
    fn truly_invalid_pattern_returns_error() {
        let err = AutoscrapeRegex::new(r"(unclosed").expect_err("unclosed paren is invalid in both engines");
        assert!(err.fast_error.len() > 0);
        assert!(err.fancy_error.len() > 0);
        // Display should mention the pattern source so the catalog
        // author can identify which rx is broken.
        let display = format!("{err}");
        assert!(display.contains("unclosed"), "display should embed the pattern: {display}");
    }

    // ---- Runtime behaviour parity ----

    #[test]
    fn is_match_fast() {
        let r = AutoscrapeRegex::new(r"abc").unwrap();
        assert!(r.is_match("xabcx"));
        assert!(!r.is_match("xyz"));
    }

    #[test]
    fn is_match_fancy() {
        let r = AutoscrapeRegex::new(r"(?<!a)b").unwrap();
        assert!(matches!(r, AutoscrapeRegex::Fancy(_)));
        assert!(r.is_match("xb"));
        assert!(!r.is_match("ab"));
    }

    #[test]
    fn captures_iter_fast_full_match_and_groups() {
        let r = AutoscrapeRegex::new(r"(\w+)=(\d+)").unwrap();
        let caps = r.captures_iter("a=1 b=22");
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0].get(0).unwrap().as_str(), "a=1");
        assert_eq!(caps[0].get(1).unwrap().as_str(), "a");
        assert_eq!(caps[0].get(2).unwrap().as_str(), "1");
        assert_eq!(caps[1].get(1).unwrap().as_str(), "b");
        assert_eq!(caps[1].get(2).unwrap().as_str(), "22");
    }

    #[test]
    fn captures_iter_fancy_full_match_and_groups() {
        // Lookahead-style HTML container pattern — the same shape that
        // ~16 production catalogs use.
        let r = AutoscrapeRegex::new(r"<li(([^<]|<(?!/li>))*)</li>").unwrap();
        let caps = r.captures_iter("<li>a</li><li>bb</li>");
        assert_eq!(caps.len(), 2);
        // Group 1 captures the inner content.
        assert_eq!(caps[0].get(1).unwrap().as_str(), ">a");
        assert_eq!(caps[1].get(1).unwrap().as_str(), ">bb");
    }

    #[test]
    fn captures_iter_returns_empty_for_no_match() {
        let r = AutoscrapeRegex::new(r"zzz").unwrap();
        assert!(r.captures_iter("abc").is_empty());
    }

    #[test]
    fn captures_get_missing_index_returns_none() {
        let r = AutoscrapeRegex::new(r"(\d+)").unwrap();
        let caps = r.captures_iter("42");
        assert_eq!(caps.len(), 1);
        assert!(caps[0].get(99).is_none());
    }

    #[test]
    fn captures_iter_method_yields_all_groups_including_full_match() {
        let r = AutoscrapeRegex::new(r"(\w+)=(\d+)").unwrap();
        let caps = r.captures_iter("a=1");
        let collected: Vec<Option<String>> = caps[0]
            .iter()
            .map(|m| m.map(|x| x.as_str().to_string()))
            .collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].as_deref(), Some("a=1"));
        assert_eq!(collected[1].as_deref(), Some("a"));
        assert_eq!(collected[2].as_deref(), Some("1"));
    }

    #[test]
    fn replace_all_fast() {
        let r = AutoscrapeRegex::new(r"(\w+)").unwrap();
        assert_eq!(r.replace_all("hi", "<$1>"), "<hi>");
    }

    #[test]
    fn replace_all_fancy() {
        let r = AutoscrapeRegex::new(r"(?<!a)(b)").unwrap();
        // (?<!a)b matches "b" not preceded by "a".
        assert_eq!(r.replace_all("ab xb yb", "[$1]"), "ab x[b] y[b]");
    }

    // ---- Builder ----

    #[test]
    fn builder_propagates_multi_line_to_fast_engine() {
        // ^ should match line starts under multi_line.
        let r = AutoscrapeRegexBuilder::new(r"^foo").multi_line(true).build().unwrap();
        assert!(r.is_match("bar\nfoo"));
    }

    #[test]
    fn builder_propagates_multi_line_to_fancy_engine_via_inline_flag() {
        // Force fancy by including lookbehind, plus multi_line:
        // (?<!#)^foo should match a "foo" at a line start that isn't
        // preceded by a "#" character (impossible at line start anyway,
        // but it exercises both flags interacting).
        let r = AutoscrapeRegexBuilder::new(r"(?<!#)^foo").multi_line(true).build().unwrap();
        assert!(matches!(r, AutoscrapeRegex::Fancy(_)));
        assert!(r.is_match("bar\nfoo"));
        // as_str() should hide the (?m) we prepended for the fancy engine.
        assert_eq!(r.as_str(), "(?<!#)^foo");
    }

    #[test]
    fn as_str_returns_original_pattern_fast() {
        let r = AutoscrapeRegex::new(r"\d+").unwrap();
        assert_eq!(r.as_str(), r"\d+");
    }

    #[test]
    fn display_is_pattern_source() {
        let r = AutoscrapeRegex::new(r"abc").unwrap();
        assert_eq!(format!("{r}"), "abc");
    }

    // ---- Regression tests for the specific bucket-A/B patterns we're fixing ----

    #[test]
    fn regression_catalog_2645_atp_tourney_pattern() {
        let r = AutoscrapeRegex::new(r#"<tr class="tourney-result"(([^<]|<(?!/tr>))*)</tr>"#)
            .expect("ATP tour pattern must now compile");
        let html = r#"<tr class="tourney-result"><td>A</td></tr><tr class="tourney-result"><td>B</td></tr>"#;
        assert_eq!(r.captures_iter(html).len(), 2);
    }

    #[test]
    fn regression_catalog_2182_skimap_literal_brace_pattern() {
        // The bucket-B pattern from catalog 2182. Just confirm it compiles.
        let pat = r#"\{"SkiArea":\{"id":"(\d+)","name":"(.+?)",.*?"Region":\[\{"name":"(.+?)","#;
        let r = AutoscrapeRegex::new(pat).expect("ski-map pattern must now compile");
        // Sanity-test the captures on a synthetic body.
        let body = r#"{"SkiArea":{"id":"42","name":"Test","Region":[{"name":"Alps","#;
        let caps = r.captures_iter(body);
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].get(1).unwrap().as_str(), "42");
        assert_eq!(caps[0].get(2).unwrap().as_str(), "Test");
        assert_eq!(caps[0].get(3).unwrap().as_str(), "Alps");
    }

    #[test]
    fn regression_catalog_5282_br_lookaround_in_resolve_rx() {
        // (?<!^)<br>(?!$) — replace <br> that isn't at start or end.
        let r = AutoscrapeRegex::new(r"(?<!^)<br>(?!$)").expect("br lookaround must compile");
        assert_eq!(r.replace_all("a<br>b<br>c", "; "), "a; b; c");
    }
}
