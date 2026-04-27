use crate::autoscrape::{AutoscrapeError, AutoscrapeRegex, JsonStuff};
use crate::auxiliary_data::AuxiliaryRow;
use anyhow::Result;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use serde_json::{Value, json};
use std::collections::HashMap;

lazy_static! {
    pub static ref RE_SIMPLE_SPACE: Regex = RegexBuilder::new(r"\s+")
        .multi_line(true)
        .ignore_whitespace(true)
        .build()
        .expect("Regex error");
    static ref RE_HTML: Regex = Regex::new(r"(<.*?>)").expect("Regex error");
}

#[derive(Debug, Clone, Default)]
pub struct AutoscrapeResolve {
    use_pattern: String,
    regexs: Vec<(AutoscrapeRegex, String)>,
}

impl JsonStuff for AutoscrapeResolve {}

impl AutoscrapeResolve {
    //TODO test
    pub fn from_json(json: &Value, key: &str) -> Result<Self, AutoscrapeError> {
        let json = match json.get(key) {
            Some(json) => json,
            None => {
                return Ok(Self {
                    use_pattern: String::new(),
                    regexs: vec![],
                });
            }
        };
        //.ok_or_else(||AutoscrapeError::UnknownLevelType(json.to_owned()))?;
        let regexs_str = Self::from_json_get_regexs_str(json);
        let mut regexs = vec![];
        for regex in regexs_str {
            Self::from_json_regex(regex, json, &mut regexs)?;
        }
        let use_pattern = Self::json_as_str(json, "use")?;
        Ok(Self {
            use_pattern,
            regexs,
        })
    }

    //TODO test
    pub fn replace_vars(&self, map: &HashMap<String, String>) -> String {
        let effective_map = self.expand_with_replaced_captures(map);
        let mut ret = self.use_pattern.to_owned();
        for (key, value) in &effective_map {
            ret = ret.replace(key, value);
        }
        for regex in &self.regexs {
            ret = regex.0.replace_all(&ret, &regex.1).into();
        }
        Self::fix_html(&ret).trim().into()
    }

    /// Adds `$R1`, `$R2`, … entries — each is the matching `$1`, `$2`, …
    /// capture run through this field's `rx` replacements in isolation.
    /// Lets a `use` pattern combine raw and per-capture-replaced values
    /// (e.g. anchor-based replacements that would otherwise need to be
    /// rewritten to match against the full composed string).
    fn expand_with_replaced_captures(
        &self,
        map: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut out = map.clone();
        for (key, value) in map {
            let Some(num) = key.strip_prefix('$') else {
                continue;
            };
            if num.is_empty() || !num.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let mut v = value.to_owned();
            for regex in &self.regexs {
                v = regex.0.replace_all(&v, &regex.1).into();
            }
            out.insert(format!("$R{num}"), v);
        }
        out
    }

    //TODO test
    fn fix_html(s: &str) -> String {
        let ret = html_escape::decode_html_entities(s);
        let ret = RE_HTML.replace_all(&ret, " ");
        RE_SIMPLE_SPACE.replace_all(&ret, " ").trim().into()
    }

    // #lizard forgives
    fn from_json_regex(
        regex: Value,
        json: &Value,
        regexs: &mut Vec<(Regex, String)>,
    ) -> Result<(), AutoscrapeError> {
        let arr = regex
            .as_array()
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?;
        let pattern = arr
            .first()
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?
            .as_str()
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?;
        let replacement = arr
            .get(1)
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?
            .as_str()
            .ok_or_else(|| AutoscrapeError::UnknownLevelType(json.to_string()))?;
        let re_pattern = &Self::fix_regex(pattern);
        let regex_ok = AutoscrapeRegex::new(re_pattern).ok();
        let err = AutoscrapeError::UnknownLevelType(json.to_string());
        let regex_final = regex_ok.ok_or(err)?;
        regexs.push((regex_final, replacement.to_string()));
        Ok(())
    }

    fn from_json_get_regexs_str(json: &Value) -> Vec<Value> {
        json.get("rx")
            .map(|x| x.to_owned())
            .unwrap_or_else(|| json!([]))
            .as_array()
            .map(|x| x.to_owned())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct AutoscrapeResolveAux {
    property: usize,
    id: String,
}

impl JsonStuff for AutoscrapeResolveAux {}

impl AutoscrapeResolveAux {
    //TODO test
    pub fn from_json(json: &Value) -> Result<Self, AutoscrapeError> {
        let property = Self::json_as_str(json, "prop")?.replace('P', "");
        let property = match property.parse::<usize>() {
            Ok(property) => property,
            _ => return Err(AutoscrapeError::BadType(json.to_owned())),
        };
        let id = Self::json_as_str(json, "id")?;
        Ok(Self { property, id })
    }

    //TODO test
    pub fn replace_vars(&self, map: &HashMap<String, String>) -> AuxiliaryRow {
        let mut ret = self.id.to_owned();
        for (key, value) in map {
            ret = ret.replace(key, value);
        }
        let ret = AutoscrapeResolve::fix_html(&ret);
        AuxiliaryRow::new(self.property, ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_autoscrape_resolve_from_json() {
        let json = json!({"test":{
            "use": "use",
            "rx": [
                ["rx", "replace"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        assert_eq!(resolve.use_pattern, "use");
        assert_eq!(resolve.regexs.len(), 1);
        assert_eq!(resolve.regexs[0].0.to_string(), "rx");
        assert_eq!(resolve.regexs[0].1, "replace");
    }

    #[test]
    fn test_autoscrape_resolve_from_json_no_rx() {
        let json = json!({"test":{
            "use": "use"}
        });
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        assert_eq!(resolve.use_pattern, "use");
        assert_eq!(resolve.regexs.len(), 0);
    }

    #[test]
    fn test_autoscrape_resolve_from_json_nothing() {
        let json = json!({});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        assert_eq!(resolve.use_pattern, "");
        assert_eq!(resolve.regexs.len(), 0);
    }

    #[test]
    fn test_autoscrape_resolve_replace_vars() {
        let json = json!({"test":{
            "use": "use",
            "rx": [
                ["rx", "replace"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("use".to_string(), "replace".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "replace");
    }

    #[test]
    fn test_autoscrape_resolve_replace_vars_no_regex() {
        let json = json!({"test":{
            "use": "use"
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("use".to_string(), "replace".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "replace");
    }

    #[test]
    fn test_autoscrape_resolve_aux_from_json() {
        let json = json!({"prop": "P123", "id": "id"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        assert_eq!(resolve.property, 123);
        assert_eq!(resolve.id, "id");
    }

    #[test]
    fn test_autoscrape_resolve_aux_replace_vars() {
        let json = json!({"prop": "P123", "id": "id"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        let mut map = HashMap::new();
        map.insert("id".to_string(), "replace".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, AuxiliaryRow::new(123, "replace".to_string()));
    }

    #[test]
    fn test_autoscrape_resolve_aux_replace_vars_no_replace() {
        let json = json!({"prop": "P123", "id": "id"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        let map = HashMap::new();
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, AuxiliaryRow::new(123, "id".to_string()));
    }

    #[test]
    fn test_fix_html_strips_tags() {
        assert_eq!(AutoscrapeResolve::fix_html("<b>bold</b>"), "bold");
        assert_eq!(
            AutoscrapeResolve::fix_html("<a href=\"x\">link</a> text"),
            "link text"
        );
    }

    #[test]
    fn test_fix_html_decodes_entities() {
        assert_eq!(AutoscrapeResolve::fix_html("A &amp; B"), "A & B");
        assert_eq!(AutoscrapeResolve::fix_html("caf&eacute;"), "café");
        // Note: &lt;...&gt; gets decoded to <...> which is then stripped as an HTML tag
        assert_eq!(AutoscrapeResolve::fix_html("&lt;not a tag&gt;"), "");
    }

    #[test]
    fn test_fix_html_collapses_whitespace() {
        assert_eq!(
            AutoscrapeResolve::fix_html("  hello   world  "),
            "hello world"
        );
        assert_eq!(AutoscrapeResolve::fix_html("a\n\nb"), "a b");
    }

    #[test]
    fn test_fix_html_combined() {
        assert_eq!(
            AutoscrapeResolve::fix_html("  <b>Hello</b>   &amp;   <i>World</i>  "),
            "Hello & World"
        );
    }

    #[test]
    fn test_replace_vars_applies_regex() {
        let json = json!({"test":{
            "use": "Hello World 123",
            "rx": [
                ["\\d+", "NUM"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let map = HashMap::new();
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "Hello World NUM");
    }

    #[test]
    fn test_replace_vars_applies_multiple_regexes() {
        let json = json!({"test":{
            "use": "foo bar baz",
            "rx": [
                ["foo", "FOO"],
                ["baz", "BAZ"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let map = HashMap::new();
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "FOO bar BAZ");
    }

    #[test]
    fn test_replace_vars_var_substitution_then_regex() {
        let json = json!({"test":{
            "use": "$NAME ($YEAR)",
            "rx": [
                ["\\((.+?)\\)", "- $1"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$NAME".to_string(), "John".to_string());
        map.insert("$YEAR".to_string(), "2024".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "John - 2024");
    }

    #[test]
    fn test_replace_vars_strips_html_from_result() {
        let json = json!({"test":{
            "use": "<b>$VAR</b>",
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$VAR".to_string(), "value".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, "value");
    }

    #[test]
    fn test_autoscrape_resolve_aux_from_json_without_p_prefix() {
        let json = json!({"prop": "456", "id": "some-id"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        assert_eq!(resolve.property, 456);
        assert_eq!(resolve.id, "some-id");
    }

    #[test]
    fn test_autoscrape_resolve_aux_from_json_bad_prop() {
        let json = json!({"prop": "not-a-number", "id": "id"});
        assert!(AutoscrapeResolveAux::from_json(&json).is_err());
    }

    #[test]
    fn test_autoscrape_resolve_aux_replace_vars_strips_html() {
        let json = json!({"prop": "P1", "id": "<b>$X</b>"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        let mut map = HashMap::new();
        map.insert("$X".to_string(), "val".to_string());
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, AuxiliaryRow::new(1, "val".to_string()));
    }

    #[test]
    fn test_autoscrape_resolve_from_json_bad_regex() {
        let json = json!({"test":{
            "use": "x",
            "rx": [
                ["(unclosed", "replace"]
            ]
        }});
        assert!(AutoscrapeResolve::from_json(&json, "test").is_err());
    }

    #[test]
    fn test_replace_vars_dollar_r_applies_field_rx_to_capture() {
        // $R1 should be capture group 1 with this field's rx applied to it
        // alone, independently of the rest of the use_pattern.
        let json = json!({"test":{
            "use": "$R1",
            "rx": [
                ["^foo", "BAR"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$1".to_string(), "foofoo".to_string());
        assert_eq!(resolve.replace_vars(&map), "BARfoo");
    }

    #[test]
    fn test_replace_vars_dollar_r_anchored_only_in_capture() {
        // The `^` anchor in a per-field rx normally only matches the start
        // of the composed string. $R1 lets it match the start of the
        // capture itself instead. Here the use_pattern leads with a
        // literal so the whole-string rx pass at the end can't fire.
        let json = json!({"test":{
            "use": "X $R1",
            "rx": [
                ["^foo", "BAR"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$1".to_string(), "foofoo".to_string());
        assert_eq!(resolve.replace_vars(&map), "X BARfoo");
    }

    #[test]
    fn test_replace_vars_dollar_r_with_no_rx_equals_capture() {
        // With no rx defined, $R1 just mirrors $1 — useful so users can
        // sprinkle $R1 in templates without breaking when rx is empty.
        let json = json!({"test":{
            "use": "[$R1] [$1]"
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$1".to_string(), "abc".to_string());
        assert_eq!(resolve.replace_vars(&map), "[abc] [abc]");
    }

    #[test]
    fn test_replace_vars_dollar_r_does_not_apply_to_levels() {
        // $L1 represents a level value, not a capture; it must not be
        // rewritten as $RL1, and `$1` substitution must not corrupt it.
        let json = json!({"test":{
            "use": "$L1 $R1",
            "rx": [
                ["a", "Z"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$1".to_string(), "aaa".to_string());
        map.insert("$L1".to_string(), "level".to_string());
        // $L1 stays raw; $R1 = "ZZZ"; whole-string rx then turns the
        // remaining "level" into "Zlevel" (its single 'a'... wait, "level"
        // has no 'a' — assert literally what the pipeline gives).
        assert_eq!(resolve.replace_vars(&map), "level ZZZ");
    }

    #[test]
    fn test_replace_vars_dollar_r_alongside_raw_capture() {
        // Mixing $1 (raw) with $R1 (per-capture rx applied) should keep
        // the two distinct, even though the field-level rx still runs
        // over the final composed string.
        let json = json!({"test":{
            "use": "raw=$1 cooked=$R1",
            "rx": [
                ["^foo", "BAR"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        let mut map = HashMap::new();
        map.insert("$1".to_string(), "foofoo".to_string());
        // Final whole-string rx anchors at start of "raw=foofoo cooked=BARfoo",
        // which has no "foo" prefix, so it doesn't fire — leaving the
        // raw and cooked forms side by side.
        assert_eq!(
            resolve.replace_vars(&map),
            "raw=foofoo cooked=BARfoo"
        );
    }

    #[test]
    fn test_autoscrape_resolve_from_json_multiple_rx() {
        let json = json!({"test":{
            "use": "x",
            "rx": [
                ["a", "A"],
                ["b", "B"],
                ["c", "C"]
            ]
        }});
        let resolve = AutoscrapeResolve::from_json(&json, "test").unwrap();
        assert_eq!(resolve.regexs.len(), 3);
    }
}
