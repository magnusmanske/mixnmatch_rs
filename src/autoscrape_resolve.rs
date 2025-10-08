use crate::autoscrape::{AutoscrapeError, AutoscrapeRegex, JsonStuff};
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
        let mut ret = self.use_pattern.to_owned();
        for (key, value) in map {
            ret = ret.replace(key, value);
        }
        for regex in &self.regexs {
            ret = regex.0.replace_all(&ret, &regex.1).into();
        }
        Self::fix_html(&ret).trim().into()
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
    pub fn replace_vars(&self, map: &HashMap<String, String>) -> (usize, String) {
        let mut ret = self.id.to_owned();
        for (key, value) in map {
            ret = ret.replace(key, value);
        }
        let ret = AutoscrapeResolve::fix_html(&ret);
        (self.property, ret)
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
        assert_eq!(ret, (123, "replace".to_string()));
    }

    #[test]
    fn test_autoscrape_resolve_aux_replace_vars_no_replace() {
        let json = json!({"prop": "P123", "id": "id"});
        let resolve = AutoscrapeResolveAux::from_json(&json).unwrap();
        let map = HashMap::new();
        let ret = resolve.replace_vars(&map);
        assert_eq!(ret, (123, "id".to_string()));
    }
}
