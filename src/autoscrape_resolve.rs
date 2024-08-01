use crate::autoscrape::{AutoscrapeError, AutoscrapeRegex, JsonStuff};
use anyhow::Result;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use serde_json::{json, Value};
use std::collections::HashMap;

lazy_static! {
    pub static ref RE_SIMPLE_SPACE: Regex = RegexBuilder::new(r"\s+")
        .multi_line(true)
        .ignore_whitespace(true)
        .build()
        .expect("Regex error");
    static ref RE_HTML: Regex = Regex::new(r"(<.*?>)").expect("Regex error");
}

#[derive(Debug, Clone)]
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
                })
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
        let regex = AutoscrapeRegex::new(re_pattern).ok();
        let err = AutoscrapeError::UnknownLevelType(json.to_string());
        let regex = regex.ok_or(err)?;
        regexs.push((regex, replacement.to_string()));
        Ok(())
    }

    fn from_json_get_regexs_str(json: &Value) -> Vec<Value> {
        let regexs_str = json
            .get("rx")
            .map(|x| x.to_owned())
            .unwrap_or_else(|| json!([]))
            .as_array()
            .map(|x| x.to_owned())
            .unwrap_or_default();
        regexs_str
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
