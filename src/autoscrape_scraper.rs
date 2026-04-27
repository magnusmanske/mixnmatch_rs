use crate::autoscrape::{
    Autoscrape, AutoscrapeError, AutoscrapeRegex, AutoscrapeRegexBuilder, JsonStuff,
};
use crate::autoscrape_resolve::{AutoscrapeResolve, AutoscrapeResolveAux};
use crate::entry::Entry;
use crate::extended_entry::ExtendedEntry;
use anyhow::Result;
use rand::prelude::*;
use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct AutoscrapeScraper {
    url: String,
    regex_block: Option<AutoscrapeRegex>,
    regex_entry: Vec<AutoscrapeRegex>,
    resolve_id: AutoscrapeResolve,
    resolve_name: AutoscrapeResolve,
    resolve_desc: AutoscrapeResolve,
    resolve_url: AutoscrapeResolve,
    resolve_type: AutoscrapeResolve,
    resolve_aux: Vec<AutoscrapeResolveAux>,
}

impl JsonStuff for AutoscrapeScraper {}

impl AutoscrapeScraper {
    // #lizard forgives
    pub fn from_json(json: &Value) -> Result<Self> {
        let resolve = json
            .get("resolve")
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
        Ok(Self {
            url: Self::json_as_str(json, "url")?,
            regex_block: Self::regex_block_from_json(json)?,
            regex_entry: Self::regex_entry_from_json(json)?,
            resolve_id: AutoscrapeResolve::from_json(resolve, "id")?,
            resolve_name: AutoscrapeResolve::from_json(resolve, "name")?,
            resolve_desc: AutoscrapeResolve::from_json(resolve, "desc")?,
            resolve_url: AutoscrapeResolve::from_json(resolve, "url")?,
            resolve_type: AutoscrapeResolve::from_json(resolve, "type")?,
            resolve_aux: Self::resolve_aux_from_json(json)?,
        })
    }

    fn resolve_aux_from_json(json: &Value) -> Result<Vec<AutoscrapeResolveAux>> {
        Ok(json // TODO test aux, eg catalog 287
            .get("aux")
            .map(|x| x.to_owned())
            .unwrap_or_else(|| json!([]))
            .as_array()
            .map(|x| x.to_owned())
            .unwrap_or_default()
            .iter()
            .filter_map(|x| AutoscrapeResolveAux::from_json(x).ok())
            .collect())
    }

    fn regex_entry_from_json(json: &Value) -> Result<Vec<AutoscrapeRegex>> {
        let rx_entry = json
            .get("rx_entry")
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
        if rx_entry.is_string() {
            Self::regex_entry_from_json_string(rx_entry, json)
        } else {
            // Assuming array
            Self::regex_entry_from_json_array(rx_entry, json)
        }
    }

    fn regex_block_from_json(json: &Value) -> Result<Option<AutoscrapeRegex>> {
        Ok(
            // TODO test
            if let Some(v) = json.get("rx_block") {
                if let Some(s) = v.as_str() {
                    if s.is_empty() {
                        None
                    } else {
                        let r = AutoscrapeRegexBuilder::new(&Self::fix_regex(s))
                            .multi_line(true)
                            .build()?;
                        Some(r)
                    }
                } else {
                    None
                }
            } else {
                None
            },
        )
    }

    pub fn process_html_page(&self, html: &str, autoscrape: &Autoscrape) -> Vec<ExtendedEntry> {
        self.regex_block.as_ref().map_or_else(
            || self.process_html_block(html, autoscrape),
            |regex_block| {
                regex_block
                    .captures_iter(html)
                    //.filter_map(|caps|caps.ok())
                    .filter_map(|cap| cap.get(1))
                    .map(|s| s.as_str().to_string())
                    .flat_map(|s| self.process_html_block(&s, autoscrape))
                    .collect()
            },
        )
    }

    /// Instrumented variant of `process_html_page` used by the scraper-
    /// test UI. Mirrors the production code path exactly but also
    /// collects per-stage counts so the frontend can tell the user
    /// where their regex stopped matching (most common "0 results"
    /// cause is a block regex that works but an entry regex that
    /// doesn't match inside the extracted blocks, or vice versa).
    ///
    /// Not used by the autoscrape runner itself — the extra bookkeeping
    /// is only worth paying for one-shot tests.
    pub fn analyze_html_page(
        &self,
        html: &str,
        autoscrape: &Autoscrape,
    ) -> AutoscrapeAnalysis {
        let regex_block_source = self.regex_block.as_ref().map(|r| r.as_str().to_string());
        let regex_entry_sources: Vec<String> =
            self.regex_entry.iter().map(|r| r.as_str().to_string()).collect();
        let mut regex_entry_match_counts = vec![0_usize; self.regex_entry.len()];
        let mut entries: Vec<ExtendedEntry> = vec![];

        // If a block regex is defined, feed each captured block through
        // the per-block entry pass; otherwise treat the whole page as one
        // block. Matches process_html_page's behaviour.
        let (block_match_count, blocks): (Option<usize>, Vec<String>) =
            if let Some(rb) = self.regex_block.as_ref() {
                let caps: Vec<String> = rb
                    .captures_iter(html)
                    .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
                    .collect();
                (Some(caps.len()), caps)
            } else {
                (None, vec![html.to_string()])
            };

        for block in &blocks {
            for (i, regex_entry) in self.regex_entry.iter().enumerate() {
                if !regex_entry.is_match(block) {
                    continue;
                }
                for cap in regex_entry.captures_iter(block) {
                    let entry_ex = self.process_html_block_generate_entry_ex(cap, autoscrape);
                    entries.push(entry_ex);
                    regex_entry_match_counts[i] += 1;
                }
                break; // first regex wins, matching process_html_block
            }
        }

        AutoscrapeAnalysis {
            regex_block_source,
            block_match_count,
            regex_entry_sources,
            regex_entry_match_counts,
            entries,
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn process_html_block(&self, html: &str, autoscrape: &Autoscrape) -> Vec<ExtendedEntry> {
        let mut ret = vec![];
        for regex_entry in &self.regex_entry {
            if !regex_entry.is_match(html) {
                continue;
            }
            for cap in regex_entry.captures_iter(html) {
                let entry_ex = self.process_html_block_generate_entry_ex(cap, autoscrape);
                ret.push(entry_ex);
            }
            break; // First regexp to match wins
        }
        ret
    }

    fn process_html_block_generate_entry_ex(
        &self,
        cap: regex::Captures,
        autoscrape: &Autoscrape,
    ) -> ExtendedEntry {
        let mut map = Self::process_html_block_generate_map(cap, autoscrape);
        // Resolve the id first, then publish it as $RID so all the
        // other resolve fields (and aux) can reference the post-rx id —
        // typically used to build URL/name patterns from the canonical
        // ext_id rather than the raw capture.
        let ext_id = self.resolve_id.replace_vars(&map);
        map.insert("$RID".to_string(), ext_id.clone());
        let type_name = self.resolve_type.replace_vars(&map);
        let type_name = if type_name.is_empty() {
            None
        } else {
            Some(type_name)
        };
        ExtendedEntry {
            entry: Entry {
                catalog: autoscrape.catalog_id(),
                ext_id,
                ext_url: self.resolve_url.replace_vars(&map),
                ext_name: self.resolve_name.replace_vars(&map),
                ext_desc: self.resolve_desc.replace_vars(&map),
                random: rand::rng().random(),
                type_name,
                app: Some(autoscrape.app().clone()),
                ..Default::default()
            },
            aux: self
                .resolve_aux
                .iter()
                .map(|aux| aux.replace_vars(&map))
                .collect(),
            born: None,
            died: None,
            aliases: vec![],
            descriptions: HashMap::new(),
            location: None,
        }
    }

    fn regex_entry_from_json_array(rx_entry: &Value, json: &Value) -> Result<Vec<Regex>> {
        let arr = rx_entry
            .as_array()
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
        let mut ret = vec![];
        for x in arr {
            if let Some(s) = x.as_str() {
                ret.push(
                    AutoscrapeRegexBuilder::new(&Self::fix_regex(s))
                        .multi_line(true)
                        .build()?,
                );
            }
        }
        Ok(ret)
    }

    fn regex_entry_from_json_string(rx_entry: &Value, json: &Value) -> Result<Vec<Regex>> {
        let s = rx_entry
            .as_str()
            .ok_or_else(|| AutoscrapeError::BadType(json.to_owned()))?;
        Ok(vec![
            AutoscrapeRegexBuilder::new(&Self::fix_regex(s))
                .multi_line(true)
                .build()?,
        ])
    }

    fn process_html_block_generate_map(
        cap: regex::Captures,
        autoscrape: &Autoscrape,
    ) -> HashMap<String, String> {
        let values: Vec<String> = cap
            .iter()
            .map(|v| v.map(|x| x.as_str().to_string()).unwrap_or_default())
            .collect();
        let mut map: HashMap<String, String> = values
            .iter()
            .enumerate()
            .skip(1)
            .map(|(num, value)| (format!("${num}"), value.to_owned()))
            .collect();
        for (num, level) in autoscrape.levels().iter().enumerate() {
            map.insert(format!("$L{}", num + 1), level.current());
        }
        map
    }
}

/// Per-stage counters produced by `analyze_html_page`. Drives the
/// scraper-test diagnostics panel in the frontend.
#[derive(Debug, Default)]
pub struct AutoscrapeAnalysis {
    /// Source string of the block regex as the runtime sees it (after
    /// `fix_regex` normalisation). `None` if no block regex is configured.
    pub regex_block_source: Option<String>,
    /// Number of block-regex captures. `None` means no block regex was
    /// defined (the whole HTML is used as the single "block").
    pub block_match_count: Option<usize>,
    /// Source of each entry regex, in declaration order.
    pub regex_entry_sources: Vec<String>,
    /// Per-entry-regex match counts, summed across all blocks.
    pub regex_entry_match_counts: Vec<usize>,
    /// Extracted entries (same shape as `process_html_page`).
    pub entries: Vec<ExtendedEntry>,
}
