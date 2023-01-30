use serde_json::{Value,json};
use std::collections::HashMap;
use crate::entry::*;

pub type WikidataCommandPropertyValueGroup = Vec<WikidataCommandPropertyValue>;
pub type WikidataCommandPropertyValueGroups = Vec<WikidataCommandPropertyValueGroup>;

#[derive(Debug, Clone)]
pub enum WikidataCommandWhat {
    Property(usize), // Property ID
    //Label(String), // Language
    //Alias(String), // Language
    //Description(String), // Language
    //Sitelink(String), // wiki
}

#[derive(Debug, Clone, PartialEq)]
pub enum WikidataCommandValue {
    String(String),
    Item(usize),
    //Time(String),
    Location(CoordinateLocation),
    //SomeValue,
    //NoValue,
}

#[derive(Debug, Clone)]
pub enum WikidataCommandRank {
    Normal,
    Preferred,
    Deprecated
}

impl WikidataCommandRank {
    //TODO test
    pub fn as_str(&self) -> &str {
        match self {
            WikidataCommandRank::Normal => "normal",
            WikidataCommandRank::Preferred => "preferred",
            WikidataCommandRank::Deprecated => "deprecated",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WikidataCommandPropertyValue {
    pub property: usize,
    pub value: WikidataCommandValue
}

#[derive(Debug, Clone)]
pub struct WikidataCommand {
    pub item_id: usize,
    pub what: WikidataCommandWhat,
    pub value: WikidataCommandValue,
    pub references: WikidataCommandPropertyValueGroups,
    pub qualifiers: Vec<WikidataCommandPropertyValue>,
    pub comment: Option<String>,
    pub rank: Option<WikidataCommandRank>
}

impl WikidataCommand {
    //TODO test
    pub fn edit_entity(&self, json: &mut Value) {
        // Assiuming "create"
        match &self.what {
            WikidataCommandWhat::Property(property) => {
                let mainsnak = self.value_as_snak(*property, &self.value);
                let mut claim = json!({
                    "mainsnak":mainsnak,
                    "type":"statement",
                    "rank":self.rank_as_str()
                });
                
                if !self.references.is_empty() {
                    let mut reference_groups = vec![];
                    for reference_group in &self.references {
                        if reference_group.is_empty() {
                            continue;
                        }
                        let mut snaks: HashMap<String,Vec<Value>> = HashMap::new();
                        for reference in reference_group {
                            let snak = self.value_as_snak(reference.property, &reference.value);
                            let prop = format!("P{}",reference.property);
                            snaks.entry(prop).and_modify(|v| v.push(snak.to_owned())).or_insert(vec![snak.to_owned()]);

                        }
                        reference_groups.push(json!({"snaks":snaks}));
                    }
                    claim["references"] = json!(reference_groups);
                }
                
                if !self.qualifiers.is_empty() {
                    let mut snaks: HashMap<String,Vec<Value>> = HashMap::new();
                    for qualifier in &self.qualifiers {
                        let snak = self.value_as_snak(qualifier.property, &qualifier.value);
                        let prop = format!("P{}",qualifier.property);
                        snaks.entry(prop).and_modify(|v| v.push(snak.to_owned())).or_insert(vec![snak.to_owned()]);
                    }
                    claim["qualifiers"] = json!(snaks);
                }
                
                if json.get("claims").is_none() {
                    json["claims"] = json!([]);
                }
                if let Some(claims) = json["claims"].as_array_mut() {
                    claims.push(claim)
                }
            }
        }
    }

    //TODO test
    fn datavalue_as_snak(&self, property: usize, datavalue: Value) -> Value {
        json!({"snaktype":"value","property":format!("P{}",property),"datavalue":datavalue})
    }

    //TODO test
    fn value_as_snak(&self, property: usize, value: &WikidataCommandValue) -> Value {
        let datavalue = self.as_datavalue(value);
        self.datavalue_as_snak(property, datavalue)
    }

    //TODO test
    fn rank_as_str(&self) -> &str {
        match &self.rank {
            Some(rank) => rank.as_str(),
            None => WikidataCommandRank::Normal.as_str()
        }
    }

    //TODO test
    fn as_datavalue(&self, value: &WikidataCommandValue) -> Value {
        match value {
            WikidataCommandValue::String(s) => json!({"value":s.to_owned(),"type":"string"}),
            WikidataCommandValue::Item(q) => json!({"value":{"entity-type":"item","numeric-id":q,"id":format!("Q{}",q)},"type":"wikibase-entityid"}),
            WikidataCommandValue::Location(cl) => json!({"value":{"latitude":cl.lat,"longitude":cl.lon,"globe":"http://www.wikidata.org/entity/Q2"},"type":"globecoordinate"}),
            //_ => {panic!("WikidataCommand::as_datavalue: not implemented: {:?}",&self)}
        }
    }
}