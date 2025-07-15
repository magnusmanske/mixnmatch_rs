use serde_json::{json, Value};
use std::collections::HashMap;

use crate::entry::CoordinateLocation;

pub type WikidataCommandPropertyValueGroup = Vec<WikidataCommandPropertyValue>;
pub type WikidataCommandPropertyValueGroups = Vec<WikidataCommandPropertyValueGroup>;

#[derive(Debug, Clone, Copy)]
pub enum WikidataCommandWhat {
    Property(usize), // Property ID
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

#[derive(Debug, Clone, Copy)]
pub enum WikidataCommandRank {
    Normal,
    Preferred,
    Deprecated,
}

impl WikidataCommandRank {
    pub const fn as_str(&self) -> &str {
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
    pub value: WikidataCommandValue,
}

#[derive(Debug, Clone)]
pub struct WikidataCommand {
    pub item_id: usize,
    pub what: WikidataCommandWhat,
    pub value: WikidataCommandValue,
    pub references: WikidataCommandPropertyValueGroups,
    pub qualifiers: Vec<WikidataCommandPropertyValue>,
    pub comment: Option<String>,
    pub rank: Option<WikidataCommandRank>,
}

impl WikidataCommand {
    //TODO test
    pub fn edit_entity(&self, json: &mut Value) {
        // Assiuming "create"
        match &self.what {
            WikidataCommandWhat::Property(property) => {
                let mut claim = self.edit_entity_generate_claim(property);
                self.edit_entity_references(&mut claim);
                self.edit_entity_qualifiers(&mut claim);
                Self::edit_entity_add_claim(json, claim);
            }
        }
    }

    fn edit_entity_generate_claim(&self, property: &usize) -> Value {
        json!({
            "mainsnak":Self::value_as_snak(*property, &self.value),
            "type":"statement",
            "rank":self.rank_as_str()
        })
    }

    fn edit_entity_qualifiers(&self, claim: &mut Value) {
        if !self.qualifiers.is_empty() {
            let mut snaks: HashMap<String, Vec<Value>> = HashMap::new();
            for qualifier in &self.qualifiers {
                let snak = Self::value_as_snak(qualifier.property, &qualifier.value);
                let prop = format!("P{}", qualifier.property);
                snaks
                    .entry(prop)
                    .and_modify(|v| v.push(snak.to_owned()))
                    .or_insert(vec![snak.to_owned()]);
            }
            claim["qualifiers"] = json!(snaks);
        }
    }

    fn edit_entity_references(&self, claim: &mut Value) {
        if !self.references.is_empty() {
            let mut reference_groups = vec![];
            for reference_group in &self.references {
                if reference_group.is_empty() {
                    continue;
                }
                let mut snaks: HashMap<String, Vec<Value>> = HashMap::new();
                for reference in reference_group {
                    let snak = Self::value_as_snak(reference.property, &reference.value);
                    let prop = format!("P{}", reference.property);
                    snaks
                        .entry(prop)
                        .and_modify(|v| v.push(snak.to_owned()))
                        .or_insert(vec![snak.to_owned()]);
                }
                reference_groups.push(json!({ "snaks": snaks }));
            }
            claim["references"] = json!(reference_groups);
        }
    }

    //TODO test
    fn datavalue_as_snak(property: usize, datavalue: Value) -> Value {
        json!({"snaktype":"value","property":format!("P{}",property),"datavalue":datavalue})
    }

    //TODO test
    fn value_as_snak(property: usize, value: &WikidataCommandValue) -> Value {
        let datavalue = Self::as_datavalue(value);
        Self::datavalue_as_snak(property, datavalue)
    }

    //TODO test
    const fn rank_as_str(&self) -> &str {
        match &self.rank {
            Some(rank) => rank.as_str(),
            None => WikidataCommandRank::Normal.as_str(),
        }
    }

    //TODO test
    fn as_datavalue(value: &WikidataCommandValue) -> Value {
        match value {
            WikidataCommandValue::String(s) => json!({"value":s.to_owned(),"type":"string"}),
            WikidataCommandValue::Item(q) => {
                json!({"value":{"entity-type":"item","numeric-id":q,"id":format!("Q{}",q)},"type":"wikibase-entityid"})
            }
            WikidataCommandValue::Location(cl) => {
                json!({"value":{"latitude":cl.lat(),"longitude":cl.lon(),"globe":"http://www.wikidata.org/entity/Q2"},"type":"globecoordinate"})
            } //_ => {panic!("WikidataCommand::as_datavalue: not implemented: {:?}",&self)}
        }
    }

    fn edit_entity_add_claim(json: &mut Value, claim: Value) {
        if json.get("claims").is_none() {
            json["claims"] = json!([]);
        }
        if let Some(claims) = json["claims"].as_array_mut() {
            claims.push(claim);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::CoordinateLocation;

    #[test]
    fn test_as_datavalue1() {
        let value = WikidataCommandValue::String("test".to_owned());
        let datavalue = WikidataCommand::as_datavalue(&value);
        assert_eq!(
            datavalue,
            json!({"value":"test","type":"string"}),
            "as_datavalue failed"
        );
    }

    #[test]
    fn test_as_datavalue2() {
        let value = WikidataCommandValue::Item(0);
        let datavalue = WikidataCommand::as_datavalue(&value);
        assert_eq!(
            datavalue,
            json!({"value":{"entity-type":"item","numeric-id":0,"id":"Q0"},"type":"wikibase-entityid"}),
            "as_datavalue failed"
        );
    }

    #[test]
    fn test_as_datavalue3() {
        let value = WikidataCommandValue::Location(CoordinateLocation::new(0.0, 0.0));
        let datavalue = WikidataCommand::as_datavalue(&value);
        assert_eq!(
            datavalue,
            json!({"value":{"latitude":0.0,"longitude":0.0,"globe":"http://www.wikidata.org/entity/Q2"},"type":"globecoordinate"}),
            "as_datavalue failed"
        );
    }

    #[test]
    fn test_rank_as_str() {
        let rank1 = WikidataCommandRank::Normal;
        assert_eq!(rank1.as_str(), "normal", "rank_as_str failed");

        let rank2 = WikidataCommandRank::Preferred;
        assert_eq!(rank2.as_str(), "preferred", "rank_as_str failed");

        let rank3 = WikidataCommandRank::Deprecated;
        assert_eq!(rank3.as_str(), "deprecated", "rank_as_str failed");
    }

    #[test]
    fn test_value_as_snak() {
        let value = WikidataCommandValue::String("test".to_owned());
        let snak = WikidataCommand::value_as_snak(0, &value);
        assert_eq!(
            snak,
            json!({"snaktype":"value","property":"P0","datavalue":{"value":"test","type":"string"}}),
            "value_as_snak failed"
        );
    }

    #[test]
    fn test_value_as_snak2() {
        let value = WikidataCommandValue::Item(0);
        let snak = WikidataCommand::value_as_snak(0, &value);
        assert_eq!(
            snak,
            json!({"snaktype":"value","property":"P0","datavalue":{"value":{"entity-type":"item","numeric-id":0,"id":"Q0"},"type":"wikibase-entityid"}}),
            "value_as_snak failed"
        );
    }

    #[test]
    fn test_value_as_snak3() {
        let value = WikidataCommandValue::Location(CoordinateLocation::new(0.0, 0.0));
        let snak = WikidataCommand::value_as_snak(0, &value);
        assert_eq!(
            snak,
            json!({"snaktype":"value","property":"P0","datavalue":{"value":{"latitude":0.0,"longitude":0.0,"globe":"http://www.wikidata.org/entity/Q2"},"type":"globecoordinate"}}),
            "value_as_snak failed"
        );
    }

    #[test]
    fn test_datavalue_as_snak() {
        let datavalue = json!({"value":"test","type":"string"});
        let snak = WikidataCommand::datavalue_as_snak(0, datavalue);
        assert_eq!(
            snak,
            json!({"snaktype":"value","property":"P0","datavalue":{"value":"test","type":"string"}}),
            "datavalue_as_snak failed"
        );
    }

    #[test]
    fn test_command_rank_as_str() {
        assert_eq!(
            WikidataCommandRank::Normal.as_str(),
            "normal",
            "rank_as_str failed"
        );
        assert_eq!(
            WikidataCommandRank::Preferred.as_str(),
            "preferred",
            "rank_as_str failed"
        );
        assert_eq!(
            WikidataCommandRank::Deprecated.as_str(),
            "deprecated",
            "rank_as_str failed"
        );
    }
}
