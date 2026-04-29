use crate::util::wikidata_props as wp;
use crate::{DbId, PropertyId};
use mysql_async::Row;
use serde::{Deserialize, Serialize};
use wikimisc::wikibase::{Entity, EntityTrait, Reference, Snak, SnakDataType, Statement};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub struct AuxiliaryRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    row_id: Option<DbId>,
    prop_numeric: PropertyId,
    value: String,
    #[serde(default)]
    in_wikidata: bool,
    #[serde(default)]
    entry_is_matched: bool,
}

impl AuxiliaryRow {
    pub fn new(prop_numeric: PropertyId, value: String) -> Self {
        Self {
            row_id: None,
            prop_numeric,
            value,
            in_wikidata: false,
            entry_is_matched: false,
        }
    }

    //TODO test
    pub fn from_row(row: &Row) -> Option<Self> {
        // Decoded by column name so a SELECT reorder (or extra
        // prepended columns) doesn't silently misalign the fields.
        // SQL columns: `id`, `aux_p`, `aux_name`, `in_wikidata`, `entry_is_matched`.
        Some(Self {
            row_id: row.get("id"),
            prop_numeric: row.get("aux_p")?,
            value: row.get("aux_name")?,
            in_wikidata: row.get("in_wikidata")?,
            entry_is_matched: row.get("entry_is_matched")?,
        })
    }

    pub fn row_id(&self) -> Option<DbId> {
        self.row_id
    }

    pub fn clear_row_id(&mut self) {
        self.row_id = None;
    }

    pub fn prop_numeric(&self) -> PropertyId {
        self.prop_numeric
    }

    pub fn prop_as_string(&self) -> String {
        format!("P{}", self.prop_numeric)
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn in_wikidata(&self) -> bool {
        self.in_wikidata
    }

    pub fn entry_is_matched(&self) -> bool {
        self.entry_is_matched
    }

    pub fn fix_external_id(prop: &str, value: &str) -> String {
        match prop {
            p if p == wp::P_ISNI => value.replace(' ', ""), // ISNI
            _ => value.to_string(),
        }
    }

    pub fn get_claim_for_aux(&self, prop: Entity, references: &[Reference]) -> Option<Statement> {
        let prop = match prop {
            Entity::Property(prop) => prop,
            _ => return None, // Ignore
        };
        let snak = match prop.datatype().to_owned()? {
            SnakDataType::Time => todo!(),
            SnakDataType::WikibaseItem => Snak::new_item(prop.id(), &self.value),
            SnakDataType::WikibaseProperty => todo!(),
            SnakDataType::WikibaseLexeme => todo!(),
            SnakDataType::WikibaseSense => todo!(),
            SnakDataType::WikibaseForm => todo!(),
            SnakDataType::String => Snak::new_string(prop.id(), &self.value),
            SnakDataType::ExternalId => {
                Snak::new_external_id(prop.id(), &Self::fix_external_id(prop.id(), &self.value))
            }
            SnakDataType::GlobeCoordinate => todo!(),
            SnakDataType::MonolingualText => todo!(),
            SnakDataType::Quantity => todo!(),
            SnakDataType::Url => todo!(),
            SnakDataType::CommonsMedia => Snak::new_string(prop.id(), &self.value),
            SnakDataType::Math => todo!(),
            SnakDataType::TabularData => todo!(),
            SnakDataType::MusicalNotation => todo!(),
            SnakDataType::GeoShape => todo!(),
            SnakDataType::NotSet => todo!(),
            SnakDataType::NoValue => todo!(),
            SnakDataType::SomeValue => todo!(),
            SnakDataType::EntitySchema => todo!(),
        };
        Some(Statement::new_normal(snak, vec![], references.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_claim_for_aux() {
        let aux = AuxiliaryRow {
            row_id: Some(1),
            prop_numeric: 12345,
            value: "Q5678".to_string(),
            in_wikidata: true,
            entry_is_matched: true,
        };
        let property = wikimisc::wikibase::PropertyEntity::new(
            "P12345".to_string(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(SnakDataType::WikibaseItem),
            false,
        );
        let prop = Entity::Property(property);
        let claim = aux.get_claim_for_aux(prop, &[]);
        let expected = Snak::new_item("P12345", "Q5678");
        assert_eq!(*claim.unwrap().main_snak(), expected);
    }

    #[test]
    fn test_auxiliary_row_accessors() {
        let row = AuxiliaryRow {
            row_id: Some(10),
            prop_numeric: 214,
            value: "12345678".to_string(),
            in_wikidata: true,
            entry_is_matched: false,
        };
        assert_eq!(row.prop_numeric(), 214);
        assert_eq!(row.value(), "12345678");
        assert!(row.in_wikidata());
        assert!(!row.entry_is_matched());
    }
}
