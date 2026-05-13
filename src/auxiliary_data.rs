use crate::util::wikidata_props as wp;
use crate::{DbId, PropertyId};
use mysql_async::Row;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use wikimisc::wikibase::{Entity, EntityTrait, Reference, Snak, SnakDataType, Statement};

/// Wikidata canonical time form: `±YYYY-MM-DDTHH:MM:SSZ`. Year may be more
/// than 4 digits (Wikidata accepts up to 16 digits for prehistoric dates).
static RE_WIKIDATA_TIME: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^[+-]\d{1,16}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
        .expect("RE_WIKIDATA_TIME regex must compile")
});

/// Parse an auxiliary `Time` value of the form
/// `+YYYY-MM-DDTHH:MM:SSZ/PRECISION` (e.g. `+1079-01-01T00:00:00Z/9`).
///
/// Returns `Some((time_string, precision))` when both halves are present and
/// well-formed, or `None` otherwise. The precision suffix is **required** —
/// without it we cannot tell whether `+1079-01-01T00:00:00Z` means "year
/// 1079" (precision 9) or "1 January 1079" (precision 11), and guessing
/// would risk writing wrong claims.
fn parse_time_aux_value(value: &str) -> Option<(String, u64)> {
    let (time, precision_str) = value.rsplit_once('/')?;
    if !RE_WIKIDATA_TIME.is_match(time) {
        return None;
    }
    let precision: u64 = precision_str.parse().ok()?;
    // Wikidata defines time precision values 0..=14 (0 = billion years … 14 = second).
    if precision > 14 {
        return None;
    }
    Some((time.to_string(), precision))
}

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
        let datatype = prop.datatype().to_owned()?;
        // Only the snak datatypes we know how to project from a plain
        // string `self.value` are emitted; anything else returns None so
        // the caller skips the row rather than crashing the worker.
        // (Earlier `todo!()` placeholders would panic on real auxiliary
        // rows pointing at e.g. Time / Quantity / GlobeCoordinate / Url
        // properties.)
        let snak = match datatype {
            SnakDataType::WikibaseItem => Snak::new_item(prop.id(), &self.value),
            SnakDataType::String => Snak::new_string(prop.id(), &self.value),
            SnakDataType::ExternalId => {
                Snak::new_external_id(prop.id(), &Self::fix_external_id(prop.id(), &self.value))
            }
            SnakDataType::CommonsMedia => Snak::new_string(prop.id(), &self.value),
            SnakDataType::Url => Snak::new_url(prop.id(), &self.value),
            SnakDataType::Time => match parse_time_aux_value(&self.value) {
                Some((time, precision)) => Snak::new_time(prop.id(), &time, precision),
                None => {
                    log::warn!(
                        "auxiliary_data: skipping Time property {} — value {:?} is not in the expected `+YYYY-MM-DDTHH:MM:SSZ/PRECISION` form",
                        prop.id(),
                        self.value
                    );
                    return None;
                }
            },
            SnakDataType::WikibaseProperty
            | SnakDataType::WikibaseLexeme
            | SnakDataType::WikibaseSense
            | SnakDataType::WikibaseForm
            | SnakDataType::GlobeCoordinate
            | SnakDataType::MonolingualText
            | SnakDataType::Quantity
            | SnakDataType::Math
            | SnakDataType::TabularData
            | SnakDataType::MusicalNotation
            | SnakDataType::GeoShape
            | SnakDataType::EntitySchema
            | SnakDataType::NotSet
            | SnakDataType::NoValue
            | SnakDataType::SomeValue => {
                log::warn!(
                    "auxiliary_data: skipping property {} — datatype {:?} not supported from plain string value",
                    prop.id(),
                    datatype
                );
                return None;
            }
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

    /// Variants without a documented aux-string convention still skip rather
    /// than panic. Time / Url were unsupported here historically but are now
    /// projected (see dedicated tests below) when the aux value is parseable.
    #[test]
    fn test_get_claim_for_aux_unsupported_datatype_returns_none() {
        let aux = AuxiliaryRow {
            row_id: Some(1),
            prop_numeric: 569,
            value: "1452-04-15".to_string(),
            in_wikidata: false,
            entry_is_matched: true,
        };
        for unsupported in [
            SnakDataType::Quantity,
            SnakDataType::GlobeCoordinate,
            SnakDataType::MonolingualText,
            SnakDataType::Math,
        ] {
            let property = wikimisc::wikibase::PropertyEntity::new(
                "P569".to_string(),
                vec![],
                vec![],
                vec![],
                vec![],
                Some(unsupported),
                false,
            );
            assert!(
                aux.get_claim_for_aux(Entity::Property(property), &[])
                    .is_none(),
                "unsupported datatype {unsupported:?} must skip, not panic"
            );
        }
    }

    fn make_property(id: &str, datatype: SnakDataType) -> Entity {
        Entity::Property(wikimisc::wikibase::PropertyEntity::new(
            id.to_string(),
            vec![],
            vec![],
            vec![],
            vec![],
            Some(datatype),
            false,
        ))
    }

    /// Aux value of the form `+YYYY-MM-DDTHH:MM:SSZ/PRECISION` becomes a Time
    /// snak with the trailing precision stripped from the time string and
    /// passed as the snak's precision integer. Pinning the canonical case
    /// from the bug report (`+1079-01-01T00:00:00Z/9`, year precision).
    #[test]
    fn test_get_claim_for_aux_time_with_precision_suffix() {
        let aux = AuxiliaryRow::new(569, "+1079-01-01T00:00:00Z/9".to_string());
        let claim = aux
            .get_claim_for_aux(make_property("P569", SnakDataType::Time), &[])
            .expect("Time aux with precision suffix must produce a claim");
        let expected = Snak::new_time("P569", "+1079-01-01T00:00:00Z", 9);
        assert_eq!(*claim.main_snak(), expected);
    }

    /// Day-level precision suffix (`/11`) must round-trip through the parser.
    #[test]
    fn test_get_claim_for_aux_time_day_precision() {
        let aux = AuxiliaryRow::new(569, "+1452-04-15T00:00:00Z/11".to_string());
        let claim = aux
            .get_claim_for_aux(make_property("P569", SnakDataType::Time), &[])
            .expect("Time aux with day precision must produce a claim");
        let expected = Snak::new_time("P569", "+1452-04-15T00:00:00Z", 11);
        assert_eq!(*claim.main_snak(), expected);
    }

    /// Negative-year (BC) time values must work — the leading `-` is part of
    /// the Wikidata canonical format and must not be confused with the
    /// precision separator.
    #[test]
    fn test_get_claim_for_aux_time_bc_year() {
        let aux = AuxiliaryRow::new(569, "-0500-01-01T00:00:00Z/9".to_string());
        let claim = aux
            .get_claim_for_aux(make_property("P569", SnakDataType::Time), &[])
            .expect("BC year time aux must produce a claim");
        let expected = Snak::new_time("P569", "-0500-01-01T00:00:00Z", 9);
        assert_eq!(*claim.main_snak(), expected);
    }

    /// When the aux value carries no `/precision` suffix we cannot tell at
    /// what fidelity the upstream source recorded the date, so skip the row
    /// rather than guess. (Matches the `cersei.rs::parse_time` contract.)
    #[test]
    fn test_get_claim_for_aux_time_without_precision_skips() {
        let aux = AuxiliaryRow::new(569, "+1079-01-01T00:00:00Z".to_string());
        assert!(
            aux.get_claim_for_aux(make_property("P569", SnakDataType::Time), &[])
                .is_none(),
            "Time aux without precision suffix must skip — guessing precision risks wrong claims"
        );
    }

    /// Malformed time strings must skip cleanly, not panic.
    #[test]
    fn test_get_claim_for_aux_time_malformed_skips() {
        for bad in ["not-a-time", "1452-04-15", "+xxxx-01-01T00:00:00Z/9", "+1079-01-01T00:00:00Z/abc"] {
            let aux = AuxiliaryRow::new(569, bad.to_string());
            assert!(
                aux.get_claim_for_aux(make_property("P569", SnakDataType::Time), &[])
                    .is_none(),
                "malformed time aux {bad:?} must skip"
            );
        }
    }

    /// Url aux values are projected straight into a Url snak — the value is
    /// already a fully-formed URL by the time it lands in the aux table.
    #[test]
    fn test_get_claim_for_aux_url() {
        let aux = AuxiliaryRow::new(856, "https://example.com/foo".to_string());
        let claim = aux
            .get_claim_for_aux(make_property("P856", SnakDataType::Url), &[])
            .expect("Url aux must produce a claim");
        let expected = Snak::new_url("P856", "https://example.com/foo");
        assert_eq!(*claim.main_snak(), expected);
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
