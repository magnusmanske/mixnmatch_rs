//! Wikidata property identifier constants used across multiple modules.
//!
//! Keeping them here prevents typos and makes a property-ID change a one-line
//! edit instead of a grep-and-replace across the codebase.
//!
//! Source: <https://www.wikidata.org/wiki/Wikidata:List_of_properties>

/// P18 — image
pub const P_IMAGE: &str = "P18";
/// P31 — instance of
pub const P_INSTANCE_OF: &str = "P31";
/// P213 — ISNI
pub const P_ISNI: &str = "P213";
/// P248 — stated in
pub const P_STATED_IN: &str = "P248";
/// P569 — date of birth
pub const P_DATE_OF_BIRTH: &str = "P569";
/// P570 — date of death
pub const P_DATE_OF_DEATH: &str = "P570";
/// P625 — coordinate location
pub const P_COORDINATES: &str = "P625";
/// P813 — retrieved
pub const P_RETRIEVED: &str = "P813";
/// P854 — reference URL
pub const P_REFERENCE_URL: &str = "P854";
/// P1630 — formatter URL (template that turns an external-id value into a clickable URL)
pub const P_FORMATTER_URL: &str = "P1630";
/// P9073 — applicable "stated in" value (per-property hint pointing at the
/// reference item that should be used as the P248 source when citing
/// statements that use this property)
pub const P_APPLICABLE_STATED_IN: &str = "P9073";
