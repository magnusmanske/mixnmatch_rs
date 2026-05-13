//! Build a fresh Wikidata `ItemEntity` from an `Entry`'s data.
//!
//! The chain that turns a Mix'n'match entry into the new-item payload
//! `wbeditentity new=item` consumes when the operator clicks
//! "create item" on an unmatched row. It composes per-section
//! contributions — own ID, type, label + aliases, descriptions,
//! coordinates, person dates, auxiliary statements — driven entirely
//! by the supplied `Entry`, its `Catalog`, and the
//! `kv_catalog` toggles (`use_description_for_new`, `no_descriptions`,
//! `no_dates_import`).
//!
//! Previously lived inside `entry.rs`. Lifted here because the
//! algorithm is a Wikidata-write pipeline concern, not an `Entry`
//! model concern — `Entry` should stay a passive data struct (cf.
//! `audits/code_solid.md` #4). `EntryWriter::add_to_item` is now a
//! one-line forwarder to [`build_item_from_entry`].

use crate::app_state::AppContext;
use crate::auxiliary_data::AuxiliaryRow;
use crate::catalog::Catalog;
use crate::claim_dedup::add_claim_or_references;
use crate::entry::Entry;
use crate::person::Person;
use crate::person_date::PersonDate;
use crate::util::wikidata_props as wp;
use anyhow::Result;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::locale_string::LocaleString;
use wikimisc::wikibase::{EntityTrait, ItemEntity, Reference, Snak, Statement};

/// Languages whose Q5 labels are written as `mul` (covers every
/// Latin-script language at once). Mirrors the PHP behaviour for
/// human-item creation.
pub const WESTERN_LANGUAGES: &[&str] = &["en", "de", "fr", "es", "nl", "it", "pt"];

/// Default precision (1 arcsecond ≈ 31 m) used when we need to emit a
/// `P625` globe-coordinate claim but the source didn't record a precision.
/// `wbeditentity` rejects coordinates with a `null`/missing precision, so
/// we must always hand it a concrete number. 1/3600 is Wikidata's own
/// default when picking a point from the map UI, so it matches the fidelity
/// of hand-entered coordinates at city/landmark level.
const DEFAULT_COORDINATE_PRECISION: f64 = 1.0 / 3600.0;

/// Build a `P625` snak. `Snak::new_coordinate` from wikibase sets precision
/// to `None`, which `wbeditentity` rejects — so construct the snak
/// ourselves and fill in a concrete precision (the stored value if we have
/// one, otherwise the arcsecond default).
fn build_p625_snak(lat: f64, lon: f64, precision: Option<f64>) -> Snak {
    use wikimisc::wikibase::{Coordinate, DataValue, DataValueType, SnakDataType, SnakType};
    Snak::new(
        SnakDataType::GlobeCoordinate,
        wp::P_COORDINATES,
        SnakType::Value,
        Some(DataValue::new(
            DataValueType::GlobeCoordinate,
            wikimisc::wikibase::Value::Coordinate(Coordinate::new(
                None,
                "http://www.wikidata.org/entity/Q2".to_string(),
                lat,
                lon,
                Some(precision.unwrap_or(DEFAULT_COORDINATE_PRECISION)),
            )),
        )),
    )
}

/// Build the contents of a fresh Wikidata item from `entry`.
///
/// Catalog-level toggles read from `kv_catalog`:
/// - `use_description_for_new=0` — don't copy `ext_desc` into the
///   new item's descriptions
/// - `no_descriptions=1` — suppress all descriptions
/// - `no_dates_import=1` — suppress P569/P570
pub async fn build_item_from_entry(
    ctx: &dyn AppContext,
    entry: &Entry,
    item: &mut ItemEntity,
) -> Result<()> {
    let catalog = Catalog::from_id(entry.catalog, ctx).await?;
    let references = catalog.references(ctx, entry).await;
    let language = catalog.search_wp().to_string();
    let kv = catalog.get_key_value_pairs(ctx).await.unwrap_or_default();
    let use_desc = kv
        .get("use_description_for_new")
        .map(|v| v != "0")
        .unwrap_or(true);
    let no_descriptions = kv.get("no_descriptions").map(|v| v == "1").unwrap_or(false);
    let no_dates_import = kv.get("no_dates_import").map(|v| v == "1").unwrap_or(false);
    add_own_id_to_item(entry, &catalog, &references, item);
    add_type_to_item(entry, &references, item);
    add_name_and_aliases_to_item(ctx, entry, &language, item).await?;
    if !no_descriptions {
        add_descriptions_to_item(ctx, entry, language, use_desc, item).await?;
    }
    add_coordinates_to_item(ctx, entry, &references, item).await?;
    if !no_dates_import {
        add_person_dates_to_item(ctx, entry, &references, item).await?;
    }
    add_auxiliary_to_item(ctx, entry, references, item).await?;
    Ok(())
}

async fn add_auxiliary_to_item(
    ctx: &dyn AppContext,
    entry: &Entry,
    references: Vec<Reference>,
    item: &mut ItemEntity,
) -> Result<()> {
    let auxiliary = ctx.storage().entry_get_aux(entry.get_valid_id()?).await?;
    if auxiliary.is_empty() {
        return Ok(());
    }
    let api = ctx.wikidata().get_mw_api().await?;
    let ec = EntityContainer::new();
    let props2load: Vec<String> = auxiliary.iter().map(|a| a.prop_as_string()).collect();
    // Pre-load all property entities in one batch; per-entity load below is
    // a no-op for ones the batch already cached.
    let _ = ec.load_entities(&api, &props2load).await;
    for aux in auxiliary {
        if let Ok(prop) = ec.load_entity(&api, aux.prop_as_string()).await {
            if let Some(claim) = aux.get_claim_for_aux(prop, &references) {
                add_claim_or_references(item, claim);
            }
        }
    }
    Ok(())
}

async fn add_person_dates_to_item(
    ctx: &dyn AppContext,
    entry: &Entry,
    references: &[Reference],
    item: &mut ItemEntity,
) -> Result<()> {
    let (born_str, died_str) = ctx
        .storage()
        .entry_get_person_dates(entry.get_valid_id()?)
        .await?;
    let born = born_str.as_deref().and_then(PersonDate::from_db_string);
    let died = died_str.as_deref().and_then(PersonDate::from_db_string);
    if let Some(pd) = born {
        let snak = Snak::new_time(
            wp::P_DATE_OF_BIRTH,
            &pd.to_wikidata_time(),
            pd.wikidata_precision(),
        );
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        add_claim_or_references(item, claim);
    }
    if let Some(pd) = died {
        let snak = Snak::new_time(
            wp::P_DATE_OF_DEATH,
            &pd.to_wikidata_time(),
            pd.wikidata_precision(),
        );
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        add_claim_or_references(item, claim);
    }
    Ok(())
}

async fn add_coordinates_to_item(
    ctx: &dyn AppContext,
    entry: &Entry,
    references: &[Reference],
    item: &mut ItemEntity,
) -> Result<()> {
    if let Some(coord) = ctx
        .storage()
        .entry_get_coordinate_location(entry.get_valid_id()?)
        .await?
    {
        let snak = build_p625_snak(coord.lat(), coord.lon(), coord.precision());
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        add_claim_or_references(item, claim);
    }
    Ok(())
}

async fn add_descriptions_to_item(
    ctx: &dyn AppContext,
    entry: &Entry,
    language: String,
    use_ext_desc: bool,
    item: &mut ItemEntity,
) -> Result<()> {
    let mut descriptions = ctx
        .storage()
        .entry_get_language_descriptions(entry.get_valid_id()?)
        .await?;
    if use_ext_desc && !entry.ext_desc.is_empty() {
        descriptions.insert(language.to_owned(), entry.ext_desc.to_owned());
    }
    for (lang, desc) in descriptions {
        if item.description_in_locale(&lang).is_none() {
            let desc = LocaleString::new(&lang, &desc);
            item.descriptions_mut().push(desc);
        }
    }
    Ok(())
}

async fn add_name_and_aliases_to_item(
    ctx: &dyn AppContext,
    entry: &Entry,
    language: &str,
    item: &mut ItemEntity,
) -> Result<()> {
    let mut aliases = ctx
        .storage()
        .entry_get_aliases(entry.get_valid_id()?)
        .await?;
    let name = Person::sanitize_name(&entry.ext_name);
    let locale_string = LocaleString::new(language, &name);
    // Q5 + a Western-script language → write the label as `mul` so it
    // covers every Latin-script language at once. Mirrors PHP behaviour.
    let names = if entry.type_name == Some("Q5".into()) && WESTERN_LANGUAGES.contains(&language) {
        vec![LocaleString::new("mul", &name)]
    } else {
        vec![locale_string.to_owned()]
    };
    for ls in names {
        if item.label_in_locale(ls.language()).is_none() {
            item.labels_mut().push(ls);
        } else {
            aliases.push(ls);
        }
    }
    for alias in aliases {
        if !item.labels().contains(&alias) && !item.aliases().contains(&alias) {
            item.aliases_mut().push(alias);
        }
    }
    Ok(())
}

fn add_type_to_item(entry: &Entry, references: &[Reference], item: &mut ItemEntity) {
    if let Some(tn) = &entry.type_name {
        if !tn.is_empty() {
            let snak = Snak::new_item(wp::P_INSTANCE_OF, tn);
            let claim = Statement::new_normal(snak, vec![], references.to_owned());
            add_claim_or_references(item, claim);
        }
    }
}

fn add_own_id_to_item(
    entry: &Entry,
    catalog: &Catalog,
    references: &[Reference],
    item: &mut ItemEntity,
) {
    if let (Some(prop), None) = (catalog.wd_prop(), catalog.wd_qual()) {
        let prop_str = format!("P{prop}");
        // Normalize the value the same way get_claim_for_aux does, so that
        // claim_core_equivalent can merge own-ID and aux-sourced statements
        // for the same property+value (e.g. ISNI P213 without spaces).
        let value = AuxiliaryRow::fix_external_id(&prop_str, &entry.ext_id);
        let snak = Snak::new_external_id(&prop_str, &value);
        let claim = Statement::new_normal(snak, vec![], references.to_owned());
        add_claim_or_references(item, claim);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snak_precision(snak: &Snak) -> Option<f64> {
        let dv = snak.data_value().as_ref()?;
        match dv.value() {
            wikimisc::wikibase::Value::Coordinate(c) => *c.precision(),
            _ => None,
        }
    }

    #[test]
    fn build_p625_snak_applies_stored_precision() {
        let snak = build_p625_snak(51.5, -0.1, Some(0.001));
        assert_eq!(snak_precision(&snak), Some(0.001));
    }

    #[test]
    fn build_p625_snak_falls_back_to_arcsecond_default() {
        // Missing precision is what was triggering "Missing required field
        // 'precision'" in wbeditentity — make sure we always emit a number.
        let snak = build_p625_snak(51.5, -0.1, None);
        let prec = snak_precision(&snak).expect("precision must be set");
        assert!((prec - 1.0 / 3600.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_p625_snak_serializes_precision_as_number() {
        let snak = build_p625_snak(51.5, -0.1, None);
        let json = serde_json::to_value(&snak).unwrap();
        let precision = json
            .pointer("/datavalue/value/precision")
            .expect("precision key must be present in JSON");
        assert!(
            precision.is_number(),
            "precision must serialize as a number, got {precision:?}"
        );
    }
}
