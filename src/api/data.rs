//! Data & analysis read endpoints (props, common names, person batches,
//! the "creation candidates" wrapper that delegates to micro_api).

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use axum::response::Response;
use std::sync::{Arc, OnceLock};

fn re_q_only() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^Q\d+$").expect("valid regex"))
}

pub async fn query_get_wd_props(app: &AppState) -> Result<Response, ApiError> {
    let props = app.storage().api_get_wd_props().await?;
    Ok(json_resp(serde_json::json!(props)))
}

pub async fn query_top_missing(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalogs: String = common::get_param(params, "catalogs", "")
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == ',')
        .collect();
    if catalogs.is_empty() {
        return Err(ApiError::BadRequest("No catalogs given".into()));
    }
    let data = app.storage().api_get_top_missing(&catalogs).await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_get_common_names(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let limit = common::get_param_int(params, "limit", 50) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let min = common::get_param_int(params, "min", 3) as usize;
    let max = common::get_param_int(params, "max", 15) as usize + 1;
    let type_q = common::get_param(params, "type", "");
    let type_q = if re_q_only().is_match(&type_q) {
        type_q
    } else {
        String::new()
    };
    let other_cats_desc = common::get_param_int(params, "other_cats_desc", 0) != 0;
    let data = app
        .storage()
        .api_get_common_names(
            cid,
            &type_q,
            crate::storage::CommonNamesQuery { other_cats_desc, min, max, limit, offset },
        )
        .await?;
    Ok(ok(serde_json::json!({"entries": data})))
}

pub async fn query_same_names(app: &AppState) -> Result<Response, ApiError> {
    let (name, entries) = app.storage().api_get_same_names().await?;
    let data = common::entries_to_json_data(&entries, app).await?;
    let mut out = serde_json::json!({"status": "OK", "data": data});
    out["data"]["name"] = serde_json::json!(name);
    Ok(json_resp(out))
}

pub async fn query_random_person_batch(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let gender = common::get_param(params, "gender", "");
    let has_desc = common::get_param_int(params, "has_desc", 0) != 0;
    let data = app
        .storage()
        .api_get_random_person_batch(&gender, has_desc)
        .await?;
    Ok(ok(serde_json::json!(data)))
}

pub async fn query_get_property_cache(app: &AppState) -> Result<Response, ApiError> {
    let (prop2item, item_label) = app.storage().api_get_property_cache().await?;
    Ok(ok(
        serde_json::json!({"prop2item": prop2item, "item_label": item_label}),
    ))
}

pub async fn query_mnm_unmatched_relations(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let property = common::get_param_int(params, "property", 0) as usize;
    let offset = common::get_param_int(params, "offset", 0) as usize;
    let limit = 25;
    let (id_cnts, entries) = app
        .storage()
        .api_get_mnm_unmatched_relations(property, offset, limit)
        .await?;
    let mut data = common::entries_with_extended_data(&entries, app).await?;
    let entry2cnt: serde_json::Map<String, serde_json::Value> = id_cnts
        .iter()
        .map(|(id, cnt)| (id.to_string(), serde_json::json!(cnt)))
        .collect();
    let entry_order: Vec<usize> = id_cnts.iter().map(|(id, _)| *id).collect();
    data["entry2cnt"] = serde_json::Value::Object(entry2cnt);
    data["entry_order"] = serde_json::json!(entry_order);
    Ok(ok(data))
}

pub async fn query_creation_candidates(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    Ok(ok(crate::api::creation_candidates::run(app, params).await?))
}

/// `prep_new_item`: fetch a list of MnM entries, build a single Wikibase
/// item JSON suitable for `action=wbeditentity&new=item`, and hand it back
/// to the frontend. The frontend then signs that body via Widar so the
/// edit is attributed to the user — we only build the payload here.
///
/// Mirrors the PHP `query_prep_new_item` shape: returns the entity JSON
/// under the standard `data` envelope key. The caller does the
/// `wbeditentity` POST itself.
pub async fn query_prep_new_item(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let entry_ids = parse_entry_ids(common::get_param(params, "entry_ids", ""));
    if entry_ids.is_empty() {
        return Err(ApiError::BadRequest("missing or empty 'entry_ids' parameter".into()));
    }
    let mut ic = crate::item_creator::ItemCreator::new(Arc::new(app.clone()));
    ic.add_entries_by_id(&entry_ids)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to load entries: {e}")))?;
    let item = ic
        .generate_item()
        .await
        .map_err(|e| ApiError::Internal(format!("failed to build item: {e}")))?;
    use wikimisc::wikibase::EntityTrait;
    Ok(ok(item.to_json()))
}

/// `prep_match_claim`: build the `wbeditentity` data payload for confirming
/// an automatch. The frontend used to dispatch the match via Widar's
/// `set_string` action, which sets the catalog property without a
/// reference, leaving every confirmed match unsourced on Wikidata. This
/// endpoint returns the same catalog-property claim that the new-item
/// path produces, with the canonical reference set (P248 stated_in,
/// P{wd_prop}/P_REFERENCE_URL, P_RETRIEVED) attached. The frontend
/// wraps the returned `data` in a `wbeditentity` action targeting the
/// existing item. Codeberg #49.
///
/// Optional `q` parameter: when supplied, the server fetches that item
/// and returns an empty `claims` list if the catalog property+value is
/// already present — preventing duplicate external-id statements when
/// multiple entries from the same catalog get matched to the same Q
/// (e.g. via `assignQToChecked` in the frontend).
pub async fn query_prep_match_claim(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    use wikimisc::wikibase::EntityTrait;
    let entry_id = common::get_param_int(params, "entry", 0);
    if entry_id <= 0 {
        return Err(ApiError::BadRequest("missing or invalid 'entry' parameter".into()));
    }
    let target_q = parse_q_param(&common::get_param(params, "q", ""));
    let item = build_match_claim_item(app, entry_id as usize, target_q.as_deref())
        .await
        .map_err(|e| ApiError::Internal(format!("failed to build claim: {e}")))?;
    Ok(ok(item.to_json()))
}

/// Parse `Q123` / `123` / empty into `Some("Q123")` or `None`. Anything
/// non-numeric or zero yields `None` so the existence check is skipped
/// (preserving pre-`q`-aware behaviour for legacy callers).
fn parse_q_param(s: &str) -> Option<String> {
    let trimmed = s.trim().trim_start_matches(['Q', 'q']);
    let n: u64 = trimmed.parse().ok()?;
    if n == 0 {
        return None;
    }
    Some(format!("Q{n}"))
}

/// Build an `ItemEntity` containing only the catalog's primary
/// external-id claim (P{wd_prop}=ext_id) with the catalog's reference
/// set attached, or an empty item when the catalog has no `wd_prop`
/// or has a `wd_qual` (qualifier-based catalog — primary statement is
/// constructed differently and not handled by the confirm-match path).
///
/// If `target_q` is `Some`, the function fetches that item and returns
/// an empty `ItemEntity` when the claim is already present, so the
/// frontend's follow-up `wbeditentity` becomes a no-op rather than
/// stamping a duplicate statement on the existing item.
async fn build_match_claim_item(
    app: &AppState,
    entry_id: usize,
    target_q: Option<&str>,
) -> anyhow::Result<wikimisc::wikibase::ItemEntity> {
    use wikimisc::wikibase::{ItemEntity, Snak, Statement};
    let entry = crate::entry::Entry::from_id(entry_id, app).await?;
    let catalog = crate::catalog::Catalog::from_id(entry.catalog, app).await?;
    let mut item = ItemEntity::new_empty();
    let (Some(prop), None) = (catalog.wd_prop(), catalog.wd_qual()) else {
        return Ok(item);
    };
    let prop_str = format!("P{prop}");
    // Normalize value the same way `add_own_id_to_item` does (e.g. ISNI
    // P213 strips spaces). This must match so the existence check below
    // recognises a claim that was written by the new-item path.
    let value = crate::auxiliary_data::AuxiliaryRow::fix_external_id(&prop_str, &entry.ext_id);
    let snak = Snak::new_external_id(&prop_str, &value);

    if let Some(q) = target_q {
        // Skip the edit if the target item already has a claim with this
        // property and value. Uses `wbgetclaims` (lightweight,
        // unauthenticated, single property) so we don't pay for a full
        // entity fetch on every match. This is the server-side guard
        // against the Q139681605-style bug: when prep_new_item creates an
        // item with N catalog-property claims and the frontend then
        // fires prep_match_claim per entry, the follow-up POSTs would
        // otherwise stamp duplicate copies of every one of those claims.
        if crate::claim_dedup::external_id_claim_exists(q, &prop_str, &value).await {
            return Ok(item);
        }
    }

    let references = catalog.references(app, &entry).await;
    let claim = Statement::new_normal(snak, vec![], references);
    // Route through the dedup helper so the reference block is
    // normalised — strips the self-referential P-X = value snak that
    // `Catalog::references` includes, which is what made the duplicates
    // on Q139680563 visually distinguishable from the original.
    crate::claim_dedup::add_claim_or_references(&mut item, claim);
    Ok(item)
}

/// Parse the `entry_ids=1,2,3` query parameter, dropping anything that
/// isn't a positive integer. Pure so it's covered by unit tests.
fn parse_entry_ids(s: String) -> Vec<usize> {
    s.split(',')
        .filter_map(|p| p.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use wikimisc::wikibase::EntityTrait;

    #[test]
    fn parse_entry_ids_csv() {
        assert_eq!(parse_entry_ids("1,2,3".into()), vec![1, 2, 3]);
        assert_eq!(parse_entry_ids("42".into()), vec![42]);
    }

    #[test]
    fn parse_entry_ids_filters_non_numeric_and_zero() {
        assert_eq!(parse_entry_ids("1,foo,2,,0,3".into()), vec![1, 2, 3]);
    }

    #[test]
    fn parse_entry_ids_empty_input() {
        assert!(parse_entry_ids("".into()).is_empty());
        assert!(parse_entry_ids(",,,".into()).is_empty());
    }

    #[test]
    fn parse_entry_ids_trims_whitespace() {
        assert_eq!(parse_entry_ids(" 1 , 2 ".into()), vec![1, 2]);
    }

    /// `build_match_claim_item` must produce an item with a single claim on
    /// the catalog's `wd_prop`, and that claim must carry a non-empty
    /// reference list — otherwise confirming a match writes an unsourced
    /// statement to Wikidata. Codeberg #49.
    #[tokio::test]
    async fn build_match_claim_item_includes_references() {
        let app = test_support::test_app().await;
        let (_catalog_id, entry_id) =
            test_support::seed_entry_with_catalog_wd_prop(214, 28054658).await.unwrap();
        let item = build_match_claim_item(&app, entry_id, None).await.unwrap();
        let claims = item.claims();
        assert_eq!(claims.len(), 1, "exactly one claim for the catalog property");
        assert_eq!(claims[0].property(), "P214", "claim is on wd_prop");
        let refs = claims[0].references();
        assert!(!refs.is_empty(), "claim must carry at least one reference");
        // Reference must include P248 (stated_in) pointing at the catalog source item.
        let stated_in_present = refs.iter().any(|r| {
            r.snaks().iter().any(|s| s.property() == "P248")
        });
        assert!(stated_in_present, "reference must include P248 stated_in");
    }

    /// The reference block coming out of `Catalog::references` includes
    /// the catalog's wd_prop = ext_id snak (which is also the main snak
    /// of the claim being built). That is a self-reference and adds no
    /// provenance — the dedup pass strips it before the claim leaves the
    /// builder. Without this, the claim that the frontend POSTs into an
    /// existing item carries a redundant snak that visually matches the
    /// shape of the dupes seen on Q139680563.
    #[tokio::test]
    async fn build_match_claim_item_strips_self_referential_ref_snak() {
        let app = test_support::test_app().await;
        let (_catalog_id, entry_id) =
            test_support::seed_entry_with_catalog_wd_prop(214, 28054658).await.unwrap();
        let item = build_match_claim_item(&app, entry_id, None).await.unwrap();
        let claim = &item.claims()[0];
        let main_prop = claim.main_snak().property().to_string();
        for r in claim.references() {
            assert!(
                !r.snaks().iter().any(|s| s.property() == main_prop),
                "reference must not include the main property as a self-ref snak"
            );
        }
    }

    /// When the catalog has `wd_qual` set, the existing new-item path
    /// skips the catalog-property claim entirely (qualifier-based catalogs
    /// don't have a primary external-id statement). The match-claim helper
    /// must mirror that and return an empty item.
    #[tokio::test]
    async fn build_match_claim_item_skips_when_wd_qual_set() {
        let app = test_support::test_app().await;
        let catalog_id = test_support::seed_catalog_with_wd_qual(195, 217).await.unwrap();
        let entry_id = test_support::seed_entry_in_catalog(catalog_id, "qual_entry")
            .await
            .unwrap();
        let item = build_match_claim_item(&app, entry_id, None).await.unwrap();
        assert!(item.claims().is_empty(), "wd_qual catalogs produce no match claim");
    }

    #[test]
    fn parse_q_param_handles_common_shapes() {
        assert_eq!(parse_q_param("Q123"), Some("Q123".to_string()));
        assert_eq!(parse_q_param("q123"), Some("Q123".to_string()));
        assert_eq!(parse_q_param("123"), Some("Q123".to_string()));
        assert_eq!(parse_q_param(" Q42 "), Some("Q42".to_string()));
    }

    #[test]
    fn parse_q_param_rejects_zero_and_garbage() {
        assert_eq!(parse_q_param(""), None);
        assert_eq!(parse_q_param("Q0"), None);
        assert_eq!(parse_q_param("0"), None);
        assert_eq!(parse_q_param("abc"), None);
        assert_eq!(parse_q_param("Q-1"), None);
    }

}
