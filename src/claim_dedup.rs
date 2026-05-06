//! Claim and reference deduplication helpers.
//!
//! Used during in-memory construction of new items (see `entry.rs`) and
//! also during post-creation cleanup when AC2WD's wbeditentity merge
//! has appended duplicate external-id statements that should be folded
//! into the originals (see `merge_duplicate_claims_on_wikidata`).

use anyhow::{Result, anyhow};
use mediawiki::api::Api;
use serde_json::{Value, json};
use std::collections::HashMap;
use wikimisc::wikibase::entity_container::EntityContainer;
use wikimisc::wikibase::{EntityTrait, ItemEntity, Reference, Snak, Statement};

/// Two snaks have the same identity for deduplication purposes: same
/// property, same snak_type, same data_value. `SnakDataType` (e.g.
/// ExternalId vs String) is intentionally ignored — both use
/// `DataValueType::StringType` internally, so the data_value comparison
/// already captures the actual value. This prevents spurious duplicates
/// when one code path emits ExternalId snaks and another emits String
/// snaks for the same property+value combination.
pub fn snaks_value_equivalent(a: &Snak, b: &Snak) -> bool {
    a.property() == b.property()
        && a.snak_type() == b.snak_type()
        && a.data_value() == b.data_value()
}

/// Two reference blocks are equivalent if they carry the same snaks in
/// any order. `Reference`'s derived `PartialEq` is order-sensitive, so
/// callers that merge references across entries need this instead.
/// Both inputs are expected to be dedup'd already (via `normalise_claim`).
pub fn reference_equivalent(a: &Reference, b: &Reference) -> bool {
    let sa = a.snaks();
    let sb = b.snaks();
    sa.len() == sb.len() && sa.iter().all(|s| sb.contains(s))
}

/// Two claims are equivalent enough to merge (i.e. same main snak and
/// same qualifier set, order-insensitive). Qualifier-bearing variants
/// are kept separate from bare claims so we don't lose qualifiers.
/// Ignores rank/type/id — those don't affect the claim's identity for
/// the new-item-creation use case.
pub fn claim_core_equivalent(a: &Statement, b: &Statement) -> bool {
    if !snaks_value_equivalent(a.main_snak(), b.main_snak()) {
        return false;
    }
    let aq = a.qualifiers();
    let bq = b.qualifiers();
    aq.len() == bq.len() && aq.iter().all(|s| bq.contains(s))
}

/// Clean up a claim before it enters the item: dedupe qualifiers and
/// references without changing the claim's meaning.
pub fn normalise_claim(claim: &mut Statement) {
    // Qualifiers: drop exact duplicates, preserving order.
    let mut qs: Vec<Snak> = Vec::with_capacity(claim.qualifiers().len());
    for q in claim.qualifiers() {
        if !qs.contains(q) {
            qs.push(q.clone());
        }
    }
    claim.set_qualifier_snaks(qs);

    // References: per block, drop snaks equal to the main snak (those
    // are circular and add no provenance value) and drop duplicate
    // snaks. Drop blocks that end up empty. Then dedupe whole blocks.
    let main = claim.main_snak().clone();
    let mut new_refs: Vec<Reference> = Vec::new();
    for r in claim.references() {
        let mut snaks: Vec<Snak> = Vec::with_capacity(r.snaks().len());
        for s in r.snaks() {
            if snaks_value_equivalent(s, &main) {
                continue;
            }
            if snaks.contains(s) {
                continue;
            }
            snaks.push(s.clone());
        }
        if snaks.is_empty() {
            continue;
        }
        let candidate = Reference::new(snaks);
        if !new_refs
            .iter()
            .any(|existing| reference_equivalent(existing, &candidate))
        {
            new_refs.push(candidate);
        }
    }
    claim.set_references(new_refs);
}

/// Add a claim to an item, normalising the incoming claim first and then
/// merging with any structurally-equivalent existing claim (same main
/// snak AND same multiset of qualifier snaks). Reference blocks from
/// equivalent claims are unioned; self-referential ref snaks (those that
/// echo the claim's main snak) are dropped during normalisation.
pub fn add_claim_or_references(item: &mut ItemEntity, mut claim: Statement) {
    normalise_claim(&mut claim);

    for existing in item.claims_mut() {
        if claim_core_equivalent(existing, &claim) {
            let mut refs = existing.references().to_vec();
            for r in claim.references() {
                if !refs
                    .iter()
                    .any(|existing_ref| reference_equivalent(existing_ref, r))
                {
                    refs.push(r.clone());
                }
            }
            existing.set_references(refs);
            return;
        }
    }

    item.add_claim(claim);
}

/// Result of analysing an item for in-place duplicate-claim merging.
///
/// `keepers` are full `Statement`s ready to round-trip into a
/// `wbeditentity` payload — their `id` is preserved from the fetched
/// entity, and their `references` have been replaced with the union of
/// every duplicate's references (filtered through `normalise_claim`).
///
/// `removed_ids` are statement GUIDs whose owning claims should be
/// deleted (their references have already been folded into the matching
/// keeper).
#[derive(Debug, Default)]
pub struct DedupPlan {
    pub keepers: Vec<Statement>,
    pub removed_ids: Vec<String>,
}

impl DedupPlan {
    pub fn is_empty(&self) -> bool {
        self.keepers.is_empty() && self.removed_ids.is_empty()
    }
}

/// Walk the item's claims and produce a plan to fold duplicates: any two
/// claims that share property + main value + qualifier set are collapsed
/// into the first occurrence, with their references merged (and self-
/// referential snaks dropped).
///
/// Only returns entries for buckets that actually changed — an item with
/// no duplicates yields an empty plan, which callers can short-circuit on.
pub fn plan_duplicate_merge(item: &dyn EntityTrait) -> DedupPlan {
    let claims = item.claims();
    // Group by structural identity. We index into `claims` instead of
    // cloning so we can correlate the kept claim with its original id.
    let mut buckets: Vec<Vec<usize>> = Vec::new();
    'outer: for (i, c) in claims.iter().enumerate() {
        for bucket in buckets.iter_mut() {
            if claim_core_equivalent(&claims[bucket[0]], c) {
                bucket.push(i);
                continue 'outer;
            }
        }
        buckets.push(vec![i]);
    }

    let mut plan = DedupPlan::default();
    for bucket in &buckets {
        if bucket.len() < 2 {
            continue;
        }
        let keeper_src = &claims[bucket[0]];
        let main = keeper_src.main_snak().clone();

        // Merge references from every member of the bucket; reuse
        // `normalise_claim`'s rules: filter self-refs, dedupe within
        // a block, dedupe block-as-whole.
        let mut merged_refs: Vec<Reference> = Vec::new();
        for &idx in bucket {
            for r in claims[idx].references() {
                let mut snaks: Vec<Snak> = Vec::new();
                for s in r.snaks() {
                    if snaks_value_equivalent(s, &main) {
                        continue;
                    }
                    if snaks.contains(s) {
                        continue;
                    }
                    snaks.push(s.clone());
                }
                if snaks.is_empty() {
                    continue;
                }
                let cand = Reference::new(snaks);
                if !merged_refs.iter().any(|e| reference_equivalent(e, &cand)) {
                    merged_refs.push(cand);
                }
            }
        }

        let mut keeper = keeper_src.clone();
        keeper.set_references(merged_refs);
        plan.keepers.push(keeper);

        for &idx in bucket.iter().skip(1) {
            if let Some(id) = claims[idx].id() {
                plan.removed_ids.push(id);
            }
        }
    }
    plan
}

/// Fetch `q` from Wikidata, plan a duplicate-claim merge, and submit a
/// single `wbeditentity` edit that replaces the keeper's references with
/// the merged set and removes the duplicates. No-op if the item has no
/// duplicates. Errors from the fetch/POST are returned to the caller —
/// item-creation flows treat this step as best-effort.
pub async fn merge_duplicate_claims_on_wikidata(
    api: &mut Api,
    q: &str,
    summary: &str,
) -> Result<()> {
    let ec = EntityContainer::new();
    ec.load_entities(api, &vec![q.to_string()])
        .await
        .map_err(|e| anyhow!("merge_duplicate_claims: load {q} failed: {e}"))?;
    let entity = ec
        .get_entity(q.to_string())
        .ok_or_else(|| anyhow!("merge_duplicate_claims: {q} not found"))?;

    let plan = plan_duplicate_merge(&entity);
    if plan.is_empty() {
        return Ok(());
    }

    let claims = build_wbeditentity_claims(&plan)?;
    let data = json!({ "claims": claims });
    let mut params: HashMap<String, String> = HashMap::new();
    params.insert("action".to_string(), "wbeditentity".to_string());
    params.insert("id".to_string(), q.to_string());
    params.insert("data".to_string(), data.to_string());
    params.insert("token".to_string(), api.get_edit_token().await?);
    params.insert("summary".to_string(), summary.to_string());
    api.post_query_api_json_mut(&params).await?;
    Ok(())
}

/// Render a plan as the array passed to `wbeditentity`'s `data.claims`.
/// Keepers go in as full statements (id + merged refs preserved); removed
/// ids go in as `{"id": …, "remove": ""}` stubs.
pub(crate) fn build_wbeditentity_claims(plan: &DedupPlan) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::with_capacity(plan.keepers.len() + plan.removed_ids.len());
    for keeper in &plan.keepers {
        out.push(serde_json::to_value(keeper)?);
    }
    for id in &plan.removed_ids {
        out.push(json!({ "id": id, "remove": "" }));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext_id(prop: &str, v: &str) -> Snak {
        Snak::new_external_id(prop, v)
    }

    fn item_snak(prop: &str, q: &str) -> Snak {
        Snak::new_item(prop, q)
    }

    fn time_snak(prop: &str, time: &str) -> Snak {
        Snak::new_time(prop, time, 11)
    }

    fn ref_block(snaks: Vec<Snak>) -> Reference {
        Reference::new(snaks)
    }

    /// Builds an `ItemEntity` whose claims mirror the structure of the
    /// real-world Q139680563 bug: two P-X statements with identical
    /// values, where one reference block is a strict superset of the
    /// other (it carries the self-referential P-X = value snak that AC2WD
    /// emits but mixnmatch's normaliser strips).
    fn item_with_q139680563_shape() -> ItemEntity {
        let mut item = ItemEntity::new_empty();
        // P4432 #1: short ref block.
        let mut s1 = Statement::new_normal(
            ext_id("P4432", "8b39df21"),
            vec![],
            vec![ref_block(vec![item_snak("P248", "Q41640909")])],
        );
        s1.set_id("Q139680563$claim-1");
        // P4432 #2: superset ref block (with the self-referencing snak).
        let mut s2 = Statement::new_normal(
            ext_id("P4432", "8b39df21"),
            vec![],
            vec![ref_block(vec![
                item_snak("P248", "Q41640909"),
                ext_id("P4432", "8b39df21"),
            ])],
        );
        s2.set_id("Q139680563$claim-2");
        // P3368 #1.
        let mut s3 = Statement::new_normal(
            ext_id("P3368", "779241"),
            vec![],
            vec![ref_block(vec![
                item_snak("P248", "Q25328680"),
                time_snak("P813", "+2022-02-10T00:00:00Z"),
            ])],
        );
        s3.set_id("Q139680563$claim-3");
        // P3368 #2: superset.
        let mut s4 = Statement::new_normal(
            ext_id("P3368", "779241"),
            vec![],
            vec![ref_block(vec![
                item_snak("P248", "Q25328680"),
                ext_id("P3368", "779241"),
                time_snak("P813", "+2022-02-10T00:00:00Z"),
            ])],
        );
        s4.set_id("Q139680563$claim-4");
        item.add_claim(s1);
        item.add_claim(s2);
        item.add_claim(s3);
        item.add_claim(s4);
        item
    }

    #[test]
    fn plan_drops_duplicate_external_id_claims() {
        let item = item_with_q139680563_shape();
        let plan = plan_duplicate_merge(&item);
        assert_eq!(plan.keepers.len(), 2, "two distinct keepers (P4432, P3368)");
        assert_eq!(plan.removed_ids.len(), 2, "two duplicates removed");
        assert!(
            plan.removed_ids
                .iter()
                .all(|id| id == "Q139680563$claim-2" || id == "Q139680563$claim-4")
        );
    }

    #[test]
    fn plan_strips_self_referential_snaks_in_merged_refs() {
        let item = item_with_q139680563_shape();
        let plan = plan_duplicate_merge(&item);
        for keeper in &plan.keepers {
            let main = keeper.main_snak().clone();
            for r in keeper.references() {
                for s in r.snaks() {
                    assert!(
                        !snaks_value_equivalent(s, &main),
                        "self-referential snak should be stripped from merged refs"
                    );
                }
            }
        }
    }

    #[test]
    fn plan_keeps_keeper_id_and_unions_useful_ref_snaks() {
        let item = item_with_q139680563_shape();
        let plan = plan_duplicate_merge(&item);
        let p3368 = plan
            .keepers
            .iter()
            .find(|k| k.property() == "P3368")
            .expect("P3368 keeper must be present");
        assert_eq!(p3368.id().as_deref(), Some("Q139680563$claim-3"));
        let snaks = &p3368.references()[0].snaks();
        // Should keep P248 and P813 from both refs, drop the self-ref.
        assert!(snaks.iter().any(|s| s.property() == "P248"));
        assert!(snaks.iter().any(|s| s.property() == "P813"));
        assert!(!snaks.iter().any(|s| s.property() == "P3368"));
    }

    #[test]
    fn plan_no_op_when_no_duplicates() {
        let mut item = ItemEntity::new_empty();
        let mut s = Statement::new_normal(ext_id("P31", "Q5"), vec![], vec![]);
        s.set_id("Q1$claim-1");
        item.add_claim(s);
        let plan = plan_duplicate_merge(&item);
        assert!(plan.is_empty());
    }

    #[test]
    fn plan_keeps_distinct_values_separate() {
        let mut item = ItemEntity::new_empty();
        let mut a = Statement::new_normal(ext_id("P213", "0000000012345678"), vec![], vec![]);
        a.set_id("Q1$claim-a");
        let mut b = Statement::new_normal(ext_id("P213", "9999999987654321"), vec![], vec![]);
        b.set_id("Q1$claim-b");
        item.add_claim(a);
        item.add_claim(b);
        let plan = plan_duplicate_merge(&item);
        assert!(plan.is_empty());
    }

    #[test]
    fn plan_does_not_merge_claims_with_different_qualifiers() {
        let mut item = ItemEntity::new_empty();
        // Same main snak, different qualifiers => meaningful difference,
        // must stay separate.
        let mut a = Statement::new_normal(
            item_snak("P39", "Q11696"),
            vec![item_snak("P580", "Q1")],
            vec![],
        );
        a.set_id("Q1$claim-a");
        let mut b = Statement::new_normal(
            item_snak("P39", "Q11696"),
            vec![item_snak("P580", "Q2")],
            vec![],
        );
        b.set_id("Q1$claim-b");
        item.add_claim(a);
        item.add_claim(b);
        let plan = plan_duplicate_merge(&item);
        assert!(plan.is_empty());
    }

    #[test]
    fn build_wbeditentity_claims_emits_keeper_and_remove_stub() {
        let item = item_with_q139680563_shape();
        let plan = plan_duplicate_merge(&item);
        let claims = build_wbeditentity_claims(&plan).unwrap();
        // 2 keepers + 2 remove stubs
        assert_eq!(claims.len(), 4);
        // The remove stubs are recognisable.
        let removes: Vec<&Value> = claims.iter().filter(|c| c.get("remove").is_some()).collect();
        assert_eq!(removes.len(), 2);
        for r in removes {
            assert_eq!(r.get("remove").and_then(|v| v.as_str()), Some(""));
            assert!(r.get("id").and_then(|v| v.as_str()).is_some());
        }
        // Keepers carry an id and references.
        let keepers: Vec<&Value> = claims.iter().filter(|c| c.get("remove").is_none()).collect();
        assert_eq!(keepers.len(), 2);
        for k in keepers {
            assert!(k.get("id").is_some());
            assert!(k.get("mainsnak").is_some());
            assert!(k.get("references").is_some());
        }
    }

    #[test]
    fn build_wbeditentity_claims_for_empty_plan_is_empty() {
        let plan = DedupPlan::default();
        let claims = build_wbeditentity_claims(&plan).unwrap();
        assert!(claims.is_empty());
    }

    /// Smoke test for the main use case: `add_claim_or_references` should
    /// fold a self-ref-bearing duplicate into the original at construction
    /// time, so items built locally never ship with duplicates in the first
    /// place. This is the path mixnmatch uses; the post-creation cleanup
    /// only exists because AC2WD bypasses this code.
    #[test]
    fn add_claim_or_references_dedups_self_ref_bearing_duplicate() {
        let mut item = ItemEntity::new_empty();
        let claim_a = Statement::new_normal(
            ext_id("P4432", "8b39df21"),
            vec![],
            vec![ref_block(vec![item_snak("P248", "Q41640909")])],
        );
        let claim_b = Statement::new_normal(
            ext_id("P4432", "8b39df21"),
            vec![],
            vec![ref_block(vec![
                item_snak("P248", "Q41640909"),
                ext_id("P4432", "8b39df21"),
            ])],
        );
        add_claim_or_references(&mut item, claim_a);
        add_claim_or_references(&mut item, claim_b);
        assert_eq!(item.claims().len(), 1);
        let refs = item.claims()[0].references();
        assert_eq!(refs.len(), 1);
        let snaks = refs[0].snaks();
        assert_eq!(snaks.len(), 1, "self-ref P4432 should be stripped");
        assert_eq!(snaks[0].property(), "P248");
    }

    #[test]
    fn merge_duplicate_claims_on_wikidata_is_no_op_when_already_clean() {
        // Just verifies the plan path: an item with no dupes produces no
        // updates and so we never even need to call out to the API. The
        // network call is exercised only when `plan` is non-empty.
        let mut item = ItemEntity::new_empty();
        let mut s = Statement::new_normal(ext_id("P31", "Q5"), vec![], vec![]);
        s.set_id("Q1$x");
        item.add_claim(s);
        let plan = plan_duplicate_merge(&item);
        assert!(plan.is_empty());
        let claims = build_wbeditentity_claims(&plan).unwrap();
        assert!(claims.is_empty());
    }
}
