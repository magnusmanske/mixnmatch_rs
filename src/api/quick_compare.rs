//! `quick_compare`: pull a small batch of catalog entries that have a
//! candidate Q, look those Qs up on Wikidata, attach image/coordinate
//! comparisons, and gate by an optional max-distance.

use crate::api::common::{ApiError, Params, ok};
use crate::app_state::AppState;
use crate::util::wikidata_props as wp;
use axum::response::Response;
use serde_json::{Value, json};
use std::sync::OnceLock;
use wikimisc::wikibase::EntityTrait;

const MAX_RESULTS: usize = 10;
const RETRY_COUNT: u8 = 3;

/// Axum-shape entry point for `?query=quick_compare&catalog=…`.
pub async fn query_quick_compare(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    Ok(ok(run(app, params).await?))
}

fn re_meters() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^(\d+)m$").expect("valid regex"))
}

fn re_kilometers() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^(\d+)km$").expect("valid regex"))
}

/// Great-circle distance in meters between two (lat, lon) points.
pub fn haversine_distance_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0; // Earth radius in meters
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

/// Parse a `"500m"` / `"5km"` distance string into meters.
pub fn parse_location_distance(s: &str) -> Option<f64> {
    if let Some(caps) = re_meters().captures(s) {
        return caps[1].parse::<f64>().ok();
    }
    if let Some(caps) = re_kilometers().captures(s) {
        return caps[1].parse::<f64>().ok().map(|v| v * 1000.0);
    }
    None
}

/// Parse all `quick_compare` params and run the comparison.
#[allow(clippy::cognitive_complexity)]
pub async fn run(app: &AppState, params: &Params) -> Result<Value, ApiError> {
    let catalog_id = parse_catalog(params)?;
    let entry_id = opt_usize(params, "entry_id");
    let require_image = opt_str(params, "require_image") == Some("1");
    let require_coordinates = opt_str(params, "require_coordinates") == Some("1");

    // Resolve max-distance: catalog default (kv pair) overridden by request
    // param if provided.
    let mut max_distance_m: Option<f64> = None;
    let catalog_kvs = app
        .storage()
        .get_catalog_key_value_pairs(catalog_id)
        .await
        .unwrap_or_default();
    if let Some(ld) = catalog_kvs.get("location_distance") {
        max_distance_m = parse_location_distance(ld);
    }
    if let Some(d) = opt_str(params, "max_distance_m").and_then(|s| s.parse::<f64>().ok()) {
        max_distance_m = Some(d);
    }

    let mut result_entries: Vec<Value> = vec![];

    for retry in 0..RETRY_COUNT {
        // Last retry uses a deterministic 0.0 threshold so we can pick up the
        // earliest matching rows even if random chunks all came up empty.
        let random_threshold = if retry < RETRY_COUNT - 1 {
            rand::random::<f64>()
        } else {
            0.0
        };

        let rows = app
            .storage()
            .qc_get_entries(
                catalog_id,
                entry_id,
                require_image,
                require_coordinates,
                random_threshold,
                MAX_RESULTS,
            )
            .await
            .map_err(|e| ApiError(format!("query failed: {e}")))?;

        if rows.is_empty() {
            continue;
        }

        // Pull all candidate Q items in a single Wikidata batch.
        let q_values: Vec<String> = rows
            .iter()
            .filter_map(|r| r["q"].as_i64().filter(|&q| q > 0).map(|q| format!("Q{q}")))
            .collect();

        let mw_api = app
            .wikidata()
            .get_mw_api()
            .await
            .map_err(|e| ApiError(format!("Wikidata API error: {e}")))?;
        let ec = wikimisc::wikibase::entity_container::EntityContainer::new();
        let _ = ec.load_entities(&mw_api, &q_values).await;

        for row in &rows {
            let q_num = match row["q"].as_i64() {
                Some(q) if q > 0 => q,
                _ => continue,
            };
            let q_str = format!("Q{q_num}");
            let item = match ec.get_entity(q_str.clone()) {
                Some(i) => i,
                None => continue,
            };

            // Wikidata-side gates.
            if require_image && item.claims_with_property(wp::P_IMAGE.to_string()).is_empty() {
                continue;
            }
            if require_coordinates
                && item
                    .claims_with_property(wp::P_COORDINATES.to_string())
                    .is_empty()
            {
                continue;
            }

            let lang = row["language"].as_str().unwrap_or("en");
            let mut entry_json = row.clone();
            let mut item_json = json!({
                "q": q_str,
                "label": item.label_in_locale(lang).unwrap_or(&q_str),
                "description": item.description_in_locale(lang).unwrap_or(""),
            });

            // Coordinates + distance gate.
            if let Some((lat_item, lon_item)) = extract_p625(&item) {
                item_json["coordinates"] = json!({"lat": lat_item, "lon": lon_item});
                if let (Some(lat_e), Some(lon_e)) = (row["lat"].as_f64(), row["lon"].as_f64()) {
                    let dist = haversine_distance_m(lat_item, lon_item, lat_e, lon_e);
                    if max_distance_m.is_some_and(|max| dist > max) {
                        continue;
                    }
                    entry_json["distance_m"] = json!(dist);
                }
            }

            // Image from Wikidata (P18).
            if let Some(img) = extract_p18(&item) {
                item_json["image"] = json!(img);
            }

            // Entry image (require gate).
            if let Some(img) = row.get("image_url").and_then(|v| v.as_str()) {
                if !img.is_empty() {
                    entry_json["ext_img"] = json!(img);
                } else if require_image {
                    continue;
                }
            }

            entry_json["item"] = item_json;
            result_entries.push(entry_json);
        }

        if !result_entries.is_empty() {
            break;
        }
    }

    Ok(json!({
        "entries": result_entries,
        "max_distance_m": max_distance_m,
    }))
}

fn parse_catalog(params: &Params) -> Result<usize, ApiError> {
    let raw = params
        .get("catalog")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError("missing required parameter: catalog".into()))?;
    raw.parse::<usize>()
        .map_err(|_| ApiError("parameter 'catalog' must be a positive integer".into()))
}

fn opt_str<'a>(params: &'a Params, key: &str) -> Option<&'a str> {
    params.get(key).filter(|v| !v.is_empty()).map(String::as_str)
}

fn opt_usize(params: &Params, key: &str) -> Option<usize> {
    opt_str(params, key).and_then(|v| v.parse().ok())
}

/// Extract (lat, lon) from a Wikidata item's first P625 claim, if present.
fn extract_p625(item: &wikimisc::wikibase::Entity) -> Option<(f64, f64)> {
    let claims = item.claims_with_property(wp::P_COORDINATES.to_string());
    let claim = claims.first()?;
    let dv = claim.main_snak().data_value().as_ref()?;
    let val_json = serde_json::to_value(dv.value()).ok()?;
    Some((val_json["latitude"].as_f64()?, val_json["longitude"].as_f64()?))
}

/// Extract the image filename (P18) string value, if any.
fn extract_p18(item: &wikimisc::wikibase::Entity) -> Option<String> {
    let claims = item.claims_with_property(wp::P_IMAGE.to_string());
    let claim = claims.first()?;
    let dv = claim.main_snak().data_value().as_ref()?;
    if let wikimisc::wikibase::Value::StringValue(s) = dv.value() {
        Some(s.clone())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_same_point_is_zero() {
        let d = haversine_distance_m(52.5, 13.4, 52.5, 13.4);
        assert!(d < 0.01);
    }

    #[test]
    fn haversine_berlin_to_paris() {
        let d = haversine_distance_m(52.52, 13.405, 48.8566, 2.3522);
        // ~878 km
        assert!((d - 878_000.0).abs() < 10_000.0);
    }

    #[test]
    fn parses_meters() {
        assert_eq!(parse_location_distance("500m"), Some(500.0));
    }

    #[test]
    fn parses_kilometers() {
        assert_eq!(parse_location_distance("5km"), Some(5000.0));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(parse_location_distance("five"), None);
        assert_eq!(parse_location_distance(""), None);
    }
}
