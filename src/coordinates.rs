use crate::{DbId, ItemId};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref RE_POINT: Regex =
        Regex::new(r"^\s*POINT\s*\(\s*(\S+?)[, ]\s*(\S+?)\s*\)\s*$").expect("Regexp construction");
    static ref RE_LAT_LON: Regex =
        Regex::new(r"^\@?([0-9.\-]+)[,/]([0-9.\-]+)$").expect("Regexp construction");
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CoordinateLocation {
    lat: f64,
    lon: f64,
    precision: Option<f64>,
}

impl CoordinateLocation {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self {
            lat,
            lon,
            precision: None,
        }
    }

    pub fn new_with_precision(lat: f64, lon: f64, precision: Option<f64>) -> Self {
        Self {
            lat,
            lon,
            precision,
        }
    }

    /// Parse a coordinate from common string formats:
    /// - `"lat/lon"` or `"lat,lon"` (with optional leading `@`)
    /// - `"POINT(lat lon)"` or `"POINT(lat,lon)"`
    pub fn parse(s: &str) -> Option<Self> {
        if let Some(caps) = RE_POINT.captures(s) {
            let lat = caps.get(1)?.as_str().parse::<f64>().ok()?;
            let lon = caps.get(2)?.as_str().parse::<f64>().ok()?;
            return Some(Self::new(lat, lon));
        }
        if let Some(caps) = RE_LAT_LON.captures(s) {
            let lat = caps.get(1)?.as_str().parse::<f64>().ok()?;
            let lon = caps.get(2)?.as_str().parse::<f64>().ok()?;
            return Some(Self::new(lat, lon));
        }
        None
    }

    pub fn lat(&self) -> f64 {
        self.lat
    }

    pub fn lon(&self) -> f64 {
        self.lon
    }

    pub fn precision(&self) -> Option<f64> {
        self.precision
    }
}

#[derive(Debug, Clone)]
pub struct LocationRow {
    pub lat: f64,
    pub lon: f64,
    pub entry_id: DbId,
    pub catalog_id: DbId,
    pub ext_name: String,
    pub entry_type: String,
    pub q: Option<ItemId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_coordinate_location_equality() {
        let a = CoordinateLocation::new(51.5, -0.1);
        let b = CoordinateLocation::new(51.5, -0.1);
        let c = CoordinateLocation::new(48.8, 2.3);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_coordinate_location_clone() {
        let original = CoordinateLocation::new(12.34, 56.78);
        let cloned = original;
        assert_eq!(original, cloned);
        assert!((cloned.lat() - 12.34).abs() < f64::EPSILON);
        assert!((cloned.lon() - 56.78).abs() < f64::EPSILON);
    }

    #[test]
    fn test_coordinate_location_negative_coords() {
        let cl = CoordinateLocation::new(-33.8688, 151.2093);
        assert!((cl.lat() - (-33.8688)).abs() < f64::EPSILON);
        assert!((cl.lon() - 151.2093).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_slash_separated() {
        let cl = CoordinateLocation::parse("1.5/-2.5").unwrap();
        assert!((cl.lat() - 1.5).abs() < f64::EPSILON);
        assert!((cl.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_comma_separated() {
        let cl = CoordinateLocation::parse("51.5,-0.1").unwrap();
        assert!((cl.lat() - 51.5).abs() < f64::EPSILON);
        assert!((cl.lon() - (-0.1)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_at_prefix() {
        let cl = CoordinateLocation::parse("@51.5,0.1").unwrap();
        assert!((cl.lat() - 51.5).abs() < f64::EPSILON);
        assert!((cl.lon() - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_point_format() {
        let cl = CoordinateLocation::parse("POINT(1.5 -2.5)").unwrap();
        assert!((cl.lat() - 1.5).abs() < f64::EPSILON);
        assert!((cl.lon() - (-2.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_invalid() {
        assert!(CoordinateLocation::parse("").is_none());
        assert!(CoordinateLocation::parse("not a coord").is_none());
    }
}
