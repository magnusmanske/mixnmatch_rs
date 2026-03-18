use crate::DbId;
use serde::{Deserialize, Serialize};

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
    pub q: Option<usize>,
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
}
