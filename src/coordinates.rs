use crate::DbId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CoordinateLocation {
    pub lat: f64,
    pub lon: f64,
    pub precision: Option<f64>,
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
