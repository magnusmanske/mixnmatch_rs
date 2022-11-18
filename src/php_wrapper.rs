use crate::app_state::*;
use std::process::Command;

pub struct PhpWrapper {
}

impl PhpWrapper {
    pub fn update_person_dates(catalog_id: usize) -> Result<(),GenericError> {
        let _ = Command::new("/data/project/mix-n-match/scripts/person_dates/update_person_dates.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }
}