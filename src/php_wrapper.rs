use crate::app_state::*;
use std::process::Command;

pub struct PhpWrapper {
}

impl PhpWrapper {
    pub fn update_person_dates(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/person_dates/update_person_dates.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn generate_aux_from_description(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/generate_aux_from_description.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn bespoke_scraper(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/bespoke_scraper.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn automatch(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/automatch.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn update_descriptions_from_url(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/update_descriptions_from_url.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn import_aux_from_url(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/import_aux_from_url.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

    pub fn match_by_coordinates(catalog_id: usize) -> Result<(),GenericError> {
        Command::new("/data/project/mix-n-match/scripts/match_by_coordinates.php")
        .arg(format!("{}",catalog_id))
        .output()?;
        Ok(())
    }

}