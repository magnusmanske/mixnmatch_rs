use crate::{app_state::*, mixnmatch::MixNMatch};
use chrono::Utc;
use std::process::Command;

pub struct PhpWrapper {}

impl PhpWrapper {
    fn new_command(script: &str) -> Command {
        let mut ret = if MixNMatch::is_on_toolforge() {
            let mut ret = Command::new("php8.1");
            let _ = ret.args(["-c", "/data/project/mix-n-match/mixnmatch_rs/php.ini"]);
            ret
        } else {
            Command::new("php")
        };
        let _ = ret.arg(format!("{}/scripts/{script}", MixNMatch::tool_root_dir()));
        ret
    }

    pub fn update_person_dates(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: update_person_dates {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command("person_dates/update_person_dates.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: update_person_dates {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }

    pub fn generate_aux_from_description(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: generate_aux_from_description {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command("generate_aux_from_description.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: generate_aux_from_description {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }

    pub fn bespoke_scraper(catalog_id: usize) -> Result<(), GenericError> {
        println!("PHP: bespoke_scraper {catalog_id} START [{}]", Utc::now());
        let output = Self::new_command("bespoke_scraper.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: bespoke_scraper {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }

    pub fn update_descriptions_from_url(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: update_descriptions_from_url {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command("update_descriptions_from_url.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: update_descriptions_from_url {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }

    pub fn import_aux_from_url(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: import_aux_from_url {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command("import_aux_from_url.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: import_aux_from_url {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }

    pub fn match_by_coordinates(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: match_by_coordinates {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command("match_by_coordinates.php")
            .arg(format!("{catalog_id}"))
            .output()?;
        println!(
            "PHP: match_by_coordinates {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }
}
