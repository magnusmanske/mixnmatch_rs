use crate::app_state::*;
use chrono::Utc;
use std::process::Command;

pub struct PhpWrapper {}

impl PhpWrapper {
    fn new_command(command: &str) -> Command {
        let docker_php = "/layers/fagiani_apt/apt/usr/bin/php8.1";
        if std::path::Path::new(docker_php).exists() {
            Command::new(format!("{docker_php} {command}"))
        } else {
            Command::new(command)
        }
    }

    pub fn update_person_dates(catalog_id: usize) -> Result<(), GenericError> {
        println!(
            "PHP: update_person_dates {catalog_id} START [{}]",
            Utc::now()
        );
        let output = Self::new_command(
            "/data/project/mix-n-match/scripts/person_dates/update_person_dates.php",
        )
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
        let output = Self::new_command(
            "/data/project/mix-n-match/scripts/generate_aux_from_description.php",
        )
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
        let output = Self::new_command("/data/project/mix-n-match/scripts/bespoke_scraper.php")
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
        let output =
            Self::new_command("/data/project/mix-n-match/scripts/update_descriptions_from_url.php")
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
        let output = Self::new_command("/data/project/mix-n-match/scripts/import_aux_from_url.php")
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
        let output =
            Self::new_command("/data/project/mix-n-match/scripts/match_by_coordinates.php")
                .arg(format!("{catalog_id}"))
                .output()?;
        println!(
            "PHP: match_by_coordinates {catalog_id} END [{}]\n{output:?}",
            Utc::now()
        );
        Ok(())
    }
}
