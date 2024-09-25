use crate::{app_state::AppState, bespoke_scrapers::*};
use anyhow::Result;
use chrono::Utc;
use std::process::Command;

pub struct PhpWrapper {}

impl PhpWrapper {
    fn new_command(script: &str) -> Command {
        let root_dir = AppState::tool_root_dir();
        let mut ret = if AppState::is_on_toolforge() {
            let mut ret = Command::new("php8.1");
            let _ = ret.arg("-c");
            let _ = ret.arg(format!("{root_dir}/mixnmatch_rs/php.ini"));
            ret
        } else {
            Command::new("php")
        };
        let _ = ret.arg(format!("{root_dir}/scripts/{script}"));
        ret
    }

    fn run_command_with_catalog_id(catalog_id: usize, command: &str) -> Result<()> {
        println!("PHP: {command} {catalog_id} START [{}]", Utc::now());
        let output = Self::new_command(command)
            .arg(format!("{catalog_id}"))
            .output()?;
        println!("PHP: {command} {catalog_id} END [{}]", Utc::now());
        println!("{output:?}");
        Ok(())
    }

    pub fn update_person_dates(catalog_id: usize) -> Result<()> {
        Self::run_command_with_catalog_id(catalog_id, "person_dates/update_person_dates.php")
    }

    pub fn generate_aux_from_description(catalog_id: usize) -> Result<()> {
        Self::run_command_with_catalog_id(catalog_id, "generate_aux_from_description.php")
    }

    pub async fn bespoke_scraper(catalog_id: usize, app: &AppState) -> Result<()> {
        match catalog_id {
            121 => BespokeScraper121::new(app).run().await,
            6479 => BespokeScraper6479::new(app).run().await,
            _ => Self::run_command_with_catalog_id(catalog_id, "bespoke_scraper.php"),
        }
    }

    pub fn update_descriptions_from_url(catalog_id: usize) -> Result<()> {
        Self::run_command_with_catalog_id(catalog_id, "update_descriptions_from_url.php")
    }

    pub fn import_aux_from_url(catalog_id: usize) -> Result<()> {
        Self::run_command_with_catalog_id(catalog_id, "import_aux_from_url.php")
    }
}
