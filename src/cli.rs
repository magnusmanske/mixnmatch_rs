use crate::{
    app_state::AppState, bespoke_scrapers::BespokeScraper, extended_entry::ExtendedEntry,
    process::Process,
};
use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
// use wikibase::{EntityTrait, entity_container::EntityContainer};
// use wikibase_rest_api::prelude::*;

#[derive(Parser)]
#[command(arg_required_else_help = true)]
#[command(name = "Mix'n'match")]
#[command(author = "Magnus Manske <magnusmanske@gmail.com>")]
// #[command(version = "0.1")]
#[command(about = "Mix'n'match server and command-line functionality", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// scans a directory tree
    Server {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
    },

    /// runs a single job
    Job {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        // Job ID
        #[arg(short, long)]
        id: usize,
    },

    /// create unmatched entries with no search results
    CreateUnmatched {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        // Catalog ID
        #[arg(long)]
        catalog_id: usize,

        // Minimum number of person dates (1,2)
        #[arg(long)]
        min_dates: Option<u8>,

        // Minimum number of auxiliary values
        #[arg(long)]
        min_aux: Option<usize>,

        // Entry type (eg Q5)
        #[arg(short, long)]
        entry_type: Option<String>,

        // No search
        #[arg(short, long)]
        no_search: bool,

        // Description hint
        #[arg(short, long)]
        desc_hint: Option<String>,
    },

    /// Delete catalog
    DeleteCatalog {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        // Catalog ID
        #[arg(short, long)]
        id: usize,

        // Really validator
        #[arg(short, long, required = true)]
        really: bool,
    },

    /// wikibase.cloud
    WB {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
    },

    /// test
    Test {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ShellCommands;

impl ShellCommands {
    fn path2str(path: &Option<PathBuf>) -> String {
        path.to_owned()
            .and_then(|p| p.into_os_string().into_string().ok())
            .unwrap_or("config.json".to_string())
    }

    fn path2app(path: &Option<PathBuf>) -> Result<AppState> {
        let config_file = Self::path2str(path);
        let app = AppState::from_config_file(&config_file)?;
        Ok(app)
    }

    pub async fn run(&self) -> Result<()> {
        let cli = Cli::parse();
        match &cli.command {
            Some(Commands::Server { config }) => {
                Self::path2app(config)?.forever_loop().await?;
            }
            Some(Commands::Job { config, id }) => {
                Self::path2app(config)?.run_single_job(*id).await?;
            }
            Some(Commands::DeleteCatalog { config, id, really }) => {
                let _ = really; // To suppress warning, flag is not actually used
                let app = Self::path2app(config)?;
                crate::catalog::Catalog::from_id(*id, &app)
                    .await?
                    .delete()
                    .await?;
            }
            Some(Commands::CreateUnmatched {
                config,
                catalog_id,
                min_dates,
                min_aux,
                entry_type,
                no_search: try_search,
                desc_hint,
            }) => {
                let app = Self::path2app(config)?;
                let mut process = Process::new(app);
                process
                    .create_unmatched(
                        catalog_id, min_dates, min_aux, entry_type, try_search, desc_hint,
                    )
                    .await?;
            }
            Some(Commands::WB { config }) => {
                let config_file = Self::path2str(config);
                let config_json = AppState::load_config(&config_file)?;
                let app = AppState::from_config(&config_json)?;
                let mut wb = app.get_wikibase_from_config(&config_json).await?;

                let catalog_id = 2974;
                let catalog_item = wb.get_or_create_catalog(&app, catalog_id).await?;
                // println!("https://mix-n-match.wikibase.cloud/wiki/Item:{catalog_item}");
                let limit: usize = 1;
                let mut offset: usize = 0;
                loop {
                    let entries = app
                        .storage()
                        .get_entry_batch(catalog_id, limit, offset)
                        .await?;

                    // let ext_ids = entries
                    //     .iter()
                    //     .map(|entry| &entry.ext_id)
                    //     .collect::<Vec<_>>();

                    for entry in &entries {
                        // println!("{entry:?}");
                        let mut ext_entry = ExtendedEntry {
                            entry: entry.to_owned(),
                            ..Default::default()
                        };
                        ext_entry.load_extended_data().await?;
                        let item = match wb
                            .generate_entry_item(&app, &ext_entry, &catalog_item)
                            .await
                        {
                            Some(item) => item,
                            None => {
                                eprintln!("Error generating item for entry {:?}", ext_entry);
                                continue;
                            }
                        };
                        println!("{item:?}");
                    }

                    // Should be <limit but for testing... FIXME
                    if entries.len() < 50 {
                        break;
                    }
                    offset += limit;
                }
            }
            Some(Commands::Test { config }) => {
                let app = Self::path2app(config)?;
                crate::bespoke_scrapers::BespokeScraper7433::new(&app)
                    .run()
                    .await?;
            }
            _other => return Err(anyhow!("Unrecognized command")),
        }
        Ok(())
    }
}
