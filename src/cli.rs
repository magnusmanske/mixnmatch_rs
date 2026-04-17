use crate::import_catalog::ImportMode;
use crate::{app_state::AppState, extended_entry::ExtendedEntry, process::Process};
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

    /// Import or update a catalog from a MetaEntry JSON file
    ImportCatalog {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        /// Catalog ID to import into
        #[arg(long)]
        catalog_id: usize,

        /// Path to JSON file (array of MetaEntry) or JSONL file (one MetaEntry per line)
        #[arg(long)]
        file: PathBuf,

        /// Import mode: "add_replace" (default) only adds/updates entries;
        /// "add_replace_delete" also deletes catalog entries absent from the file
        /// (fully-matched entries are never deleted).
        #[arg(long, default_value = "add_replace")]
        mode: ImportMode,
    },

    /// Run the micro-API server on a given port
    MicroApi {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        /// Port to listen on
        #[arg(short, long, default_value = "8089")]
        port: u16,
    },

    /// Run the public web server: serves /api.php (the Rust port of the PHP API)
    /// and static files from the `html/` directory.
    Webserver {
        #[arg(short, long, value_name = "FILE")]
        config: Option<PathBuf>,

        /// Port to listen on
        #[arg(short, long, default_value = "8000")]
        port: u16,

        /// Path to the static HTML directory (defaults to ./html)
        #[arg(long, default_value = "html")]
        html_dir: PathBuf,
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

    /// Start the public web server.
    /// Routes:
    ///   GET/POST /api.php       -> Rust replacement for the PHP API
    ///   POST     /api/v1/import_catalog
    ///   GET      everything else -> static files from `html_dir`
    async fn run_webserver(app: AppState, port: u16, html_dir: &PathBuf) -> Result<()> {
        use axum::Router;
        use tower_http::services::ServeDir;

        if !html_dir.exists() {
            return Err(anyhow!(
                "html directory not found: {}",
                html_dir.display()
            ));
        }

        let api_router = crate::api::router(app);
        let static_service = ServeDir::new(html_dir)
            .append_index_html_on_directories(true);

        let router: Router = api_router.fallback_service(static_service);

        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        log::info!("webserver: listening on http://127.0.0.1:{port}");
        axum::serve(listener, router).await?;
        Ok(())
    }

    #[allow(clippy::print_stdout, clippy::print_stderr)]
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
                        let _item = match wb
                            .generate_entry_item(&app, &ext_entry, &catalog_item)
                            .await
                        {
                            Some(item) => item,
                            None => {
                                // eprintln!("Error generating item for entry {:?}", ext_entry);
                                continue;
                            }
                        };
                        // println!("{item:?}");
                    }

                    // Should be <limit but for testing... FIXME
                    if entries.len() < 50 {
                        break;
                    }
                    offset += limit;
                }
            }
            Some(Commands::ImportCatalog {
                config,
                catalog_id,
                file,
                mode,
            }) => {
                let app = Self::path2app(config)?;
                let result =
                    crate::import_catalog::import_from_file(&app, *catalog_id, file, *mode).await?;
                println!(
                    "Import complete: {} created, {} updated, {} skipped (fully matched), {} deleted",
                    result.created, result.updated, result.skipped_fully_matched, result.deleted
                );
                if !result.errors.is_empty() {
                    eprintln!("{} errors:", result.errors.len());
                    for e in &result.errors {
                        eprintln!("  {e}");
                    }
                }
            }
            Some(Commands::MicroApi { config, port }) => {
                let app = Self::path2app(config)?;
                crate::micro_api::serve(app, *port).await;
            }
            Some(Commands::Webserver {
                config,
                port,
                html_dir,
            }) => {
                let app = Self::path2app(config)?;
                Self::run_webserver(app, *port, html_dir).await?;
            }
            Some(Commands::Test { config }) => {
                let app = Self::path2app(config)?;
                crate::issue::Issue::fix_wd_duplicates(&app).await?;
            }
            _other => return Err(anyhow!("Unrecognized command")),
        }
        Ok(())
    }
}
