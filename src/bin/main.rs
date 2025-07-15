use anyhow::Result;
use log::error;
use mixnmatch::{app_state::AppState, bespoke_scrapers::BespokeScraper};
use std::env;

async fn run() -> Result<()> {
    let argv: Vec<String> = env::args_os().map(|s| s.into_string().unwrap()).collect();
    let config_file = argv
        .get(2)
        .map(|s| s.to_owned())
        .unwrap_or("config.json".into());
    let app = AppState::from_config_file(&config_file)?;
    match argv.get(1).map(|s| s.as_str()) {
        Some("job") => {
            app.run_single_job(
                argv.get(3)
                    .expect("Job ID as third parameter required")
                    .parse::<usize>()
                    .unwrap(),
            )
            .await
        }
        Some("hpjob") => app.run_single_hp_job().await,
        // Some("from_props") => {
        //     let props: Vec<u32> = argv
        //         .get(3)
        //         .expect("Comma-separated props as third parameter")
        //         .split(',')
        //         .filter_map(|s| s.parse::<u32>().ok())
        //         .collect();
        //     let min_entries = argv.get(4).and_then(|s| s.parse::<u16>().ok()).unwrap_or(2);
        //     app.run_from_props(props, min_entries).await
        // }
        Some("test") => {
            match mixnmatch::bespoke_scrapers::BespokeScraper7043::new(&app)
                .run()
                .await
            {
                Ok(_) => println!("Test completed successfully"),
                Err(e) => println!("Test failed with error: {e}"),
            };
            Ok(())
            // let maintenance = maintenance::Maintenance::new(&app);
            // maintenance.match_by_name_and_full_dates().await
            //
            //  ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
            // let am = AutoMatch::new(&app);
            // am.automatch_people_with_initials(13).await
        }
        Some("server") => app.forever_loop().await,
        Some(other) => panic!("Unrecodnized command '{other}'"),
        None => panic!("Command required: server CONFIG_FILE | job CONFIG_FILE JOB_ID"),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> Result<()> {
    match run().await {
        Ok(_) => {}
        Err(e) => error!("CATASTROPHIC FAILURE: {e}"),
    }
    Ok(())
}
