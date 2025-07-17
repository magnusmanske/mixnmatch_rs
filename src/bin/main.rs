use anyhow::Result;
use mixnmatch::cli::ShellCommands;

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> Result<()> {
    ShellCommands.run().await
}
