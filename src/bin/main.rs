use anyhow::Result;
use mixnmatch::cli::ShellCommands;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    ShellCommands.run().await
}
