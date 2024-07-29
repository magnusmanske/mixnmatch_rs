// THIS IS NOT USED YET

pub use crate::storage::Storage;
// use anyhow::Result;
// use serde_json::Value;

#[derive(Debug, Clone)]
pub struct StorageWikibase {
    // mnm_pool: mysql_async::Pool,
}

impl Storage for StorageWikibase {
    // fn new(j: &Value) -> Box<Self> {
    //     // let mnm_pool = AppState::create_pool(j);
    //     Box::new(Self {})
    // }

    // async fn disconnect(&self) -> Result<()> {
    //     // self.mnm_pool.disconnect().await?;
    //     Ok(())
    // }
}
