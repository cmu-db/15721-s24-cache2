// server/src/storage/storage_connector.rs
use async_trait::async_trait;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

#[async_trait]
pub trait StorageConnector {
    async fn fetch_and_cache_file(
        &self,
        file_name: &str,
        cache_path: &PathBuf,
    ) -> IoResult<PathBuf>;
}
