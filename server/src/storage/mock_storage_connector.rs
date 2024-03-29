use async_trait::async_trait;
use rocket::futures::StreamExt;
use tokio::{fs::File, io::AsyncWriteExt};
use reqwest::{self, Error as ReqwestError};
use std::path::{Path, PathBuf};
use std::io;
use std::io::Result as IoResult;
use super::storage_connector::StorageConnector;
pub struct MockS3StorageConnector {
    s3_endpoint: String,
}

impl MockS3StorageConnector {
    pub fn new(s3_endpoint: String) -> Self {
        Self { s3_endpoint }
    }
}

#[async_trait]
impl StorageConnector for MockS3StorageConnector {
    async fn fetch_and_cache_file(&self, file_name: &str, cache_path: &PathBuf) -> IoResult<PathBuf> {
        let s3_file_url = format!("{}/{}", self.s3_endpoint, file_name);
        let response = reqwest::get(&s3_file_url).await.map_err(|e| io_error_from_reqwest(e))?;
        
        if !response.status().is_success() {
            return Err(io::Error::new(io::ErrorKind::Other, format!("Failed to fetch file with status: {}", response.status())));
        }
        
        let cache_file_path = cache_path.join(file_name);
        let mut file = File::create(&cache_file_path).await?;
        
        // Stream the response body directly to the file
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let data = chunk.map_err(|e| io_error_from_reqwest(e))?;
            file.write_all(&data).await?;
        }

        Ok(Path::new("").join(file_name))
    }
}

// Helper function to map a `reqwest::Error` to `std::io::Error`
fn io_error_from_reqwest(e: ReqwestError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}