use std::io;
use std::path::PathBuf;
use std::io::Result as IoResult;
use async_trait::async_trait;
use aws_sdk_s3::{Client, Config, Credentials, Region};
use aws_sdk_s3::error::ListBucketsError;
use aws_sdk_s3::SdkError;
use rocket::futures::StreamExt;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use super::storage_connector::StorageConnector;


pub struct S3StorageConnector {
    client: Client,
    bucket: String,
}

impl S3StorageConnector {
    pub async fn new(bucket: String, region_name: &str, access_key: &str, secret_key: &str) -> Result<Self, SdkError<ListBucketsError>> {
        let credentials = Credentials::new(access_key, secret_key, None, None, "manual");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(region_name.to_string()))
            .build();
    
        let s3_client = Client::from_conf(config);
    
        Ok(S3StorageConnector {
            client: s3_client,
            bucket,
        })
    }
}

#[async_trait]
impl StorageConnector for S3StorageConnector {
    async fn fetch_and_cache_file(&self, file_name: &str, cache_path: &PathBuf) -> IoResult<PathBuf> {
        // Assemble the object key with the file name
        let object_key = file_name;

        // Fetch the object using the GetObject command
        let resp = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // Stream the response body directly to the file
        let cache_file_path = cache_path.join(file_name);
        let mut file = File::create(&cache_file_path).await?;
        
        let mut stream = resp.body;
        while let Some(chunk) = stream.next().await {
            let data = chunk.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            file.write_all(&data).await?;
        }

        Ok(cache_file_path)
    }
}