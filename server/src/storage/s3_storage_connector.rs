use async_trait::async_trait;
use aws_sdk_s3::{Client, Config, Credentials, Region};
use log::debug;
use rocket::futures::StreamExt;
use std::io;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;


use super::storage_connector::StorageConnector;

pub struct S3StorageConnector {
    client: Client,
    bucket: String,
}

impl S3StorageConnector {
    pub fn new(
        bucket: String,
        region_name: String,
        access_key: String,
        secret_key: String,
    ) -> Self {
        debug!("Creating S3StorageConnector for bucket: {}", bucket);
        let credentials = Credentials::new(access_key, secret_key, None, None, "manual");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new(region_name.to_string()))
            .build();

        let s3_client = Client::from_conf(config);
        debug!("S3 client created successfully for region: {}", region_name);
        S3StorageConnector {
            client: s3_client,
            bucket,
        }
    }
}

#[async_trait]
impl StorageConnector for S3StorageConnector {
    async fn fetch_and_cache_file(
        &self,
        file_name: &str,
        cache_path: &PathBuf,
    ) -> IoResult<PathBuf> {
        debug!(
            "Fetching object '{}' from S3 bucket '{}'",
            file_name, self.bucket
        );
        // Assemble the object key with the file name
        let object_key = file_name;

        // Attempt to fetch the object from S3
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await;

        // Handle the case where the object does not exist
        match result {
            Ok(resp) => {
                let cache_file_path = cache_path.join(file_name);
                let mut file = File::create(&cache_file_path).await?;

                let mut stream = resp.body;
                while let Some(chunk) = stream.next().await {
                    let data =
                        chunk.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
                    file.write_all(&data).await?;
                }

                debug!("Object '{}' fetched successfully", file_name);
                Ok(Path::new("").join(file_name))
            }
            Err(aws_sdk_s3::SdkError::ServiceError { err, .. }) => {
                match err.kind {
                    aws_sdk_s3::error::GetObjectErrorKind::NoSuchKey(_) => {
                        // Handle the object not found error
                        Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "Object not found in S3",
                        ))
                    }
                    _ => {
                        // Handle other service errors
                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Service error: {}", err),
                        ))
                    }
                }
            }
            Err(e) => {
                // Handle non-service errors
                Err(io::Error::new(io::ErrorKind::Other, e.to_string()))
            }
        }
    }
}
