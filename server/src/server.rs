extern crate fern;
extern crate log;
use crate::storage::mock_storage_connector::MockS3StorageConnector;
use crate::storage::s3_storage_connector::S3StorageConnector;
use crate::storage::storage_connector::StorageConnector;
use crate::util::hash;
use log::debug;
use rocket::State;
use rocket::{get, post, routes, Rocket};
use std::path::PathBuf;
use std::sync::Arc;

use crate::cache::{self, ConcurrentDiskCache};

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/stats")]
async fn cache_stats(cache: &State<Arc<ConcurrentDiskCache>>) -> String {
    cache.get_stats().await
}

#[get("/s3/<uid>?<rid>")]
async fn get_file(
    uid: String,
    rid: Option<String>,
    cache: &State<Arc<ConcurrentDiskCache>>,
    s3_connectors: &State<Vec<Arc<dyn StorageConnector + Send + Sync>>>,
) -> cache::GetFileResult {
    let request_id = rid.unwrap_or_else(|| "0".to_string());  // Default to "0" if `rid` is not provided

    debug!("Requested file: {}", uid);
    debug!("Request ID: {}", request_id);

    let index = hash(&uid) % s3_connectors.len();
    let s3_connector = &s3_connectors[index];

    cache
        .inner()
        .clone()
        .get_file(PathBuf::from(uid), request_id, s3_connector.clone())
        .await
}

#[post("/clear")]
async fn clear(cache: &State<Arc<ConcurrentDiskCache>>) -> String {
    cache.inner().clone().empty().await;
    String::from("cleared")
}

pub struct ServerNode {
    pub cache_manager: Arc<ConcurrentDiskCache>,
    pub s3_connectors: Vec<Arc<dyn StorageConnector + Send + Sync>>,
    config: ServerConfig,
}

pub struct ServerConfig {
    pub server_ip: String,
    pub redis_port: u16,
    pub cache_dir: String,
    pub bucket: Option<String>,
    pub region_name: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub use_mock_s3_endpoint: Option<String>,
    pub max_size: u64,
    pub bucket_size: u64,
}

impl ServerNode {
    pub fn new(config: ServerConfig) -> Self {
        let mut s3_connectors = Vec::new();
        for _ in 0..config.bucket_size {
            let s3_connector: Arc<dyn StorageConnector + Send + Sync> =
                if config.use_mock_s3_endpoint.is_some() {
                    println!("Using Mock S3 Storage Connector.");
                    Arc::new(MockS3StorageConnector::new(
                        config.use_mock_s3_endpoint.clone().unwrap(),
                    ))
                } else {
                    println!("Using Real S3 Storage Connector.");
                    Arc::new(S3StorageConnector::new(
                        config.bucket.clone().unwrap(),
                        config.region_name.clone().unwrap(),
                        config.access_key.clone().unwrap(),
                        config.secret_key.clone().unwrap(),
                    ))
                };
            s3_connectors.push(s3_connector);
        }

        let cache_manager = Arc::new(ConcurrentDiskCache::new(
            PathBuf::from(&config.cache_dir),
            config.max_size,
            config.bucket_size,
            vec![format!(
                "redis://{}:{}",
                config.server_ip, config.redis_port
            )],
            config.redis_port,
        ));
        ServerNode {
            cache_manager,
            s3_connectors,
            config,
        }
    }
    pub fn build(&self) -> Rocket<rocket::Build> {
        let rocket_port = cache::PORT_OFFSET_TO_WEB_SERVER + self.config.redis_port;
        let cache_state = self.cache_manager.clone();
        let s3_connector_state = self.s3_connectors.clone(); // Now cloning the vector of connectors
        rocket::build()
            .configure(
                rocket::Config::figment()
                    .merge(("address", &self.config.server_ip))
                    .merge(("port", rocket_port)),
            )
            .manage(cache_state)
            .manage(s3_connector_state)
            .mount("/", routes![health_check, get_file, cache_stats, clear])
    }
}
