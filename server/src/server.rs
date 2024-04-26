extern crate fern;
extern crate log;
use crate::storage::mock_storage_connector::MockS3StorageConnector;
use crate::storage::s3_storage_connector::S3StorageConnector;
use crate::storage::storage_connector::StorageConnector;
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

#[get("/s3/<uid..>")]
async fn get_file(
    uid: PathBuf,
    cache: &State<Arc<ConcurrentDiskCache>>,
    s3_connector: &State<Arc<dyn StorageConnector + Send + Sync>>,
) -> cache::GetFileResult {
    cache
        .inner()
        .clone()
        .get_file(uid, s3_connector.inner().clone())
        .await
}

#[post("/clear")]
async fn clear(cache: &State<Arc<ConcurrentDiskCache>>) -> String {
    cache.inner().clone().empty().await;
    String::from("cleared")
}

pub struct ServerNode {
    pub cache_manager: Arc<ConcurrentDiskCache>,
    pub s3_connector: Arc<dyn StorageConnector + Send + Sync>,
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
}

impl ServerNode {
    pub fn new(config: ServerConfig) -> Self {
        let s3_connector: Arc<dyn StorageConnector + Send + Sync> =
            if config.use_mock_s3_endpoint.is_some() {
                println!("Using Mock S3 Storage Connector.");
                Arc::new(MockS3StorageConnector::new(
                    config.use_mock_s3_endpoint.clone().unwrap(),
                ))
            } else {
                println!("Using Real S3 Storage Connector.");
                // [TODO] check if they has some value
                Arc::new(S3StorageConnector::new(
                    config.bucket.clone().unwrap(),
                    config.region_name.clone().unwrap(),
                    config.access_key.clone().unwrap(),
                    config.secret_key.clone().unwrap(),
                ))
            };

        let cache_manager = Arc::new(ConcurrentDiskCache::new(
            PathBuf::from(&config.cache_dir),
            6, // [TODO] make this configurable
            vec![format!(
                "redis://{}:{}",
                config.server_ip, config.redis_port
            )],
            config.redis_port,
        ));
        ServerNode {
            cache_manager,
            s3_connector,
            config,
        }
    }
    pub fn build(&self) -> Rocket<rocket::Build> {
        let rocket_port = cache::PORT_OFFSET_TO_WEB_SERVER + self.config.redis_port;
        let cache_state = self.cache_manager.clone();
        let s3_connector_state = self.s3_connector.clone();
        rocket::build()
            .configure(rocket::Config::figment().merge(("address", &self.config.server_ip)).merge(("port", rocket_port)))
            .manage(cache_state)
            .manage(s3_connector_state)
            .mount("/", routes![health_check, get_file, cache_stats, clear])
    }
}
