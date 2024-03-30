extern crate fern;
extern crate log;
use rocket::{get, post, routes, Ignite, Rocket};
use crate::storage::mock_storage_connector::MockS3StorageConnector;
use crate::storage::s3_storage_connector::S3StorageConnector;
use crate::storage::storage_connector::StorageConnector;
use rocket::{fs::NamedFile, response::Redirect, State};
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
) -> Result<NamedFile, Redirect> {
    cache
        .inner()
        .clone()
        .get_file(uid, s3_connector.inner().clone())
        .await
}

#[post("/clear")]
async fn clear(
    cache: &State<Arc<ConcurrentDiskCache>>
) -> String {
    cache.inner().clone().empty().await;
    String::from("cleared")
}


pub struct ServerNode {
    cache_manager: Arc<ConcurrentDiskCache>,
    s3_connector: Arc<dyn StorageConnector + Send + Sync>,
    rocket: Rocket<Ignite>
}

pub struct ServerConfig {
    pub redis_port: u16,
    pub cache_dir: String,
    pub bucket: Option<String>,
    pub region_name: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub use_mock_s3_endpoint: Option<String>
}

impl ServerNode {
    pub async fn new(config: ServerConfig) -> Result<Self, rocket::Error> {
        let rocket_port = cache::PORT_OFFSET_TO_WEB_SERVER + config.redis_port;
        let s3_connector: Arc<dyn StorageConnector + Send + Sync> = if config.use_mock_s3_endpoint.is_some() {
            println!("Using Mock S3 Storage Connector.");
            Arc::new(MockS3StorageConnector::new(config.use_mock_s3_endpoint.unwrap().clone()))
        } else {
            println!("Using Real S3 Storage Connector.");
            // [TODO] check if they has some value
            Arc::new(S3StorageConnector::new(
                config.bucket.unwrap(),
                config.region_name.unwrap(),
                config.access_key.unwrap(),
                config.secret_key.unwrap(),
            ))
        };

        let cache_manager = Arc::new(ConcurrentDiskCache::new(
            PathBuf::from(config.cache_dir),
            6, // [TODO] make this configurable
            String::from(""), // [TODO] remove this redundant thing
            vec![format!("redis://0.0.0.0:{}", config.redis_port)],
        ));
        let rocket = rocket::build()
            .configure(rocket::Config::figment().merge(("port", rocket_port)))
            .manage(cache_manager.clone())
            .manage(s3_connector.clone())
            .mount(
                "/",
                routes![
                    health_check,
                    get_file,
                    cache_stats,
                ],
            ).launch().await?;
        Ok(ServerNode {
            cache_manager,
            s3_connector,
            rocket
        })
    }
}
