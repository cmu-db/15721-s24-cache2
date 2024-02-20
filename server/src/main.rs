#[macro_use] extern crate rocket;

use rocket::fs::NamedFile;
use rocket::http::Status;
use rocket::response::status;
use rocket::State;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

mod cache;
use cache::{DiskCache, FileUid, RedisServer};

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<path..>")]
async fn get_file(
    path: PathBuf,
    cache: &State<Arc<Mutex<DiskCache>>>,
    redis: &State<RedisServer>
) -> Result<NamedFile, status::Custom<&'static str>> {
    match redis.get_file(path.into_os_string().into_string().unwrap()).await {
        Some(path) => DiskCache::get_file(cache.inner().clone(), &path).await.ok_or(status::Custom(Status::NotFound, "File not found")),
        None => Err(status::Custom(Status::NotFound, "File not found"))
    }
}

#[get("/stats")]
async fn cache_stats(cache: &State<Arc<Mutex<DiskCache>>>) -> String {
    let stats = DiskCache::get_stats(cache.inner().clone()).await;
    format!("Cache Stats: {:?}", stats)
}

#[post("/size/<new_size>")]
async fn set_cache_size(
    new_size: u64,
    cache: &State<Arc<Mutex<DiskCache>>>,
) -> &'static str {
    DiskCache::set_max_size(cache.inner().clone(), new_size).await;
    "Cache size updated"
}

#[launch]
fn rocket() -> _ {
    let redis_server = cache::RedisServer::new("redis://127.0.0.1:6739").unwrap();
    let cache_manager = DiskCache::new(PathBuf::from("cache/"), 3); // use 3 for testing
    rocket::build()
        .manage(cache_manager)
        .manage(redis_server)
        .mount("/", routes![health_check, get_file, cache_stats, set_cache_size])
}

