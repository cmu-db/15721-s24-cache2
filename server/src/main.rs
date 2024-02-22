#[macro_use] extern crate rocket;
extern crate fern;
#[macro_use]
extern crate log;
use redis::Commands;

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}

use rocket::fs::NamedFile;
use rocket::http::Status;
use rocket::response::status;
use rocket::State;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

mod cache;
use cache::{DiskCache, FileUid, RedisServer};

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<uid..>")]
async fn get_file(
    uid: PathBuf,
    cache: &State<Arc<Mutex<DiskCache>>>,
) -> Result<NamedFile, status::Custom<&'static str>> {
    DiskCache::get_file(cache.inner().clone(), uid).await
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
    setup_logger();
    let cache_manager = DiskCache::new(PathBuf::from("cache/"), 3, "redis://127.0.0.1:6379"); // use 3 for testing
    rocket::build()
        .manage(cache_manager)
        .mount("/", routes![health_check, get_file, cache_stats, set_cache_size])
}

