#[macro_use]
extern crate rocket;

use rocket::fs::NamedFile;
use rocket::State;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

mod cache;
use cache::DiskCache;
mod s3_fifo; // Update to use s3_fifo
use s3_fifo::S3FIFOCache; // Use S3FIFOCache instead of DiskCache


#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<path..>")]
async fn get_file(
    path: PathBuf,
    cache: &State<Arc<Mutex<DiskCache>>>,
) -> Option<NamedFile> {
    DiskCache::get_file(cache.inner().clone(), &path).await
}

#[launch]
fn rocket() -> _ {
    let cache_manager = DiskCache::new(PathBuf::from("cache/"), 3); // 10 MB Max Size
    rocket::build()
        .manage(cache_manager)
        .mount("/", routes![health_check, get_file])
}

