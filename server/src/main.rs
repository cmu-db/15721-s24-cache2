#[macro_use]
extern crate rocket;
extern crate fern;
#[macro_use]
extern crate log;

use rocket::fs::NamedFile;
use rocket::response::{status, Redirect};
use rocket::State;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

mod cache;
use cache::DiskCache;

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

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<uid..>")]
async fn get_file(
    uid: PathBuf,
    cache: &State<Arc<Mutex<DiskCache>>>,
) -> Result<NamedFile, Redirect> {
    DiskCache::get_file(cache.inner().clone(), uid).await
}

#[get("/stats")]
async fn cache_stats(cache: &State<Arc<Mutex<DiskCache>>>) -> String {
    let stats = DiskCache::get_stats(cache.inner().clone()).await;
    format!("Cache Stats: {:?}", stats)
}

#[post("/size/<new_size>")]
async fn set_cache_size(new_size: u64, cache: &State<Arc<Mutex<DiskCache>>>) -> &'static str {
    DiskCache::set_max_size(cache.inner().clone(), new_size).await;
    "Cache size updated"
}

#[launch]
fn rocket() -> _ {
    let _ = setup_logger();
    let _ = std::fs::create_dir_all("/data/cache");
    let cache_manager = DiskCache::new(PathBuf::from("/data/cache/"), 3, vec!["redis://node1:6379", "redis://node2:6379", "redis://node3:6379"]); // [TODO] make the args configurable from env
    rocket::build().manage(cache_manager).mount(
        "/",
        routes![health_check, get_file, cache_stats, set_cache_size],
    )
}
