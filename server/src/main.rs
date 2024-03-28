#[macro_use]
extern crate rocket;
extern crate fern;
#[macro_use]
extern crate log;

use rocket::{fs::NamedFile, response::Redirect, serde::json::Json, State};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use istziio_server_node::cache::{self, ConcurrentDiskCache};
use istziio_server_node::util::KeyslotId;

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

#[get("/stats")]
async fn cache_stats(cache: &State<Arc<ConcurrentDiskCache>>) -> String {
    cache.get_stats().await
}

#[get("/s3/<uid..>")]
async fn get_file(
    uid: PathBuf,
    cache: &State<Arc<ConcurrentDiskCache>>,
) -> Result<NamedFile, Redirect> {
    cache.inner().get_file(uid).await
}

/*
#[get("/scale-out")]
async fn scale_out(cache: &State<Arc<Mutex<DiskCache>>>) -> &'static str {
    DiskCache::scale_out(cache.inner().clone()).await;
    "success"
}


#[get("/keyslots/yield/<p>")]
async fn yield_keyslots(
    p: f64,
    cache_guard: &State<Arc<Mutex<DiskCache>>>,
) -> Json<Vec<KeyslotId>> {
    let cache_mutex = cache_guard.inner().clone();
    let mut cache = cache_mutex.lock().await;
    let keyslots = cache.redis.yield_keyslots(p).await;
    Json(keyslots)
}

#[post(
    "/keyslots/import",
    format = "application/json",
    data = "<keyslots_json>"
)]
async fn import_keyslots(
    keyslots_json: Json<Vec<KeyslotId>>,
    cache_guard: &State<Arc<Mutex<DiskCache>>>,
) {
    let cache_mutex = cache_guard.inner().clone();
    let mut cache = cache_mutex.lock().await;
    cache.redis.import_keyslot(keyslots_json.into_inner()).await;
}

#[post(
    "/keyslots/migrate_to/<node_id>",
    format = "application/json",
    data = "<keyslots_json>"
)]
async fn migrate_keyslots_to(
    keyslots_json: Json<Vec<KeyslotId>>,
    node_id: String,
    cache_guard: &State<Arc<Mutex<DiskCache>>>,
) {
    let cache_mutex = cache_guard.inner().clone();
    let cache = cache_mutex.lock().await;
    cache
        .redis
        .migrate_keyslot_to(keyslots_json.into_inner(), node_id)
        .await;
}

#[post("/size/<new_size>")]
async fn set_cache_size(new_size: u64, cache: &State<Arc<Mutex<DiskCache>>>) -> &'static str {
    DiskCache::set_max_size(cache.inner().clone(), new_size).await;
    "Cache size updated"
}
*/
#[launch]
fn rocket() -> _ {
    let _ = setup_logger();
    let _ = std::fs::create_dir_all("/data/cache");
    let redis_port = std::env::var("REDIS_PORT")
        .unwrap_or(String::from("6379"))
        .parse::<u16>()
        .unwrap();
    let rocket_port = cache::PORT_OFFSET_TO_WEB_SERVER + redis_port;
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or(format!("./cache_{}", rocket_port));
    let s3_endpoint = std::env::var("S3_ENDPOINT").unwrap_or(String::from("http://0.0.0.0:6333"));
    let cache_manager = Arc::new(ConcurrentDiskCache::new(
        PathBuf::from(cache_dir),
        6,
        s3_endpoint,
        vec![format!("redis://0.0.0.0:{}", redis_port)],
    )); // [TODO] make the args configurable from env
    rocket::build()
        .configure(rocket::Config::figment().merge(("port", rocket_port)))
        .manage(cache_manager)
        .mount(
            "/",
            routes![
                health_check,
                get_file,
                cache_stats,
                /*
                set_cache_size,
                scale_out,
                yield_keyslots,
                migrate_keyslots_to,
                import_keyslots
                */
            ],
        )
}
