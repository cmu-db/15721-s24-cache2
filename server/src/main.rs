#[macro_use]
extern crate rocket;
extern crate fern;
#[macro_use]
extern crate log;
use clap::{App, Arg};
use istziio_server_node::storage::mock_storage_connector::MockS3StorageConnector;
use istziio_server_node::storage::s3_storage_connector::S3StorageConnector;
use istziio_server_node::storage::storage_connector::StorageConnector;
use rocket::{fs::NamedFile, response::Redirect, serde::json::Json, State};
use std::env;
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
    s3_connector: &State<Arc<dyn StorageConnector + Send + Sync>>,
) -> Result<NamedFile, Redirect> {
    cache
        .inner()
        .clone()
        .get_file(uid, s3_connector.inner().clone())
        .await
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
    let matches = App::new("My Rocket Application")
        .version("1.0")
        .author("Your Name")
        .about("Description of your application")
        .arg(
            Arg::with_name("use_mock_s3")
                .long("use-mock-s3")
                .help("Use the mock S3 storage connector"),
        )
        .arg(
            Arg::with_name("s3_endpoint")
                .long("s3-endpoint")
                .takes_value(true)
                .default_value("http://0.0.0.0:6333")
                .help("Endpoint for mock S3 storage"),
        )
        .arg(
            Arg::with_name("bucket")
                .long("bucket")
                .takes_value(true)
                .required_unless("use_mock_s3")
                .help("S3 bucket name"),
        )
        .arg(
            Arg::with_name("region")
                .long("region")
                .takes_value(true)
                .default_value("us-east-1")
                .help("AWS region for S3"),
        )
        .arg(
            Arg::with_name("access_key")
                .long("access-key")
                .takes_value(true)
                .help("AWS access key ID"),
        )
        .arg(
            Arg::with_name("secret_key")
                .long("secret-key")
                .takes_value(true)
                .help("AWS secret access key"),
        )
        .get_matches();
    let _ = setup_logger();
    let _ = std::fs::create_dir_all("/data/cache");
    let use_mock_s3 = matches.is_present("use_mock_s3");
    // Define mock and real S3 settings

    let redis_port = std::env::var("REDIS_PORT")
        .unwrap_or(String::from("6379"))
        .parse::<u16>()
        .unwrap();
    let rocket_port = cache::PORT_OFFSET_TO_WEB_SERVER + redis_port;
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or(format!("./cache_{}", rocket_port));
    let s3_endpoint = matches.value_of("s3_endpoint").unwrap_or_default();
    let bucket = matches.value_of("bucket").unwrap_or("istziio-bucket");
    let region_name = matches.value_of("region").unwrap_or("us-east-1");
    let access_key = matches.value_of("access_key").unwrap_or_default();
    let secret_key = matches.value_of("secret_key").unwrap_or_default();
    let s3_connector: Arc<dyn StorageConnector + Send + Sync> = if use_mock_s3 {
        println!("Using Mock S3 Storage Connector.");
        Arc::new(MockS3StorageConnector::new(s3_endpoint.to_string()))
    } else {
        println!("Using Real S3 Storage Connector.");
        Arc::new(S3StorageConnector::new(
            (&bucket).to_string(),
            &region_name,
            &access_key,
            &secret_key,
        ))
    };

    let cache_manager = Arc::new(ConcurrentDiskCache::new(
        PathBuf::from(cache_dir),
        6,
        s3_endpoint.to_string(),
        vec![format!("redis://0.0.0.0:{}", redis_port)],
    )); // [TODO] make the args configurable from env
    rocket::build()
        .configure(rocket::Config::figment().merge(("port", rocket_port)))
        .manage(cache_manager)
        .manage(s3_connector)
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
