// In src/main.rs
#[macro_use] extern crate rocket;

mod cache;
mod db;

use std::path::PathBuf;
use rocket::fs::NamedFile;
use rocket::State;

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<path..>")]
async fn get_file(path: PathBuf, cache_server: &State<cache::CacheServer>) -> Option<NamedFile> {
    let uid = cache::FileUid::from_path(path);
    cache_server.request_file(uid).await
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let cache_server = cache::CacheServer::new("redis://127.0.0.1:6739").unwrap();
    let _rocket = rocket::build()
        .manage(cache_server)
        .mount("/", routes![health_check, get_file])
        .launch()
        .await?;
    Ok(())
}
