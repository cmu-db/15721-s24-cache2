// In src/main.rs
#[macro_use] extern crate rocket;

mod cache;
mod db;

use std::path::PathBuf;
use rocket::fs::NamedFile;
use crate::cache::*;

#[get("/")]
fn health_check() -> &'static str {
    "Healthy\n"
}

#[get("/s3/<path..>")]
async fn get_file(path: PathBuf) -> Option<NamedFile> {
    let uid = FileUid::from_path(path);
    request_file(uid).await
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let _rocket = rocket::build()
        .mount("/", routes![health_check, get_file])
        .launch()
        .await?;
    Ok(())
}
