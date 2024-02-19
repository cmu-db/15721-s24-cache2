use rocket::fs::NamedFile;
use std::path::{Path, PathBuf};
use log::{info};

pub struct FileUid {
    file_name: PathBuf
}

pub struct CacheServer{
    conn: redis::Connection
}

impl FileUid {
    pub fn from_path(path: PathBuf) -> FileUid {
        FileUid {
            file_name: path
        }
    }
}

impl CacheServer{
    pub fn new(addr: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(addr)?;
        let conn = client.get_connection()?;
        info!("Connected with server {}", addr);
        Ok(CacheServer {
            conn
        })
    }
    pub async fn request_file(&self, uid: FileUid) -> Option<NamedFile> {
            /* [DEBUG USE] "data/" is tmp dummy directory */
            NamedFile::open(Path::new("debug_data/").join(uid.file_name)).await.ok()
        } 
        /* [TODO] check if <uid> file exist on local disk */

        /* [TODO] if <uid> doesn't exist, fetch from cloud storage */
}
