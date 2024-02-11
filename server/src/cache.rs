use rocket::fs::NamedFile;
use std::path::{Path, PathBuf};

pub struct FileUid {
    file_name: PathBuf
}

impl FileUid {
    pub fn from_path(path: PathBuf) -> FileUid {
        FileUid {
            file_name: path
        }
    }
}

pub async fn request_file(uid: FileUid) -> Option<NamedFile> {
    /* [TODO] check if <uid> file exist on local disk */

    /* [TODO] if <uid> doesn't exist, fetch from cloud storage */

    /* [DEBUG USE] "data/" is tmp dummy directory */
    NamedFile::open(Path::new("debug_data/").join(uid.file_name)).await.ok()
}