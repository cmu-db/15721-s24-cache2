use rocket::fs::NamedFile;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{info};

pub type FileUid = String;

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    access_order: VecDeque<String>, // Track access order for LRU eviction
    cache_contents: HashMap<String, u64>, // Simulate file size, could be more complex metadata
}

impl DiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64) -> Arc<Mutex<Self>> {
        let current_size = 0; // Start with an empty cache for simplicity
        Arc::new(Mutex::new(Self {
            cache_dir,
            max_size,
            current_size,
            access_order: VecDeque::new(),
            cache_contents: HashMap::new(),
        }))
    }

    pub async fn get_file(cache: Arc<Mutex<Self>>, file_name: &Path) -> Option<NamedFile> {
        let mut cache = cache.lock().await;
        let file_name_str = file_name.to_str().unwrap_or_default().to_string();

        // If file is in cache, update its access position
        if cache.cache_contents.contains_key(&file_name_str) {
            info!("cache hit! ({})", &file_name_str);
            cache.update_access(&file_name_str);
            let cache_file_path = cache.cache_dir.join(file_name);
            return NamedFile::open(cache_file_path).await.ok();
        }

        // Load from "S3", simulate adding to cache
        let s3_file_path = Path::new("S3/").join(file_name);
        if s3_file_path.exists() {
            info!("fetch from S3 ({})", &file_name_str);
            // Before adding the new file, ensure there's enough space
            cache.ensure_capacity().await;
            if let Ok(_) = cache.add_file_to_cache(&s3_file_path).await {
                // Simulate file size for demonstration
                let file_size = 1; // Assume each file has size 1 for simplicity
                cache.current_size += file_size;
                cache.cache_contents.insert(file_name_str.clone(), file_size);
                cache.access_order.push_back(file_name_str);
            }
            return NamedFile::open(s3_file_path).await.ok();
        }

        None
    }

    async fn add_file_to_cache(&mut self, file_path: &Path) -> IoResult<()> {
        let target_path = self.cache_dir.join(file_path.file_name().unwrap());
        fs::copy(file_path, &target_path)?;
        Ok(())
    }

    async fn ensure_capacity(&mut self) {
        // Trigger eviction if the cache is full or over its capacity
        while self.current_size >= self.max_size && !self.access_order.is_empty() {
            if let Some(evicted_file_name) = self.access_order.pop_front() {
                let evicted_path = self.cache_dir.join(&evicted_file_name);
                match fs::metadata(&evicted_path) {
                    Ok(metadata) => {
                        let file_size = metadata.len();
                        if let Ok(_) = fs::remove_file(&evicted_path) {
                            // Ensure the cache size is reduced by the actual size of the evicted file
                            self.current_size -= 1;
                            self.cache_contents.remove(&evicted_file_name);
                            println!("Evicted file: {}", evicted_file_name);
                        } else {
                            eprintln!("Failed to delete file: {}", evicted_path.display());
                        }
                    },
                    Err(e) => eprintln!("Failed to get metadata for file: {}. Error: {}", evicted_path.display(), e),
                }
            }
        }
    }
    // Update a file's position in the access order
    fn update_access(&mut self, file_name: &String) {
        self.access_order.retain(|x| x != file_name);
        self.access_order.push_back(file_name.clone());
    }

    pub async fn get_stats(cache: Arc<Mutex<Self>>) -> HashMap<String, u64> {
        let cache = cache.lock().await;
        let mut stats = HashMap::new();
        stats.insert("current_size".to_string(), cache.current_size);
        stats.insert("max_size".to_string(), cache.max_size);
        stats.insert("cache_entries".to_string(), cache.cache_contents.len() as u64);
        stats
    }

    pub async fn set_max_size(cache: Arc<Mutex<Self>>, new_size: u64) {
        let mut cache = cache.lock().await;
        cache.max_size = new_size;
        // Optionally trigger capacity enforcement immediately
        Self::ensure_capacity(&mut *cache).await;
    }
}
pub struct RedisServer{
    conn: redis::Connection
}


impl RedisServer{
    pub fn new(addr: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(addr)?;
        let conn = client.get_connection()?;
        info!("Connected with server {}", addr);
        Ok(RedisServer {
            conn
        })
    }
    pub async fn get_file(&self, uid: FileUid) -> Option<PathBuf> {
        Some(PathBuf::from("/data/foo.txt"))        
    } 
    pub async fn set_file(&self, uid: FileUid, loc: PathBuf) -> Result<(), ()> {
        Ok(())
    }
}
