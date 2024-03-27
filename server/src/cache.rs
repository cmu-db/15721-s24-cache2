// cache.rs
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::io::{ErrorKind, Result as IoResult, Write};
use std::path::Path;
use std::fs;
use log::{debug, info};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use rocket::{fs::NamedFile, response::Redirect};

use crate::redis::RedisServer;
use crate::util::hash;

// Constants
const SHARD_COUNT: u64 = 3;

// Cache Structures -----------------------------------------------------------

pub struct ConcurrentDiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    shards: Vec<Arc<Mutex<DiskCache>>>,
    pub redis: Arc<RwLock<RedisServer>>,
}

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    access_order: VecDeque<String>,
}

// DiskCache Implementation ---------------------------------------------------

impl DiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64) -> Arc<Mutex<Self>> {
        let current_size = 0; // Start with an empty cache for simplicity
        Arc::new(Mutex::new(Self {
            cache_dir,
            max_size,
            current_size,
            access_order: VecDeque::new(),
        }))
    }

    pub async fn get_file(cache: Arc<Mutex<Self>>, uid: PathBuf, redis_read: &RwLockReadGuard<'_, RedisServer>) -> Result<NamedFile, Redirect> {
        let uid_str = uid.into_os_string().into_string().unwrap();
        let file_name: PathBuf;
        let mut cache = cache.lock().await;
        let redirect = redis_read.location_lookup(uid_str.clone()).await;
        if let Some(x) = redirect {
            return Err(Redirect::to(format!("http://{}:8000/s3/{}", x, &uid_str)));
        }
        if let Some(redis_res) = redis_read.get_file(uid_str.clone()).await {
            debug!("{} found in cache", &uid_str);
            file_name = redis_res;
        } else {
            match cache.get_s3_file_to_cache(&uid_str, &redis_read).await {
                Ok(cache_file_name) => {
                    debug!("{} fetched from S3", &uid_str);
                    file_name = cache_file_name;
                    let _ = redis_read
                        .set_file_cache_loc(uid_str.clone(), file_name.clone())
                        .await;
                }
                Err(e) => {
                    info!("{}", e.to_string());
                    return Err(Redirect::to("/not_found_on_this_disk"));
                }
            }
        }
        let file_name_str = file_name.to_str().unwrap_or_default().to_string();
        debug!("get_file: {}", file_name_str);
        cache.update_access(&file_name_str);
        let cache_file_path = cache.cache_dir.join(file_name);
        return NamedFile::open(cache_file_path)
            .await
            .map_err(|_| Redirect::to("/not_found_on_this_disk"));
    }

    async fn get_s3_file_to_cache(&mut self, s3_file_name: &str, redis_read: &RwLockReadGuard<'_, RedisServer> ) -> IoResult<PathBuf> {
        // Load from "S3", simulate adding to cache
        let s3_endpoint = "http://mocks3";
        let s3_file_path = Path::new(s3_endpoint).join(s3_file_name);
        let resp = reqwest::get(s3_file_path.into_os_string().into_string().unwrap())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Check if the file was not found in S3
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(std::io::Error::new(ErrorKind::NotFound, "File not found in S3"));
        }

        // Ensure the response status is successful, otherwise return an error
        if !resp.status().is_success() {
            return Err(std::io::Error::new(ErrorKind::Other, format!("Failed to fetch file from S3 with status: {}", resp.status())));
        }
        let file = resp.bytes().await.unwrap(); // [TODO] error handling
        self.ensure_capacity(redis_read).await;
        fs::File::create(Path::new(&self.cache_dir).join(s3_file_name))?.write_all(&file[..])?;
        let file_size = 1; // Assume each file has size 1 for simplicity
        self.current_size += file_size;
        self.access_order.push_back(String::from(s3_file_name));
        return Ok(Path::new("").join(s3_file_name));
    }

    async fn ensure_capacity(&mut self, redis_read: &RwLockReadGuard<'_, RedisServer> ) {
        // Trigger eviction if the cache is full or over its capacity
        while self.current_size >= self.max_size && !self.access_order.is_empty() {
            if let Some(evicted_file_name) = self.access_order.pop_front() {
                let evicted_path = self.cache_dir.join(&evicted_file_name);
                match fs::metadata(&evicted_path) {
                    Ok(metadata) => {
                        let _file_size = metadata.len();
                        if let Ok(_) = fs::remove_file(&evicted_path) {
                            // Ensure the cache size is reduced by the actual size of the evicted file
                            self.current_size -= 1;
                            let _ = redis_read.remove_file(evicted_file_name.clone()).await;

                            info!("Evicted file: {}", evicted_file_name);
                        } else {
                            eprintln!("Failed to delete file: {}", evicted_path.display());
                        }
                    }
                    Err(e) => eprintln!(
                        "Failed to get metadata for file: {}. Error: {}",
                        evicted_path.display(),
                        e
                    ),
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
        stats.insert(
            "cache_entries".to_string(),
            0, // [TODO] get cache entries from redis
        );
        stats
    }
}

// ConcurrentDiskCache Implementation -----------------------------------------

impl ConcurrentDiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64, redis_addrs: Vec<&str>) -> Self {
        let shard_max_size = max_size / SHARD_COUNT as u64;
        let redis_server = RedisServer::new(redis_addrs).unwrap();
        let redis = Arc::new(RwLock::new(redis_server));
        let shards = (0..SHARD_COUNT).map(|_| {
            DiskCache::new(cache_dir.clone(), shard_max_size)
        }).collect::<Vec<_>>();
        
        Self {
            cache_dir,
            max_size,
            shards,
            redis,
        }
    }
    pub async fn get_file(&self, uid: PathBuf) -> Result<NamedFile, Redirect> {
        let uid = uid.into_os_string().into_string().unwrap();
        // Use read lock for read operations
        let redis_read = self.redis.read().await; // Acquiring a read lock
        if !redis_read.mapping_initialized {
            drop(redis_read); // Drop read lock before acquiring write lock
            
            let mut redis_write = self.redis.write().await; // Acquiring a write lock
            if let Err(e) = redis_write.update_slot_to_node_mapping().await {
                eprintln!("Error updating slot-to-node mapping: {:?}", e);
                return Err(Redirect::to("/error_updating_mapping"));
            }
            redis_write.get_myid();
            redis_write.mapping_initialized = true;
            drop(redis_write);
            debug!("Initialization complete, dropped Redis write lock");
        } else {
            drop(redis_read);
        }
        let redis_read = self.redis.read().await;
        let shard_index = hash(&uid) % self.shards.len(); // Hash UID to select a shard
        let shard = &self.shards[shard_index];
        // Debug message showing shard selection
        debug!(
            "Selected shard index: {} for uid: {}",
            shard_index, &uid
        );
        let result = DiskCache::get_file(shard.clone(), uid.into(), &redis_read).await;
        drop(redis_read);
        self.print_cache_state().await;
        result
    }

    pub async fn print_cache_state(&self) {
        debug!("Current cache state:");
        for (index, shard) in self.shards.iter().enumerate() {
            let shard_guard = shard.lock().await; // Lock each shard to access its state safely
            let files_in_shard: Vec<_> = shard_guard.access_order.iter().collect();
            debug!(
                "Hash Slot/Shard {}: Current Size = {}, Files = {:?}",
                index,
                shard_guard.current_size,
                files_in_shard
            );
        }
    }
    /* 
    pub async fn set_max_size(cache: Arc<Mutex<Self>>, new_size: u64) {
        let mut cache = cache.lock().await;
        cache.max_size = new_size;
        // Optionally trigger capacity enforcement immediately
        Self::ensure_capacity(&mut *cache).await;
    }

    pub async fn scale_out(cache: Arc<Mutex<Self>>) {
        let mut cache = cache.lock().await;
        let to_move = cache.redis.yield_keyslots(0.01).await;
        debug!("These slots are to move: {:?}", to_move);
    }
    */
}