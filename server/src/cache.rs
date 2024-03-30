// cache.rs
use chrono::{self, Utc};
use log::{debug, error, info};
use rocket::{fs::NamedFile, response::Redirect};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{ErrorKind, Result as IoResult, Write};
use std::net::IpAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio::time::{timeout, Duration};
use url::Url;

use crate::redis::RedisServer;
use crate::storage::storage_connector::StorageConnector;
use crate::util::hash;

// Constants
const SHARD_COUNT: u64 = 3;
pub const PORT_OFFSET_TO_WEB_SERVER: u16 = 20000;
// Cache Structures -----------------------------------------------------------

pub struct ConcurrentDiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    s3_endpoint: String,
    shards: Vec<Arc<Mutex<DiskCache>>>,
    pub redis: Arc<RwLock<RedisServer>>,
}

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    s3_endpoint: String,
    access_order: VecDeque<String>,
}

// DiskCache Implementation ---------------------------------------------------

impl DiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64, s3_endpoint: String) -> Arc<Mutex<Self>> {
        let current_size = 0; // Start with an empty cache for simplicity
        Arc::new(Mutex::new(Self {
            cache_dir,
            max_size,
            current_size,
            s3_endpoint,
            access_order: VecDeque::new(),
        }))
    }

    pub async fn get_file(
        cache: Arc<Mutex<Self>>,
        uid: PathBuf,
        connector: Arc<dyn StorageConnector + Send + Sync>,
        redis_read: &RwLockReadGuard<'_, RedisServer>,
    ) -> Result<NamedFile, Redirect> {
        let uid_str = uid.into_os_string().into_string().unwrap();
        let mut file_name: PathBuf = Path::new("").to_path_buf();
        let mut cache = cache.lock().await;
        let redirect = redis_read.location_lookup(uid_str.clone()).await;
        if let Some((x, p)) = redirect {
            let mut url = Url::parse("http://localhost").unwrap();
            let address: IpAddr = x.parse().unwrap();
            if address.is_loopback() {
                url.set_host(Some("localhost")).unwrap();
            } else {
                url.set_ip_host(address).unwrap();
            }
            url.set_port(Some(p + PORT_OFFSET_TO_WEB_SERVER)).unwrap();
            url.set_path(&format!("s3/{}", &uid_str)[..]);
            debug!("tell client to redirect to {}", url.to_string());
            return Err(Redirect::to(url.to_string()));
        }
        if let Some(redis_res) = redis_read.get_file(uid_str.clone()).await {
            debug!("{} found in cache", &uid_str);
            file_name = redis_res;
        } else {
            match cache
                .get_s3_file_to_cache(&uid_str, connector, &redis_read)
                .await
            {
                Ok(local_file_name) => {
                    debug!("{} fetched from S3", &uid_str);
                    file_name = local_file_name;
                    cache.ensure_capacity(redis_read).await;
                    let file_size = 1; // Assume each file has size 1 for simplicity
                    cache.current_size += file_size;
                    cache.access_order.push_back(uid_str.clone());
                    let _ = redis_read
                        .set_file_cache_loc(uid_str.clone(), file_name.clone())
                        .await;
                }
                Err(e) => {
                    info!("{}", e.to_string());
                    return Err(Redirect::to("/not_found_on_S3"));
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

    async fn get_s3_file_to_cache(
        &mut self,
        s3_file_name: &str,
        connector: Arc<dyn StorageConnector + Send + Sync>,
        redis_read: &RwLockReadGuard<'_, RedisServer>,
    ) -> IoResult<PathBuf> {
        connector
            .fetch_and_cache_file(s3_file_name, &self.cache_dir)
            .await
    }

    async fn ensure_capacity(&mut self, redis_read: &RwLockReadGuard<'_, RedisServer>) {
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
    
    async fn empty(&mut self, redis_read: &RwLockReadGuard<'_, RedisServer>) {
        self.current_size = 0;
        while let Some(x) = self.access_order.pop_front() {
            let evicted_path = self.cache_dir.join(&x);
            let _ = fs::remove_file(&evicted_path);
            let _ = redis_read.remove_file(x).await;
        }
    }
}

// ConcurrentDiskCache Implementation -----------------------------------------

impl ConcurrentDiskCache {
    pub fn new(
        cache_dir: PathBuf,
        max_size: u64,
        s3_endpoint: String,
        redis_addrs: Vec<String>,
    ) -> Self {
        let _ = std::fs::create_dir_all(cache_dir.clone());
        let shard_max_size = max_size / SHARD_COUNT as u64;
        let redis_server = RedisServer::new(redis_addrs).unwrap();
        let redis = Arc::new(RwLock::new(redis_server));
        let shards = (0..SHARD_COUNT)
            .map(|_| DiskCache::new(cache_dir.clone(), shard_max_size, s3_endpoint.clone()))
            .collect::<Vec<_>>();

        Self {
            cache_dir,
            max_size,
            s3_endpoint,
            shards,
            redis,
        }
    }
    pub async fn get_file(
        &self,
        uid: PathBuf,
        connector: Arc<dyn StorageConnector + Send + Sync>,
    ) -> Result<NamedFile, Redirect> {
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
        debug!("Selected shard index: {} for uid: {}", shard_index, &uid);
        let result = DiskCache::get_file(shard.clone(), uid.into(), connector, &redis_read).await;
        drop(redis_read);
        debug!("{}", self.get_stats().await);
        result
    }

    pub async fn get_stats(&self) -> String {
        let current_time = chrono::Utc::now();
        let mut stats_summary = format!("Cache Stats at {}\n", current_time.to_rfc3339());
        stats_summary.push_str(&format!(
            "{:<15} | {:<12} | {:<12} | {:<10} | {}\n",
            "Shard", "Curr Size", "% Used", "Total Files", "Files"
        ));
        stats_summary.push_str(&"-".repeat(80));
        stats_summary.push('\n');

        for (index, shard) in self.shards.iter().enumerate() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), shard.lock()).await {
                Ok(shard_guard) => {
                    let files_in_shard: Vec<_> = shard_guard.access_order.iter().collect();
                    let total_files = files_in_shard.len();
                    let used_capacity_pct =
                        (shard_guard.current_size as f64 / shard_guard.max_size as f64) * 100.0;
                    stats_summary.push_str(&format!(
                        "{:<15} | {:<12} | {:<12.2} | {:<10} | {:?}\n",
                        format!("Shard {}", index),
                        shard_guard.current_size,
                        used_capacity_pct,
                        total_files,
                        files_in_shard
                    ));
                }
                Err(_) => {
                    stats_summary
                        .push_str(&format!("Timeout while trying to lock shard {}\n", index));
                }
            }
        }

        stats_summary
    }

    pub async fn empty(&self) {
        for shard in self.shards.iter() {
            let redis_read = self.redis.read().await;
            let _ = shard.lock().await.empty(&redis_read).await;
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
