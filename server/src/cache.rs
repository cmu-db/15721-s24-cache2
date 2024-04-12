// cache.rs
use chrono;
use log::{debug, info};
use rocket::{fs::NamedFile, response::Redirect};
use std::collections::VecDeque;
use std::fs;
use std::io::Result as IoResult;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use url::Url;

use crate::redis::RedisServer;
use crate::storage::storage_connector::StorageConnector;
use crate::util::hash;

// Constants
const BUCKET_SIZE: u64 = 3;
pub const PORT_OFFSET_TO_WEB_SERVER: u16 = 20000;
// Cache Structures -----------------------------------------------------------

pub struct ConcurrentDiskCache {
    shards: Vec<Arc<Mutex<DiskCache>>>,
    pub redis: Arc<RwLock<RedisServer>>,
    redis_port: u16,
}

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    access_order: VecDeque<(String, u64)>,
}

#[derive(rocket::Responder)]
pub enum GetFileResult {
    #[response(status = 200)]
    Hit(NamedFile),
    #[response(status = 303)]
    Redirect(Box<Redirect>), // Box this Redirect to avoid [warn] clippy::large_enum_variant
    #[response(status = 404)]
    NotFoundOnS3(String),
    #[response(status = 500)]
    InitFailed(String),
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

    pub async fn get_file(
        cache: Arc<Mutex<Self>>,
        uid: PathBuf,
        connector: Arc<dyn StorageConnector + Send + Sync>,
        redis_read: &RwLockReadGuard<'_, RedisServer>,
    ) -> GetFileResult {
        let uid_str = uid.into_os_string().into_string().unwrap();
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
            return GetFileResult::Redirect(Box::new(Redirect::to(url.to_string())));
        }
        let file_name = if let Some(redis_res) = redis_read.get_file(uid_str.clone()).await {
            debug!("{} found in cache", &uid_str);
            redis_res
        } else {
            match cache.get_s3_file_to_cache(&uid_str, connector).await {
                Ok(local_file_name) => {
                    debug!("{} fetched from S3", &uid_str);
                    let metadata = fs::metadata(&cache.cache_dir.join(&local_file_name)).ok();
                    let file_size = metadata.unwrap().len();
                    debug!("File size: {} bytes", file_size);
                    cache.ensure_capacity(&redis_read, file_size).await;
                    cache.current_size += file_size;
                    cache.access_order.push_back((uid_str.clone(), file_size.clone()));
                    let _ = redis_read
                        .set_file_cache_loc(uid_str.clone(), local_file_name.clone())
                        .await;
                    local_file_name
                }
                Err(e) => {
                    info!("{}", e.to_string());
                    return GetFileResult::NotFoundOnS3(uid_str);
                }
            }
        };
        let file_name_str = file_name.to_str().unwrap_or_default().to_string();
        debug!("get_file: {}", file_name_str);
        cache.update_access(&file_name_str);
        let cache_file_path = cache.cache_dir.join(file_name);
        match NamedFile::open(cache_file_path).await {
            Ok(x) => GetFileResult::Hit(x),
            Err(_) => GetFileResult::NotFoundOnS3(uid_str),
        }
    }

    async fn get_s3_file_to_cache(
        &mut self,
        s3_file_name: &str,
        connector: Arc<dyn StorageConnector + Send + Sync>,
    ) -> IoResult<PathBuf> {
        connector
            .fetch_and_cache_file(s3_file_name, &self.cache_dir)
            .await
    }

    async fn ensure_capacity(&mut self, redis_read: &RwLockReadGuard<'_, RedisServer>, new_file_size: u64) {
        while self.current_size + new_file_size > self.max_size && !self.access_order.is_empty() {
            if let Some((evicted_file_name, evicted_file_size)) = self.access_order.pop_front() {
                let evicted_path = self.cache_dir.join(&evicted_file_name);
                if fs::remove_file(&evicted_path).is_ok() {
                    self.current_size -= evicted_file_size;
                    let _ = redis_read.remove_file(evicted_file_name.clone()).await;
                    info!("Evicted file: {}", evicted_file_name);
                } else {
                        eprintln!("Failed to delete file: {}", evicted_path.display());
                }
            }
        }
    }
    // Update a file's position in the access order
    fn update_access(&mut self, file_name: &str) {
        let mut file_size: Option<u64> = None;
        self.access_order.retain(|(name, size)| {
            if name == file_name {
                file_size = Some(*size);
                false
            } else {
                true
            }
        });
        if let Some(size) = file_size {
            self.access_order.push_back((file_name.to_string(), size.clone()));
        }
    }

    async fn empty(&mut self, redis_read: &RwLockReadGuard<'_, RedisServer>) {
        self.current_size = 0;
        while let Some((x, _)) = self.access_order.pop_front() {
            let evicted_path = self.cache_dir.join(&x);
            let _ = fs::remove_file(&evicted_path);
            let _ = redis_read.remove_file(x).await;
        }
        redis_read.flush_all();
    }
}

// ConcurrentDiskCache Implementation -----------------------------------------

impl ConcurrentDiskCache {
    pub fn new(
        cache_dir: PathBuf,
        max_size: u64,
        redis_addrs: Vec<String>,
        redis_port: u16,
    ) -> Self {
        let _ = std::fs::create_dir_all(cache_dir.clone());
        let shard_max_size = max_size / BUCKET_SIZE as u64;
        let redis_server = RedisServer::new(redis_addrs).unwrap();
        let redis = Arc::new(RwLock::new(redis_server));
        let shards = (0..BUCKET_SIZE)
            .map(|_| DiskCache::new(cache_dir.clone(), shard_max_size))
            .collect::<Vec<_>>();

        Self {
            shards,
            redis,
            redis_port,
        }
    }
    pub async fn get_file(
        &self,
        uid: PathBuf,
        connector: Arc<dyn StorageConnector + Send + Sync>,
    ) -> GetFileResult {
        let uid = uid.into_os_string().into_string().unwrap();
        // Use read lock for read operations
        let redis_read = self.redis.read().await; // Acquiring a read lock
        if !redis_read.mapping_initialized {
            drop(redis_read); // Drop read lock before acquiring write lock

            let mut redis_write = self.redis.write().await; // Acquiring a write lock
            if let Err(e) = redis_write.update_slot_to_node_mapping().await {
                return GetFileResult::InitFailed(format!(
                    "Error updating slot-to-node mapping: {:?}",
                    e
                ));
            }
            redis_write.get_myid(self.redis_port);
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
        let result =
            DiskCache::get_file(shard.clone(), uid.into(), connector.clone(), &redis_read).await;
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
                    let files_in_shard: Vec<_> = shard_guard.access_order.iter()
                    .map(|(name, size)| format!("{} ({}B)", name, size))
                    .collect::<Vec<String>>();
                    let total_files = files_in_shard.len();
                    let calculated_current_size: u64 = shard_guard.access_order.iter().map(|(_, size)| size).sum();
                    let used_capacity_pct = (calculated_current_size as f64 / shard_guard.max_size as f64) * 100.0;
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
