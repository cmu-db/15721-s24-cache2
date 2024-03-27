// Standard Library imports
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    io::{prelude::*, Result as IoResult, ErrorKind},
    path::{Path, PathBuf},
    str::from_utf8,
    sync::Arc,
};

// External crate imports
use log::info;
use redis::Commands;
use rocket::{fs::NamedFile, response::Redirect};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

// Type Definitions
pub type FileUid = String;
pub type KeyslotId = i16;

// Constants
const SHARD_COUNT: u64 = 3;

// Utilities -----------------------------------------------------------------

/// Calculates a consistent hash for the given UID.
fn hash(uid: &FileUid) -> usize {
    let mut hasher = DefaultHasher::new();
    uid.hash(&mut hasher);
    hasher.finish() as usize
}


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

#[derive(Eq)]
struct RedisKeyslot {
    /* This struct is used when the cluster tries to scale out.
     * During scaling out, keyslots are decided to be migrated to some other nodes, and
     * the number of key stored in one key slot implies the number of cached files to
     * be deleted and handover to other nodes. To reduce the lost of cached file due to data
     * migration in this scenario, we want to find key slots that contains the least key to
     * handover. */
    pub id: KeyslotId,
    pub key_cnt: i64,
}

impl Ord for RedisKeyslot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key_cnt.cmp(&other.key_cnt)
    }
}

impl PartialOrd for RedisKeyslot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RedisKeyslot {
    fn eq(&self, other: &Self) -> bool {
        self.key_cnt == other.key_cnt
    }
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub endpoint: String,
}
pub struct RedisServer {
    pub client: redis::cluster::ClusterClient,
    pub myid: String,
    slot_to_node_mapping: HashMap<KeyslotId, NodeInfo>,
    mapping_initialized: bool,
}

impl RedisServer {
    pub fn new(addrs: Vec<&str>) -> Result<Self, redis::RedisError> {
        let client = redis::cluster::ClusterClient::new(addrs)?;
        let server = RedisServer {
            client,
            myid: String::from(""),
            slot_to_node_mapping: HashMap::new(),
            mapping_initialized: false,
        };
        Ok(server)
    }
    async fn own_slots_from_shards_info(
        &self,
        shards_info: Vec<Vec<redis::Value>>,
    ) -> Result<Vec<[KeyslotId; 2]>, String> {
        let mut shard_iter = shards_info.iter();
        let myid = &self.myid;
        loop {
            if let Some(shard_info) = shard_iter.next() {
                if let redis::Value::Bulk(nodes_info) = &shard_info[3] {
                    if let redis::Value::Bulk(fields) = &nodes_info[0] {
                        let mut node_id = String::from("");
                        if let redis::Value::Data(x) = &fields[1] {
                            node_id = String::from_utf8(x.to_vec()).unwrap();
                        }
                        if node_id == *myid {
                            if let redis::Value::Bulk(slot_range) = &shard_info[1] {
                                let mut own_slot_ranges = Vec::new();
                                for i in (0..slot_range.len()).step_by(2) {
                                    if let (redis::Value::Int(low), redis::Value::Int(high)) =
                                        (&slot_range[i], &slot_range[i + 1])
                                    {
                                        let low_id = *low as KeyslotId;
                                        let high_id = *high as KeyslotId;
                                        own_slot_ranges.push([low_id, high_id]);
                                        debug!("this node has slot {} to {}", low_id, high_id);
                                    }
                                }
                                return Ok(own_slot_ranges);
                            }
                        }
                    }
                }
            } else {
                debug!("This id is not found in the cluster");
                return Err(String::from("This id is not found in the cluster"));
            }
        }
    }
    pub fn get_myid(&mut self) -> &String {
        // self.myid cannot be determined at the instantiation moment because the cluster is formed
        // via an external script running redis-cli command. This is a workaround to keep cluster
        // id inside the struct.
        if self.myid.len() == 0 {
            let result = std::process::Command::new("redis-cli")
                .arg("-c")
                .arg("cluster")
                .arg("myid")
                .output()
                .expect("redis command failed to start");
            self.myid = String::from_utf8(result.stdout).unwrap();
            self.myid = String::from(self.myid.trim());
        }
        &self.myid
    }
    // Function to update the slot-to-node mapping
    pub async fn update_slot_to_node_mapping(&mut self) -> Result<(), ()> {
        let mut conn = self.client.get_connection().unwrap();
        let shards = redis::cmd("CLUSTER").arg("SHARDS").query::<Vec<Vec<redis::Value>>>(&mut conn).unwrap();
        let mut new_mapping: HashMap<KeyslotId, NodeInfo> = HashMap::new();
    
        for shard_info in shards {
            if let [_, redis::Value::Bulk(slot_ranges), _, redis::Value::Bulk(nodes_info)] = &shard_info[..] {
                if let Some(redis::Value::Bulk(node_info)) = nodes_info.first() {
                    // Initialize variables to hold id and endpoint
                    let mut node_id = String::new();
                    let mut endpoint = String::new();
    
                    // Iterate through the node_info array
                    let mut iter = node_info.iter();
                    while let Some(redis::Value::Data(key)) = iter.next() {
                        if let Ok(key_str) = std::str::from_utf8(key) {
                            debug!("key_str: {}", key_str);
                            // Match the key to decide what to do with the value
                            match key_str {
                                "id" => {
                                    if let Some(redis::Value::Data(value)) = iter.next() {
                                        node_id = String::from_utf8(value.clone()).expect("Invalid UTF-8 for node_id");
                                        debug!("Node ID: {}", node_id);
                                    }
                                }
                                "endpoint" => {
                                    if let Some(redis::Value::Data(value)) = iter.next() {
                                        endpoint = String::from_utf8(value.clone()).expect("Invalid UTF-8 for endpoint");
                                        debug!("Endpoint: {}", endpoint);
                                    }
                                }
                                _ => {
                                    iter.next();
                                } // Ignore other keys
                            }
                        }
                    }
    
                    // Check if we have both id and endpoint
                    if !node_id.is_empty() && !endpoint.is_empty() {
                        for slots in slot_ranges.chunks(2) {
                            if let [redis::Value::Int(start), redis::Value::Int(end)] = slots {
                                for slot in *start..=*end {
                                    let info = NodeInfo { node_id: node_id.clone(), endpoint: endpoint.clone() };
                                    new_mapping.insert(slot as KeyslotId, info);
                                }
                            }
                        }
                    }
                }
            }
        }
    
        if new_mapping.is_empty() {
            debug!("No slots were found for any nodes. The mapping might be incorrect.");
            return Err(());
        }
    
        self.slot_to_node_mapping = new_mapping;
        debug!("Updated slot-to-node mapping: {:?}", self.slot_to_node_mapping);
        Ok(())
    } 
    // Location lookup function that uses the updated mapping
    pub async fn location_lookup(& self, uid: FileUid) -> Option<String> {
        let slot = self.which_slot(uid).await;
        debug!("Looking up location for slot: {}", slot);
        
        self.slot_to_node_mapping.get(&slot).map(|node_info| {
            if node_info.node_id == self.myid {
                debug!("Slot {} is local to this node", slot);
                None // If the slot is local, we do not need to redirect.
            } else {
                debug!("Redirecting slot {} to node ID {} at {}", slot, node_info.node_id, node_info.endpoint);
                Some(node_info.endpoint.clone())
            }
        }).flatten()
    }
    pub async fn get_file(&self, uid: FileUid) -> Option<PathBuf> {
        let mut conn = self.client.get_connection().unwrap();
        conn.get(uid).map(|u: String| PathBuf::from(u)).ok()
    }
    pub async fn set_file_cache_loc(&self, uid: FileUid, loc: PathBuf) -> Result<(), ()> {
        let mut conn = self.client.get_connection().unwrap();
        let loc_str = loc.into_os_string().into_string().unwrap();
        debug!("try to set key [{}], value [{}] in redis", &uid, &loc_str);
        let _ = conn.set::<String, String, String>(uid.clone(), loc_str); // [TODO] Error handling
        Ok(())
    }
    pub async fn remove_file(&self, uid: FileUid) -> Result<(), ()> {
        let mut conn = self.client.get_connection().unwrap();
        debug!("remove key [{}] in redis", &uid);
        let _ = conn.del::<String, u8>(uid); // [TODO] Error handling
        Ok(())
    }
    async fn which_slot(&self, uid: FileUid) -> KeyslotId {
        let mut conn = self.client.get_connection().unwrap();
        let keyslot = redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(uid)
            .query::<KeyslotId>(&mut conn)
            .unwrap();
        keyslot
    }
    pub async fn import_keyslot(&self, keyslots: Vec<KeyslotId>) {
        let mut conn = self.client.get_connection().unwrap();
        for keyslot in keyslots.iter() {
            let _ = std::process::Command::new("redis-cli") // TODO: error handling
                .arg("-c")
                .arg("cluster")
                .arg("setslot")
                .arg(keyslot.to_string())
                .arg("NODE")
                .arg(&self.myid)
                .output()
                .expect("redis command setslot failed to start");
            if let Ok(_) = redis::cmd("CLUSTER") // TODO: error handling
                .arg("SETSLOT")
                .arg(keyslot)
                .arg("NODE")
                .arg(&self.myid)
                .query::<()>(&mut conn)
            {}
        }
    }
    pub async fn migrate_keyslot_to(&self, keyslots: Vec<KeyslotId>, destination_node_id: String) {
        let mut conn = self.client.get_connection().unwrap();
        for keyslot in keyslots.iter() {
            while let Ok(keys_to_remove) = redis::cmd("CLUSTER")
                .arg("GETKEYSINSLOT")
                .arg(keyslot)
                .arg(10000)
                .query::<Vec<String>>(&mut conn)
            {
                if keys_to_remove.len() == 0 {
                    break;
                }
                let _ = redis::cmd("DEL").arg(keys_to_remove).query::<()>(&mut conn);
            }
            let _ = std::process::Command::new("redis-cli") // TODO: error handling
                .arg("-c")
                .arg("cluster")
                .arg("setslot")
                .arg(keyslot.to_string())
                .arg("NODE")
                .arg(destination_node_id.clone())
                .output()
                .expect("redis command setslot failed to start");
        }
    }
    pub async fn yield_keyslots(&self, p: f64) -> Vec<KeyslotId> {
        let mut conn = self.client.get_connection().unwrap();
        let shards_info = redis::cmd("CLUSTER")
            .arg("SHARDS")
            .query::<Vec<Vec<redis::Value>>>(&mut conn)
            .unwrap();
        let mut slot_ranges = self.own_slots_from_shards_info(shards_info).await.unwrap();
        let mut own_slot_cnt = 0;
        let mut heap = BinaryHeap::new();
        while let Some([low, high]) = slot_ranges.pop() {
            own_slot_cnt += (high - low) + 1;
            for keyslot_id in low..(high + 1) {
                let key_cnt = redis::cmd("CLUSTER")
                    .arg("COUNTKEYSINSLOT")
                    .arg(keyslot_id)
                    .query::<i64>(&mut conn)
                    .unwrap();
                heap.push(Reverse(RedisKeyslot {
                    id: keyslot_id,
                    key_cnt: key_cnt,
                }));
            }
        }
        let migrate_slot_cnt = (own_slot_cnt as f64 * p).floor() as i64;
        let mut result = Vec::new();
        let top_slot = &heap.peek().unwrap().0;
        debug!(
            "the least loaded key slot: {} ({} keys)",
            top_slot.id, top_slot.key_cnt
        );
        debug!(
            "{} of the total {} slot of this node is {}",
            p, own_slot_cnt, migrate_slot_cnt
        );
        for _ in 0..migrate_slot_cnt {
            if let Some(keyslot) = heap.pop() {
                result.push(keyslot.0.id);
            } else {
                break;
            }
        }
        debug!("These are slots to be migrate: {:?}", result);
        result
    }
}
