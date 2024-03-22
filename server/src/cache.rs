use log::info;
use redis::Commands;
use rocket::fs::NamedFile;
use rocket::response::Redirect;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::prelude::*;
use std::io::Result as IoResult;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use std::sync::Arc;
use url::Url;
use tokio::sync::Mutex;

pub type FileUid = String;
pub type KeyslotId = i16;

pub const PORT_OFFSET_TO_WEB_SERVER: u16 = 20000;

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    access_order: VecDeque<String>, // Track access order for LRU eviction
    s3_endpoint: String,
    pub redis: RedisServer,
}

impl DiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64, s3_endpoint: String, redis_addrs: Vec<String>) -> Arc<Mutex<Self>> {
        let current_size = 0; // Start with an empty cache for simplicity
        let _ = std::fs::create_dir_all(cache_dir.clone());
        Arc::new(Mutex::new(Self {
            cache_dir,
            max_size,
            current_size,
            access_order: VecDeque::new(),
            s3_endpoint,
            redis: RedisServer::new(redis_addrs).unwrap(), // [TODO]: Error Handling
        }))
    }

    pub async fn get_file(cache: Arc<Mutex<Self>>, uid: PathBuf) -> Result<NamedFile, Redirect> {
        let uid_str = uid.into_os_string().into_string().unwrap();
        let file_name: PathBuf;
        let mut cache = cache.lock().await;
        let redirect = cache.redis.location_lookup(uid_str.clone()).await;
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
        if let Some(redis_res) = cache.redis.get_file(uid_str.clone()).await {
            debug!("{} found in cache", &uid_str);
            file_name = redis_res;
        } else {
            match cache.get_s3_file_to_cache(&uid_str).await {
                Ok(cache_file_name) => {
                    debug!("{} fetched from S3", &uid_str);
                    file_name = cache_file_name;
                    let _ = cache
                        .redis
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

    async fn get_s3_file_to_cache(&mut self, s3_file_name: &str) -> IoResult<PathBuf> {
        // Load from "S3", simulate adding to cache
        let s3_file_path = Path::new(&self.s3_endpoint).join(s3_file_name);
        let resp = reqwest::get(s3_file_path.into_os_string().into_string().unwrap())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let file = resp.bytes().await.unwrap(); // [TODO] error handling
        self.ensure_capacity().await;
        fs::File::create(Path::new(&self.cache_dir).join(s3_file_name))?.write_all(&file[..])?;
        let file_size = 1; // Assume each file has size 1 for simplicity
        self.current_size += file_size;
        self.access_order.push_back(String::from(s3_file_name));
        return Ok(Path::new("").join(s3_file_name));
    }

    async fn ensure_capacity(&mut self) {
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
                            let _ = self.redis.remove_file(evicted_file_name.clone()).await;

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

pub struct RedisServer {
    pub client: redis::cluster::ClusterClient,
    pub myid: String,
}

impl RedisServer {
    pub fn new(addrs: Vec<String>) -> Result<Self, redis::RedisError> {
        let client = redis::cluster::ClusterClient::new(addrs)?;
        Ok(RedisServer {
            client,
            myid: String::from(""),
        })
    }
    async fn own_slots_from_shards_info(
        &mut self,
        shards_info: Vec<Vec<redis::Value>>,
    ) -> Result<Vec<[KeyslotId; 2]>, String> {
        let mut shard_iter = shards_info.iter();
        let myid = self.get_myid();
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
        let redis_port = std::env::var("REDIS_PORT").unwrap_or(String::from("6379")).parse::<u16>().unwrap();
        if self.myid.len() == 0 {
            let result = std::process::Command::new("redis-cli")
                .arg("-c")
                .arg("-p")
                .arg(redis_port.to_string())
                .arg("cluster")
                .arg("myid")
                .output()
                .expect("redis command failed to start");
            self.myid = String::from_utf8(result.stdout).unwrap();
            self.myid = String::from(self.myid.trim());
        }
        &self.myid
    }
    pub async fn location_lookup(&mut self, uid: FileUid) -> Option<(String, u16)> {
        let mut conn = self.client.get_connection().unwrap();
        let keyslot = self.which_slot(uid).await;
        debug!("keyslot of my_key: {}", keyslot);
        let shards = redis::cmd("CLUSTER")
            .arg("SHARDS")
            .query::<Vec<Vec<redis::Value>>>(&mut conn)
            .unwrap();
        let mut shard_iter = shards.iter();
        let target_shard: Option<redis::Value> = loop {
            if let Some(shard_info) = shard_iter.next() {
                if let redis::Value::Bulk(ranges) = &shard_info[1] {
                    let mut low_index = 0;
                    let mut high_index = 1;
                    let node_info = loop {
                        if let (redis::Value::Int(range_low), redis::Value::Int(range_high)) =
                            (&ranges[low_index], &ranges[high_index])
                        {
                            if keyslot <= *range_high as KeyslotId
                                && keyslot >= *range_low as KeyslotId
                            {
                                if let redis::Value::Bulk(nodes_info) = &shard_info[3] {
                                    break Some(&nodes_info[0]);
                                } else {
                                    break None;
                                }
                            }
                        }
                        low_index += 2;
                        high_index += 2;
                        if low_index >= ranges.len() {
                            break None;
                        }
                    };
                    if node_info.is_some() {
                        break node_info.cloned();
                    }
                }
            } else {
                break None;
            }
        };
        let mut endpoint = String::from("");
        let mut node_id = String::from("");
        let mut port: u16 = 6379;
        match target_shard {
            Some(info) => {
                if let redis::Value::Bulk(fields) = info {
                    let mut fields_iter = fields.iter();
                    while let Some(redis::Value::Data(field_name)) = fields_iter.next() {
                        if let Ok(x) = from_utf8(field_name) {
                            match x {
                                "ip" => {
                                    if let Some(redis::Value::Data(x)) = fields_iter.next() {
                                        endpoint = String::from_utf8(x.to_vec()).unwrap();
                                    }
                                }
                                "port" => {
                                    if let Some(redis::Value::Int(x)) = fields_iter.next() {
                                        port = *x as u16;
                                    }
                                }
                                "id" => {
                                    if let Some(redis::Value::Data(x)) = fields_iter.next() {
                                        node_id = String::from_utf8(x.to_vec()).unwrap();
                                    }
                                }
                                _ => {
                                    fields_iter.next();
                                }
                            }
                        }
                    }
                    let myid = self.get_myid();
                    debug!("node_id: {}", node_id);
                    debug!("myid: {}", myid);
                    if node_id.eq(myid) {
                        debug!("this is the node!");
                        return None;
                    } else if node_id.len() == 0 {
                        debug!("Cannot find node!");
                    } else {
                        debug!("redirect to {}:{}", endpoint, port);
                        return Some((endpoint, port));
                    }
                }
            }
            None => debug!("No shard match!"),
        }
        None
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
    pub async fn import_keyslot(&mut self, keyslots: Vec<KeyslotId>) {
        let mut conn = self.client.get_connection().unwrap();
        for keyslot in keyslots.iter() {
            let _ = std::process::Command::new("redis-cli") // TODO: error handling
                .arg("-c")
                .arg("cluster")
                .arg("setslot")
                .arg(keyslot.to_string())
                .arg("NODE")
                .arg(self.get_myid())
                .output()
                .expect("redis command setslot failed to start");
            if let Ok(_) = redis::cmd("CLUSTER") // TODO: error handling
                .arg("SETSLOT")
                .arg(keyslot)
                .arg("NODE")
                .arg(self.get_myid())
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
    pub async fn yield_keyslots(&mut self, p: f64) -> Vec<KeyslotId> {
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
