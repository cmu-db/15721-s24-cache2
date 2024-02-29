use log::info;
use redis::Commands;
use rocket::fs::NamedFile;
use rocket::http::Status;
use rocket::response::{status, Redirect};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::str::from_utf8;
use std::io::prelude::*;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type FileUid = String;

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size: u64,
    current_size: u64,
    access_order: VecDeque<String>, // Track access order for LRU eviction
    redis: RedisServer,
}

impl DiskCache {
    pub fn new(cache_dir: PathBuf, max_size: u64, redis_addrs: Vec<&str>) -> Arc<Mutex<Self>> {
        let current_size = 0; // Start with an empty cache for simplicity
        Arc::new(Mutex::new(Self {
            cache_dir,
            max_size,
            current_size,
            access_order: VecDeque::new(),
            redis: RedisServer::new(redis_addrs).unwrap(), // [TODO]: Error Handling
        }))
    }

    pub async fn get_file(
        cache: Arc<Mutex<Self>>,
        uid: PathBuf,
    ) -> Result<NamedFile, Redirect> {
        let uid_str = uid.into_os_string().into_string().unwrap();
        let file_name: PathBuf;
        let mut cache = cache.lock().await;
        let redirect = cache.redis.location_lookup(uid_str.clone()).await;
        if let Some(x) = redirect {
            return Err(Redirect::to(format!("http://{}:8000/s3/{}", x, &uid_str)));
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
                },
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
        let s3_endpoint = "http://mocks3";
        let s3_file_path = Path::new(s3_endpoint).join(s3_file_name);
        let resp = reqwest::get(s3_file_path.into_os_string().into_string().unwrap()).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?; 
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
}
pub struct RedisServer {
    pub client: redis::cluster::ClusterClient,
    pub myid: String
}

impl RedisServer {
    pub fn new(addrs: Vec<&str>) -> Result<Self, redis::RedisError> {
        let client = redis::cluster::ClusterClient::new(addrs)?;
        Ok(RedisServer { client, myid: String::from("") })
    }
    pub async fn location_lookup(&mut self, uid: FileUid) -> Option<String> {
        let mut conn = self.client.get_connection().unwrap();
        if self.myid.len() == 0 {
            let result = std::process::Command::new("redis-cli").arg("-c").arg("cluster").arg("myid").output().expect("redis command failed to start");
            self.myid = String::from_utf8(result.stdout).unwrap();
            self.myid = String::from(self.myid.trim());
        }
        let keyslot = redis::cmd("CLUSTER").arg("KEYSLOT").arg(uid).query::<i64>(&mut conn).unwrap();
        debug!("keyslot of my_key: {}", keyslot);
        let shards = redis::cmd("CLUSTER").arg("SHARDS").query::<Vec<Vec<redis::Value>>>(&mut conn).unwrap();
        let mut shard_iter = shards.iter();
        let target_shard: Option<redis::Value> = loop {
            if let Some(shard_info) = shard_iter.next(){
                if let redis::Value::Bulk(ranges) = &shard_info[1] {
                    let mut low_index = 0;
                    let mut high_index = 1;
                    let node_info = loop {
                        if let (redis::Value::Int(range_low), redis::Value::Int(range_high)) = (&ranges[low_index],  &ranges[high_index]){
                            if keyslot <= *range_high && keyslot >= *range_low {
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
        match target_shard {
            Some(info) => {
                if let redis::Value::Bulk(fields) = info {
                    let mut fields_iter = fields.iter();
                    while let Some(redis::Value::Data(field_name)) = fields_iter.next() {
                        if let Ok(x) = from_utf8(field_name) {
                            match x {
                                "endpoint" => {
                                    if let Some(redis::Value::Data(x)) = fields_iter.next() {
                                        endpoint = String::from_utf8(x.to_vec()).unwrap();
                                    }
                                },
                                "id" => {
                                    if let Some(redis::Value::Data(x)) = fields_iter.next() {
                                        node_id = String::from_utf8(x.to_vec()).unwrap();
                                    }
                                },
                                _ => {fields_iter.next();}
                            }
                        }
                    }
                    debug!("node_id: {}", node_id);
                    debug!("myid: {}", self.myid);
                    if node_id.eq(&self.myid) {
                        debug!("this is the node!");
                        return None;
                    } else if node_id.len() == 0 {
                        debug!("Cannot find node!");
                    } else {
                        debug!("redirect to {}", endpoint);
                        return Some(endpoint);
                    }
                }
            },
            None => debug!("No shard match!")
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
        let _ = conn.set::<String, String, String>(uid, loc_str); // [TODO] Error handling
        Ok(())
    }
    pub async fn remove_file(&self, uid: FileUid) -> Result<(), ()> {
        let mut conn = self.client.get_connection().unwrap();
        debug!("remove key [{}] in redis", &uid);
        let _ = conn.del::<String, u8>(uid); // [TODO] Error handling
        Ok(())
    }
}
