//redis.rs
use std::{cmp::Reverse, collections::{BinaryHeap, HashMap}, path::PathBuf};
use redis::Commands;
use log::debug;

use crate::util::{FileUid, KeyslotId};

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
    pub port: u16,
}

pub struct RedisServer {
    pub client: redis::cluster::ClusterClient,
    pub myid: String,
    pub slot_to_node_mapping: HashMap<KeyslotId, NodeInfo>,
    pub mapping_initialized: bool,
}

impl RedisServer {
    pub fn new(addrs: Vec<String>) -> Result<Self, redis::RedisError> {
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
                    let mut port : u16 = 0;
    
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
                                "ip" => {
                                    if let Some(redis::Value::Data(value)) = iter.next() {
                                        endpoint = String::from_utf8(value.clone()).expect("Invalid UTF-8 for endpoint");
                                        debug!("Endpoint: {}", endpoint);
                                    }
                                }
                                "port" => {
                                    if let Some(redis::Value::Int(x)) = iter.next() {
                                        port = *x as u16;
                                        debug!("Port: {}", port);
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
                                    let info = NodeInfo { node_id: node_id.clone(), endpoint: endpoint.clone(), port: port.clone() };
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
    pub async fn location_lookup(& self, uid: FileUid) -> Option<(String, u16)> {
        let slot = self.which_slot(uid).await;
        debug!("Looking up location for slot: {}", slot);
        
        self.slot_to_node_mapping.get(&slot).map(|node_info| {
            if node_info.node_id == self.myid {
                debug!("Slot {} is local to this node", slot);
                None // If the slot is local, we do not need to redirect.
            } else {
                debug!("Redirecting slot {} to node ID {} at {}:{}", slot, node_info.node_id, node_info.endpoint, node_info.port);
                Some((node_info.endpoint.clone(), node_info.port.clone()))
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