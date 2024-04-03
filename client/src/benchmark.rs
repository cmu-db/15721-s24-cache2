use client::client_api::{StorageClient, StorageRequest, TableId};
use client::storage_client::StorageClientImpl;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

// This scans the bench_files dir to figure out which test files are present,
// then builds a map of TableId -> filename to init storage client(only when catalog is not available)
// and also generates workload based on table ids. Finally it runs the workload

#[tokio::main]
async fn main() {
    // Call the helper function to create the map
    let home = std::env::var("HOME").unwrap();
    let bench_files_path = format!("{}/15721-s24-cache2/bench_files", home);
    let map = create_table_file_map(&bench_files_path).unwrap();
    let client = setup_client(map.clone());
    let table_ids: Vec<TableId> = map.keys().cloned().collect();
    let load = load_gen_allonce(table_ids.clone());
    load_run(&client, load).await;
    // let skewed_load = load_gen_skewed(table_ids);
    // load_run(&client, skewed_load).await;
}

async fn load_run(client: &dyn StorageClient, requests: Vec<StorageRequest>) {
    println!("Start running workload");
    let start = Instant::now();
    for req in requests {
        let id = match req {
            StorageRequest::Table(id) => id,
            _ => panic!("Invalid request type"),
        };
        println!("Requesting data for table {:?}", id);

        let res = client.request_data_sync(req).await;
        assert!(res.is_ok());
        println!("Received data for table {:?}", id);

        // let local_cache_dir = StorageClientImpl::local_cache_path();
        // // iterate files in local cache and delete them
        // let entries = fs::read_dir(local_cache_dir).unwrap();
        // for entry in entries {
        //     let entry = entry.unwrap();
        //     let path = entry.path();
        //     // if file name ends with "parquet"
        //     if let Some(file_name) = path.file_name() {
        //         if let Some(name) = file_name.to_str() {
        //             if name.ends_with("parquet") {
        //                 fs::remove_file(path).unwrap();
        //             }
        //         }
        //     }
        // }
    }
    let duration = start.elapsed();
    println!("Time used: {:?}", duration);
}

// Generate a load of requests for all tables at once
fn load_gen_allonce(table_ids: Vec<TableId>) -> Vec<StorageRequest> {
    let mut requests = Vec::new();
    for table_id in table_ids {
        requests.push(StorageRequest::Table(table_id));
    }
    requests
}

// Generate a load of requests for all tables, but skewed
// This is not always used
#[allow(dead_code)]
fn load_gen_skewed(table_ids: Vec<TableId>) -> Vec<StorageRequest> {
    // read a random table id twice, and a random table id zero times
    let mut requests = Vec::new();
    for table_id in &table_ids {
        requests.push(StorageRequest::Table(table_id.clone()));
    }
    // remove last element
    requests.pop();
    requests.push(StorageRequest::Table(table_ids[0]));

    requests
}

fn setup_client(table_file_map: HashMap<TableId, String>) -> StorageClientImpl {
    StorageClientImpl::new_for_test(1, table_file_map)
}

fn create_table_file_map(directory: &str) -> Result<HashMap<TableId, String>, std::io::Error> {
    let mut table_file_map: HashMap<TableId, String> = HashMap::new();
    let dir = Path::new(directory);

    // Read the directory entries
    let entries = fs::read_dir(dir)?;

    // Iterate over the entries
    for (id, entry) in entries.enumerate() {
        let entry = entry?;
        if entry.path().is_file() {
            // If the entry is a file, add it to the map with an incremental ID
            let filename = entry.file_name().into_string().unwrap();
            table_file_map.insert(id as TableId, filename);
        }
    }

    Ok(table_file_map)
}
