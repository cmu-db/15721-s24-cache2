use client::client_api::{StorageClient, StorageRequest, TableId};
use client::storage_client::StorageClientImpl;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

// This scans the bench_files dir to figure out which test files are present,
// then builds a map of TableId -> filename to init storage client(only when catalog is not available)
// and also generates workload based on table ids. Finally it runs the workload

fn main() {
    // Call the helper function to create the map
    let map = create_table_file_map("/home/scott/15721-s24-cache2/bench_files").unwrap();
    let client = setup_client(map.clone());
    let table_ids: Vec<TableId> = map.keys().cloned().collect();
    let load = load_gen_allonce(table_ids);
    load_run(client, load);
}

fn load_run(client: StorageClientImpl, requests: Vec<StorageRequest>) {
    // record start time
    println!("Start running workload");
    let start = Instant::now();
    for req in requests {
        let id = match req {
            StorageRequest::Table(id) => id,
            _ => panic!("Invalid request type"),
        };
        println!("Requesting data for table {:?}", id);
        let _ = client.request_data_sync(req);
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

fn load_gen_skewed(table_ids: Vec<TableId>) -> Vec<StorageRequest> {
    todo!("Implement the skewed load generator")
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
