use istziio_client::client_api::{DataRequest, StorageClient, StorageRequest, TableId};
use istziio_client::storage_client::StorageClientImpl;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;
use std::{env, fs};
use tokio::sync::mpsc;

// This scans the bench_files dir to figure out which test files are present,
// then builds a map of TableId -> filename to init storage client(only when catalog is not available)
// and also generates workload based on table ids. Finally it runs the workload

#[tokio::main]
async fn main() {
    benchmark_sync().await;
    clear_local_cache();
    benchmark_parallel().await;
}

fn clear_local_cache() {
    let home = std::env::var("HOME").unwrap();
    let cache_path = format!("{}/15721-s24-cache2/client/parquet_files", home);
    let _ = fs::remove_dir_all(cache_path);
}

async fn benchmark_sync() {
    // Call the helper function to create the map
    let home = std::env::var("HOME").unwrap();
    let bench_files_path = format!("{}/15721-s24-cache2/bench_files", home);
    let map = create_table_file_map(&bench_files_path).unwrap();
    // get SERVER_URL from env var
    let server_url = env::var("SERVER_URL").unwrap();

    let client = setup_client(map.clone(), &server_url);
    let table_ids: Vec<TableId> = map.keys().cloned().collect();
    let load = load_gen_allonce(table_ids.clone());
    load_run(&client, load).await;
}

async fn benchmark_parallel() {
    // Call the helper function to create the map
    let home = std::env::var("HOME").unwrap();
    let bench_files_path = format!("{}/15721-s24-cache2/bench_files", home);
    let map = create_table_file_map(&bench_files_path).unwrap();
    let server_url = env::var("SERVER_URL").unwrap();
    let clients = setup_clients(map.clone(), 5, &server_url); // create 5 clients for example
    let table_ids: Vec<TableId> = map.keys().cloned().collect();
    let load = load_gen_allonce(table_ids.clone());
    parallel_load_run(clients, load).await;
}

async fn parallel_load_run(clients: Vec<Box<dyn StorageClient>>, requests: Vec<StorageRequest>) {
    println!("------------Start running workload [IN PARALLEL]!------------");

    let start = Instant::now();
    let clients_num = clients.len();
    let (tx, mut rx) = mpsc::channel(32); // Create a channel

    // Spawn tasks for each client to send requests
    for (client_id, client) in clients.into_iter().enumerate() {
        // let client = clients[client_id];
        let tx = tx.clone();
        let requests = requests.clone();
        tokio::spawn(async move {
            let client_start = Instant::now();
            let req = &requests[client_id];
            // for req in &requests {
            let table_id = match req.data_request() {
                DataRequest::Table(id) => id,
                _ => panic!("Invalid request type"),
            };
            println!(
                "Client {:?} requesting data for table {:?}",
                client_id, table_id
            );

            let res = client.request_data_sync(req.clone()).await;
            assert!(res.is_ok());
            println!(
                "Client {:?} received data for table {:?}",
                client_id, table_id
            );
            // }
            let client_duration = client_start.elapsed();
            println!("Client {:?} time used: {:?}", client_id, client_duration);
            tx.send(client_duration).await.unwrap();
        });
    }

    // Collect and print client latencies
    for _ in 0..clients_num {
        let client_duration = rx.recv().await.unwrap();
        println!("Client latency: {:?}", client_duration);
    }

    let duration = start.elapsed();
    println!("Total time used: {:?}", duration);
}

async fn load_run(client: &dyn StorageClient, requests: Vec<StorageRequest>) {
    println!("------------Start running workload [SEQUENTIALLY]!------------");
    let start = Instant::now();
    for req in requests {
        let id = match req.data_request() {
            DataRequest::Table(id) => id.to_owned(),
            _ => panic!("Invalid request type"),
        };
        println!("Requesting data for table {:?}", id);

        let res = client.request_data_sync(req).await;
        assert!(res.is_ok());
        println!("Received data for table {:?}", id);
    }
    let duration = start.elapsed();
    println!("Time used: {:?}", duration);
}

// Generate a load of requests for all tables at once
fn load_gen_allonce(table_ids: Vec<TableId>) -> Vec<StorageRequest> {
    let mut requests = Vec::new();
    for (req_id, table_id) in table_ids.into_iter().enumerate() {
        requests.push(StorageRequest::new(req_id, DataRequest::Table(table_id)));
    }
    requests
}

// Generate a load of requests for all tables, but skewed
// This is not always used
#[allow(dead_code)]
fn load_gen_skewed(table_ids: Vec<TableId>) -> Vec<StorageRequest> {
    // read a random table id twice, and a random table id zero times
    let mut requests = Vec::new();
    for (req_id, table_id) in table_ids.iter().enumerate() {
        requests.push(StorageRequest::new(req_id, DataRequest::Table(*table_id)));
    }
    // remove last element
    requests.pop();
    requests.push(StorageRequest::new(
        table_ids.len(),
        DataRequest::Table(table_ids[0]),
    ));

    requests
}

fn setup_client(table_file_map: HashMap<TableId, String>, server_url: &str) -> StorageClientImpl {
    StorageClientImpl::new_for_test(1, table_file_map, server_url, false)
}

fn setup_clients(
    table_file_map: HashMap<TableId, String>,
    num_clients: usize,
    server_url: &str,
) -> Vec<Box<dyn StorageClient>> {
    let mut clients = Vec::new();
    for i in 0..num_clients {
        let client = Box::new(StorageClientImpl::new_for_test(
            i,
            table_file_map.clone(),
            server_url,
            false,
        )) as Box<dyn StorageClient>;
        clients.push(client);
    }
    clients
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
