use anyhow::Result;
use arrow::array::{AnyDictionaryArray, RecordBatch};

use client_api::{ColumnId, StorageClient, StorageRequest, TableId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use reqwest::Error;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use tokio::runtime; // Make sure to include tokio in your Cargo.toml
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;

use crate::client_api;

/// Clien for fetching data from I/O service
pub struct StorageClientImpl {
    id: usize,
    table_file_map: HashMap<TableId, String>,
    cache_url: String,
}
impl StorageClientImpl {
    /// Create a StorageClient instance
    pub fn new(id: usize) -> Self {
        Self {
            id,
            table_file_map: HashMap::new(),
            cache_url: "http://localhost:8000".to_string(),
        }
    }

    pub fn new_for_test(id: usize, map: HashMap<TableId, String>) -> Self {
        Self {
            id,
            table_file_map: map,
            cache_url: "http://localhost:8000".to_string(),
        }
    }

    /// Fetch all data of a table, call get_path() to get the file name that stores the table
    pub async fn read_entire_table(&self, table: TableId) -> Result<Receiver<RecordBatch>> {
        let file_path = self.get_path(table)?;
        if !Path::new(&file_path).exists() {
            self.fetch_file(&file_path).await?;
        }
        let (sender, receiver) = channel::<RecordBatch>(1000);

        // Spawn a new async task to read the parquet file and send the data
        task::spawn(async move {
            if let Err(e) = Self::read_all(&file_path, sender).await {
                println!("Error reading parquet file: {:?}", e);
            }
        });
        Ok(receiver)
    }

    pub fn read_entire_table_sync(&self, table: TableId) -> Result<Vec<RecordBatch>> {
        let file_path = self.get_path(table)?;
        if !Path::new(&file_path).exists() {
            let rt = runtime::Runtime::new()?; // Create a new runtime
            let fetch_result = rt.block_on(async { self.fetch_file(&file_path).await });
            // Check the result of the fetch operation
            if let Err(e) = fetch_result {
                // You can return the error here
                return Err(e);
            }
        }
        Self::read_all_sync(&file_path)
    }

    pub async fn entire_columns(
        &self,
        table: TableId,
        columns: Vec<ColumnId>,
    ) -> Result<Receiver<RecordBatch>> {
        let file_path = self.get_path(table)?;
        let (sender, receiver) = channel::<RecordBatch>(1000);

        // Spawn a new async task to read the parquet file and send the data
        task::spawn(async move {
            if let Err(e) = Self::read_all(&file_path, sender).await {
                println!("Error reading parquet file: {:?}", e);
            }
        });
        Ok(receiver)
    }

    /// Consult locally stored table_file_map to get the url
    /// If not exist, call resolve_path to consult catalog service
    /// If catalog says no such table, return error
    fn get_path(&self, table: TableId) -> Result<String> {
        if let Some(file_path) = self.table_file_map.get(&table) {
            Ok(file_path.clone())
        } else {
            panic!(
                "Path not found in local table, catalog service is assume not available yet,
            please check if local table_file_map is correctly initialized."
            );
        }
    }

    /// Call catalog service to get url of a table
    /// Don't implement this for now! Assume catalog is not yet available
    fn consult_catalog(&self, table: TableId) -> Result<()> {
        todo!()
    }

    /// Fetch file from I/O server
    async fn fetch_file(&self, file_path: &str) -> Result<()> {
        // First, trim the path to leave only the file name after the last '/'
        let trimmed_path: Vec<&str> = file_path.split('/').collect();
        let file_name = trimmed_path.last().ok_or_else(|| {
            // Return an error if the last element does not exist
            return anyhow::Error::msg("File path is empty");
        })?;

        let url = format!("{}/s3/{}", self.cache_url, file_name);

        // Send a GET request and await the response
        let response = reqwest::get(url).await?;

        // Ensure the request was successful and extract the response body as a String
        let file_contents = response.bytes().await?;
        // println!("File contents: {}", file_contents);
        // Write the file to the local file_path
        let mut file = File::create(file_path)?;
        file.write_all(&file_contents)?;

        Ok(())
    }

    async fn read_all(file_path: &String, sender: Sender<RecordBatch>) -> Result<()> {
        // If the file exists, open it and read the data. Otherwise, call fetch_file to get the file
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        while let Some(Ok(rb)) = reader.next() {
            sender.send(rb).await?;
        }
        Ok(())
    }

    fn read_all_sync(file_path: &String) -> Result<Vec<RecordBatch>> {
        // If the file exists, open it and read the data. Otherwise, call fetch_file to get the file
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let mut result: Vec<RecordBatch> = Vec::new();
        while let Some(Ok(rb)) = reader.next() {
            result.push(rb);
        }
        Ok(result)
    }
}

#[async_trait::async_trait]
impl StorageClient for StorageClientImpl {
    async fn request_data(&self, _request: StorageRequest) -> Result<Receiver<RecordBatch>> {
        match _request {
            StorageRequest::Table(table_id) => self.read_entire_table(table_id).await,
            StorageRequest::Columns(table_id, column_ids) => {
                todo!()
            }
            StorageRequest::Tuple(record_ids) => {
                panic!("Tuple request is not supported yet")
            }
        }
    }

    async fn request_data_sync(&self, _request: StorageRequest) -> Result<Vec<RecordBatch>> {
        match _request {
            StorageRequest::Table(table_id) => self.read_entire_table_sync(table_id),
            StorageRequest::Columns(table_id, column_ids) => {
                todo!()
            }
            StorageRequest::Tuple(record_ids) => {
                panic!("Tuple request is not supported yet")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::Utf8;
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::path;
    use std::sync::Arc;
    use std::time::Duration;
    use std::{fs, path::Path};
    use tokio::runtime::Runtime;
    use tokio::time::sleep;

    fn create_sample_rb() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let names = StringArray::from(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids), Arc::new(names)]).unwrap();
        batch
    }

    fn create_sample_parquet_file(file_name: &str) -> anyhow::Result<()> {
        let mut file_path = std::env::var("CLIENT_FILES_DIR").unwrap().to_owned();

        file_path.push_str(file_name);
        let path = Path::new(&file_path);

        let message_type = "
        message schema {
            REQUIRED INT32 id;
            REQUIRED BYTE_ARRAY name (UTF8);
        }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let file = fs::File::create(&path).unwrap();

        let props: WriterProperties = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let ids = vec![1, 2, 3, 4, 5];
        let names = vec!["Alice", "Bob", "Carol", "Dave", "Eve"];
        let mut row_group_writer = writer.next_row_group().unwrap();
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            // ... write values to a column writer
            match col_writer.untyped() {
                ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                    typed_writer.write_batch(&ids, None, None)?;
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                    let byte_array_names: Vec<ByteArray> =
                        names.iter().map(|&name| ByteArray::from(name)).collect();
                    typed_writer.write_batch(&byte_array_names, None, None)?;
                }
                _ => {}
            }
            col_writer.close().unwrap()
        }
        row_group_writer.close().unwrap();
        writer.close().unwrap();

        Ok(())
    }

    fn setup_local() -> (StorageClientImpl, String) {
        let mut table_file_map: HashMap<TableId, String> = HashMap::new();
        let file_name: &str = "sample.parquet";
        let mut file_path = std::env::var("CLIENT_FILES_DIR").unwrap().to_owned();
        file_path.push_str(file_name);
        table_file_map.insert(0, file_path);

        // Create a sample parquet file

        create_sample_parquet_file(file_name).unwrap();

        (
            StorageClientImpl::new_for_test(1, table_file_map),
            file_name.to_string(),
        )
    }

    fn setup_remote() -> StorageClientImpl {
        let mut table_file_map: HashMap<TableId, String> = HashMap::new();
        let file_name: &str = "sample.parquet";
        let mut file_path = std::env::var("CLIENT_FILES_DIR").unwrap().to_owned();
        file_path.push_str(file_name);
        table_file_map.insert(0, file_path);

        StorageClientImpl::new_for_test(1, table_file_map)
    }

    #[test]
    fn test_entire_table_local() {
        let (client, _file_name) = setup_local();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut receiver = client.read_entire_table(0).await.unwrap();
            // Wait for the channel to be ready
            sleep(Duration::from_secs(1)).await;
            // Assert that the channel is ready
            let res = receiver.try_recv();
            assert!(res.is_ok());
            let record_batch = res.unwrap();
            let sample_rb = create_sample_rb();
            assert_eq!(record_batch, sample_rb);
            println!("RecordBatch: {:?}", record_batch);
            println!("SampleRecordBatch: {:?}", sample_rb);
        });
    }

    #[test]
    fn test_entire_table_local_sync() {
        let (client, _file_name) = setup_local();
        // let rt = Runtime::new().unwrap();
        let res = client.read_entire_table_sync(0);
        assert!(res.is_ok());
        let record_batch = res.unwrap();
        let rb = record_batch.get(0);
        assert!(rb.is_some());
        let sample_rb = create_sample_rb();
        assert_eq!(rb.unwrap().clone(), sample_rb);
        println!("RecordBatch: {:?}", rb);
        println!("SampleRecordBatch: {:?}", sample_rb);
    }

    #[test]
    #[ignore]
    fn test_entire_table_remote() {
        let client = setup_remote();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut receiver = client.read_entire_table(0).await.unwrap();
            // Wait for the channel to be ready
            sleep(Duration::from_secs(1)).await;
            // Assert that the channel is ready
            let res: std::prelude::v1::Result<RecordBatch, tokio::sync::mpsc::error::TryRecvError> =
                receiver.try_recv();
            assert!(res.is_ok());
            let record_batch = res.unwrap();
            let sample_rb = create_sample_rb();
            assert_eq!(record_batch, sample_rb);
            println!("RecordBatch: {:?}", record_batch);
            println!("SampleRecordBatch: {:?}", sample_rb);
        });
    }

    #[test]
    fn test_request_data_table_local() {
        let (client, _file_name) = setup_local();
        let rt: Runtime = Runtime::new().unwrap();
        rt.block_on(async {
            let mut receiver = client.request_data(StorageRequest::Table(0)).await.unwrap();
            // Wait for the channel to be ready
            sleep(Duration::from_secs(1)).await;
            // Assert that the channel is ready
            let res = receiver.try_recv();
            assert!(res.is_ok());
            let record_batch = res.unwrap();
            let sample_rb = create_sample_rb();
            assert_eq!(record_batch, sample_rb);
            println!("RecordBatch: {:?}", record_batch);
            println!("SampleRecordBatch: {:?}", sample_rb);
        });
    }

    #[test]
    #[ignore]
    fn test_request_data_table_remote() {
        let client = setup_remote();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut receiver = client.request_data(StorageRequest::Table(0)).await.unwrap();
            // Wait for the channel to be ready
            sleep(Duration::from_secs(1)).await;
            // Assert that the channel is ready
            let res = receiver.try_recv();
            assert!(res.is_ok());
            let record_batch = res.unwrap();
            let sample_rb: RecordBatch = create_sample_rb();
            assert_eq!(record_batch, sample_rb);
            println!("RecordBatch: {:?}", record_batch);
            println!("SampleRecordBatch: {:?}", sample_rb);
        });
    }

    #[test]
    fn test_request_data_table_local_sync() {
        let (client, _file_name) = setup_local();
        let rt: Runtime = Runtime::new().unwrap();
        rt.block_on(async {
            let res = client.request_data_sync(StorageRequest::Table(0)).await;
            assert!(res.is_ok());
            let record_batch = res.unwrap();
            let rb = record_batch.get(0);
            assert!(rb.is_some());
            let sample_rb = create_sample_rb();
            assert_eq!(rb.unwrap().clone(), sample_rb);
            println!("RecordBatch: {:?}", record_batch);
            println!("SampleRecordBatch: {:?}", sample_rb);
        });
    }
}
