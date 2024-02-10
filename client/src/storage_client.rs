use anyhow::Result;
use arrow::array::RecordBatch;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::{
    collections::HashMap,
    sync::mpsc::{channel, Receiver, Sender},
};
use thiserror::Error;
use tokio::task;

/// Error type for storage client
#[derive(Error, Debug)]
pub enum SCError {
    #[error("Error happens on connection with I/O server")]
    ServerConnectionError,
    #[error("Error happens on connection with catalog service")]
    CatalogConnectionError,
    #[error("Table does not exist(after consulting catalog service)")]
    TableNotExistError,
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),
}

/// Clien for fetching data from I/O service
pub struct StorageClient {
    id: usize,
    table_file_map: HashMap<String, String>,
}
impl StorageClient {
    /// Create a StorageClient instance
    pub fn new(id: usize) -> Self {
        Self {
            id,
            table_file_map: HashMap::new(),
        }
    }

    pub fn new_for_test(id: usize, map: HashMap<String, String>) -> Self {
        Self {
            id,
            table_file_map: map,
        }
    }

    /// Fetch all data of a table, call get_path() to get the file name that stores the table
    pub async fn entire_table(&self, table: &str) -> Result<Receiver<RecordBatch>> {
        let file_path = self.get_path(table)?;
        let (sender, receiver) = channel::<RecordBatch>();

        // Spawn a new async task to read the parquet file and send the data
        task::spawn(async move {
            if let Err(e) = Self::read_parquet_file(&file_path, sender).await {
                println!("Error reading parquet file: {:?}", e);
            }
        });
        Ok(receiver)
    }

    /// Fetch all data of a specific column of a table, call get_path() to get the file name that stores the table
    pub async fn one_col(&self, table: &str, col: &str) -> Receiver<RecordBatch> {
        // Whether to support this granularity is TBD!!
        todo!()
    }

    /// Fetch all data of some specific columns of a table, call get_path() to get the file name that stores the table
    pub async fn mut_cols(&self, table: &str, cols: Vec<&str>) -> Receiver<RecordBatch> {
        // Whether to support this granularity is TBD!!
        todo!()
    }

    /// Consult locally stored table_file_map to get the url
    /// If not exist, call resolve_path to consult catalog service
    /// If catalog says no such table, return error
    fn get_path(&self, table: &str) -> Result<String> {
        todo!()
    }

    /// Call catalog service to get url of a table
    /// Don't implement this for now!
    fn resolve_path(&self, table: &str) -> Result<()> {
        todo!()
    }

    /// Fetch file from I/O server
    fn fetch_file(&self, file_path: &str) -> Result<()> {
        // copy code from main.rs?
        todo!()
    }

    async fn read_parquet_file(file_path: &String, sender: Sender<RecordBatch>) -> Result<()> {
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        while let Some(Ok(rb)) = reader.next() {
            sender.send(rb)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;
    use std::{fs, path::Path};
    use tokio::runtime::Runtime;

    fn create_sample_parquet_file(file_name: &str) -> anyhow::Result<()> {
        let path = Path::new(file_name);

        let message_type = "
        message schema {
            REQUIRED INT32 id;
            REQUIRED BYTE_ARRAY name (UTF8)
        }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());
        let file = fs::File::create(&path).unwrap();

        let props = WriterProperties::builder().build();
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

    fn setup() -> (StorageClient, String) {
        let mut table_file_map = HashMap::new();
        // Suppose "sample_table" is mapped to a "sample.parquet" file
        table_file_map.insert("sample_table".to_string(), "sample.parquet".to_string());

        // Create a sample parquet file
        let file_name = "sample.parquet";
        create_sample_parquet_file(file_name).unwrap();

        (
            StorageClient::new_for_test(1, table_file_map),
            file_name.to_string(),
        )
    }

    #[test]
    fn test_entire_table() {
        let (client, _file_name) = setup();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let receiver = client.entire_table("sample_table").await.unwrap();
            assert!(receiver.try_recv().is_ok());
            // TODO: add more detailed assertions here, like checking the content of the RecordBatch
        });
    }
}
