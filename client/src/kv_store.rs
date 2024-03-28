// In src/kv_store.rs

use anyhow::Result;
use log::info;
use std::sync::Mutex;
use tokio::io::AsyncBufReadExt;
use tokio::io::{self, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
pub struct KVStore {
    address: String,
    port: u16,
    connection: Mutex<Option<TcpStream>>,
}

impl KVStore {
    pub fn new(address: &str, port: u16) -> Self {
        Self {
            address: address.to_string(),
            port,
            connection: Mutex::new(None),
        }
    }

    pub async fn connect(&self) -> Result<()> {
        let stream = TcpStream::connect(format!("{}:{}", &self.address, self.port)).await?;
        let mut conn = self.connection.lock().unwrap();
        *conn = Some(stream);
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<String> {
        let request = format!("get {}\n", key);
        self.send_and_receive(request).await
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<String> {
        let request = format!("put {} {}\n", key, value);
        self.send_and_receive(request).await
    }

    async fn send_and_receive(&self, message: String) -> Result<String> {
        let mut conn = self.connection.lock().unwrap();
        if let Some(stream) = conn.as_mut() {
            stream.write_all(message.as_bytes()).await?;
            stream.flush().await?;

            // Use a loop to read lines from the server.
            let mut response = String::new();
            let mut reader = BufReader::new(stream);
            reader.read_line(&mut response).await?;
            info!("received back");
            Ok(response.trim().to_string())
        } else {
            Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected to server").into())
        }
    }

    pub fn disconnect(&self) {
        let mut conn = self.connection.lock().unwrap();
        if conn.is_some() {
            let _ = conn.take();
        }
    }
}
