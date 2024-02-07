// In src/client_connection.rs

use crate::db::Database;
use log::info;
use tokio::net::TcpStream;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;

pub struct ClientConnection {
    socket: TcpStream,
    db: Arc<Database>,
}

impl ClientConnection {
    pub fn new(socket: TcpStream, db: Arc<Database>) -> Self {
        Self { socket, db }
    }

    pub async fn handle_client(&mut self) -> io::Result<()> {
        let mut buffer = [0; 1024];
        let peer = self.socket.peer_addr()?;
        info!("Client connected from {}", peer);

        loop {
            let nbytes = self.socket.read(&mut buffer).await?;
            if nbytes == 0 { break; }

            let request = String::from_utf8_lossy(&buffer[..nbytes]);
            let parts: Vec<&str> = request.trim().splitn(3, ' ').collect();
            match parts.as_slice() {
                ["get", key] => {
                    if let Some(value) = self.db.get(key) {
                        let response = format!("{}\n", value);
                        self.socket.write_all(response.as_bytes()).await?;
                    } else {
                        self.socket.write_all(b"not found\n").await?;
                    }
                }
                ["put", key, value] => {
                    info!("put {}, {}", key, value);
                    self.db.put(key.to_string(), value.to_string());
                    self.socket.write_all(b"ok\n").await?;
                    info!("ack back to client");
                }
                _ => {
                    self.socket.write_all(b"error: invalid command\n").await?;
                }
            }
        }

        Ok(())
    }
}

