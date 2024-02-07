// In src/main.rs

use tokio::io::{self, AsyncBufReadExt, BufReader};
use clap::{App, Arg};
use std::sync::Arc;

mod kv_store;
use kv_store::KVStore;

#[tokio::main]
async fn main() -> io::Result<()> {
    let matches = App::new("KVStore Client")
        .version("1.0")
        .author("Author Name")
        .about("Talks to a KVStore server")
        .arg(Arg::new("server")
             .short('s')
             .long("server")
             .takes_value(true)
             .help("Server address (IP:Port)"))
        .get_matches();

    let server = matches.value_of("server").unwrap_or("127.0.0.1:7878");
    let addr_parts: Vec<&str> = server.split(':').collect();
    if addr_parts.len() != 2 {
        eprintln!("Server address must be in the format IP:Port");
        return Ok(());
    }
    let address = addr_parts[0];
    let port = addr_parts[1].parse::<u16>().expect("Invalid port number");

    let kv_store = Arc::new(KVStore::new(address, port));

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    println!("KVStore Client");
    println!("Type 'help' for a list of commands");

    while reader.read_line(&mut line).await? > 0 {
        {
            let line = line.trim();
            match line {
                "connect" => {
                    kv_store.connect().await.expect("Failed to connect");
                    println!("Connected to {}:{}", address, port);
                }
                cmd if cmd.starts_with("get") => {
                    let parts: Vec<&str> = cmd.split_whitespace().collect();
                    if parts.len() == 2 {
                        let key = parts[1];
                        match kv_store.get(key).await {
                            Ok(value) => println!("Value: {}", value),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    } else {
                        eprintln!("Usage: get <key>");
                    }
                }
                cmd if cmd.starts_with("put") => {
                    let parts: Vec<&str> = cmd.split_whitespace().collect();
                    if parts.len() == 3 {
                        let key = parts[1];
                        let value = parts[2];
                        match kv_store.put(key, value).await {
                            Ok(_) => println!("Put successful"),
                            Err(e) => eprintln!("Error: {}", e),
                        }
                    } else {
                        eprintln!("Usage: put <key> <value>");
                    }
                }
                "disconnect" => {
                    kv_store.disconnect();
                    println!("Disconnected");
                }
                "help" => {
                    println!("Available commands:");
                    println!("  connect                - Connect to the KVStore server");
                    println!("  get <key>              - Retrieve the value for a key");
                    println!("  put <key> <value>      - Set the value for a key");
                    println!("  disconnect             - Disconnect from the server");
                    println!("  help                   - Show this message");
                    println!("  quit                   - Exit the client");
                }
                "quit" => break,
                _ => eprintln!("Unknown command: '{}'", line),
            }
        }
        line.clear();
    }

    Ok(())
}
