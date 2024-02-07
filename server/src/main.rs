// In src/main.rs

use std::sync::Arc;
use tokio::{io, net::TcpListener};
use log::{error, info, LevelFilter};
use env_logger::Builder;
use clap::{App, Arg};

mod client_connection;
mod db;

use client_connection::ClientConnection;
use db::Database;

#[tokio::main]
async fn main() -> io::Result<()> {
    let matches = App::new("KVStore Server")
        .arg(Arg::new("port")
            .long("port")
            .takes_value(true)
            .default_value("7878")
            .help("Sets the server port"))
        .arg(Arg::new("address")
            .long("address")
            .takes_value(true)
            .default_value("127.0.0.1")
            .help("Sets the IP address"))
        .get_matches();

    let port = matches.value_of("port").unwrap().parse::<u16>().expect("Invalid port number");
    let address = matches.value_of("address").unwrap();

    Builder::new().filter_level(LevelFilter::Info).init();

    let db = Arc::new(Database::new());
    let listener = TcpListener::bind(format!("{}:{}", address, port)).await?;

    info!("Server running on {}:{}", address, port);

    loop {
        let (socket, _) = listener.accept().await?;
        let db_clone: Arc<Database> = db.clone();
        tokio::spawn(async move {
            let mut client = ClientConnection::new(socket, db_clone);
            if let Err(e) = client.handle_client().await {
                error!("Failed to handle client: {}", e);
            }
        });
    }
}


