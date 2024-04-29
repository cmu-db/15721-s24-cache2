use clap::{App, Arg};
use istziio_server_node::server::{ServerConfig, ServerNode};

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let matches = App::new("istziio-server-node")
        .version("1.0")
        .author("istziio")
        .about("A distributed server node to serve as cache to S3")
        .arg(
            Arg::with_name("use_mock_s3")
                .long("use-mock-s3")
                .help("Use the mock S3 storage connector"),
        )
        .arg(
            Arg::with_name("s3_endpoint")
                .long("s3-endpoint")
                .takes_value(true)
                .default_value("http://0.0.0.0:6333")
                .help("Endpoint for mock S3 storage"),
        )
        .arg(
            Arg::with_name("bucket")
                .long("bucket")
                .takes_value(true)
                .required_unless("use_mock_s3")
                .help("S3 bucket name"),
        )
        .arg(
            Arg::with_name("region")
                .long("region")
                .takes_value(true)
                .default_value("us-east-1")
                .help("AWS region for S3"),
        )
        .arg(
            Arg::with_name("access_key")
                .long("access-key")
                .takes_value(true)
                .help("AWS access key ID"),
        )
        .arg(
            Arg::with_name("secret_key")
                .long("secret-key")
                .takes_value(true)
                .help("AWS secret access key"),
        )
        .arg(
            Arg::with_name("max_size")
                .long("max-size")
                .takes_value(true)
                .default_value("192")
                .help("Maximum cache size in megabytes"),
        )
        .arg(
            Arg::with_name("bucket_size")
                .long("bucket-size")
                .takes_value(true)
                .default_value("3")
                .help("Bucket size for cache management"),
        )
        .get_matches();
    let _ = setup_logger();
    let _ = std::fs::create_dir_all("/data/cache");
    let use_mock_s3 = matches.is_present("use_mock_s3");
    let redis_port = std::env::var("REDIS_PORT")
        .unwrap_or(String::from("6379"))
        .parse::<u16>()
        .unwrap();
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or(format!("./cache_{}", redis_port));
    let s3_endpoint = matches.value_of("s3_endpoint").unwrap_or_default();
    let bucket = matches.value_of("bucket").unwrap_or("istziio-bucket");
    let region_name = matches.value_of("region").unwrap_or("us-east-1");
    let access_key = matches.value_of("access_key").unwrap_or_default();
    let secret_key = matches.value_of("secret_key").unwrap_or_default();
    let max_size = matches.value_of("max_size").unwrap().parse::<u64>().unwrap(); // Bytes
    let bucket_size = matches.value_of("bucket_size").unwrap().parse::<u64>().unwrap();
    let config = if use_mock_s3 {
        ServerConfig {
            redis_port,
            cache_dir,
            bucket: Some(String::from(bucket)),
            region_name: Some(String::from(region_name)),
            access_key: Some(String::from(access_key)),
            secret_key: Some(String::from(secret_key)),
            use_mock_s3_endpoint: Some(String::from(s3_endpoint)),
            max_size,
            bucket_size,
        }
    } else {
        ServerConfig {
            redis_port,
            cache_dir,
            bucket: Some(String::from(bucket)),
            region_name: Some(String::from(region_name)),
            access_key: Some(String::from(access_key)),
            secret_key: Some(String::from(secret_key)),
            use_mock_s3_endpoint: None,
            max_size,
            bucket_size,
        }
    };
    let server_node = ServerNode::new(config);
    server_node.build().launch().await?;
    Ok(())
}
