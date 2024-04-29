use istziio_server_node::server::{ServerConfig, ServerNode};
use rocket::local::blocking::Client;
use std::env;

pub fn get_server_config_mocks3(redis_port: u16) -> ServerConfig {
    ServerConfig {
        redis_port,
        cache_dir: format!("./cache_{}", redis_port),
        use_mock_s3_endpoint: Some(String::from("http://0.0.0.0:6333")),
        bucket: None,
        region_name: None,
        access_key: None,
        secret_key: None,
        max_size: 192,
        bucket_size: 3,
    }
}

pub fn get_server_config_s3(
    redis_port: u16,
    aws_access_key: String,
    aws_secret_key: String,
) -> ServerConfig {
    ServerConfig {
        redis_port,
        cache_dir: format!("./cache_{}", redis_port),
        use_mock_s3_endpoint: None,
        bucket: Some("istziio-bucket".into()),
        region_name: Some("us-east-1".into()),
        access_key: Some(aws_access_key),
        secret_key: Some(aws_secret_key),
        max_size: 192,
        bucket_size: 3,
    }
}

pub fn launch_server_node_size_3(mocks3: bool) -> ([ServerNode; 3], [Client; 3]) {
    let (config_1, config_2, config_3) = if mocks3 {
        (
            get_server_config_mocks3(6379),
            get_server_config_mocks3(6380),
            get_server_config_mocks3(6381),
        )
    } else {
        let access_key = env::var("AWS_ACCESS_KEY_ID").expect("$AWS_ACCESS_KEY_ID not set!");
        let secret_key =
            env::var("AWS_SECRET_ACCESS_KEY").expect("$AWS_SECRET_ACCESS_KEY not set!");
        (
            get_server_config_s3(6379, access_key.clone(), secret_key.clone()),
            get_server_config_s3(6380, access_key.clone(), secret_key.clone()),
            get_server_config_s3(6381, access_key.clone(), secret_key.clone()),
        )
    };

    let node_1 = ServerNode::new(config_1);
    let node_2 = ServerNode::new(config_2);
    let node_3 = ServerNode::new(config_3);

    let client_1 = Client::tracked(node_1.build()).expect("valid rocket instance");
    let client_2 = Client::tracked(node_2.build()).expect("valid rocket instance");
    let client_3 = Client::tracked(node_3.build()).expect("valid rocket instance");
    ([node_1, node_2, node_3], [client_1, client_2, client_3])
}
