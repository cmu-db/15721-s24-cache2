use istziio_server_node::server::{ServerConfig, ServerNode};
use rocket::local::blocking::Client;

pub fn get_server_config_mocks3(redis_port: u16) -> ServerConfig {
    ServerConfig {
        redis_port,
        cache_dir: format!("./cache_{}", redis_port),
        use_mock_s3_endpoint: Some(String::from("http://0.0.0.0:6333")),
        bucket: None,
        region_name: None,
        access_key: None,
        secret_key: None,
    }
}

pub fn launch_server_node_size_3() -> ([ServerNode; 3], [Client; 3]) {
    let config_1 = get_server_config_mocks3(6379);
    let config_2 = get_server_config_mocks3(6380);
    let config_3 = get_server_config_mocks3(6381);

    let node_1 = ServerNode::new(config_1);
    let node_2 = ServerNode::new(config_2);
    let node_3 = ServerNode::new(config_3);

    let client_1 = Client::tracked(node_1.build()).expect("valid rocket instance");
    let client_2 = Client::tracked(node_2.build()).expect("valid rocket instance");
    let client_3 = Client::tracked(node_3.build()).expect("valid rocket instance");
    ([node_1, node_2, node_3], [client_1, client_2, client_3])
}
