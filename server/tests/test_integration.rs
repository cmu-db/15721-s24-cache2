use rocket::http::Status;

mod utils;

#[test]
fn test_healthy() {
    let (_, [client_1, client_2, client_3]) = utils::launch_server_node_size_3();

    let response = client_1.get("/").dispatch();
    assert_eq!(response.status(), Status::Ok);
    assert_eq!(response.into_string(), Some("Healthy\n".into()));

    let response = client_2.get("/").dispatch();
    assert_eq!(response.status(), Status::Ok);
    assert_eq!(response.into_string(), Some("Healthy\n".into()));

    let response = client_3.get("/").dispatch();
    assert_eq!(response.status(), Status::Ok);
    assert_eq!(response.into_string(), Some("Healthy\n".into()));
}

#[test]
fn test_clear() {
    let (_, [client_1, _, _]) = utils::launch_server_node_size_3();

    let _ = client_1.get("/s3/test2.txt").dispatch();
    let response = client_1.get("/stats").dispatch();
    let stats = response.into_string().unwrap();
    assert!(stats.contains("test2"));

    let response = client_1.post("/clear").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_1.get("/stats").dispatch();
    let stats = response.into_string().unwrap();
    assert!(!stats.contains("test2"));
}

#[test]
fn test_get_file() {
    let (_, [client_1, client_2, client_3]) = utils::launch_server_node_size_3();
    let response = client_1.get("/s3/test1.txt").dispatch();
    assert_eq!(response.status(), Status::SeeOther);
    let response = client_1.get("/s3/test2.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_2.get("/s3/test3.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_2.get("/s3/test4.txt").dispatch();
    assert_eq!(response.status(), Status::SeeOther);
    let response = client_3.get("/s3/test5.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_3.get("/s3/test6.txt").dispatch();
    assert_eq!(response.status(), Status::SeeOther);

    let response = client_1.post("/clear").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_2.post("/clear").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_3.post("/clear").dispatch();
    assert_eq!(response.status(), Status::Ok);
}

#[test]
fn test_evict() {
    let (_, [client_1, _, _]) = utils::launch_server_node_size_3();

    let response = client_1.post("/clear").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_1.get("/s3/test6.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_1.get("/s3/test8.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_1.get("/s3/test12.txt").dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client_1.get("/stats").dispatch();
    let stats = response.into_string().unwrap();
    assert!(!stats.contains("test6"));
    assert!(stats.contains("test8"));
}
