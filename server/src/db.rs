// In src/db.rs

use std::collections::HashMap;
use std::sync::Mutex;

pub struct Database {
    store: Mutex<HashMap<String, String>>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let store = self.store.lock().unwrap();
        store.get(key).cloned()
    }

    pub fn put(&self, key: String, value: String) {
        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
    }
}
