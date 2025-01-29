use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};

use crate::btree::BTree;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DataType {
    Int(i64),
    String(String),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    id: u64,
    values: HashMap<String, DataType>,
}

struct Table {
    name: String,
    records: Arc<RwLock<HashMap<u64, Record>>>,
    storage: Arc<RwLock<BTree>>,
}

impl Table {
    fn new(name: &str, storage: Arc<RwLock<BTree>>) -> Self {
        Table {
            name: name.to_string(),
            records: Arc::new(RwLock::new(HashMap::new())),
            storage,
        }
    }
    
    fn insert_record(&self, record: Record) {
        let mut records = self.records.write().unwrap();
        self.storage.write().unwrap().insert(record.id, serde_json::to_string(&record).unwrap());
        records.insert(record.id, record);
    }
    
    fn get_record(&self, id: u64) -> Option<Record> {
        if let Some(record) = self.records.read().unwrap().get(&id) {
            return Some(record.clone());
        }
        if let Some(record_data) = self.storage.read().unwrap().search(id) {
            return serde_json::from_str(&record_data).ok();
        }
        None
    }

    fn delete_record(&self, id: u64) {
        let mut records = self.records.write().unwrap();
        if records.remove(&id).is_some() {
            self.storage.write().unwrap().delete(id);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StorageType {
    Hybrid,
    OnDisk,
}

#[derive(Debug, Serialize, Deserialize)]
struct Database {
    name: String,
    #[serde(skip_serializing, skip_deserializing)]
    tables: HashMap<String, Table>,
    storage_type: StorageType,
    #[serde(skip_serializing, skip_deserializing)]
    storage: Arc<RwLock<BTree>>,
}

impl Database {
    pub fn new(name: &str, storage_type: StorageType, storage: Arc<RwLock<BTree>>) -> Self {
        Database {
            name: name.to_string(),
            tables: HashMap::new(),
            storage_type,
            storage,
        }
    }
    
    pub fn create_table(&mut self, table_name: &str) {
        let table = Table::new(table_name, self.storage.clone());
        self.tables.insert(table_name.to_string(), table);
    }
    
    fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.get(table_name)
    }
    
    fn delete_table(&mut self, table_name: &str) {
        self.tables.remove(table_name);
    }
}