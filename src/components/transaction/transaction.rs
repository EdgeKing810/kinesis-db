use serde::{Deserialize, Serialize};

use crate::components::database::{database::Database, record::Record};

use super::isolation_level::IsolationLevel;

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub id: u64,                                     // Unique transaction ID
    pub isolation_level: IsolationLevel,             // Isolation level of the transaction
    pub pending_inserts: Vec<(String, Record)>,      // (table, record)
    pub pending_deletes: Vec<(String, u64, Record)>, // (table, record_id, record)
    pub read_set: Vec<(String, u64, u64)>,           // (table, record_id, version)
    pub write_set: Vec<(String, u64)>,               // (table, record_id)
    pub snapshot: Option<Database>, // Snapshot of the database at the start of the transaction
    pub start_timestamp: u64,       // Start time of the transaction
    pub pending_table_creates: Vec<String>, // List of tables to create
    pub pending_table_drops: Vec<String>, // List of tables to drop
}

impl Transaction {
    pub fn new(id: u64, isolation_level: IsolationLevel, snapshot: Option<Database>) -> Self {
        Transaction {
            id,
            isolation_level,
            pending_inserts: Vec::new(),
            pending_deletes: Vec::new(),
            read_set: Vec::new(),
            write_set: Vec::new(),
            snapshot,
            start_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            pending_table_creates: Vec::new(),
            pending_table_drops: Vec::new(),
        }
    }
}
