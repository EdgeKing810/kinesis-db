use serde::{Deserialize, Serialize};

use crate::components::database::{database::Database, table::Record};

use super::isolation_level::IsolationLevel;

// A simplified transaction struct
#[derive(Serialize, Deserialize)]
pub struct Transaction {
    pub id: u64,
    pub isolation_level: IsolationLevel,
    pub pending_inserts: Vec<(String, Record)>,
    pub pending_deletes: Vec<(String, u64, Record)>,
    pub read_set: Vec<(String, u64, u64)>, // (table, record_id, version)
    pub write_set: Vec<(String, u64)>,     // (table, record_id)
    pub snapshot: Option<Database>,
    pub start_timestamp: u64,
    pub pending_table_creates: Vec<String>,
    pub pending_table_drops: Vec<String>,
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