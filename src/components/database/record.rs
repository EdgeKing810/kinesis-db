use serde::{Deserialize, Serialize};

use super::value_type::ValueType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,                // The unique ID of the record
    pub values: Vec<ValueType>, // The values of the record
    pub version: u64,           // The version of the record for MVCC
    pub timestamp: u64,         // The timestamp for temporal queries
}
