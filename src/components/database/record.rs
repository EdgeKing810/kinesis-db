use super::value_type::ValueType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,                            // The unique ID of the record
    pub values: HashMap<String, ValueType>, // The values of the record
    pub version: u64,                       // The version of the record for MVCC
    pub timestamp: u64,                     // The timestamp for temporal queries
}

impl Record {
    pub fn new(id: u64) -> Self {
        Record {
            id,
            values: HashMap::new(),
            version: 0,
            timestamp: 0,
        }
    }

    pub fn set_field(&mut self, field: &str, value: ValueType) {
        self.values.insert(field.to_string(), value);
    }

    #[allow(dead_code)]
    pub fn get_field(&self, field: &str) -> Option<&ValueType> {
        self.values.get(field)
    }
}
