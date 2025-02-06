use std::{collections::BTreeMap, fmt::{self, Formatter}, sync::{Arc, RwLock}};

use serde::{de::{MapAccess, Visitor}, ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValueType {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
    pub values: Vec<ValueType>,
    pub version: u64,   // Add version for MVCC
    pub timestamp: u64, // Add timestamp for temporal queries
}

// ========== Table ==========

#[derive(Debug, Clone)]
pub struct Table {
    // For demonstration, we map from record ID to Record
    // BTreeMap is our "B-Tree" structure
    pub data: BTreeMap<u64, Arc<RwLock<Record>>>,
}

// Custom serialization for Table
impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.data.len()))?;
        for (key, value) in &self.data {
            let record = value.read().unwrap();
            map.serialize_entry(key, &*record)?;
        }
        map.end()
    }
}

// Custom deserialization for Table
impl<'de> Deserialize<'de> for Table {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TableVisitor;

        impl<'de> Visitor<'de> for TableVisitor {
            type Value = Table;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                formatter.write_str("a map of record IDs to records")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut data = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    data.insert(key, Arc::new(RwLock::new(value)));
                }
                Ok(Table { data })
            }
        }

        deserializer.deserialize_map(TableVisitor)
    }
}

impl Table {
    pub fn new() -> Self {
        Table {
            data: BTreeMap::new(),
        }
    }

    pub fn insert_record(&mut self, record: Record) {
        self.data.insert(record.id, Arc::new(RwLock::new(record)));
    }

    pub fn get_record(&self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        self.data.get(id).cloned()
    }

    pub fn delete_record(&mut self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        self.data.remove(id)
    }

    // Simple search function: we search for records where *any* field matches a string query
    pub fn search_by_string(&self, query: &str) -> Vec<Arc<RwLock<Record>>> {
        self.data
            .values()
            .filter(|rec| {
                let record = rec.read().unwrap();
                record.values.iter().any(|value| match value {
                    ValueType::Str(s) => s.contains(query),
                    _ => false,
                })
            })
            .cloned()
            .collect()
    }
}