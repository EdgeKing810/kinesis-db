use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use serde::{
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::{record::Record, value_type::ValueType};
use super::schema::TableSchema;

#[derive(Debug, Clone)]
pub struct Table {
    pub data: BTreeMap<u64, Arc<RwLock<Record>>>, // A map of record IDs to records
    pub schema: TableSchema,
}

// Custom serialization for Table
impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("schema", &self.schema)?;
        // Convert to regular records for serialization
        let records: BTreeMap<u64, Record> = self.data
            .iter()
            .map(|(k, v)| (*k, v.read().unwrap().clone()))
            .collect();
        map.serialize_entry("data", &records)?;
        map.end()
    }
}

// Custom deserialization for Table
impl<'de> Deserialize<'de> for Table {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TableData {
            schema: TableSchema,
            data: BTreeMap<u64, Record>,
        }

        let TableData { schema, data } = TableData::deserialize(deserializer)?;
        
        Ok(Table {
            data: data.into_iter()
                .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
                .collect(),
            schema,
        })
    }
}

impl Table {
    pub fn new(schema: TableSchema) -> Self {
        Table {
            data: BTreeMap::new(),
            schema,
        }
    }

    pub fn insert_record(&mut self, record: Record) -> Result<(), String> {
        // Validate record against schema
        self.schema.validate_record(&record.values)?;
        self.data.insert(record.id, Arc::new(RwLock::new(record)));
        Ok(())
    }

    pub fn get_record(&self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        // Get a record from the table
        self.data.get(id).cloned()
    }

    pub fn delete_record(&mut self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        // Remove a record from the table
        self.data.remove(id)
    }

    // Simple search function: we search for records where *any* String field matches a string query
    pub fn search_by_string(
        &self,
        query: &str,
        case_insensitive: bool,
    ) -> Vec<Arc<RwLock<Record>>> {
        self.data
            .values()
            .filter(|rec| {
                let record = rec.read().unwrap();
                record.values.values().any(|value| match value {
                    ValueType::Str(s) => {
                        s.contains(query)
                            || (case_insensitive
                                && s.to_lowercase().contains(&query.to_lowercase()))
                    }
                    _ => false,
                })
            })
            .cloned()
            .collect()
    }
}
