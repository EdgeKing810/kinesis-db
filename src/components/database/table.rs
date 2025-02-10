use std::{
    collections::BTreeMap,
    fmt::{self, Formatter},
    sync::{Arc, RwLock},
};

use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::{record::Record, value_type::ValueType};

#[derive(Debug, Clone)]
pub struct Table {
    pub data: BTreeMap<u64, Arc<RwLock<Record>>>, // A map of record IDs to records
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
                formatter.write_str("A map of record IDs to records")
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
        // Insert new a record into the table
        self.data.insert(record.id, Arc::new(RwLock::new(record)));
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
                record.values.iter().any(|value| match value {
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
