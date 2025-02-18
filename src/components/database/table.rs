use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use serde::{ser::SerializeMap, Deserialize, Deserializer, Serialize, Serializer};

use super::schema::TableSchema;
use super::{record::Record, value_type::ValueType};

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
        let records: BTreeMap<u64, Record> = self
            .data
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
            data: data
                .into_iter()
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

    pub fn insert_record(&mut self, mut record: Record) -> Result<(), String> {
        // Add default values for missing fields
        for (field_name, constraint) in &self.schema.fields {
            if !record.values.contains_key(field_name) {
                if let Some(default_value) = &constraint.default {
                    record.set_field(field_name, default_value.clone());
                }
            }
        }

        // Validate the record (including fields with default values)
        self.schema.validate_record(&record.values)?;

        // Check unique constraints
        for (field_name, value) in &record.values {
            if let Some(constraint) = self.schema.fields.get(field_name) {
                if constraint.unique {
                    // Check if any existing record has the same value for this field
                    for existing_record in self.data.values() {
                        let existing = existing_record.read().unwrap();
                        if existing.id != record.id {
                            // Don't compare with self on updates
                            if let Some(existing_value) = existing.values.get(field_name) {
                                if existing_value == value
                                    && (constraint.default.is_none()
                                        || existing_value != constraint.default.as_ref().unwrap())
                                {
                                    return Err(format!(
                                        "Unique constraint violation: value already exists for field '{}'",
                                        field_name
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // If all validations pass, insert the record
        self.data.insert(record.id, Arc::new(RwLock::new(record)));
        Ok(())
    }

    pub fn update_record(
        &mut self,
        id: u64,
        timestamp: u64,
        updates: &HashMap<String, ValueType>,
    ) -> Result<(), String> {
        // First validate without holding any locks
        let record_arc = self
            .data
            .get(&id)
            .ok_or_else(|| format!("Record {} not found", id))?;

        let current_values = {
            let record = record_arc
                .read()
                .map_err(|_| "Failed to read record".to_string())?;
            record.values.clone()
        };

        // Create temporary copy with updates for validation
        let mut updated_values = current_values.clone();
        updated_values.extend(updates.iter().map(|(k, v)| (k.clone(), v.clone())));

        // Validate against schema
        self.schema.validate_record(&updated_values)?;

        // Check unique constraints without holding the write lock
        for (field_name, new_value) in updates {
            if let Some(constraint) = self.schema.fields.get(field_name) {
                if constraint.unique {
                    for other_record in self.data.values() {
                        let other = other_record.read().map_err(|_| {
                            "Failed to read record for uniqueness check".to_string()
                        })?;
                        if other.id != id
                            && other
                                .values
                                .get(field_name)
                                .map_or(false, |v| v == new_value)
                        {
                            return Err(format!(
                                "Unique constraint violation: value already exists for field '{}'",
                                field_name
                            ));
                        }
                    }
                }
            }
        }

        // Only now acquire the write lock and update
        let mut record = record_arc
            .write()
            .map_err(|_| "Failed to acquire write lock".to_string())?;

        record
            .values
            .extend(updates.iter().map(|(k, v)| (k.clone(), v.clone())));
        record.version += 1;
        record.timestamp = timestamp;

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

    pub fn update_schema(&mut self, new_schema: TableSchema) -> Result<(), String> {
        if new_schema.version <= self.schema.version {
            return Err("New schema version must be greater than the current version".to_string());
        }

        // Verify schema compatibility
        new_schema.can_migrate_from(&self.schema)?;

        // Collect all records for migration
        let mut records: Vec<_> = self
            .data
            .values()
            .map(|r| r.read().unwrap().clone())
            .collect();

        // Migrate each record
        for record in &mut records {
            new_schema.migrate_record(record)?;
        }

        // Verify unique constraints across all records
        for (field_name, constraint) in &new_schema.fields {
            if constraint.unique {
                let mut seen_values = HashMap::new();
                for record in &records {
                    if let Some(value) = record.values.get(field_name) {
                        if let Some(existing_id) = seen_values.insert(value.clone(), record.id) {
                            return Err(format!(
                                "Unique constraint violation for field '{}': duplicate value in records {} and {}",
                                field_name, existing_id, record.id
                            ));
                        }
                    }
                }
            }
        }

        // If all validations pass, update the schema and records
        self.schema = new_schema;
        self.data.clear();
        for record in records {
            self.data.insert(record.id, Arc::new(RwLock::new(record)));
        }

        Ok(())
    }
}
