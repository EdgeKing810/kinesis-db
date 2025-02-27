use super::{record::Record, value_type::ValueType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
}

impl FieldType {
    pub fn matches_value_type(&self, value: &ValueType) -> bool {
        matches!(
            (self, value),
            (FieldType::String, ValueType::Str(_))
                | (FieldType::Integer, ValueType::Int(_))
                | (FieldType::Float, ValueType::Float(_))
                | (FieldType::Float, ValueType::Int(_))
                | (FieldType::Boolean, ValueType::Bool(_))
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConstraint {
    pub field_type: FieldType,
    pub required: bool,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub pattern: Option<String>,
    pub unique: bool,
    pub default: Option<ValueType>, // New field
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: HashMap<String, FieldConstraint>,
    pub version: u32, // Add schema version
}

impl FieldConstraint {
    #[allow(dead_code)]
    pub fn create_required(field_type: FieldType) -> Self {
        FieldConstraint {
            field_type,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: None, // No default value
        }
    }

    #[allow(dead_code)]
    pub fn create_optional(field_type: FieldType) -> Self {
        FieldConstraint {
            field_type,
            required: false,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: None, // No default value
        }
    }

    pub fn validate(&self, value: &ValueType) -> Result<(), String> {
        // First check type matching
        if !self.field_type.matches_value_type(value) {
            return Err(format!(
                "Type mismatch: expected {:?}, got {:?}",
                self.field_type, value
            ));
        }

        // Then perform the specific validations
        match (value, &self.field_type) {
            (ValueType::Str(s), FieldType::String) => {
                if let Some(min) = self.min {
                    if s.len() < min.round() as usize {
                        return Err(format!("String length below minimum of {}", min));
                    }
                }
                if let Some(max) = self.max {
                    if s.len() > max.round() as usize {
                        return Err(format!("String length exceeds maximum of {}", max));
                    }
                }
                if let Some(pattern) = &self.pattern {
                    if !Regex::new(pattern)
                        .map_err(|e| format!("Invalid regex pattern: {}", e))?
                        .is_match(s)
                    {
                        return Err(format!("String doesn't match pattern: {}", pattern));
                    }
                }
            }
            (ValueType::Int(i), FieldType::Integer) => {
                if let Some(min) = self.min {
                    if (*i as f64) < min {
                        return Err(format!("Integer below minimum of {}", min));
                    }
                }
                if let Some(max) = self.max {
                    if (*i as f64) > max {
                        return Err(format!("Integer exceeds maximum of {}", max));
                    }
                }
            }
            (ValueType::Float(f), FieldType::Float) => {
                if let Some(min) = self.min {
                    if *f < min {
                        return Err(format!("Float below minimum of {}", min));
                    }
                }
                if let Some(max) = self.max {
                    if *f > max {
                        return Err(format!("Float exceeds maximum of {}", max));
                    }
                }
            }
            (ValueType::Int(i), FieldType::Float) => {
                if let Some(min) = self.min {
                    if (*i as f64) < min {
                        return Err(format!("Integer below minimum of {}", min));
                    }
                }
                if let Some(max) = self.max {
                    if (*i as f64) > max {
                        return Err(format!("Integer exceeds maximum of {}", max));
                    }
                }
            }
            (ValueType::Bool(_), FieldType::Boolean) => (),
            _ => unreachable!(), // Type matching was already done
        }
        Ok(())
    }
}

impl TableSchema {
    pub fn validate_record(
        &self,
        values: &HashMap<String, super::value_type::ValueType>,
    ) -> Result<(), String> {
        // First check for unknown fields
        for field_name in values.keys() {
            if !self.fields.contains_key(field_name) && self.fields.len() > 0 {
                return Err(format!(
                    "Field '{}' is not defined in the schema for table '{}'",
                    field_name, self.name
                ));
            }
        }

        // Then check for required fields and validate values
        for (field_name, constraint) in &self.fields {
            match values.get(field_name) {
                Some(value) => constraint.validate(value)?,
                None => {
                    if constraint.required && constraint.default.is_none() {
                        return Err(format!("Required field '{}' is missing", field_name));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn can_migrate_from(&self, old_schema: &TableSchema) -> Result<(), String> {
        if self.version <= old_schema.version {
            return Err("New schema version must be greater than the current version".to_string());
        }

        // Check compatibility between schemas
        for (field_name, new_constraint) in &self.fields {
            match old_schema.fields.get(field_name) {
                Some(old_constraint) => {
                    // Check if field type changed (not allowed)
                    if old_constraint.field_type != new_constraint.field_type {
                        return Err(format!(
                            "Cannot change type of existing field '{}'",
                            field_name
                        ));
                    }

                    // Check if field became required but has no default
                    if !old_constraint.required
                        && new_constraint.required
                        && new_constraint.default.is_none()
                    {
                        return Err(format!(
                            "Cannot make field '{}' required without default value",
                            field_name
                        ));
                    }

                    // Check if unique constraint was added
                    if !old_constraint.unique && new_constraint.unique {
                        return Err(format!(
                            "Cannot add unique constraint to existing field '{}' without validation",
                            field_name
                        ));
                    }
                }
                None => {
                    // New field must either be optional or have a default value
                    if new_constraint.required && new_constraint.default.is_none() {
                        return Err(format!(
                            "New required field '{}' must have a default value",
                            field_name
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn migrate_record(&self, record: &mut Record) -> Result<(), String> {
        // Remove fields that no longer exist in the schema
        record
            .values
            .retain(|field_name, _| self.fields.contains_key(field_name));

        // Add new fields with default values
        for (field_name, constraint) in &self.fields {
            if !record.values.contains_key(field_name) {
                if let Some(default_value) = &constraint.default {
                    record.set_field(field_name, default_value.clone());
                } else if !constraint.required {
                    // Field is optional, skip it
                    continue;
                } else {
                    return Err(format!(
                        "Missing required field '{}' with no default value",
                        field_name
                    ));
                }
            }
        }

        // Validate unique constraints
        self.validate_record(&record.values)?;

        Ok(())
    }
}
