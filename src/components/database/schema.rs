use super::value_type::ValueType;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: HashMap<String, FieldConstraint>,
}

impl FieldConstraint {
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
            if constraint.required {
                if !values.contains_key(field_name) {
                    return Err(format!("Required field '{}' is missing", field_name));
                }
            }

            if let Some(value) = values.get(field_name) {
                constraint.validate(value)?;
            }
        }

        Ok(())
    }
}
