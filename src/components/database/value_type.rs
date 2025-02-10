use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValueType {
    Int(i64),    // Store integers as i64
    Float(f64),  // Store floats as f64
    Bool(bool),  // Store booleans as bool
    Str(String), // Store strings as String
}
