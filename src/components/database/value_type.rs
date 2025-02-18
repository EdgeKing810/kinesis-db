use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

// Implement PartialEq to handle float comparison
impl PartialEq for ValueType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => {
                // Handle NaN and regular float comparison
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    (a - b).abs() < f64::EPSILON
                }
            }
            _ => false,
        }
    }
}

// Implement Eq for ValueType
// This is safe because our PartialEq implementation
// handles float comparison in a consistent way
impl Eq for ValueType {}

// Since we're using ValueType as a key in HashMap (for uniqueness checks),
// we also need to implement Hash
impl Hash for ValueType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Str(s) => {
                0.hash(state);
                s.hash(state);
            }
            Self::Int(i) => {
                1.hash(state);
                i.hash(state);
            }
            Self::Bool(b) => {
                2.hash(state);
                b.hash(state);
            }
            Self::Float(f) => {
                3.hash(state);
                // Convert float to integer bits for consistent hashing
                f.to_bits().hash(state);
            }
        }
    }
}
