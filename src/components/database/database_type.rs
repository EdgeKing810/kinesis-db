use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabaseType {
    OnDisk, // Store data on disk only
    Hybrid, // Store data on disk and in memory
}
