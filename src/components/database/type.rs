use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabaseType {
    OnDisk,   // Store data on disk only
    Hybrid,   // Store data on disk and in memory
    InMemory, // Store data in memory only
}
