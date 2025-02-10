use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{db_type::DatabaseType, table::Table};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub db_type: DatabaseType, // Whether the database is in-memory or on-disk
    pub tables: BTreeMap<String, Table>, // The tables in the database
}
