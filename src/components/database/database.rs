use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{db_type::DatabaseType, table::Table};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub db_type: DatabaseType,
    // The tables in this database
    pub tables: BTreeMap<String, Table>,
}
