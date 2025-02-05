use page::PageStore;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::Duration;

mod buffer;
mod page;
mod tests;

use buffer::{BufferPool, DiskManager};

// Add transaction timeout configuration
const TX_TIMEOUT_SECS: u64 = 30; // 30 second timeout

// ========== Value and Record ==========

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueType {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: u64,
    pub values: Vec<ValueType>,
    pub version: u64,   // Add version for MVCC
    pub timestamp: u64, // Add timestamp for temporal queries
}

// ========== Table ==========

#[derive(Debug, Clone)]
pub struct Table {
    // For demonstration, we map from record ID to Record
    // BTreeMap is our "B-Tree" structure
    pub data: BTreeMap<u64, Arc<RwLock<Record>>>,
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
                formatter.write_str("a map of record IDs to records")
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
        self.data.insert(record.id, Arc::new(RwLock::new(record)));
    }

    pub fn get_record(&self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        self.data.get(id).cloned()
    }

    pub fn delete_record(&mut self, id: &u64) -> Option<Arc<RwLock<Record>>> {
        self.data.remove(id)
    }

    // Simple search function: we search for records where *any* field matches a string query
    pub fn search_by_string(&self, query: &str) -> Vec<Arc<RwLock<Record>>> {
        self.data
            .values()
            .filter(|rec| {
                let record = rec.read().unwrap();
                record.values.iter().any(|value| match value {
                    ValueType::Str(s) => s.contains(query),
                    _ => false,
                })
            })
            .cloned()
            .collect()
    }
}

// Define the WriteAheadLog structure
struct WriteAheadLog {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    tx_id: u64,
    timestamp: u64,
    checksum: u64,
    data: Vec<u8>,
    status: String,
}

impl WriteAheadLog {
    fn new(path: &str) -> Self {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        WriteAheadLog {
            path: path.to_string(),
        }
    }

    fn load_transactions(&self, policy: &RestorePolicy) -> Result<Vec<Transaction>, String> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Ok(Vec::new()),
        };

        // Early return for Discard policy
        if *policy == RestorePolicy::Discard {
            return Ok(Vec::new());
        }

        let reader = BufReader::new(file);
        let mut transactions = Vec::new();

        for line in reader.lines() {
            if let Ok(line) = line {
                let entry: serde_json::Value = serde_json::from_str(&line)
                    .map_err(|e| format!("Failed to parse WAL entry: {}", e))?;

                if *policy == RestorePolicy::RecoverPending
                    && entry["status"].as_str() != Some("pending")
                {
                    continue;
                }

                // Rest of transaction loading logic
                let mut tx = Transaction::new(
                    entry["tx_id"].as_u64().unwrap(),
                    IsolationLevel::Serializable,
                    None,
                );

                // Restore table creates
                if let Some(creates) = entry["table_creates"].as_array() {
                    tx.pending_table_creates = creates
                        .iter()
                        .map(|v| v.as_str().unwrap().to_string())
                        .collect();
                }

                // Restore table drops
                if let Some(drops) = entry["table_drops"].as_array() {
                    tx.pending_table_drops = drops
                        .iter()
                        .map(|v| v.as_str().unwrap().to_string())
                        .collect();
                }

                // Restore inserts
                if let Some(inserts) = entry["inserts"].as_array() {
                    for insert in inserts {
                        let table = insert[0].as_str().unwrap();
                        let record: Record = serde_json::from_value(insert[1].clone()).unwrap();
                        tx.pending_inserts.push((table.to_string(), record));
                    }
                }

                // Restore deletes
                if let Some(deletes) = entry["deletes"].as_array() {
                    for delete in deletes {
                        let table = delete[0].as_str().unwrap();
                        let id = delete[1].as_u64().unwrap();
                        let record: Record = serde_json::from_value(delete[2].clone()).unwrap();
                        tx.pending_deletes.push((table.to_string(), id, record));
                    }
                }

                // Add to write set
                for (table_name, record) in &tx.pending_inserts {
                    tx.write_set.push((table_name.clone(), record.id));
                }
                for (table_name, id, _) in &tx.pending_deletes {
                    tx.write_set.push((table_name.clone(), *id));
                }

                transactions.push(tx);
            }
        }

        println!(
            "Loaded {} transactions according to {:?} policy:",
            transactions.len(),
            policy
        );
        for tx in &transactions {
            println!(
                "  TX {}: {} creates, {} drops, {} inserts, {} deletes",
                tx.id,
                tx.pending_table_creates.len(),
                tx.pending_table_drops.len(),
                tx.pending_inserts.len(),
                tx.pending_deletes.len()
            );
        }
        Ok(transactions)
    }

    fn calculate_entry_checksum(entry: &serde_json::Value) -> u64 {
        // Create a copy without the checksum and status fields
        let mut entry = entry.as_object().unwrap().clone();
        entry.remove("checksum");
        entry.remove("status");

        // Sort the keys to ensure consistent ordering
        let mut sorted_entry = serde_json::Map::new();
        let mut keys: Vec<_> = entry.keys().collect();
        keys.sort();

        for key in keys {
            sorted_entry.insert(key.clone(), entry[key].clone());
        }

        // Calculate checksum from the sorted, filtered entry
        let mut hasher = Sha256::new();
        hasher.update(
            serde_json::Value::Object(sorted_entry)
                .to_string()
                .as_bytes(),
        );
        u64::from_be_bytes(hasher.finalize()[..8].try_into().unwrap())
    }

    fn log_transaction(&mut self, tx: &Transaction) -> Result<u64, String> {
        // Create initial entry
        let entry = json!({
            "tx_id": tx.id,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            "table_creates": &tx.pending_table_creates,
            "table_drops": &tx.pending_table_drops,
            "inserts": &tx.pending_inserts,
            "deletes": &tx.pending_deletes,
            "status": "pending"
        });

        // Calculate checksum
        let computed_checksum = Self::calculate_entry_checksum(&entry);

        // Add checksum to entry
        let mut entry = entry.as_object().unwrap().clone();
        entry.insert("checksum".to_string(), json!(computed_checksum));
        let entry = serde_json::Value::Object(entry);

        // Write to WAL
        let entry_str = serde_json::to_string(&entry)
            .map_err(|e| format!("Failed to serialize WAL entry to JSON: {}", e))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open WAL for appending: {}", e))?;

        writeln!(file, "{}", entry_str).map_err(|e| format!("Failed to write WAL entry: {}", e))?;

        file.flush()
            .map_err(|e| format!("Failed to flush WAL: {}", e))?;

        Ok(computed_checksum)
    }

    fn mark_transaction_complete(&mut self, tx_id: u64) -> Result<(), String> {
        let file = File::open(&self.path).map_err(|e| format!("Failed to open WAL: {}", e))?;
        let reader = BufReader::new(file);
        let mut content = Vec::new();
        let mut found = false;

        // Read all entries, modifying the status of the matching transaction
        for line in reader.lines() {
            if let Ok(line) = line {
                let mut entry: serde_json::Value = serde_json::from_str(&line)
                    .map_err(|e| format!("Failed to parse WAL entry: {}", e))?;

                if entry["tx_id"].as_u64() == Some(tx_id) {
                    // println!("Marking transaction {} as completed", tx_id);
                    entry["status"] = json!("completed");
                    found = true;
                }
                content.push(entry);
            }
        }

        if !found {
            return Err(format!("Transaction {} not found in WAL", tx_id));
        }

        // Write all entries back to the file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open WAL for writing: {}", e))?;

        let mut writer = BufWriter::new(file);
        for entry in content {
            writeln!(writer, "{}", entry.to_string())
                .map_err(|e| format!("Failed to write WAL entry: {}", e))?;
        }
        writer
            .flush()
            .map_err(|e| format!("Failed to flush WAL: {}", e))?;

        Ok(())
    }

    fn is_transaction_valid(&self, tx: &Transaction) -> Result<bool, String> {
        let file = File::open(&self.path).map_err(|e| format!("Failed to open WAL: {}", e))?;

        let reader = BufReader::new(file);

        for line in reader.lines() {
            if let Ok(line) = line {
                let entry: serde_json::Value = serde_json::from_str(&line)
                    .map_err(|e| format!("Failed to parse WAL entry: {}", e))?;

                if entry["tx_id"].as_u64() == Some(tx.id) {
                    let stored_checksum = entry["checksum"]
                        .as_u64()
                        .ok_or_else(|| "Invalid checksum in WAL entry".to_string())?;

                    let computed_checksum = Self::calculate_entry_checksum(&entry);

                    return Ok(computed_checksum == stored_checksum);
                }
            }
        }

        Ok(false)
    }

    fn sync(&self) -> Result<(), String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open WAL for writing: {}", e))?;

        file.sync_all()
            .map_err(|e| format!("Failed to sync WAL: {}", e))
    }

    fn rotate_log(&mut self) -> Result<(), String> {
        let backup_path = format!(
            "{}.{}",
            &self.path,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        // Create backup with timestamp
        std::fs::rename(&self.path, &backup_path)
            .map_err(|e| format!("Failed to create WAL backup: {}", e))?;

        // Create new log file
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to create new WAL: {}", e))?;

        // Keep backup for recovery if needed
        Ok(())
    }

    fn cleanup_completed_transactions(&mut self) -> Result<(), String> {
        let mut content = String::new();
        let file = File::open(&self.path).map_err(|e| format!("Failed to open WAL: {}", e))?;
        BufReader::new(file)
            .read_to_string(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        let completed_count = content
            .lines()
            .filter(|line| {
                let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                entry["status"].as_str() == Some("completed")
            })
            .count();

        // If we have too many completed transactions, rotate the log
        if completed_count > 100 {
            self.rotate_log()?;

            // Write only pending transactions to new log
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| format!("Failed to open new WAL: {}", e))?;

            let mut writer = BufWriter::new(&mut file);
            for line in content.lines() {
                let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                if entry["status"].as_str() == Some("pending") {
                    writeln!(writer, "{}", line)
                        .map_err(|e| format!("Failed to write to new WAL: {}", e))?;
                }
            }
            writer
                .flush()
                .map_err(|e| format!("Failed to flush WAL: {}", e))?;
        }

        Ok(())
    }
}

// ========== Database System ==========

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DatabaseType {
    OnDisk,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub db_type: DatabaseType,
    // The tables in this database
    pub tables: BTreeMap<String, Table>,
}

// Add isolation level enum
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Lowest isolation level. Allows:
    /// - Dirty reads (reading uncommitted changes)
    /// - Non-repeatable reads (same query may return different results)
    /// - Phantom reads (new records may appear in repeated queries)
    /// Use when maximum performance is needed and data consistency is less critical
    ReadUncommitted,

    /// Standard isolation level. Prevents:
    /// - Dirty reads (only sees committed data)
    /// Allows:
    /// - Non-repeatable reads
    /// - Phantom reads
    /// Good balance between performance and data consistency
    ReadCommitted,

    /// Higher isolation level. Prevents:
    /// - Dirty reads
    /// - Non-repeatable reads (guarantees same data when reading same records)
    /// Allows:
    /// - Phantom reads
    /// Use when queries must be consistent within a transaction
    RepeatableRead,

    /// Highest isolation level. Prevents:
    /// - Dirty reads
    /// - Non-repeatable reads
    /// - Phantom reads (guarantees consistent result set for repeated queries)
    /// Provides complete transaction isolation
    /// Use when absolute data consistency is required
    Serializable,
}

// A simplified transaction struct
#[derive(Serialize, Deserialize)]
pub struct Transaction {
    id: u64,
    isolation_level: IsolationLevel,
    pending_inserts: Vec<(String, Record)>,
    pending_deletes: Vec<(String, u64, Record)>,
    read_set: Vec<(String, u64, u64)>, // (table, record_id, version)
    write_set: Vec<(String, u64)>,     // (table, record_id)
    snapshot: Option<Database>,
    start_timestamp: u64,
    pending_table_creates: Vec<String>,
    pending_table_drops: Vec<String>,
}

impl Transaction {
    pub fn new(id: u64, isolation_level: IsolationLevel, snapshot: Option<Database>) -> Self {
        Transaction {
            id,
            isolation_level,
            pending_inserts: Vec::new(),
            pending_deletes: Vec::new(),
            read_set: Vec::new(),
            write_set: Vec::new(),
            snapshot,
            start_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            pending_table_creates: Vec::new(),
            pending_table_drops: Vec::new(),
        }
    }
}

// Add a transaction manager to handle concurrent transactions
struct TransactionManager {
    active_transactions: HashMap<u64, (std::time::SystemTime, Transaction)>,
    locks: HashMap<(String, u64), u64>, // (table, id) -> tx_id
    wait_for_graph: HashMap<u64, HashSet<u64>>, // Changed Vec to HashSet
}

impl TransactionManager {
    fn new() -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            locks: HashMap::new(),
            wait_for_graph: HashMap::new()
        }
    }

    fn start_transaction(&mut self, tx_id: u64) {
        self.active_transactions
            .insert(tx_id, (std::time::SystemTime::now(), Transaction::new(tx_id, IsolationLevel::Serializable, None)));
    }

    fn end_transaction(&mut self, tx_id: u64) {
        // Remove all locks held by this transaction
        self.locks.retain(|_, &mut holding_tx| holding_tx != tx_id);
        
        // Remove from active transactions
        self.active_transactions.remove(&tx_id);
        
        // Remove from wait-for graph
        self.wait_for_graph.remove(&tx_id);
        
        // Remove edges pointing to this transaction
        for edges in self.wait_for_graph.values_mut() {
            edges.remove(&tx_id);
        }
    }

    fn acquire_lock(&mut self, tx_id: u64, table_name: &str, record_id: u64) -> bool {
        let key = (table_name.to_string(), record_id);

        if let Some(&holding_tx) = self.locks.get(&key) {
            // If we already hold this lock, return true
            if holding_tx == tx_id {
                return true;
            }

            // If lock is held by another transaction
            // Check for deadlock immediately
            if self.has_deadlock(tx_id) {
                return false;
            } else {
                // Add edge to wait-for graph
                self.wait_for_graph
                .entry(tx_id)
                .or_insert_with(HashSet::new)
                .insert(holding_tx);
            }
            return false;
        }

        // If we get here, we can acquire the lock
        self.locks.insert(key, tx_id);
        true
    }

    fn release_lock(&mut self, tx_id: u64, table_name: &str, record_id: u64) {
        let key = (table_name.to_string(), record_id);
        
        if let Some(&holding_tx) = self.locks.get(&key) {
            if holding_tx == tx_id {
                self.locks.remove(&key);
                
                // Remove this transaction from the wait-for graph
                self.wait_for_graph.remove(&tx_id);
                
                // Remove edges pointing to this transaction
                for edges in self.wait_for_graph.values_mut() {
                    edges.remove(&tx_id);
                }
            }
        }
    }

    fn is_transaction_expired(&self, tx_id: u64) -> bool {
        if let Some((start_time, _)) = self.active_transactions.get(&tx_id) {
            if let Ok(elapsed) = start_time.elapsed() {
                return elapsed > Duration::from_secs(TX_TIMEOUT_SECS);
            }
        }
        false
    }

    fn cleanup_expired_transactions(&mut self) {
        let expired: Vec<_> = self
            .active_transactions
            .iter()
            .filter(|(_, &(start_time, _))| {
                start_time
                    .elapsed()
                    .map(|e| e > Duration::from_secs(TX_TIMEOUT_SECS))
                    .unwrap_or(true)
            })
            .map(|(&tx_id, _)| tx_id)
            .collect();

        for tx_id in expired {
            self.end_transaction(tx_id);
        }
    }

    fn has_deadlock(&self, tx_id: u64) -> bool {
        let mut visited = HashSet::new();
        let mut path = HashSet::new();
        
        fn detect_cycle(
            graph: &HashMap<u64, HashSet<u64>>,
            current: u64,
            visited: &mut HashSet<u64>,
            path: &mut HashSet<u64>,
        ) -> bool {
            if !visited.contains(&current) {
                visited.insert(current);
                path.insert(current);

                if let Some(neighbors) = graph.get(&current) {
                    for &next in neighbors {
                        if !visited.contains(&next) {
                            if detect_cycle(graph, next, visited, path) {
                                return true;
                            }
                        } else if path.contains(&next) {
                            return true;
                        }
                    }
                }
            }
            path.remove(&current);
            false
        }

        detect_cycle(&self.wait_for_graph, tx_id, &mut visited, &mut path)
    }
}

pub struct DBEngine {
    // The actual database data (in memory)
    db: Arc<RwLock<Database>>,
    // Path to the disk file
    file_path: String,
    // The Write Ahead Log
    wal: WriteAheadLog,
    // The transaction manager
    tx_manager: TransactionManager,
    // The page store
    page_store: PageStore,
    // The buffer pool
    buffer_pool: Arc<Mutex<BufferPool>>,
    // The restore policy for the WAL
    restore_policy: RestorePolicy,
}

// Add a restore policy enum
#[derive(Debug, PartialEq)]
pub enum RestorePolicy {
    RecoverPending,
    RecoverAll,
    Discard,
}

// Add a CommitGuard to ensure proper cleanup
struct CommitGuard<'a> {
    engine: &'a mut DBEngine,
    tx: Option<Transaction>,
    success: bool,
}

impl<'a> CommitGuard<'a> {
    fn new(engine: &'a mut DBEngine, tx: Transaction) -> Self {
        CommitGuard {
            engine,
            tx: Some(tx),
            success: false,
        }
    }

    fn commit(mut self) -> Result<(), String> {
        if let Some(tx) = self.tx.take() {
            let result = self.engine.commit_internal(tx);
            self.success = result.is_ok();
            result
        } else {
            Err("Transaction already consumed".to_string())
        }
    }
}

impl<'a> Drop for CommitGuard<'a> {
    fn drop(&mut self) {
        if !self.success {
            if let Some(tx) = self.tx.take() {
                if let Err(e) = self.engine.rollback_failed_commit(&tx) {
                    eprintln!("Failed to rollback transaction: {}", e);
                }
            }
        }
    }
}

impl DBEngine {
    pub fn new(
        db_type: DatabaseType,
        restore_policy: RestorePolicy,
        file_path: &str,
        wal_path: &str,
    ) -> Self {
        let db = Database {
            db_type,
            tables: BTreeMap::new(),
        };

        let mut engine = DBEngine {
            db: Arc::new(RwLock::new(db)),
            file_path: file_path.to_string(),
            wal: WriteAheadLog::new(wal_path),
            tx_manager: TransactionManager::new(),
            page_store: PageStore::new(&format!("{}.pages", file_path)).unwrap(),
            buffer_pool: Arc::new(Mutex::new(BufferPool::new(1000))),
            restore_policy,
        };

        // First try to recover from any previous crash
        if let Err(e) = engine.recover_from_crash() {
            eprintln!("Warning: Recovery failed: {}", e);
        }

        // Then load from disk (this will give us the latest committed state)
        engine.load_from_disk();

        engine
    }

    // ========== Transaction Management ==========

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Transaction {
        let tx_id = rand::random::<u64>();
        self.tx_manager.start_transaction(tx_id);

        let snapshot = match isolation_level {
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                Some(self.db.read().unwrap().clone())
            }
            _ => None,
        };

        Transaction::new(tx_id, isolation_level, snapshot)
    }

    fn validate_transaction(&self, tx: &Transaction) -> Result<(), String> {
        let db_read = self.db.read().unwrap();

        match tx.isolation_level {
            IsolationLevel::ReadUncommitted => Ok(()), // No validation needed

            IsolationLevel::ReadCommitted => {
                // Ensure no writes have occurred to records we've read since we last read them
                for (table_name, record_id, version) in &tx.read_set {
                    if let Some(table) = db_read.tables.get(table_name) {
                        if let Some(record_arc) = table.get_record(record_id) {
                            let record = record_arc.read().unwrap();
                            if record.version > *version {
                                return Err("Read committed violation: record modified".to_string());
                            }
                        }
                    }
                }
                Ok(())
            }

            IsolationLevel::RepeatableRead => {
                // Ensure no modifications to our read set
                if !self.validate_repeatable_read(&tx, &db_read) {
                    return Err("Repeatable read violation detected".to_string());
                }
                Ok(())
            }

            IsolationLevel::Serializable => {
                // Check both read and write sets against all concurrent transactions
                if !self.validate_serializable(&tx, &db_read) {
                    return Err("Serialization conflict detected".to_string());
                }
                Ok(())
            }
        }
    }

    fn validate_repeatable_read(&self, tx: &Transaction, current_db: &Database) -> bool {
        // For repeatable read, we need to ensure that:
        // 1. All records we've read still exist
        // 2. None of the records we've read have been modified
        for (table_name, record_id, version) in &tx.read_set {
            if let Some(table) = current_db.tables.get(table_name) {
                match table.get_record(record_id) {
                    Some(current_record) => {
                        let record = current_record.read().unwrap();
                        // If the version has changed, validation fails
                        if record.version != *version {
                            return false;
                        }
                    }
                    None => {
                        // If the record has been deleted, validation fails
                        return false;
                    }
                }
            } else {
                // If the table has been dropped, validation fails
                return false;
            }
        }
        true
    }

    fn validate_serializable(&self, tx: &Transaction, current_db: &Database) -> bool {
        // For serializable isolation, we need to ensure:
        // 1. All repeatable read conditions are met
        // 2. No phantom reads are possible (check write set against snapshot)
        // 3. No write-write conflicts

        // First, check repeatable read conditions
        if !self.validate_repeatable_read(tx, current_db) {
            return false;
        }

        // Check for write-write conflicts
        for (table_name, record_id) in &tx.write_set {
            if let Some(table) = current_db.tables.get(table_name) {
                if let Some(current_record) = table.get_record(record_id) {
                    let record = current_record.read().unwrap();
                    // If the record was modified after our transaction started
                    if record.timestamp > tx.start_timestamp {
                        return false;
                    }
                }
            }
        }

        // Check for phantom reads by comparing the snapshot with current state
        if let Some(snapshot) = &tx.snapshot {
            for (table_name, table) in &current_db.tables {
                let snapshot_table = snapshot.tables.get(table_name);

                // If this table exists in our snapshot
                if let Some(snap_table) = snapshot_table {
                    // Check if any records were added or removed
                    if table.data.len() != snap_table.data.len() {
                        return false;
                    }

                    // Check if any records in our read set were modified
                    for (id, record) in &table.data {
                        if let Some(snap_record) = snap_table.data.get(id) {
                            let current_rec = record.read().unwrap();
                            let snap_rec = snap_record.read().unwrap();
                            if current_rec.version != snap_rec.version {
                                return false;
                            }
                        } else {
                            // Record exists now but didn't in our snapshot
                            return false;
                        }
                    }
                } else if !table.data.is_empty() {
                    // New table with data was created during our transaction
                    return false;
                }
            }
        }

        true
    }

    pub fn commit(&mut self, tx: Transaction) -> Result<(), String> {
        CommitGuard::new(self, tx).commit()
    }

    fn commit_internal(&mut self, tx: Transaction) -> Result<(), String> {
        // Check for transaction timeout
        if self.tx_manager.is_transaction_expired(tx.id) {
            self.tx_manager.end_transaction(tx.id);
            return Err("Transaction timeout".to_string());
        }

        // Cleanup any expired transactions
        self.tx_manager.cleanup_expired_transactions();

        // Check for deadlocks before proceeding
        if self.tx_manager.has_deadlock(tx.id) {
            self.tx_manager.end_transaction(tx.id);
            return Err("Deadlock detected".to_string());
        }

        // Acquire locks for all writes
        for (table_name, id) in &tx.write_set {
            if !self.tx_manager.acquire_lock(tx.id, table_name, *id) {
                self.tx_manager.end_transaction(tx.id);
                return Err("Failed to acquire lock".to_string());
            }
        }

        let result = (|| {
            // Add table existence validation
            {
                let db_read = self.db.read().unwrap();
                for (table_name, _) in &tx.pending_inserts {
                    if !db_read.tables.contains_key(table_name) {
                        return Err(format!("Table '{}' does not exist", table_name));
                    }
                }
            }

            self.validate_transaction(&tx)?;

            let mut db_lock = self.db.write().unwrap();
            let current_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Now pass ownership of the lock guard
            self.apply_transaction_changes(&mut db_lock, &tx, current_timestamp)?;

            // Calculate checksum after changes
            let checksum = self.calculate_checksum(&db_lock)?;

            // Log to WAL using bincode
            self.wal.log_transaction(&tx)?;
            self.wal.sync()?;

            // Persist changes if needed
            if db_lock.db_type == DatabaseType::OnDisk || db_lock.db_type == DatabaseType::Hybrid {
                drop(db_lock);
                self.save_to_disk_with_verification(checksum)?;
            }

            // Mark transaction as complete
            self.wal.mark_transaction_complete(tx.id)?;

            // Cleanup WAL periodically (every 100 transactions)
            if let Err(e) = self.wal.cleanup_completed_transactions() {
                eprintln!("Warning: WAL cleanup failed: {}", e);
            }

            Ok(())
        })();

        // Release locks and end transaction
        for (table_name, id) in &tx.write_set {
            self.tx_manager.release_lock(tx.id, table_name, *id);
        }
        self.tx_manager.end_transaction(tx.id);

        result
    }

    fn rollback_failed_commit(&mut self, tx: &Transaction) -> Result<(), String> {
        let result = {
            let mut db_lock = self.db.write().unwrap();

            // Restore original state for modified records
            for (table_name, id) in &tx.write_set {
                if let Some(snapshot_record) = tx
                    .snapshot
                    .as_ref()
                    .and_then(|s| s.tables.get(table_name))
                    .and_then(|t| t.get_record(id))
                {
                    if let Some(table) = db_lock.tables.get_mut(table_name) {
                        table.insert_record(snapshot_record.read().unwrap().clone());
                    }
                }
            }
            Ok(())
        };

        // Release locks and end transaction
        for (table_name, id) in &tx.write_set {
            self.tx_manager.release_lock(tx.id, table_name, *id);
        }
        self.tx_manager.end_transaction(tx.id);

        result
    }

    fn apply_transaction_changes(
        &self,
        db_lock: &mut RwLockWriteGuard<'_, Database>,
        tx: &Transaction,
        timestamp: u64,
    ) -> Result<(), String> {
        // Track pages to be freed
        let mut pages_to_free = Vec::new();

        // Handle table drops first
        for table_name in &tx.pending_table_drops {
            if let Some(table_locations) = self.get_table_page_ids(table_name) {
                pages_to_free.extend(table_locations);
            }
            db_lock.tables.remove(table_name);
        }

        // Handle deletes
        for (table_name, id, _) in &tx.pending_deletes {
            if let Some(tbl) = db_lock.tables.get_mut(table_name) {
                if let Some(page_id) = self.get_record_page_id(table_name, *id) {
                    pages_to_free.push(page_id);
                }
                tbl.delete_record(id);
            }
        }

        // Free the collected pages
        for page_id in pages_to_free {
            self.page_store.free_page(page_id);
        }

        // Handle remaining changes
        for table_name in &tx.pending_table_creates {
            db_lock
                .tables
                .entry(table_name.clone())
                .or_insert(Table::new());
        }

        for (table_name, mut record) in tx.pending_inserts.clone() {
            record.version += 1;
            record.timestamp = timestamp;
            let tbl = db_lock.tables.entry(table_name).or_insert(Table::new());
            tbl.insert_record(record);
        }

        Ok(())
    }

    fn get_table_page_ids(&self, table_name: &str) -> Option<Vec<u64>> {
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        let toc_frame = buffer_pool.get_page(0, &self.page_store);
        let toc = toc_frame.get_page().read().unwrap(); // Updated this line
        let table_locations: HashMap<String, Vec<u64>> =
            bincode::deserialize(&toc.data).unwrap_or_default();
        table_locations.get(table_name).cloned()
    }

    // Helper method to get page ID for a record
    fn get_record_page_id(&self, table_name: &str, record_id: u64) -> Option<u64> {
        if let Some(page_ids) = self.get_table_page_ids(table_name) {
            let mut buffer_pool = self.buffer_pool.lock().unwrap();
            for page_id in page_ids {
                let frame = buffer_pool.get_page(page_id, &self.page_store);
                let page_data = frame.get_page().read().unwrap(); // Updated this line
                let records: Vec<Record> =
                    bincode::deserialize(&page_data.data).unwrap_or_default();
                if records.iter().any(|r| r.id == record_id) {
                    return Some(page_id);
                }
            }
        }
        None
    }

    fn get_record(&self, tx: &mut Transaction, table_name: &str, id: u64) -> Option<Record> {
        // Check for transaction timeout
        if self.tx_manager.is_transaction_expired(tx.id) {
            return None;
        }

        // First check if this record is in our write set
        if tx.write_set.contains(&(table_name.to_string(), id)) {
            // Check pending inserts first
            if let Some((_, record)) = tx
                .pending_inserts
                .iter()
                .find(|(t, r)| t == table_name && r.id == id)
            {
                return Some(record.clone());
            }
            // If not in pending inserts, it might be deleted
            if tx
                .pending_deletes
                .iter()
                .any(|(t, rid, _)| t == table_name && *rid == id)
            {
                return None;
            }
        }

        let db_lock = self.db.read().unwrap();

        let record = match tx.isolation_level {
            IsolationLevel::ReadUncommitted => self.get_latest_record(&db_lock, table_name, id),
            IsolationLevel::ReadCommitted => self.get_committed_record(&db_lock, table_name, id),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                if let Some(snapshot) = &tx.snapshot {
                    self.get_snapshot_record(snapshot, table_name, id)
                } else {
                    self.get_committed_record(&db_lock, table_name, id)
                }
            }
        };

        if let Some(rec) = &record {
            // Only track reads for records we haven't written to
            if !tx.write_set.contains(&(table_name.to_string(), id)) {
                tx.read_set.push((table_name.to_string(), id, rec.version));
            }
        }

        record
    }

    fn get_latest_record(&self, db: &Database, table_name: &str, id: u64) -> Option<Record> {
        db.tables
            .get(table_name)
            .and_then(|tbl| tbl.get_record(&id))
            .map(|arc| arc.read().unwrap().clone())
    }

    fn get_committed_record(&self, db: &Database, table_name: &str, id: u64) -> Option<Record> {
        // Only return records that have been committed (have a timestamp)
        db.tables
            .get(table_name)
            .and_then(|tbl| tbl.get_record(&id))
            .map(|arc| arc.read().unwrap().clone())
            .filter(|rec| rec.timestamp > 0)
    }

    fn get_snapshot_record(
        &self,
        snapshot: &Database,
        table_name: &str,
        id: u64,
    ) -> Option<Record> {
        snapshot
            .tables
            .get(table_name)
            .and_then(|tbl| tbl.get_record(&id))
            .map(|arc| arc.read().unwrap().clone())
    }

    // ========== Database / Table Management ==========

    pub fn create_table(&self, tx: &mut Transaction, table_name: &str) {
        tx.pending_table_creates.push(table_name.to_string());
    }

    pub fn drop_table(&self, tx: &mut Transaction, table_name: &str) {
        tx.pending_table_drops.push(table_name.to_string());
    }

    // ========== Record Operations (within a transaction) ==========

    pub fn insert_record(&self, tx: &mut Transaction, table_name: &str, record: Record) {
        // First check if the table exists
        let db_lock = self.db.read().unwrap();
        if !db_lock.tables.contains_key(table_name) {
            // Add the insert to pending_inserts anyway so that commit will fail
            tx.pending_inserts.push((table_name.to_string(), record));
            return;
        }
        drop(db_lock);

        // Track the write in the transaction's write set
        tx.write_set.push((table_name.to_string(), record.id));
        // Store the insert in the transaction
        tx.pending_inserts.push((table_name.to_string(), record));
    }

    pub fn delete_record(&mut self, tx: &mut Transaction, table_name: &str, id: u64) {
        // First try to acquire the lock
        if !self.tx_manager.acquire_lock(tx.id, table_name, id) {
            // If we can't acquire the lock, check for deadlock
            if self.tx_manager.has_deadlock(tx.id) {
                // If there's a deadlock, mark the transaction as failed
                tx.write_set.push((table_name.to_string(), id));  // Add to write set so commit will fail
                return;
            }
            // Wait for lock (in real system this would be async)
            std::thread::sleep(std::time::Duration::from_millis(10));
            if !self.tx_manager.acquire_lock(tx.id, table_name, id) {
                return; // Give up if still can't acquire lock
            }
        }

        let db_lock = self.db.read().unwrap();
        if let Some(tbl) = db_lock.tables.get(table_name) {
            if let Some(old_rec_arc) = tbl.get_record(&id) {
                let old_rec = old_rec_arc.read().unwrap();
                // Track the write in the transaction's write set
                tx.write_set.push((table_name.to_string(), id));
                // Store the delete in the transaction
                tx.pending_deletes.push((table_name.to_string(), id, old_rec.clone()));
            }
        }
    }

    // Make search operation transactional
    pub fn search_records(
        &self,
        tx: &mut Transaction,
        table_name: &str,
        query: &str,
    ) -> Vec<Record> {
        let db_lock = self.db.read().unwrap();

        match tx.isolation_level {
            IsolationLevel::ReadUncommitted => self.search_latest(&db_lock, table_name, query),
            IsolationLevel::ReadCommitted => self.search_committed(&db_lock, table_name, query),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                if let Some(snapshot) = &tx.snapshot {
                    self.search_snapshot(snapshot, table_name, query)
                } else {
                    self.search_committed(&db_lock, table_name, query)
                }
            }
        }
    }

    // Helper methods for search
    fn search_latest(&self, db: &Database, table_name: &str, query: &str) -> Vec<Record> {
        if let Some(table) = db.tables.get(table_name) {
            table
                .search_by_string(query)
                .into_iter()
                .map(|arc| arc.read().unwrap().clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn search_committed(&self, db: &Database, table_name: &str, query: &str) -> Vec<Record> {
        if let Some(table) = db.tables.get(table_name) {
            table
                .search_by_string(query)
                .into_iter()
                .filter_map(|arc| {
                    let record = arc.read().unwrap();
                    if record.timestamp > 0 {
                        Some(record.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    fn search_snapshot(&self, snapshot: &Database, table_name: &str, query: &str) -> Vec<Record> {
        if let Some(table) = snapshot.tables.get(table_name) {
            table
                .search_by_string(query)
                .into_iter()
                .map(|arc| arc.read().unwrap().clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    // ========== Disk Persistence ==========

    fn load_from_disk(&mut self) {
        if !Path::new(&format!("{}.pages", self.file_path)).exists() {
            return;
        }

        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        let mut db_lock = self.db.write().unwrap();

        // Read TOC
        let toc_frame = buffer_pool.get_page(0, &self.page_store);
        let table_locations: HashMap<String, Vec<u64>> = {
            let toc = toc_frame.get_page().read().unwrap();
            bincode::deserialize(&toc.data).unwrap_or_default()
        };
        buffer_pool.unpin_page(0, false);

        // Load each table
        for (table_name, page_ids) in table_locations {
            let mut table = Table::new();

            for page_id in page_ids {
                let frame = buffer_pool.get_page(page_id, &self.page_store);
                let records: Vec<Record> = {
                    let page = frame.get_page().read().unwrap();
                    bincode::deserialize(&page.data).unwrap_or_default()
                };

                for record in records {
                    table.insert_record(record);
                }

                buffer_pool.unpin_page(page_id, false);
            }

            db_lock.tables.insert(table_name, table);
        }
    }

    fn save_to_disk(&mut self) -> Result<u64, String> {
        let db_lock = self.db.read().unwrap();
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        let mut table_locations = HashMap::new();

        // Reduce chunk size to fit within page size
        let chunk_size = 50; // Reduced from 100

        for (table_name, table) in &db_lock.tables {
            let mut page_ids = Vec::new();
            let records: Vec<Record> = table
                .data
                .values()
                .map(|r| r.read().unwrap().clone())
                .collect();

            // Process chunks with smaller size
            for chunk in records.chunks(chunk_size) {
                let page_id = self
                    .page_store
                    .allocate_page()
                    .map_err(|e| format!("Failed to allocate page: {}", e))?;

                let frame = buffer_pool.get_page(page_id, &self.page_store);
                {
                    let serialized = bincode::serialize(chunk)
                        .map_err(|e| format!("Failed to serialize records: {}", e))?;

                    let mut page = frame.get_page().write().unwrap();
                    page.write_data(0, &serialized)
                        .map_err(|e| format!("Failed to write data: {}", e))?;
                }

                page_ids.push(page_id);
                buffer_pool.unpin_page(page_id, true);
            }

            if !page_ids.is_empty() {
                table_locations.insert(table_name.clone(), page_ids);
            }
        }

        // Write table of contents
        let toc_frame = buffer_pool.get_page(0, &self.page_store);
        {
            let serialized = bincode::serialize(&table_locations)
                .map_err(|e| format!("Failed to serialize TOC: {}", e))?;

            let mut toc = toc_frame.get_page().write().unwrap();
            toc.write_data(0, &serialized)
                .map_err(|e| format!("Failed to write TOC: {}", e))?;
        }

        buffer_pool.unpin_page(0, true);
        buffer_pool.flush_all(&self.page_store);

        let checksum = self.calculate_checksum(&*db_lock)?;
        Ok(checksum)
    }

    fn save_to_disk_with_verification(&mut self, expected_checksum: u64) -> Result<(), String> {
        let actual_checksum = self.save_to_disk()?;

        if actual_checksum != expected_checksum {
            return Err(format!(
                "Checksum verification failed. Expected: {}, Got: {}",
                expected_checksum, actual_checksum
            ));
        }

        Ok(())
    }

    // Add recovery handling
    fn recover_from_crash(&mut self) -> Result<(), String> {
        // Load database from disk first
        self.load_from_disk();

        // Now using the consolidated load_transactions method
        let transactions = self.wal.load_transactions(&self.restore_policy)?;

        // Rest of the recovery process remains the same
        // Apply each valid transaction
        for tx in transactions {
            if self.wal.is_transaction_valid(&tx)? {
                // println!("Recovering transaction {}", tx.id);

                // First handle table creations
                if !tx.pending_table_creates.is_empty() {
                    let mut db_lock = self.db.write().unwrap();
                    for table_name in &tx.pending_table_creates {
                        db_lock
                            .tables
                            .entry(table_name.clone())
                            .or_insert_with(Table::new);
                    }
                }

                // Then handle the records
                let mut db_lock = self.db.write().unwrap();
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                self.apply_transaction_changes(&mut db_lock, &tx, timestamp)?;

                // Save changes to disk immediately
                drop(db_lock);
                self.save_to_disk()?;

                // Mark as complete only after successful save
                self.wal.mark_transaction_complete(tx.id)?;

                println!("Successfully recovered transaction {}", tx.id);
            } else {
                println!("Skipping invalid transaction {}", tx.id);
            }
        }

        Ok(())
    }

    pub fn rollback(&mut self, tx: Transaction) -> Result<(), String> {
        self.rollback_failed_commit(&tx)?;
        Ok(())
    }

    fn calculate_checksum(&self, data: &Database) -> Result<u64, String> {
        let serialized =
            bincode::serialize(data).map_err(|e| format!("Failed to serialize DB: {}", e))?;
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        Ok(u64::from_be_bytes(
            hasher.finalize()[..8].try_into().unwrap(),
        ))
    }
}

// ========== Test / Example Usage ==========

fn main() {
    create_dir_all("data").unwrap();
    let engine_ondisk = DBEngine::new(
        DatabaseType::OnDisk,
        RestorePolicy::RecoverPending,
        "data/test_db",
        "data/wal.log",
    );
    let mut engine = engine_ondisk;
    let isolation_level = IsolationLevel::Serializable;

    println!("\n🚀 Initializing Database...");
    // Create data directory

    // Create tables
    let mut tx = engine.begin_transaction(isolation_level);
    engine.create_table(&mut tx, "users");
    engine.create_table(&mut tx, "animals");
    engine.commit(tx).unwrap();
    println!("✅ Tables created successfully");

    // Insert users with more properties
    let mut tx = engine.begin_transaction(isolation_level);
    let users = vec![
        (
            1,
            "Alice",
            30,
            "Senior Developer",
            95000.0,
            true,
            "Engineering",
            "Remote",
        ),
        (
            2,
            "Bob",
            25,
            "UI Designer",
            75000.0,
            false,
            "Design",
            "Office",
        ),
        (
            3,
            "Charlie",
            35,
            "Product Manager",
            120000.0,
            true,
            "Management",
            "Hybrid",
        ),
        (
            4,
            "Diana",
            28,
            "Data Scientist",
            98000.0,
            true,
            "Analytics",
            "Remote",
        ),
        (
            5,
            "Eve",
            32,
            "DevOps Engineer",
            105000.0,
            true,
            "Infrastructure",
            "Remote",
        ),
        (6, "Frank", 41, "CTO", 180000.0, true, "Executive", "Office"),
        (
            7,
            "Grace",
            27,
            "QA Engineer",
            72000.0,
            false,
            "Quality",
            "Hybrid",
        ),
        (
            8,
            "Henry",
            38,
            "Solutions Architect",
            125000.0,
            true,
            "Engineering",
            "Remote",
        ),
    ];

    for (id, name, age, role, salary, senior, department, work_mode) in users {
        let record = Record {
            id,
            values: vec![
                ValueType::Str(name.to_string()),
                ValueType::Int(age),
                ValueType::Str(role.to_string()),
                ValueType::Float(salary),
                ValueType::Bool(senior),
                ValueType::Str(department.to_string()),
                ValueType::Str(work_mode.to_string()),
            ],
            version: 0,
            timestamp: 0,
        };
        engine.insert_record(&mut tx, "users", record);
    }
    engine.commit(tx).unwrap();
    println!("✅ Users data inserted");

    // Insert diverse animals
    let mut tx = engine.begin_transaction(isolation_level);
    let animals = vec![
        // Dogs
        (
            1,
            "Max",
            "Dog",
            5,
            15.5,
            true,
            "Golden Retriever",
            "Friendly",
            "Medium",
            true,
            "USA",
        ),
        (
            2,
            "Luna",
            "Dog",
            3,
            30.2,
            true,
            "German Shepherd",
            "Protective",
            "Large",
            false,
            "Germany",
        ),
        (
            3,
            "Rocky",
            "Dog",
            2,
            8.5,
            true,
            "French Bulldog",
            "Playful",
            "Small",
            true,
            "France",
        ),
        // Cats
        (
            4, "Bella", "Cat", 1, 3.8, true, "Persian", "Lazy", "Medium", true, "Iran",
        ),
        (
            5,
            "Oliver",
            "Cat",
            4,
            4.2,
            true,
            "Maine Coon",
            "Independent",
            "Large",
            false,
            "USA",
        ),
        (
            6, "Lucy", "Cat", 2, 3.5, false, "Siamese", "Active", "Small", true, "Thailand",
        ),
        // Exotic Pets
        (
            7,
            "Ziggy",
            "Parrot",
            15,
            0.4,
            true,
            "African Grey",
            "Talkative",
            "Medium",
            false,
            "Congo",
        ),
        (
            8,
            "Monty",
            "Snake",
            3,
            2.0,
            false,
            "Ball Python",
            "Calm",
            "Medium",
            true,
            "Africa",
        ),
        (
            9,
            "Spike",
            "Lizard",
            2,
            0.3,
            true,
            "Bearded Dragon",
            "Friendly",
            "Small",
            true,
            "Australia",
        ),
        // Farm Animals
        (
            10, "Thunder", "Horse", 8, 450.0, true, "Arabian", "Spirited", "Large", false, "Arabia",
        ),
        (
            11,
            "Wooley",
            "Sheep",
            3,
            80.0,
            true,
            "Merino",
            "Gentle",
            "Medium",
            true,
            "Australia",
        ),
        (
            12,
            "Einstein",
            "Pig",
            1,
            120.0,
            true,
            "Vietnamese Pot-Belly",
            "Smart",
            "Medium",
            true,
            "Vietnam",
        ),
    ];

    for (
        id,
        name,
        species,
        age,
        weight,
        vaccinated,
        breed,
        temperament,
        size,
        house_trained,
        origin,
    ) in animals
    {
        let record = Record {
            id,
            values: vec![
                ValueType::Str(name.to_string()),
                ValueType::Str(species.to_string()),
                ValueType::Int(age),
                ValueType::Float(weight),
                ValueType::Bool(vaccinated),
                ValueType::Str(breed.to_string()),
                ValueType::Str(temperament.to_string()),
                ValueType::Str(size.to_string()),
                ValueType::Bool(house_trained),
                ValueType::Str(origin.to_string()),
            ],
            version: 0,
            timestamp: 0,
        };
        engine.insert_record(&mut tx, "animals", record);
    }
    engine.commit(tx).unwrap();
    println!("✅ Animals data inserted");

    // Advanced Queries
    println!("\n🔍 Running Queries...");

    let mut tx = engine.begin_transaction(isolation_level);

    // User queries
    println!("\n👥 User Statistics:");
    let remote_workers = engine.search_records(&mut tx, "users", "Remote");
    println!("Remote Workers: {}", remote_workers.len());

    let senior_staff = engine.search_records(&mut tx, "users", "Senior");
    println!("Senior Staff Members: {}", senior_staff.len());

    // Animal queries
    println!("\n🐾 Animal Statistics:");
    for species in [
        "Dog", "Cat", "Parrot", "Snake", "Lizard", "Horse", "Sheep", "Pig",
    ] {
        let animals = engine.search_records(&mut tx, "animals", species);
        println!("{} count: {}", species, animals.len());
    }

    // Test complex updates
    println!("\n✏️ Testing Updates...");
    let mut tx = engine.begin_transaction(isolation_level);

    // Promote an employee
    engine.delete_record(&mut tx, "users", 4);
    let promoted_user = Record {
        id: 4,
        values: vec![
            ValueType::Str("Diana".to_string()),
            ValueType::Int(29),
            ValueType::Str("Lead Data Scientist".to_string()),
            ValueType::Float(120000.0),
            ValueType::Bool(true),
            ValueType::Str("Analytics".to_string()),
            ValueType::Str("Hybrid".to_string()),
        ],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "users", promoted_user);

    // Update animal training status
    engine.delete_record(&mut tx, "animals", 6);
    let trained_cat = Record {
        id: 6,
        values: vec![
            ValueType::Str("Lucy".to_string()),
            ValueType::Str("Cat".to_string()),
            ValueType::Int(2),
            ValueType::Float(3.5),
            ValueType::Bool(true),
            ValueType::Str("Siamese".to_string()),
            ValueType::Str("Well-behaved".to_string()),
            ValueType::Str("Small".to_string()),
            ValueType::Bool(true),
            ValueType::Str("Thailand".to_string()),
        ],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "animals", trained_cat);
    engine.commit(tx).unwrap();
    println!("✅ Updates completed successfully");

    // Final State Display
    println!("\n📊 Final Database State:");
    let mut final_tx = engine.begin_transaction(isolation_level);

    println!("\n👥 Users:");
    for id in 1..=8 {
        if let Some(user) = engine.get_record(&mut final_tx, "users", id) {
            println!(
                "ID {}: {} - {} ({})",
                id,
                match &user.values[0] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.values[2] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.values[5] {
                    ValueType::Str(s) => s,
                    _ => "?",
                }
            );
        }
    }

    println!("\n🐾 Animals:");
    for id in 1..=12 {
        if let Some(animal) = engine.get_record(&mut final_tx, "animals", id) {
            println!(
                "ID {}: {} - {} {} ({}, {})",
                id,
                match &animal.values[0] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[5] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[1] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[6] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[9] {
                    ValueType::Str(s) => s,
                    _ => "?",
                }
            );
        }
    }

    println!("\n✨ Database operations completed successfully!");
}
