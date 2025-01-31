use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::{self, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use std::time::Duration;

mod page;
use page::{Page, PageStore};

// Add transaction timeout configuration
const TX_TIMEOUT_SECS: u64 = 30;  // 30 second timeout

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
    pub version: u64,  // Add version for MVCC
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
    file: std::fs::File,
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
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        WriteAheadLog { file }
    }

    fn load_transactions(&self) -> Vec<Transaction> {
        let file = OpenOptions::new().read(true).open("wal.log").unwrap();
        let reader = BufReader::new(file);
        let mut transactions = Vec::new();

        for line in reader.lines() {
            let log_entry: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
            let entry_data = &log_entry["data"];
            let status = log_entry["status"].as_str().unwrap_or("pending");
            
            // Only process pending transactions
            if status != "pending" {
                continue;
            }

            let tx_id = entry_data["tx_id"].as_u64().unwrap();
            let mut tx = Transaction::new(
                tx_id,
                IsolationLevel::ReadCommitted,
                None,
            );

            // Update transaction data
            if let Some(creates) = entry_data["creates"].as_array() {
                tx.pending_table_creates = creates.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }

            if let Some(drops) = entry_data["drops"].as_array() {
                tx.pending_table_drops = drops.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }

            tx.pending_inserts = entry_data["inserts"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|v| {
                    serde_json::from_value(v.clone())
                        .ok()
                        .map(|(table_name, record): (String, Record)| (table_name, record))
                })
                .collect();

            tx.pending_deletes = entry_data["deletes"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|v| {
                    serde_json::from_value(v.clone())
                        .ok()
                        .map(|(table_name, id, record): (String, u64, Record)| (table_name, id, record))
                })
                .collect();

            tx.start_timestamp = entry_data["timestamp"].as_u64().unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            });

            transactions.push(tx);
        }

        transactions
    }

    fn log_transaction_with_checksum(&mut self, tx: &Transaction, checksum: u64) -> Result<u64, String> {
        let data = bincode::serialize(tx)
            .map_err(|e| format!("Failed to serialize transaction: {}", e))?;

        let entry = WalEntry {
            tx_id: tx.id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            checksum,
            data,
            status: "pending".to_string(),
        };

        bincode::serialize_into(&mut self.file, &entry)
            .map_err(|e| format!("Failed to write WAL entry: {}", e))?;
        
        Ok(checksum)
    }

    fn mark_transaction_complete(&mut self, tx_id: u64) -> Result<(), String> {
        let mut content = String::new();
        let file = File::open("wal.log").map_err(|e| format!("Failed to open WAL: {}", e))?;
        BufReader::new(file).read_to_string(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        let updated = content.lines()
            .map(|line| {
                let mut entry: serde_json::Value = serde_json::from_str(line).unwrap();
                if entry["data"]["tx_id"].as_u64() == Some(tx_id) {
                    entry["status"] = json!("completed");
                }
                entry
            })
            .collect::<Vec<_>>();

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("wal.log")
            .map_err(|e| format!("Failed to open WAL for writing: {}", e))?;
        
        let mut writer = BufWriter::new(file);
        for entry in updated {
            serde_json::to_writer(&mut writer, &entry)
                .map_err(|e| format!("Failed to write WAL entry: {}", e))?;
            writer.write_all(b"\n")
                .map_err(|e| format!("Failed to write WAL newline: {}", e))?;
        }
        
        Ok(())
    }

    fn is_transaction_valid(&self, tx: &Transaction) -> Result<bool, String> {
        let mut file = File::open("wal.log")
            .map_err(|e| format!("Failed to open WAL: {}", e))?;
        
        let mut content = Vec::new();
        file.read_to_end(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        if let Ok(entry) = bincode::deserialize::<WalEntry>(&content) {
            if entry.tx_id == tx.id {
                let mut hasher = Sha256::new();
                hasher.update(&entry.data);
                let computed_checksum = u64::from_be_bytes(hasher.finalize()[..8].try_into().unwrap());
                return Ok(entry.checksum == computed_checksum);
            }
        }

        Ok(false)
    }

    fn get_incomplete_transactions(&self) -> Result<Vec<Transaction>, String> {
        let file = File::open("wal.log")
            .map_err(|e| format!("Failed to open WAL: {}", e))?;
        
        let mut transactions = Vec::new();
        let mut reader = BufReader::new(file);
        
        let mut content = Vec::new();
        reader.read_to_end(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        if let Ok(entry) = bincode::deserialize::<WalEntry>(&content) {
            if entry.status == "pending" {
                if let Ok(tx) = bincode::deserialize(&entry.data) {
                    transactions.push(tx);
                }
            }
        }

        Ok(transactions)
    }

    fn sync(&self) -> Result<(), String> {
        self.file.sync_all()
            .map_err(|e| format!("Failed to sync WAL: {}", e))
    }

    fn rotate_log(&mut self) -> Result<(), String> {
        let backup_path = format!("wal.log.{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());

        // Create backup with timestamp
        std::fs::rename("wal.log", &backup_path)
            .map_err(|e| format!("Failed to create WAL backup: {}", e))?;

        // Create new log file
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal.log")
            .map_err(|e| format!("Failed to create new WAL: {}", e))?;

        // Keep backup for recovery if needed
        Ok(())
    }

    fn cleanup_completed_transactions(&mut self) -> Result<(), String> {
        let mut content = String::new();
        let file = File::open("wal.log").map_err(|e| format!("Failed to open WAL: {}", e))?;
        BufReader::new(file).read_to_string(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        let completed_count = content.lines()
            .filter(|line| {
                let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                entry["status"].as_str() == Some("completed")
            })
            .count();

        // If we have too many completed transactions, rotate the log
        if completed_count > 1000 {
            self.rotate_log()?;
            
            // Write only pending transactions to new log
            let mut writer = BufWriter::new(&mut self.file);
            for line in content.lines() {
                let entry: serde_json::Value = serde_json::from_str(line).unwrap();
                if entry["status"].as_str() == Some("pending") {
                    writeln!(writer, "{}", line)
                        .map_err(|e| format!("Failed to write to new WAL: {}", e))?;
                }
            }
            writer.flush()
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
    read_set: Vec<(String, u64, u64)>,  // (table, record_id, version)
    write_set: Vec<(String, u64)>,      // (table, record_id)
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
    active_transactions: HashMap<u64, std::time::SystemTime>,
    locks: HashMap<(String, u64), u64>, // (table, id) -> tx_id
}

impl TransactionManager {
    fn new() -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            locks: HashMap::new(),
        }
    }

    fn start_transaction(&mut self, tx_id: u64) {
        self.active_transactions.insert(tx_id, std::time::SystemTime::now());
    }

    fn end_transaction(&mut self, tx_id: u64) {
        self.active_transactions.remove(&tx_id);
        self.locks.retain(|_, &mut lock_tx| lock_tx != tx_id);
    }

    fn acquire_lock(&mut self, tx_id: u64, table: &str, record_id: u64) -> bool {
        let key = (table.to_string(), record_id);
        match self.locks.get(&key) {
            Some(&lock_holder) if lock_holder != tx_id => false,
            _ => {
                self.locks.insert(key, tx_id);
                true
            }
        }
    }

    fn release_lock(&mut self, tx_id: u64, table: &str, record_id: u64) {
        let key = (table.to_string(), record_id);
        if let Some(&lock_holder) = self.locks.get(&key) {
            if lock_holder == tx_id {
                self.locks.remove(&key);
            }
        }
    }

    fn is_transaction_expired(&self, tx_id: u64) -> bool {
        if let Some(start_time) = self.active_transactions.get(&tx_id) {
            if let Ok(elapsed) = start_time.elapsed() {
                return elapsed > Duration::from_secs(TX_TIMEOUT_SECS);
            }
        }
        false
    }

    fn cleanup_expired_transactions(&mut self) {
        let expired: Vec<_> = self.active_transactions
            .iter()
            .filter(|(_, &start_time)| {
                start_time.elapsed().map(|e| e > Duration::from_secs(TX_TIMEOUT_SECS)).unwrap_or(true)
            })
            .map(|(&tx_id, _)| tx_id)
            .collect();

        for tx_id in expired {
            self.end_transaction(tx_id);
        }
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
}

// Add helper struct for consistent serialization
struct CanonicalWriter<W: Write>(W);

impl<W: Write> Write for CanonicalWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
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
    pub fn new(db_type: DatabaseType, file_path: String) -> Self {
        let db = Database {
            db_type,
            tables: BTreeMap::new(),
        };

        let mut engine = DBEngine {
            db: Arc::new(RwLock::new(db)),
            file_path: file_path.clone(),
            wal: WriteAheadLog::new("wal.log"),
            tx_manager: TransactionManager::new(),
            page_store: PageStore::new(&format!("{}.pages", file_path)).unwrap(),
        };

        // First try to recover from any previous crash
        if let Err(e) = engine.recover_from_crash() {
            eprintln!("Warning: Recovery failed: {}", e);
        }

        // Then load from disk (this will give us the latest committed state)
        engine.load_from_disk();

        // Finally apply any pending WAL transactions
        let transactions = engine.wal.load_transactions();
        for tx in transactions {
            engine.commit(tx).expect("Failed to recover transactions");
        }

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
        if self.has_deadlock(&tx) {
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
            self.validate_transaction(&tx)?;

            let mut db_lock = self.db.write().unwrap();
            let current_timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Apply changes first
            self.apply_transaction_changes(&mut db_lock, &tx, current_timestamp)?;
            
            // Calculate checksum after changes
            let checksum = self.calculate_checksum(&db_lock)?;

            // Log to WAL using bincode
            self.wal.log_transaction_with_checksum(&tx, checksum)?;
            self.wal.sync()?;

            // Persist changes if needed
            if db_lock.db_type == DatabaseType::OnDisk || db_lock.db_type == DatabaseType::Hybrid {
                drop(db_lock);
                self.save_to_disk_with_verification(checksum)?;
            }

            // Mark transaction as complete
            self.wal.mark_transaction_complete(tx.id)?;

            // Cleanup WAL periodically (every 100 transactions)
            if tx.id % 100 == 0 {
                if let Err(e) = self.wal.cleanup_completed_transactions() {
                    eprintln!("Warning: WAL cleanup failed: {}", e);
                }
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

    // Add deadlock detection
    fn has_deadlock(&self, tx: &Transaction) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![tx.id];

        while let Some(current_id) = stack.pop() {
            if !visited.insert(current_id) {
                return true; // Cycle detected
            }

            // Check if any resources this transaction wants are held by others
            // This is a simplified version - in practice you'd need to track
            // actual resource locks and waiting relationships
            for (table_name, id) in &tx.write_set {
                if let Ok(db) = self.db.read() {
                    if let Some(table) = db.tables.get(table_name) {
                        if let Some(record) = table.get_record(id) {
                            let record = record.read().unwrap();
                            if record.timestamp > tx.start_timestamp {
                                // Resource is held by a newer transaction
                                stack.push(record.timestamp);
                            }
                        }
                    }
                }
            }
        }

        false
    }

    fn rollback_failed_commit(&mut self, tx: &Transaction) -> Result<(), String> {
        let result = {
            let mut db_lock = self.db.write().unwrap();
            
            // Restore original state for modified records
            for (table_name, id) in &tx.write_set {
                if let Some(snapshot_record) = tx.snapshot.as_ref()
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
        db_lock: &mut Database,
        tx: &Transaction,
        timestamp: u64
    ) -> Result<(), String> {
        // Apply DDL operations
        for table_name in &tx.pending_table_creates {
            db_lock.tables.entry(table_name.clone()).or_insert(Table::new());
        }
        for table_name in &tx.pending_table_drops {
            db_lock.tables.remove(table_name);
        }

        // Apply DML operations
        for (table_name, mut record) in tx.pending_inserts.clone() {
            record.version += 1;
            record.timestamp = timestamp;
            let tbl = db_lock.tables.entry(table_name).or_insert(Table::new());
            tbl.insert_record(record);
        }

        for (table_name, id, _) in &tx.pending_deletes {
            if let Some(tbl) = db_lock.tables.get_mut(table_name) {
                tbl.delete_record(id);
            }
        }

        Ok(())
    }

    fn get_record(&self, tx: &mut Transaction, table_name: &str, id: u64) -> Option<Record> {
        // Check for transaction timeout
        if self.tx_manager.is_transaction_expired(tx.id) {
            return None;
        }

        // First check if this record is in our write set
        if tx.write_set.contains(&(table_name.to_string(), id)) {
            // Check pending inserts first
            if let Some((_, record)) = tx.pending_inserts.iter()
                .find(|(t, r)| t == table_name && r.id == id) {
                return Some(record.clone());
            }
            // If not in pending inserts, it might be deleted
            if tx.pending_deletes.iter()
                .any(|(t, rid, _)| t == table_name && *rid == id) {
                return None;
            }
        }

        let db_lock = self.db.read().unwrap();
        
        let record = match tx.isolation_level {
            IsolationLevel::ReadUncommitted => {
                self.get_latest_record(&db_lock, table_name, id)
            },
            IsolationLevel::ReadCommitted => {
                self.get_committed_record(&db_lock, table_name, id)
            },
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
        db.tables.get(table_name)
            .and_then(|tbl| tbl.get_record(&id))
            .map(|arc| arc.read().unwrap().clone())
    }

    fn get_committed_record(&self, db: &Database, table_name: &str, id: u64) -> Option<Record> {
        // Only return records that have been committed (have a timestamp)
        db.tables.get(table_name)
            .and_then(|tbl| tbl.get_record(&id))
            .map(|arc| arc.read().unwrap().clone())
            .filter(|rec| rec.timestamp > 0)
    }

    fn get_snapshot_record(&self, snapshot: &Database, table_name: &str, id: u64) -> Option<Record> {
        snapshot.tables.get(table_name)
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
        // Track the write in the transaction's write set
        tx.write_set.push((table_name.to_string(), record.id));
        // Store the insert in the transaction
        tx.pending_inserts.push((table_name.to_string(), record));
    }

    pub fn delete_record(&self, tx: &mut Transaction, table_name: &str, id: u64) {
        let db_lock = self.db.read().unwrap();
        let tbl_opt = db_lock.tables.get(table_name);
        if let Some(tbl) = tbl_opt {
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
    pub fn search_records(&self, tx: &mut Transaction, table_name: &str, query: &str) -> Vec<Record> {
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
            table.search_by_string(query)
                .into_iter()
                .map(|arc| arc.read().unwrap().clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn search_committed(&self, db: &Database, table_name: &str, query: &str) -> Vec<Record> {
        if let Some(table) = db.tables.get(table_name) {
            table.search_by_string(query)
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
            table.search_by_string(query)
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

        // Read metadata from first page
        if let Ok(page) = self.page_store.read_page(0) {
            // Access data directly instead of using read_data
            if let Ok(db) = bincode::deserialize(&page.data) {
                let mut db_lock = self.db.write().unwrap();
                *db_lock = db;
            }
        }
    }

    fn save_to_disk(&mut self) -> Result<(), String> {
        let db_lock = self.db.read().unwrap();
        
        // Allocate a new page for the database state
        let page_id = self.page_store.allocate_page()
            .map_err(|e| format!("Failed to allocate page: {}", e))?;
        
        // Serialize database state
        let data = bincode::serialize(&*db_lock)
            .map_err(|e| format!("Failed to serialize DB: {}", e))?;
        
        // Write to newly allocated page
        let mut meta_page = Page::new(page_id);
        meta_page.write_data(0, &data)
            .map_err(|e| format!("Failed to write data: {}", e))?;
        
        // Write the page and free the old one if it exists
        if page_id > 0 {
            self.page_store.free_page(page_id - 1);
        }
        self.page_store.write_page(&meta_page)
            .map_err(|e| format!("Failed to write page: {}", e))?;
        
        Ok(())
    }

    fn save_to_disk_with_verification(&mut self, expected_checksum: u64) -> Result<(), String> {
        self.save_to_disk()?;

        // Verify by reading back
        if let Ok(page) = self.page_store.read_page(0) {
            let db: Database = bincode::deserialize(&page.data)
                .map_err(|e| format!("Failed to deserialize DB: {}", e))?;

            let actual_checksum = self.calculate_checksum(&db)?;

            if actual_checksum != expected_checksum {
                return Err(format!(
                    "Checksum verification failed. Expected: {}, Got: {}",
                    expected_checksum, actual_checksum
                ));
            }
        }

        Ok(())
    }

    // Add recovery handling
    fn recover_from_crash(&mut self) -> Result<(), String> {
        // 1. Load database from disk
        self.load_from_disk();
        
        // 2. Get incomplete transactions from WAL
        let incomplete_txs = self.wal.get_incomplete_transactions()?;
        
        // 3. Either roll forward or back based on transaction state
        for tx in incomplete_txs {
            if self.wal.is_transaction_valid(&tx)? {
                self.commit(tx)?;
            }
        }

        Ok(())
    }

    pub fn rollback(&mut self, tx: Transaction) -> Result<(), String> {
        self.rollback_failed_commit(&tx)?;
        Ok(())
    }

    fn calculate_checksum(&self, data: &Database) -> Result<u64, String> {
        let serialized = bincode::serialize(data)
            .map_err(|e| format!("Failed to serialize DB: {}", e))?;
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        Ok(u64::from_be_bytes(hasher.finalize()[..8].try_into().unwrap()))
    }
}

// ========== Test / Example Usage ==========

fn main() {
    // Create an OnDisk database
    let engine_ondisk = DBEngine::new(DatabaseType::OnDisk, "users_ondisk_db.json".to_string());
    // Or create a Hybrid database
    let _engine_hybrid = DBEngine::new(DatabaseType::Hybrid, "users_hybrid_db.json".to_string());

    // We'll just demonstrate with the OnDisk for clarity
    let mut engine = engine_ondisk;

    let isolation_level = IsolationLevel::Serializable;

    // 1) Create table (if not exist)
    let mut tx = engine.begin_transaction(isolation_level);
    engine.create_table(&mut tx, "users");
    engine.commit(tx).unwrap();

    // 2) Begin a transaction with READ_COMMITTED isolation
    let mut tx = engine.begin_transaction(isolation_level);

    // 3) Insert some records
    let record1 = Record {
        id: 1,
        values: vec![ValueType::Str("Alice".to_string()), ValueType::Int(30)],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "users", record1);

    let record2 = Record {
        id: 2,
        values: vec![ValueType::Str("Bob".to_string()), ValueType::Int(25)],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "users", record2);

    // 4) Commit
    engine.commit(tx).unwrap();

    // 5) Perform some reads
    let mut read_tx = engine.begin_transaction(isolation_level);
    if let Some(rec) = engine.get_record(&mut read_tx, "users", 1) {
        println!("Got record for user id=1: {:?}", rec);
    }

    let results = engine.search_records(&mut read_tx, "users", "Ali");
    println!("Searching for 'Ali' in 'users' table -> {:?}", results);

    // 6) Demonstrate deleting a record
    let mut tx2 = engine.begin_transaction(isolation_level);
    engine.delete_record(&mut tx2, "users", 2);
    engine.commit(tx2).unwrap();

    // Check if user with id=2 is gone
    let mut read_tx2 = engine.begin_transaction(isolation_level);
    let after_delete = engine.get_record(&mut read_tx2, "users", 2);
    println!("After deletion, user id=2: {:?}", after_delete);

    // 7) Test rollback functionality
    let mut tx3 = engine.begin_transaction(isolation_level);
    let record3 = Record {
        id: 3,
        values: vec![ValueType::Str("Charlie".to_string()), ValueType::Int(40)],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx3, "users", record3);
    // Implicit rollback since commit is not called
    
    // Explicitly rollback the transaction
    // engine.rollback(tx3);

    // Check if user with id=3 is not present
    let mut read_tx3 = engine.begin_transaction(isolation_level);
    let after_rollback = engine.get_record(&mut read_tx3, "users", 3);
    println!("After implicit rollback, user id=3: {:?}", after_rollback);

    // 8) Test atomicity by attempting to insert and delete in the same transaction
    let mut tx4 = engine.begin_transaction(isolation_level);
    let record4 = Record {
        id: 4,
        values: vec![ValueType::Str("Dave".to_string()), ValueType::Int(35)],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx4, "users", record4);
    engine.delete_record(&mut tx4, "users", 4);
    engine.commit(tx4).unwrap();

    // Check if user with id=4 is not present
    let mut read_tx4 = engine.begin_transaction(isolation_level);
    let after_atomic = engine.get_record(&mut read_tx4, "users", 4);
    println!("After atomic operation, user id=4: {:?}", after_atomic);
}
