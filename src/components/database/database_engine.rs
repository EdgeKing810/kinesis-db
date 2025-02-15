use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{Arc, Mutex, RwLock, RwLockWriteGuard},
};

use sha2::{Digest, Sha256};

use crate::components::{
    storage::{
        buffer_pool::BufferPool, disk_manager::DiskManager, page::PAGE_SIZE, page_store::PageStore,
        wal::WriteAheadLog,
    },
    transaction::{
        config::TransactionConfig, isolation_level::IsolationLevel, manager::TransactionManager,
        transaction::Transaction,
    },
};

use super::{
    commit_guard::CommitGuard, database::Database, db_type::DatabaseType, record::Record,
    restore_policy::RestorePolicy, table::Table,
};

pub struct DBEngine {
    db: Arc<RwLock<Database>>,           // The actual database data (in memory)
    file_path: String,                   // Path to the disk file
    wal: WriteAheadLog,                  // // The Path to the Write Ahead Log
    tx_manager: TransactionManager,      // The transaction manager
    page_store: Option<PageStore>,       // The page store
    buffer_pool: Arc<Mutex<BufferPool>>, // The buffer pool
    restore_policy: RestorePolicy,       // The restore policy for the WAL
    isolation_level: IsolationLevel,     // The default isolation level
}

impl DBEngine {
    pub fn new(
        db_type: DatabaseType,
        restore_policy: RestorePolicy,
        file_path: &str,
        wal_path: &str,
        tx_config: Option<TransactionConfig>,
        isolation_level: IsolationLevel,
    ) -> Self {
        // Set buffer pool size based on database type
        let buffer_pool_size = match db_type {
            DatabaseType::InMemory => 10000, // Larger in-memory buffer
            DatabaseType::Hybrid => 2500,    // Increased from 1000 to 2500
            DatabaseType::OnDisk => 100,     // Smaller buffer for disk-based
        };

        let db = Database {
            db_type,
            tables: BTreeMap::new(),
        };

        let mut engine = DBEngine {
            db: Arc::new(RwLock::new(db)),
            file_path: file_path.to_string(),
            wal: WriteAheadLog::new(wal_path, db_type != DatabaseType::InMemory),
            tx_manager: TransactionManager::new(tx_config),
            page_store: if db_type != DatabaseType::InMemory {
                Some(PageStore::new(&format!("{}.pages", file_path)).unwrap())
            } else {
                None
            },
            buffer_pool: Arc::new(Mutex::new(BufferPool::new(buffer_pool_size, db_type))),
            restore_policy,
            isolation_level,
        };

        // Only recover from disk for OnDisk and Hybrid types
        if db_type != DatabaseType::InMemory {
            if let Err(e) = engine.recover_from_crash() {
                eprintln!("Warning: Recovery failed: {}", e);
            }
            engine.load_from_disk();
        }

        engine
    }

    // ========== Transaction Management ==========

    pub fn begin_transaction(&mut self) -> Transaction {
        let tx_id = rand::random::<u64>();
        self.tx_manager
            .start_transaction(self.isolation_level, tx_id);

        let snapshot = match self.isolation_level {
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                Some(self.db.read().unwrap().clone())
            }
            _ => None,
        };

        Transaction::new(tx_id, self.isolation_level, snapshot)
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

    pub fn commit_internal(&mut self, tx: Transaction) -> Result<(), String> {
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
            if !self
                .tx_manager
                .acquire_lock_with_retry(tx.id, table_name, *id)
            {
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

            // Now pass ownership of the lock guard
            self.apply_transaction_changes(&mut db_lock, &tx, current_timestamp)?;

            // Calculate checksum after changes
            let checksum = self.calculate_checksum(&db_lock)?;

            // Log to WAL only for disk-based or hybrid databases
            if db_lock.db_type != DatabaseType::InMemory {
                self.wal.log_transaction(&tx)?;
                self.wal.sync()?;
            }

            // Persist changes if needed
            if db_lock.db_type == DatabaseType::OnDisk || db_lock.db_type == DatabaseType::Hybrid {
                drop(db_lock);
                self.save_to_disk_with_verification(checksum)?;

                self.wal.mark_transaction_complete(tx.id)?;
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

    pub fn rollback_failed_commit(&mut self, tx: &Transaction) -> Result<(), String> {
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
            if let Some(page_store) = &self.page_store {
                page_store.free_page(page_id);
            }
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

            if let Some(tbl) = db_lock.tables.get_mut(&table_name) {
                tbl.insert_record(record.clone());
            } else {
                return Err(format!("Table not found: {}", table_name));
            }
        }

        Ok(())
    }

    fn get_table_page_ids(&self, table_name: &str) -> Option<Vec<u64>> {
        // Load TOC and get page IDs for the table
        if let Some(page_store) = &self.page_store {
            let mut buffer_pool = self.buffer_pool.lock().unwrap();
            let toc_frame = buffer_pool.get_page(0, page_store);
            let toc = toc_frame.get_page().read().unwrap();
            let table_locations: HashMap<String, Vec<u64>> =
                bincode::deserialize(&toc.data).unwrap_or_default();
            table_locations.get(table_name).cloned()
        } else {
            None
        }
    }

    fn get_record_page_id(&self, table_name: &str, record_id: u64) -> Option<u64> {
        // Load TOC and get page IDs for the table
        if let Some(page_ids) = self.get_table_page_ids(table_name) {
            if let Some(page_store) = &self.page_store {
                let mut buffer_pool = self.buffer_pool.lock().unwrap();
                for page_id in page_ids {
                    // Load the page and check for the record
                    let frame = buffer_pool.get_page(page_id, page_store);
                    let page_data = frame.get_page().read().unwrap(); // Updated this line
                    let records: Vec<Record> =
                        bincode::deserialize(&page_data.data).unwrap_or_default();
                    if records.iter().any(|r| r.id == record_id) {
                        return Some(page_id);
                    }
                }
            }
        }
        None
    }

    pub fn get_record(&self, tx: &mut Transaction, table_name: &str, id: u64) -> Option<Record> {
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
        // Return the latest record we can find, regardless of timestamp
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
        // Return the record from the snapshot if it exists
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

    #[allow(dead_code)]
    pub fn drop_table(&self, tx: &mut Transaction, table_name: &str) {
        tx.pending_table_drops.push(table_name.to_string());
    }

    #[allow(dead_code)]
    pub fn get_tables(&self) -> Vec<String> {
        let db_lock: std::sync::RwLockReadGuard<'_, Database> = self.db.read().unwrap();
        let tables = db_lock.tables.clone();
        tables.keys().cloned().collect()
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
        if !self
            .tx_manager
            .acquire_lock_with_retry(tx.id, table_name, id)
        {
            // If we can't acquire the lock, check for deadlock
            if self.tx_manager.has_deadlock(tx.id) {
                // If there's a deadlock, mark the transaction as failed
                tx.write_set.push((table_name.to_string(), id)); // Add to write set so commit will fail
                return;
            }

            // Wait for lock (in real system this would be async)
            std::thread::sleep(std::time::Duration::from_millis(10));
            if !self
                .tx_manager
                .acquire_lock_with_retry(tx.id, table_name, id)
            {
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
                tx.pending_deletes
                    .push((table_name.to_string(), id, old_rec.clone()));
            }
        }
    }

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

    fn search_latest(&self, db: &Database, table_name: &str, query: &str) -> Vec<Record> {
        // Return all records that match the query, regardless of timestamp
        if let Some(table) = db.tables.get(table_name) {
            table
                .search_by_string(query, true)
                .into_iter()
                .map(|arc| arc.read().unwrap().clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn search_committed(&self, db: &Database, table_name: &str, query: &str) -> Vec<Record> {
        // Return only committed records that match the query
        if let Some(table) = db.tables.get(table_name) {
            table
                .search_by_string(query, true)
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
        // Return records from the snapshot that match the query
        if let Some(table) = snapshot.tables.get(table_name) {
            table
                .search_by_string(query, true)
                .into_iter()
                .map(|arc| arc.read().unwrap().clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    // ========== Disk Persistence ==========

    fn load_from_disk(&mut self) {
        let db_lock = self.db.read().unwrap();

        // Skip disk operations for in-memory database
        if db_lock.db_type == DatabaseType::InMemory {
            return;
        }
        drop(db_lock);

        if !Path::new(&format!("{}.pages", self.file_path)).exists() {
            return;
        }

        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        let mut db_lock = self.db.write().unwrap();

        let page_store = match &self.page_store {
            Some(store) => store,
            None => return,
        };

        // Read TOC
        let toc_frame = buffer_pool.get_page(0, page_store);
        let table_locations: HashMap<String, Vec<u64>> = {
            let toc = toc_frame.get_page().read().unwrap();
            bincode::deserialize(&toc.data).unwrap_or_default()
        };
        buffer_pool.unpin_page(0, false);

        // Load each table
        for (table_name, page_ids) in table_locations {
            let mut table = Table::new();
            let mut all_records = Vec::new();

            // Process pages in order
            for page_id in &page_ids {
                let frame = buffer_pool.get_page(*page_id, page_store);
                let page = frame.get_page().read().unwrap();

                // Read chunk size from start of page
                let size =
                    u32::from_le_bytes([page.data[0], page.data[1], page.data[2], page.data[3]])
                        as usize;

                if size > 0 && size <= page.data.len() - 4 {
                    // Deserialize chunk
                    match bincode::deserialize::<Vec<Record>>(&page.data[4..4 + size]) {
                        Ok(mut records) => all_records.append(&mut records),
                        Err(e) => {
                            eprintln!(
                                "Warning: Failed to deserialize chunk in table {}: {}",
                                table_name, e
                            );
                            continue;
                        }
                    }
                }
                buffer_pool.unpin_page(*page_id, false);
            }

            // Insert all records into table
            for record in all_records {
                table.insert_record(record);
            }

            if !table.data.is_empty() {
                db_lock.tables.insert(table_name, table);
            }
        }
    }

    fn save_to_disk(&mut self) -> Result<u64, String> {
        let db_lock = self.db.read().unwrap();

        // Skip disk operations for in-memory database
        if db_lock.db_type == DatabaseType::InMemory {
            let checksum = self.calculate_checksum(&*db_lock)?;
            return Ok(checksum);
        }

        let page_store = match &self.page_store {
            Some(store) => store,
            None => return Ok(self.calculate_checksum(&*db_lock)?),
        };

        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        buffer_pool.flush_all(page_store); // Flush existing pages first

        let mut table_locations = HashMap::new();
        const MAX_DATA_PER_PAGE: usize = PAGE_SIZE - 64; // Increased usable space

        for (table_name, table) in &db_lock.tables {
            let mut page_ids = Vec::new();
            let records: Vec<Record> = table
                .data
                .values()
                .map(|r| r.read().unwrap().clone())
                .collect();

            // Serialize records in chunks to avoid large allocations
            const CHUNK_SIZE: usize = 100;
            for chunk in records.chunks(CHUNK_SIZE) {
                // Serialize chunk
                let chunk_data = bincode::serialize(&chunk)
                    .map_err(|e| format!("Failed to serialize chunk: {}", e))?;

                // Calculate needed pages
                let pages_needed = (chunk_data.len() + MAX_DATA_PER_PAGE - 1) / MAX_DATA_PER_PAGE;

                // Write chunk data across pages
                for i in 0..pages_needed {
                    let start = i * MAX_DATA_PER_PAGE;
                    let end = (start + MAX_DATA_PER_PAGE).min(chunk_data.len());
                    let page_chunk = &chunk_data[start..end];

                    let page_id = page_store
                        .allocate_page()
                        .map_err(|e| format!("Failed to allocate page: {}", e))?;

                    let frame = buffer_pool.get_page(page_id, page_store);
                    {
                        let mut page = frame.get_page().write().unwrap();
                        page.data.fill(0);

                        // Write chunk size at start of page
                        let size_bytes = (page_chunk.len() as u32).to_le_bytes();
                        page.data[..4].copy_from_slice(&size_bytes);

                        // Write chunk data after size
                        page.data[4..4 + page_chunk.len()].copy_from_slice(page_chunk);
                    }
                    buffer_pool.unpin_page(page_id, true);
                    buffer_pool
                        .flush_page(page_id, page_store)
                        .map_err(|e| format!("Failed to flush page {}: {}", page_id, e))?;
                    page_ids.push(page_id);
                }
            }

            if !page_ids.is_empty() {
                table_locations.insert(table_name.clone(), page_ids);
            }
        }

        // Write table of contents
        let toc_data = bincode::serialize(&table_locations)
            .map_err(|e| format!("Failed to serialize TOC: {}", e))?;

        let toc_frame = buffer_pool.get_page(0, page_store);
        {
            let mut toc = toc_frame.get_page().write().unwrap();
            toc.data.fill(0);
            toc.write_data(0, &toc_data)
                .map_err(|e| format!("Failed to write TOC: {}", e))?;
        }
        buffer_pool.unpin_page(0, true);
        buffer_pool
            .flush_page(0, page_store)
            .map_err(|e| format!("Failed to flush TOC: {}", e))?;

        // Final sync to ensure all data is written
        buffer_pool.flush_all(page_store);
        page_store
            .sync()
            .map_err(|e| format!("Failed to sync disk: {}", e))?;

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

        let transactions = self
            .wal
            .load_transactions(&self.restore_policy, self.isolation_level)?;

        // Apply each valid transaction
        for tx in transactions {
            if self.wal.is_transaction_valid(&tx)? {
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

    #[allow(dead_code)]
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
