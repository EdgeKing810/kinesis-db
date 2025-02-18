use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::components::{
    database::{record::Record, restore_policy::RestorePolicy, schema::TableSchema},
    transaction::{isolation_level::IsolationLevel, transaction::Transaction},
};

// Define the WriteAheadLog structure
#[derive(Debug)]
pub struct WriteAheadLog {
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    tx_id: u64,     // Transaction ID
    timestamp: u64, // Timestamp of the transaction
    checksum: u64,  // Checksum of the entry
    data: Vec<u8>,  // Data associated with the entry
    status: String, // Status of the transaction (pending, completed)
}

impl WriteAheadLog {
    pub fn new(path: &str, create_file: bool) -> Self {
        if create_file {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
        }
        WriteAheadLog {
            path: path.to_string(),
        }
    }

    pub fn load_transactions(
        &self,
        policy: &RestorePolicy,
        isolation_level: IsolationLevel,
    ) -> Result<Vec<Transaction>, String> {
        // Open the WAL file
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Ok(Vec::new()),
        };

        // If the policy is Discard, return an empty list as no transactions need to be restored
        if *policy == RestorePolicy::Discard {
            return Ok(Vec::new());
        }

        let reader = BufReader::new(file);
        let mut transactions = Vec::new();

        for line in reader.lines() {
            // Parse each line as a JSON object
            if let Ok(line) = line {
                let entry: serde_json::Value = serde_json::from_str(&line)
                    .map_err(|e| format!("Failed to parse WAL entry: {}", e))?;

                // Check if the entry should be restored based on the policy
                if *policy == RestorePolicy::RecoverPending
                    && entry["status"].as_str() != Some("pending")
                {
                    continue;
                }

                // Create a new transaction
                let mut tx =
                    Transaction::new(entry["tx_id"].as_u64().unwrap(), isolation_level, None);

                // Restore table drops
                if let Some(drops) = entry["table_drops"].as_array() {
                    tx.pending_table_drops = drops
                        .iter()
                        .map(|v| v.as_str().unwrap().to_string())
                        .collect();
                }

                // Restore table creates
                if let Some(creates) = entry["table_creates"].as_array() {
                    for create in creates {
                        let table = create[0].as_str().unwrap();
                        let schema: TableSchema =
                            serde_json::from_value(create[1].clone()).unwrap();
                        tx.pending_table_creates.push((table.to_string(), schema));
                    }
                }

                // Restore schema updates
                if let Some(updates) = entry["schema_updates"].as_array() {
                    for update in updates {
                        let table = update[0].as_str().unwrap();
                        let schema: TableSchema =
                            serde_json::from_value(update[1].clone()).unwrap();
                        tx.pending_schema_updates.push((table.to_string(), schema));
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

                // Restore inserts
                if let Some(inserts) = entry["inserts"].as_array() {
                    for insert in inserts {
                        let table = insert[0].as_str().unwrap();
                        let record: Record = serde_json::from_value(insert[1].clone()).unwrap();
                        tx.pending_inserts.push((table.to_string(), record));
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

    pub fn log_transaction(&mut self, tx: &Transaction) -> Result<u64, String> {
        // Create initial entry
        let entry = json!({
            "tx_id": tx.id,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            "table_creates": &tx.pending_table_creates,
            "table_drops": &tx.pending_table_drops,
            "schema_updates": &tx.pending_schema_updates,
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

    pub fn mark_transaction_complete(&mut self, tx_id: u64) -> Result<(), String> {
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

    pub fn is_transaction_valid(&self, tx: &Transaction) -> Result<bool, String> {
        // Open the WAL file
        let file = File::open(&self.path).map_err(|e| format!("Failed to open WAL: {}", e))?;

        // Check if the transaction is valid by comparing checksums
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

    pub fn sync(&self) -> Result<(), String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open WAL for writing: {}", e))?;

        file.sync_all()
            .map_err(|e| format!("Failed to sync WAL: {}", e))
    }

    fn rotate_log(&mut self) -> Result<(), String> {
        // Create backup path with timestamp
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

        Ok(())
    }

    pub fn cleanup_completed_transactions(&mut self) -> Result<(), String> {
        // Read all entries from the WAL
        let mut content = String::new();
        let file = File::open(&self.path).map_err(|e| format!("Failed to open WAL: {}", e))?;
        BufReader::new(file)
            .read_to_string(&mut content)
            .map_err(|e| format!("Failed to read WAL: {}", e))?;

        // Count the number of completed transactions
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
