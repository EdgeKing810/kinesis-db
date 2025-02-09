use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};

use super::{config::TransactionConfig, isolation_level::IsolationLevel, transaction::Transaction};

// Add a transaction manager to handle concurrent transactions
pub struct TransactionManager {
    active_transactions: HashMap<u64, (SystemTime, Transaction)>,
    locks: HashMap<(String, u64), u64>, // (table, id) -> tx_id
    wait_for_graph: HashMap<u64, HashSet<u64>>, // Changed Vec to HashSet
    config: TransactionConfig,
}

impl TransactionManager {
    pub fn new(config: Option<TransactionConfig>) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            locks: HashMap::new(),
            wait_for_graph: HashMap::new(),
            config: config.unwrap_or_default(),
        }
    }

    pub fn start_transaction(&mut self, tx_id: u64) {
        self.active_transactions.insert(
            tx_id,
            (
                std::time::SystemTime::now(),
                Transaction::new(tx_id, IsolationLevel::Serializable, None),
            ),
        );
    }

    pub fn end_transaction(&mut self, tx_id: u64) {
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

    pub fn acquire_lock_with_retry(
        &mut self,
        tx_id: u64,
        table_name: &str,
        record_id: u64,
    ) -> bool {
        let mut retries = 0;
        while retries < self.config.max_retries {
            if self.acquire_lock(tx_id, table_name, record_id) {
                return true;
            }

            // Check for deadlock
            if self.has_deadlock(tx_id) {
                return false;
            }

            // Wait before retrying
            std::thread::sleep(Duration::from_millis(
                self.config.deadlock_detection_interval_ms,
            ));
            retries += 1;
        }
        false
    }

    pub fn release_lock(&mut self, tx_id: u64, table_name: &str, record_id: u64) {
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

    pub fn is_transaction_expired(&self, tx_id: u64) -> bool {
        if let Some((start_time, _)) = self.active_transactions.get(&tx_id) {
            if let Ok(elapsed) = start_time.elapsed() {
                return elapsed > Duration::from_secs(self.config.timeout_secs);
            }
        }
        false
    }

    pub fn cleanup_expired_transactions(&mut self) {
        let expired: Vec<_> = self
            .active_transactions
            .iter()
            .filter(|(_, &(start_time, _))| {
                start_time
                    .elapsed()
                    .map(|e| e > Duration::from_secs(self.config.timeout_secs))
                    .unwrap_or(true)
            })
            .map(|(&tx_id, _)| tx_id)
            .collect();

        for tx_id in expired {
            self.end_transaction(tx_id);
        }
    }

    pub fn has_deadlock(&self, tx_id: u64) -> bool {
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
