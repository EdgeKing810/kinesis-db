#[derive(Debug)]
pub struct TransactionConfig {
    pub timeout_secs: u64, // Timeout in seconds to fail a transaction
    pub max_retries: u32,  // Maximum number of retries for a transaction
    pub deadlock_detection_interval_ms: u64, // Interval in milliseconds to check for deadlocks
}

impl Default for TransactionConfig {
    fn default() -> Self {
        TransactionConfig {
            timeout_secs: 30,
            max_retries: 3,
            deadlock_detection_interval_ms: 100,
        }
    }
}
