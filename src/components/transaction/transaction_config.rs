// Add transaction timeout configuration
pub struct TransactionConfig {
    pub timeout_secs: u64,
    pub max_retries: u32,
    pub deadlock_detection_interval_ms: u64,
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
