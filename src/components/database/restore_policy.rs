// Add a restore policy enum
#[derive(Debug, PartialEq)]
pub enum RestorePolicy {
    RecoverPending, // All pending transactions are recovered
    RecoverAll,     // All transactions are recovered
    Discard,        // All transactions are discarded
}

impl Default for RestorePolicy {
    fn default() -> Self {
        RestorePolicy::RecoverPending
    }
}
