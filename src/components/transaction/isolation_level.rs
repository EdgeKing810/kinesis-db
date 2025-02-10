use serde::{Deserialize, Serialize};

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
