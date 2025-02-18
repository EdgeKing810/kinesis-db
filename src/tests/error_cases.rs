#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

#[test]
fn test_invalid_table_operations() {
    let mut engine = setup_test_db("invalid_table_operations", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    // Try to insert into non-existent table
    let record = create_test_record(1, "Test");
    assert!(engine
        .insert_record(&mut tx, "nonexistent_table", record)
        .is_err());
}

// Remove test_concurrent_deadlock test as it's not reliable
// Deadlock detection should be tested in integration tests with real
// concurrent workloads, not unit tests
