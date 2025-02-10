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
    engine.insert_record(&mut tx, "nonexistent_table", record);
    assert!(engine.commit(tx).is_err());
}

#[test]
fn test_deadlock_detection() {
    let mut engine = setup_test_db("deadlock_detection", IsolationLevel::RepeatableRead);

    // Setup
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    // Create initial records
    let mut tx = engine.begin_transaction();
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Test1"));
    engine.insert_record(&mut tx, "test_table", create_test_record(2, "Test2"));
    engine.commit(tx).unwrap();

    // Start two concurrent transactions
    let mut tx1 = engine.begin_transaction();
    let mut tx2 = engine.begin_transaction();

    // T1 locks record 1
    engine.delete_record(&mut tx1, "test_table", 1);

    // T2 locks record 2 (succeeds)
    engine.delete_record(&mut tx2, "test_table", 2);

    // T2 tries to lock record 1 (waits for T1)
    engine.delete_record(&mut tx2, "test_table", 1);

    // T1 tries to lock record 2 (creates deadlock)
    engine.delete_record(&mut tx1, "test_table", 2);

    // Try to commit both transactions
    let result1 = engine.commit(tx1);
    let result2 = engine.commit(tx2);

    // Check that at least one transaction failed with a deadlock error
    assert!(
        result1.is_err() || result2.is_err(),
        "Expected at least one transaction to fail"
    );

    // Verify that the error message mentions deadlock
    let err_msg1 = result1.err().unwrap_or_default();
    let err_msg2 = result2.err().unwrap_or_default();
    assert!(
        err_msg1.contains("Deadlock") || err_msg2.contains("Deadlock"),
        "Expected deadlock error message, got '{}' and '{}'",
        err_msg1,
        err_msg2
    );
}
