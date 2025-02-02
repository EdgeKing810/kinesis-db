#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

#[test]
fn test_invalid_table_operations() {
    let mut engine = setup_test_db("invalid_table_operations");
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);

    // Try to insert into non-existent table
    let record = create_test_record(1, "Test");
    engine.insert_record(&mut tx, "nonexistent_table", record);
    assert!(engine.commit(tx).is_err());
}

#[test]
fn test_deadlock_detection() {
    let mut engine = setup_test_db("deadlock_detection");

    // Setup
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    // Create initial records
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Test1"));
    engine.insert_record(&mut tx, "test_table", create_test_record(2, "Test2"));
    engine.commit(tx).unwrap();

    // Transaction 1 locks record 1
    let mut tx1 = engine.begin_transaction(IsolationLevel::Serializable);
    engine.delete_record(&mut tx1, "test_table", 1);

    // Transaction 2 locks record 2 and tries to access record 1
    let mut tx2 = engine.begin_transaction(IsolationLevel::Serializable);
    engine.delete_record(&mut tx2, "test_table", 2);
    engine.delete_record(&mut tx2, "test_table", 1);

    // One of these should fail due to deadlock detection
    let result1 = engine.commit(tx1);
    let result2 = engine.commit(tx2);

    assert!(result1.is_err() || result2.is_err());
}
