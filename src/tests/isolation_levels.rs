#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

#[test]
fn test_read_committed_isolation() {
    let mut engine = setup_test_db("read_committed_isolation");
    
    // Setup
    let mut tx = engine.begin_transaction(IsolationLevel::ReadCommitted);
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    // First transaction: Insert and delete records
    let mut tx1 = engine.begin_transaction(IsolationLevel::ReadCommitted);
    engine.insert_record(&mut tx1, "test_table", create_test_record(1, "Initial"));

    // Second transaction: Should not see uncommitted changes
    let mut tx2 = engine.begin_transaction(IsolationLevel::ReadCommitted);
    assert!(engine.get_record(&mut tx2, "test_table", 1).is_none(), "Read Committed should prevent dirty reads");
    
    // After T1 commits, T2 should see the changes
    engine.commit(tx1).unwrap();
    assert!(engine.get_record(&mut tx2, "test_table", 1).is_some());

    // Test that Read Committed allows non-repeatable reads
    let record1 = engine.get_record(&mut tx2, "test_table", 1).unwrap();

    // Third transaction: Delete and reinsert record
    let mut tx3 = engine.begin_transaction(IsolationLevel::ReadCommitted);
    engine.delete_record(&mut tx3, "test_table", 1);
    engine.insert_record(&mut tx3, "test_table", create_test_record(1, "Modified"));
    engine.commit(tx3).unwrap();

    // T2 should see the new version (non-repeatable read allowed)
    let record2 = engine.get_record(&mut tx2, "test_table", 1).unwrap();
    assert_ne!(record1.values, record2.values, "Read Committed should allow non-repeatable reads");
    
    engine.commit(tx2).unwrap();
}

#[test]
fn test_repeatable_read_isolation() {
    let mut engine = setup_test_db("repeatable_read_isolation");
    
    // Setup
    let mut tx = engine.begin_transaction(IsolationLevel::RepeatableRead);
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();
    
    let mut tx = engine.begin_transaction(IsolationLevel::RepeatableRead);
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Initial"));
    engine.commit(tx).unwrap();

    // Transaction 1: First read
    let mut tx1 = engine.begin_transaction(IsolationLevel::RepeatableRead);
    let initial_read = engine.get_record(&mut tx1, "test_table", 1).unwrap();

    // Transaction 2: Modify data
    let mut tx2 = engine.begin_transaction(IsolationLevel::RepeatableRead);
    engine.delete_record(&mut tx2, "test_table", 1);
    engine.insert_record(&mut tx2, "test_table", create_test_record(1, "Modified"));
    engine.commit(tx2).unwrap();

    // T1 should still see the original data (repeatable read guaranteed)
    let second_read = engine.get_record(&mut tx1, "test_table", 1).unwrap();
    assert_eq!(initial_read.values, second_read.values, "Repeatable Read should prevent non-repeatable reads");

    // Test that phantom reads are also prevented in Repeatable Read
    let mut tx3 = engine.begin_transaction(IsolationLevel::RepeatableRead);
    engine.insert_record(&mut tx3, "test_table", create_test_record(2, "Phantom"));
    engine.commit(tx3).unwrap();

    // T1 should NOT see the new record (phantom reads prevented)
    assert!(engine.get_record(&mut tx1, "test_table", 2).is_none(), "Repeatable Read should prevent phantom reads");

    engine.commit(tx1).unwrap();
}

#[test]
fn test_serializable_isolation() {
    let mut engine = setup_test_db("serializable_isolation");

    // Setup
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    // First transaction
    let mut tx1 = engine.begin_transaction(IsolationLevel::Serializable);
    engine.insert_record(&mut tx1, "test_table", create_test_record(1, "Test"));

    // Second transaction trying to modify same record
    let mut tx2 = engine.begin_transaction(IsolationLevel::Serializable);
    engine.insert_record(&mut tx2, "test_table", create_test_record(1, "Test2"));

    // First transaction should succeed
    assert!(engine.commit(tx1).is_ok());
    // Second transaction should fail
    assert!(engine.commit(tx2).is_err());
}
