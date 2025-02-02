#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

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
