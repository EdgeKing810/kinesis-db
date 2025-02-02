#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

#[test]
fn test_create_table() {
    let mut engine = setup_test_db("create_table");
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);

    engine.create_table(&mut tx, "test_table");
    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_insert_and_get_record() {
    let mut engine = setup_test_db("insert_and_get_record");
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);

    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    let record = create_test_record(1, "Test");
    engine.insert_record(&mut tx, "test_table", record);
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    let retrieved = engine.get_record(&mut tx, "test_table", 1);
    assert!(retrieved.is_some());
}

#[test]
fn test_delete_record() {
    let mut engine = setup_test_db("delete_record");
    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);

    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    let record = create_test_record(1, "Test");
    engine.insert_record(&mut tx, "test_table", record);
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    engine.delete_record(&mut tx, "test_table", 1);
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction(IsolationLevel::Serializable);
    let retrieved = engine.get_record(&mut tx, "test_table", 1);
    assert!(retrieved.is_none());
}
