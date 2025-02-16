#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel, ValueType,
};

#[test]
fn test_rollback_record_operations() {
    let mut engine = setup_test_db("rollback_records", IsolationLevel::Serializable);

    // Setup initial table
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    // Test insert rollback
    let mut tx = engine.begin_transaction();
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Test1")).unwrap();
    engine.rollback(tx).unwrap();

    // Verify insert was rolled back
    let mut tx = engine.begin_transaction();
    assert!(engine.get_record(&mut tx, "test_table", 1).is_none());
    engine.commit(tx).unwrap();

    // Test update rollback
    let mut tx = engine.begin_transaction();
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Test1")).unwrap();
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    engine.delete_record(&mut tx, "test_table", 1);
    engine.insert_record(&mut tx, "test_table", create_test_record(1, "Updated")).unwrap();
    engine.rollback(tx).unwrap();

    // Verify update was rolled back
    let mut tx = engine.begin_transaction();
    let record = engine.get_record(&mut tx, "test_table", 1).unwrap();
    assert_eq!(*record.get_field("name").unwrap(), ValueType::Str("Test1".to_string()));
    engine.commit(tx).unwrap();
}

#[test]
fn test_rollback_table_operations() {
    let mut engine = setup_test_db("rollback_tables", IsolationLevel::Serializable);

    // Test table creation rollback
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.rollback(tx).unwrap();

    // Verify table creation was rolled back
    let tx = engine.begin_transaction();
    assert!(engine.get_tables().is_empty());
    engine.commit(tx).unwrap();

    // Test table deletion rollback
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    engine.drop_table(&mut tx, "test_table");
    engine.rollback(tx).unwrap();

    // Verify table deletion was rolled back
    let tx = engine.begin_transaction();
    assert!(engine.get_tables().contains(&"test_table".to_string()));
    engine.commit(tx).unwrap();
}
