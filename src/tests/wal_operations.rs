#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, get_test_dir, setup_test_db, setup_test_db_dirty},
    IsolationLevel,
};
use std::fs;

#[test]
fn test_wal_recovery() {
    let mut engine = setup_test_db("wal_recovery", IsolationLevel::Serializable);

    // Create initial state
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    engine
        .insert_record(&mut tx, "test_table", create_test_record(1, "Test"))
        .unwrap();
    engine.commit(tx).unwrap();

    // Force a new engine instance to test recovery
    drop(engine);
    let mut engine = setup_test_db_dirty("wal_recovery", IsolationLevel::Serializable);

    let mut tx = engine.begin_transaction();
    let record = engine.get_record(&mut tx, "test_table", 1);
    assert!(record.is_some());
}

#[test]
fn test_wal_rotation() {
    let mut engine = setup_test_db("wal_rotation", IsolationLevel::Serializable);

    // Create enough transactions to trigger rotation
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    for i in 0..150 {
        // More than rotation threshold
        let mut tx = engine.begin_transaction();
        engine
            .insert_record(
                &mut tx,
                "test_table",
                create_test_record(i, &format!("Test{}", i)),
            )
            .unwrap();
        engine.commit(tx).unwrap();
    }

    // Check if WAL backup exists
    assert!(fs::read_dir(get_test_dir()).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with("test_wal_wal_rotation.log.")));
}
