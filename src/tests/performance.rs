#![allow(unused_imports)]
use crate::{
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};
use std::time::Instant;

#[test]
fn test_bulk_operations() {
    let mut engine = setup_test_db("bulk_operations", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let start = Instant::now();

    // Insert 2500 records
    let mut tx = engine.begin_transaction();
    for i in 0..2500 {
        engine.insert_record(
            &mut tx,
            "test_table",
            create_test_record(i, &format!("Test{}", i)),
        );
    }
    engine.commit(tx).unwrap();

    let duration = start.elapsed();
    assert!(duration.as_secs() < 5); // Should complete within 5 seconds
}
