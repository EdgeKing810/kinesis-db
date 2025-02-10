#![allow(unused_imports)]
use crate::{
    components::{
        database::{record::Record, value_type::ValueType},
        transaction::isolation_level::IsolationLevel,
    },
    tests::{setup_test_db, setup_test_db_dirty},
};

#[test]
fn test_large_data_persistence() {
    let mut engine = setup_test_db("large_data_persistence", IsolationLevel::Serializable);

    // Create large test data (>16KB)
    let large_string = "X".repeat(20000); // 20KB string
    let table_name = "test_table";

    // Create table and insert record with large data
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, table_name);

    let record = Record {
        id: 1,
        values: vec![ValueType::Str(large_string.clone())],
        version: 1,
        timestamp: 0,
    };

    engine.insert_record(&mut tx, table_name, record);
    engine.commit(tx).unwrap();

    // Force flush to disk
    drop(engine);

    // Create new engine instance to test loading from disk
    let mut engine = setup_test_db_dirty("large_data_persistence", IsolationLevel::Serializable);

    // Read and verify data
    let mut tx = engine.begin_transaction();
    println!("{:?}", engine.get_record(&mut tx, table_name, 1));
    let loaded_record = engine
        .get_record(&mut tx, table_name, 1)
        .expect("Record should be loaded from disk");

    let loaded_str = match loaded_record.values[0].clone() {
        ValueType::Str(s) => s,
        _ => String::new(),
    };

    assert_eq!(
        loaded_str.len(),
        large_string.len(),
        "Loaded data length should match original"
    );

    assert_eq!(
        loaded_str, large_string,
        "Loaded data content should match original"
    );

    // Additional verification for multiple large records
    let mut tx = engine.begin_transaction();
    let second_large_string = "Y".repeat(18000); // 18KB
    let record2 = Record {
        id: 2,
        values: vec![ValueType::Str(second_large_string.clone())],
        version: 1,
        timestamp: 0,
    };

    engine.insert_record(&mut tx, table_name, record2);
    engine.commit(tx).unwrap();

    // Force flush and reload
    drop(engine);

    let mut engine = setup_test_db_dirty("large_data_persistence", IsolationLevel::Serializable);

    // Verify both records
    let mut tx = engine.begin_transaction();
    let loaded_record1 = engine.get_record(&mut tx, table_name, 1).unwrap();
    let loaded_record2 = engine.get_record(&mut tx, table_name, 2).unwrap();

    let loaded_str_one = match loaded_record1.values[0].clone() {
        ValueType::Str(s) => s,
        _ => String::new(),
    };

    let loaded_str_two = match loaded_record2.values[0].clone() {
        ValueType::Str(s) => s,
        _ => String::new(),
    };

    assert_eq!(loaded_str_one, large_string);
    assert_eq!(loaded_str_two, second_large_string);
}
