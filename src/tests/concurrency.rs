#![allow(unused_imports)]
use crate::{
    components::{
        database::{record::Record, value_type::ValueType},
        transaction::isolation_level::IsolationLevel,
    },
    tests::setup_test_db,
};

use std::sync::Arc;
use std::thread;

#[test]
fn test_concurrent_operations() {
    let engine = Arc::new(parking_lot::Mutex::new(setup_test_db(
        "concurrent_operations",
        IsolationLevel::Serializable,
    )));

    // Setup initial table
    {
        let mut engine_guard = engine.lock();
        let mut tx = engine_guard.begin_transaction();
        engine_guard.create_table(&mut tx, "test_table");
        engine_guard.commit(tx).unwrap();
    }

    // Spawn multiple threads doing concurrent operations
    let num_threads = 10;
    let records_per_thread = 100;
    let threads: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = engine.clone();
            thread::spawn(move || {
                let start_id = thread_id * records_per_thread;

                // Insert records
                {
                    let mut engine_guard = engine.lock();
                    let mut tx = engine_guard.begin_transaction();

                    for i in 0..records_per_thread {
                        let record = Record {
                            id: (start_id + i) as u64,
                            values: vec![ValueType::Str(format!(
                                "Thread {} Record {}",
                                thread_id, i
                            ))],
                            version: 1,
                            timestamp: 0,
                        };
                        engine_guard.insert_record(&mut tx, "test_table", record);
                    }

                    engine_guard.commit(tx).unwrap();
                }

                // Read and verify records
                {
                    let mut engine_guard = engine.lock();
                    let mut tx = engine_guard.begin_transaction();

                    for i in 0..records_per_thread {
                        let record = engine_guard
                            .get_record(&mut tx, "test_table", (start_id + i) as u64)
                            .expect("Record should exist");

                        assert_eq!(
                            record.values[0],
                            ValueType::Str(format!("Thread {} Record {}", thread_id, i))
                        );
                    }

                    engine_guard.commit(tx).unwrap();
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for thread in threads {
        thread.join().unwrap();
    }

    // Final verification
    let mut engine_guard = engine.lock();
    let mut tx = engine_guard.begin_transaction();

    // Verify total number of records
    let mut count = 0;
    for i in 0..(num_threads * records_per_thread) {
        if engine_guard
            .get_record(&mut tx, "test_table", i as u64)
            .is_some()
        {
            count += 1;
        }
    }

    assert_eq!(
        count,
        num_threads * records_per_thread,
        "Expected {} records, found {}",
        num_threads * records_per_thread,
        count
    );
}

#[test]
fn test_concurrent_large_data() {
    let engine = Arc::new(parking_lot::Mutex::new(setup_test_db(
        "concurrent_large_data",
        IsolationLevel::Serializable,
    )));

    // Setup table
    {
        let mut engine_guard = engine.lock();
        let mut tx = engine_guard.begin_transaction();
        engine_guard.create_table(&mut tx, "large_data");
        engine_guard.commit(tx).unwrap();
    }

    // Spawn threads handling large records
    let num_threads = 5;
    let threads: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let engine = engine.clone();
            thread::spawn(move || {
                let large_data = "X".repeat(20000); // 20KB per record

                // Insert large record
                {
                    let mut engine_guard = engine.lock();
                    let mut tx = engine_guard.begin_transaction();

                    let record = Record {
                        id: thread_id as u64,
                        values: vec![ValueType::Str(large_data.clone())],
                        version: 1,
                        timestamp: 0,
                    };

                    engine_guard.insert_record(&mut tx, "large_data", record);
                    engine_guard.commit(tx).unwrap();
                }

                // Verify data
                {
                    let mut engine_guard = engine.lock();
                    let mut tx = engine_guard.begin_transaction();

                    let record = engine_guard
                        .get_record(&mut tx, "large_data", thread_id as u64)
                        .expect("Large record should exist");

                    match &record.values[0] {
                        ValueType::Str(s) => assert_eq!(s.len(), 20000),
                        _ => panic!("Unexpected value type"),
                    }

                    engine_guard.commit(tx).unwrap();
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for thread in threads {
        thread.join().unwrap();
    }
}
