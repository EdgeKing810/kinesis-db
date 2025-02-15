#![allow(unused_imports)]
use crate::{
    components::{
        database::{db_type::DatabaseType, record::Record, value_type::ValueType},
        transaction::isolation_level::IsolationLevel,
    },
    tests::get_test_dir,
    DBEngine, RestorePolicy,
};

fn create_test_db(name: &str, db_type: DatabaseType) -> DBEngine {
    let test_dir = get_test_dir();
    let db_path = test_dir.join(format!("test_db_{}.db", name));
    let wal_path = test_dir.join(format!("test_wal_{}.log", name));

    DBEngine::new(
        db_type,
        RestorePolicy::Discard,
        db_path.to_str().unwrap(),
        wal_path.to_str().unwrap(),
        None,
        IsolationLevel::ReadCommitted,
    )
}

#[test]
fn test_inmemory_database() {
    let mut engine = create_test_db("inmemory", DatabaseType::InMemory);

    // Create and insert data
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");

    let record = Record {
        id: 1,
        values: vec![ValueType::Str("test data".to_string())],
        version: 1,
        timestamp: 0,
    };

    engine.insert_record(&mut tx, "test_table", record);
    engine.commit(tx).unwrap();

    // Drop engine and create new one - data should be gone
    drop(engine);
    let mut engine = create_test_db("inmemory", DatabaseType::InMemory);

    let mut tx = engine.begin_transaction();
    assert!(
        engine.get_record(&mut tx, "test_table", 1).is_none(),
        "InMemory database should not persist data"
    );
}

#[test]
fn test_ondisk_database() {
    let mut engine = create_test_db("ondisk", DatabaseType::OnDisk);

    // Create and insert data
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");

    let record = Record {
        id: 1,
        values: vec![ValueType::Str("test data".to_string())],
        version: 1,
        timestamp: 0,
    };

    engine.insert_record(&mut tx, "test_table", record.clone());
    engine.commit(tx).unwrap();

    // Drop engine and create new one - data should persist
    drop(engine);
    let mut engine = create_test_db("ondisk", DatabaseType::OnDisk);

    let mut tx = engine.begin_transaction();
    let loaded_record = engine
        .get_record(&mut tx, "test_table", 1)
        .expect("OnDisk database should persist data");

    assert_eq!(
        loaded_record.values[0], record.values[0],
        "Loaded record should match original"
    );
}

#[test]
fn test_hybrid_database() {
    let mut engine = create_test_db("hybrid", DatabaseType::Hybrid);

    // Create and insert multiple records to test buffer behavior
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let num_records = 3000;
    const BATCH_SIZE: usize = 500;

    // Insert records in batches
    for batch_start in (0..num_records).step_by(BATCH_SIZE) {
        let mut tx = engine.begin_transaction();
        let batch_end = (batch_start + BATCH_SIZE).min(num_records);

        for i in batch_start..batch_end {
            let record = Record {
                id: i as u64,
                values: vec![ValueType::Str(format!("test data {}", i))],
                version: 1,
                timestamp: 0,
            };
            engine.insert_record(&mut tx, "test_table", record);
        }
        engine.commit(tx).unwrap();
    }

    // Verify all records are accessible
    let mut tx = engine.begin_transaction();
    for i in 0..num_records {
        assert!(
            engine.get_record(&mut tx, "test_table", i as u64).is_some(),
            "Record {} should be accessible",
            i
        );
    }

    // Drop engine and create new one - data should persist
    drop(engine);
    let mut engine = create_test_db("hybrid", DatabaseType::Hybrid);

    // Verify data persisted
    let mut tx = engine.begin_transaction();
    for i in 0..num_records {
        assert!(
            engine.get_record(&mut tx, "test_table", i as u64).is_some(),
            "Record {} should persist after restart",
            i
        );
    }

    // Verify buffer pool behavior
    let mut tx = engine.begin_transaction();
    // Access records in reverse order to test caching
    for i in (0..num_records).rev() {
        assert!(
            engine.get_record(&mut tx, "test_table", i as u64).is_some(),
            "Record {} should be accessible from cache or disk",
            i
        );
    }
}

// #[test]
// fn test_database_type_differences() {
//     for db_type in [DatabaseType::OnDisk, DatabaseType::Hybrid, DatabaseType::InMemory] {
//         let mut engine = create_test_db("type_test", db_type);
//         let mut tx = engine.begin_transaction();
//         engine.create_table(&mut tx, "test");

//         // Insert and measure small batch
//         for i in 0..50 {
//             engine.insert_record(&mut tx, "test", Record {
//                 id: i,
//                 values: vec![ValueType::Str(format!("test {}", i))],
//                 version: 1,
//                 timestamp: 0,
//             });
//         }
//         engine.commit(tx).unwrap();

//         let start = std::time::Instant::now();
//         let mut tx = engine.begin_transaction();
//         for i in 0..50 {
//             assert!(engine.get_record(&mut tx, "test", i).is_some());
//         }
//         let access_time = start.elapsed();
//         println!("Access time for {:?}: {:?}", db_type, access_time);

//         match db_type {
//             DatabaseType::InMemory => assert!(access_time.as_micros() < 1000),
//             DatabaseType::OnDisk => assert!(access_time.as_micros() > 100),
//             DatabaseType::Hybrid => assert!(access_time.as_micros() > 50 && access_time.as_micros() < 80),
//         }
//     }
// }

#[test]
fn test_database_type_persistence() {
    let test_cases = vec![
        (DatabaseType::InMemory, false), // shouldn't persist
        (DatabaseType::OnDisk, true),    // should persist
        (DatabaseType::Hybrid, true),    // should persist
    ];

    for (db_type, should_persist) in test_cases {
        let mut engine = create_test_db("test_persistence", db_type);

        // Write data
        let mut tx = engine.begin_transaction();
        engine.create_table(&mut tx, "test_table");

        let record = Record {
            id: 1,
            values: vec![ValueType::Str("test data".to_string())],
            version: 1,
            timestamp: 0,
        };

        engine.insert_record(&mut tx, "test_table", record);
        engine.commit(tx).unwrap();

        // Restart engine
        drop(engine);
        let mut new_engine = create_test_db("test_persistence", db_type);

        let mut tx = new_engine.begin_transaction();
        let exists = new_engine.get_record(&mut tx, "test_table", 1).is_some();
        assert_eq!(exists, should_persist);
    }
}

#[test]
fn test_hybrid_mode_behavior() {
    let mut engine = create_test_db("test_hybrid", DatabaseType::Hybrid);

    // Test both memory and disk operations
    let mut tx = engine.begin_transaction();

    // Memory operation
    engine.create_table(&mut tx, "memory_table");
    engine.insert_record(
        &mut tx,
        "memory_table",
        Record {
            id: 1,
            values: vec![ValueType::Int(1)],
            version: 1,
            timestamp: 0,
        },
    );

    // Force disk operation
    for i in 0..10000 {
        // Exceed buffer pool size
        engine.insert_record(
            &mut tx,
            "memory_table",
            Record {
                id: i,
                values: vec![ValueType::Int(i as i64)],
                version: 1,
                timestamp: 0,
            },
        );
    }

    engine.commit(tx).unwrap();

    // Verify both memory and disk access
    let mut tx = engine.begin_transaction();
    assert!(engine.get_record(&mut tx, "memory_table", 1).is_some());
    assert!(engine.get_record(&mut tx, "memory_table", 9999).is_some());
}

#[test]
fn test_inmemory_no_persistence() {
    let mut engine = create_test_db("test_inmemory", DatabaseType::InMemory);

    // Create and populate table
    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");
    engine.insert_record(
        &mut tx,
        "test_table",
        Record {
            id: 1,
            values: vec![ValueType::Str("test data".to_string())],
            version: 1,
            timestamp: 0,
        },
    );
    engine.commit(tx).unwrap();
    drop(engine);

    // Verify no files created
    assert!(!std::path::Path::new(&get_test_dir().join("test_db_test_inmemory.db.pages")).exists());
    assert!(!std::path::Path::new(&get_test_dir().join("test_wal_test_inmemory.log")).exists());

    // Check if data is still accessible
    let mut engine = create_test_db("test_inmemory", DatabaseType::InMemory);
    let mut tx = engine.begin_transaction();
    assert!(engine.get_record(&mut tx, "test_table", 1).is_none());
}

#[test]
fn test_ondisk_buffer_eviction() {
    let mut engine = create_test_db("test_ondisk", DatabaseType::OnDisk);

    let mut tx = engine.begin_transaction();
    engine.create_table(&mut tx, "test_table");

    // Fill buffer pool and force evictions
    for i in 0..1000 {
        // Exceed buffer pool size
        engine.insert_record(
            &mut tx,
            "test_table",
            Record {
                id: i,
                values: vec![ValueType::Int(i as i64)],
                version: 1,
                timestamp: 0,
            },
        );
    }

    engine.commit(tx).unwrap();

    // Verify data survives buffer eviction
    let mut tx = engine.begin_transaction();
    assert!(engine.get_record(&mut tx, "test_table", 0).is_some());
    assert!(engine.get_record(&mut tx, "test_table", 999).is_some());
}

#[test]
fn test_wal_per_database_type() {
    let test_cases = vec![
        (DatabaseType::InMemory, false), // no WAL
        (DatabaseType::OnDisk, true),    // uses WAL
        (DatabaseType::Hybrid, true),    // uses WAL
    ];

    for (db_type, uses_wal) in test_cases {
        let mut engine = create_test_db("test_wal", db_type);
        let _ = std::fs::remove_file(&get_test_dir().join("test_wal_test_wal.log"));

        let mut tx = engine.begin_transaction();
        engine.create_table(&mut tx, "test_table");
        engine.commit(tx).unwrap();

        let wal_exists =
            std::path::Path::new(&get_test_dir().join("test_wal_test_wal.log")).exists();
        assert_eq!(wal_exists, uses_wal);
    }
}

#[test]
fn test_recovery_by_database_type() {
    let test_cases = vec![
        (DatabaseType::InMemory, false), // no recovery
        (DatabaseType::OnDisk, true),    // recovers
        (DatabaseType::Hybrid, true),    // recovers
    ];

    for (db_type, should_recover) in test_cases {
        let mut engine = create_test_db("test_recovery", db_type);

        // Setup data
        let mut tx = engine.begin_transaction();
        engine.create_table(&mut tx, "test_table");
        engine.insert_record(
            &mut tx,
            "test_table",
            Record {
                id: 1,
                values: vec![ValueType::Str("test data".to_string())],
                version: 1,
                timestamp: 0,
            },
        );
        engine.commit(tx).unwrap();

        // Simulate crash
        drop(engine);

        // Recover
        let mut new_engine = create_test_db("test_recovery", db_type);
        let mut tx = new_engine.begin_transaction();
        let data_recovered = new_engine.get_record(&mut tx, "test_table", 1).is_some();
        assert_eq!(data_recovered, should_recover);
    }
}
