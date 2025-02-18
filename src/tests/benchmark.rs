#![allow(unused_imports)]
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, RwLock,
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    components::database::{
        db_type::DatabaseType,
        record::Record,
        schema::{FieldConstraint, FieldType, TableSchema},
        value_type::ValueType,
    },
    tests::setup_test_db,
    DBEngine, IsolationLevel, RestorePolicy,
};

#[derive(Debug)]
struct OperationStats {
    total_inserts: AtomicUsize,
    successful_inserts: AtomicUsize,
    failed_inserts: AtomicUsize,
    total_updates: AtomicUsize,
    successful_updates: AtomicUsize,
    failed_updates: AtomicUsize,
    total_deletes: AtomicUsize,
    successful_deletes: AtomicUsize,
    failed_deletes: AtomicUsize,
}

impl OperationStats {
    fn new() -> Self {
        Self {
            total_inserts: AtomicUsize::new(0),
            successful_inserts: AtomicUsize::new(0),
            failed_inserts: AtomicUsize::new(0),
            total_updates: AtomicUsize::new(0),
            successful_updates: AtomicUsize::new(0),
            failed_updates: AtomicUsize::new(0),
            total_deletes: AtomicUsize::new(0),
            successful_deletes: AtomicUsize::new(0),
            failed_deletes: AtomicUsize::new(0),
        }
    }
}

#[test]
fn test_read_benchmark() {
    const THREAD_COUNT: usize = 4;
    const INITIAL_RECORDS: usize = 10_000;
    const TEST_DURATION: u64 = 10;

    // Setup initial data
    let engine = Arc::new(RwLock::new(
        setup_test_database(
            "read_benchmark",
            INITIAL_RECORDS,
            IsolationLevel::ReadCommitted,
        )
        .unwrap(),
    ));
    let barrier = Arc::new(Barrier::new(THREAD_COUNT));
    let total_reads = Arc::new(AtomicUsize::new(0));
    let successful_reads = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let threads: Vec<_> = (0..THREAD_COUNT)
        .map(|_| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let total_reads = Arc::clone(&total_reads);
            let successful_reads = Arc::clone(&successful_reads);

            thread::spawn(move || {
                barrier.wait();

                let mut engine_guard = engine.write().unwrap();
                // Use proper transaction for reading
                let mut tx = engine_guard.begin_transaction();

                while start.elapsed() < Duration::from_secs(TEST_DURATION) {
                    let record_id = rand::random::<u64>() % INITIAL_RECORDS as u64;
                    total_reads.fetch_add(1, Ordering::Relaxed);

                    if let Some(record) = engine_guard.get_record(&mut tx, "benchmark", record_id) {
                        if let Some(ValueType::Str(_)) = record.get_field("data") {
                            successful_reads.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Commit read transaction (this is important for maintaining ACID)
                let _ = engine_guard.commit(tx);
            })
        })
        .collect();

    for handle in threads {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = total_reads.load(Ordering::Relaxed);
    let successful = successful_reads.load(Ordering::Relaxed);
    let ops_per_second = total as f64 / elapsed.as_secs_f64();

    println!("Read Performance Results:");
    println!("Total reads: {}", total);
    println!("Successful reads: {}", successful);
    println!("Time elapsed: {:.2?}", elapsed);
    println!("Reads per second: {:.2}", ops_per_second);

    assert!(
        ops_per_second >= 180_000.0,
        "Read performance below threshold"
    );
}

#[test]
fn test_write_benchmark() {
    const THREAD_COUNT: usize = 4;
    const TEST_DURATION: u64 = 10;
    const BATCH_SIZE: usize = 500; // Increased batch size
    const UPDATE_RATIO: f32 = 0.3; // 30% updates
    const DELETE_RATIO: f32 = 0.1; // 10% deletes

    // Track operations by type
    let stats = Arc::new(OperationStats::new());

    let engine = Arc::new(RwLock::new(
        setup_test_database("write_benchmark", 0, IsolationLevel::ReadCommitted).unwrap(),
    ));
    let barrier = Arc::new(Barrier::new(THREAD_COUNT));
    let start = Instant::now(); // Removed operations counter

    let threads: Vec<_> = (0..THREAD_COUNT)
        .map(|thread_id| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let stats = Arc::clone(&stats); // Clone the Arc for the thread

            thread::spawn(move || {
                let mut local_ops = 0;
                let mut inserted_ids = Vec::with_capacity(BATCH_SIZE * 2); // Pre-allocate Vec
                barrier.wait();

                while start.elapsed() < Duration::from_secs(TEST_DURATION) {
                    let mut db = engine.write().unwrap();
                    let mut tx = db.begin_transaction();
                    let mut batch_records = Vec::with_capacity(BATCH_SIZE); // Pre-allocate batch

                    // Prepare batch of records before transaction
                    for i in 0..BATCH_SIZE {
                        let id = thread_id as u64 * 10000 + local_ops as u64 + i as u64;
                        let mut record = Record::new(id);
                        record.set_field("data", ValueType::Str(format!("data_{}", id)));
                        batch_records.push(record);
                    }

                    // Bulk insert with tracking
                    for record in batch_records {
                        let record_id = record.id;
                        stats.total_inserts.fetch_add(1, Ordering::Relaxed);
                        match db.insert_record(&mut tx, "benchmark", record) {
                            Ok(_) => {
                                inserted_ids.push(record_id);
                                stats.successful_inserts.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                stats.failed_inserts.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }

                    // Commit inserts
                    if db.commit(tx).is_ok() {
                        local_ops += BATCH_SIZE;

                        // Prepare updates and deletes in a single transaction
                        if !inserted_ids.is_empty() {
                            let mut tx = db.begin_transaction();

                            // Updates with tracking
                            let update_count = (inserted_ids.len() as f32 * UPDATE_RATIO) as usize;
                            let mut updates = HashMap::new();
                            updates
                                .insert("data".to_string(), ValueType::Str("updated".to_string()));

                            for &id in inserted_ids.iter().take(update_count) {
                                stats.total_updates.fetch_add(1, Ordering::Relaxed);
                                match db.update_record(&mut tx, "benchmark", id, updates.clone()) {
                                    Ok(_) => {
                                        stats.successful_updates.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        stats.failed_updates.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }

                            // Deletes with tracking
                            let delete_start = update_count;
                            let delete_count = (inserted_ids.len() as f32 * DELETE_RATIO) as usize;

                            for &id in inserted_ids.iter().skip(delete_start).take(delete_count) {
                                stats.total_deletes.fetch_add(1, Ordering::Relaxed);
                                if db.delete_record(&mut tx, "benchmark", id) {
                                    stats.successful_deletes.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    stats.failed_deletes.fetch_add(1, Ordering::Relaxed);
                                }
                            }

                            if db.commit(tx).is_ok() {
                                // Remove deleted records from tracking
                                inserted_ids.drain(delete_start..delete_start + delete_count);
                            }
                        }
                    }

                    // Prevent unbounded growth of inserted_ids
                    if inserted_ids.len() > BATCH_SIZE * 4 {
                        inserted_ids.drain(..BATCH_SIZE);
                    }
                }
            })
        })
        .collect();

    for handle in threads {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let stats = Arc::try_unwrap(stats).unwrap();

    // Calculate total operations and percentages
    let total_insert_attempts = stats.total_inserts.load(Ordering::Relaxed);
    let total_update_attempts = stats.total_updates.load(Ordering::Relaxed);
    let total_delete_attempts = stats.total_deletes.load(Ordering::Relaxed);

    let successful_inserts = stats.successful_inserts.load(Ordering::Relaxed);
    let successful_updates = stats.successful_updates.load(Ordering::Relaxed);
    let successful_deletes = stats.successful_deletes.load(Ordering::Relaxed);

    let failed_inserts = stats.failed_inserts.load(Ordering::Relaxed);
    let failed_updates = stats.failed_updates.load(Ordering::Relaxed);
    let failed_deletes = stats.failed_deletes.load(Ordering::Relaxed);

    let total_attempts = total_insert_attempts + total_update_attempts + total_delete_attempts;
    let total_successes = successful_inserts + successful_updates + successful_deletes;
    let total_failures = failed_inserts + failed_updates + failed_deletes;

    println!("\nDetailed Operation Statistics:");
    println!(
        "Inserts: {} total ({:.1}% success, {:.1}% failure)",
        total_insert_attempts,
        (successful_inserts as f64 / total_insert_attempts as f64) * 100.0,
        (failed_inserts as f64 / total_insert_attempts as f64) * 100.0
    );
    println!(
        "Updates: {} total ({:.1}% success, {:.1}% failure)",
        total_update_attempts,
        (successful_updates as f64 / total_update_attempts as f64) * 100.0,
        (failed_updates as f64 / total_update_attempts as f64) * 100.0
    );
    println!(
        "Deletes: {} total ({:.1}% success, {:.1}% failure)",
        total_delete_attempts,
        (successful_deletes as f64 / total_delete_attempts as f64) * 100.0,
        (failed_deletes as f64 / total_delete_attempts as f64) * 100.0
    );

    println!("\nOverall Statistics:");
    println!("Total operations attempted: {}", total_attempts);
    println!(
        "Total successful operations: {} ({:.1}%)",
        total_successes,
        (total_successes as f64 / total_attempts as f64) * 100.0
    );
    println!(
        "Total failed operations: {} ({:.1}%)",
        total_failures,
        (total_failures as f64 / total_attempts as f64) * 100.0
    );

    let ops_per_second = total_successes as f64 / elapsed.as_secs_f64();
    println!("Time elapsed: {:.2?}", elapsed);
    println!("Operations per second: {:.2}", ops_per_second);

    assert!(
        ops_per_second >= 1000.0,
        "Write performance below threshold"
    );
}

// Helper function to set up test database
fn setup_test_database(
    test_id: &str,
    initial_records: usize,
    isolation_level: IsolationLevel,
) -> Result<DBEngine, String> {
    let mut engine = setup_test_db(test_id, isolation_level);

    // Create test table
    let mut fields = HashMap::new();
    fields.insert(
        "data".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: None,
        },
    );

    let schema = TableSchema {
        name: "benchmark".to_string(),
        fields,
        version: 1,
    };

    let mut tx = engine.begin_transaction();
    engine.create_table_with_schema(&mut tx, "benchmark", schema);
    engine.commit(tx).unwrap();

    // Insert initial records if needed
    if initial_records > 0 {
        let mut tx = engine.begin_transaction();
        for i in 0..initial_records {
            let mut record = Record::new(i as u64);
            record.set_field("data", ValueType::Str(format!("initial_{}", i)));
            engine.insert_record(&mut tx, "benchmark", record).unwrap();
        }
        engine.commit(tx).unwrap();
    }

    Ok(engine)
}
