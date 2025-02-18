#![allow(dead_code)]

use crate::{DBEngine, DatabaseType, IsolationLevel, Record, RestorePolicy, ValueType};
use std::fs;
use std::path::PathBuf;

mod basic_operations;
mod benchmark;
mod buffer_pool;
pub mod concurrency;
mod database_types;
mod error_cases;
mod isolation_levels;
mod large_data_persistence;
mod performance;
mod rollback;
mod schema;
mod wal_operations;

// Helper to create a unique test directory
fn get_test_dir() -> PathBuf {
    let data_dir = PathBuf::from("data");
    fs::create_dir_all(&data_dir).unwrap();

    let test_dir = PathBuf::from("data/tests");
    fs::create_dir_all(&test_dir).unwrap();
    test_dir
}

// Helper to get unique file paths for a test
fn get_test_files(test_id: &str) -> (PathBuf, PathBuf) {
    let test_dir = get_test_dir();
    let db_path = test_dir.join(format!("test_db_{}.pages", test_id));
    let wal_path = test_dir.join(format!("test_wal_{}.log", test_id));
    (db_path, wal_path)
}

// Add cleanup helper
fn cleanup_test_files(db_path: &PathBuf, wal_path: &PathBuf) {
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.pages", db_path.to_str().unwrap()));
    let _ = std::fs::remove_file(wal_path);
}

// Setup function now takes paths as parameters
pub fn setup_test_db_with_paths(
    db_path: PathBuf,
    wal_path: PathBuf,
    restore_policy: RestorePolicy,
    isolation_level: IsolationLevel,
    cleanup: bool,
) -> DBEngine {
    if cleanup {
        cleanup_test_files(&db_path, &wal_path);
    }

    DBEngine::new(
        DatabaseType::Hybrid,
        restore_policy,
        db_path.to_str().unwrap(),
        wal_path.to_str().unwrap(),
        None,
        isolation_level,
    )
}

pub fn setup_test_db(test_id: &str, isolation_level: IsolationLevel) -> DBEngine {
    let (db_path, wal_path) = get_test_files(test_id);
    setup_test_db_with_paths(
        db_path,
        wal_path,
        RestorePolicy::RecoverPending,
        isolation_level,
        true,
    )
}

pub fn setup_test_db_dirty(test_id: &str, isolation_level: IsolationLevel) -> DBEngine {
    let (db_path, wal_path) = get_test_files(test_id);
    setup_test_db_with_paths(
        db_path,
        wal_path,
        RestorePolicy::RecoverAll,
        isolation_level,
        false,
    )
}

pub fn create_test_record(id: u64, name: &str) -> Record {
    let mut record = Record::new(id);
    record.set_field("name", ValueType::Str(name.to_string()));
    record.set_field("age", ValueType::Int(30));
    record.set_field("active", ValueType::Bool(true));

    record
}
