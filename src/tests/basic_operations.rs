#![allow(unused_imports)]
use std::collections::HashMap;

use crate::{
    components::database::{record::Record, schema::{FieldConstraint, FieldType, TableSchema}, value_type::ValueType}, tests::{create_test_record, setup_test_db}, IsolationLevel
};

#[test]
fn test_create_table() {
    let mut engine = setup_test_db("create_table", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    engine.create_table(&mut tx, "test_table");
    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_insert_and_get_record() {
    let mut engine = setup_test_db("insert_and_get_record", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    let record = create_test_record(1, "Test");
    engine.insert_record(&mut tx, "test_table", record).unwrap();
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    let retrieved = engine.get_record(&mut tx, "test_table", 1);
    assert!(retrieved.is_some());
}

#[test]
fn test_delete_record() {
    let mut engine = setup_test_db("delete_record", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    engine.create_table(&mut tx, "test_table");
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    let record = create_test_record(1, "Test");
    engine.insert_record(&mut tx, "test_table", record).unwrap();
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    engine.delete_record(&mut tx, "test_table", 1);
    engine.commit(tx).unwrap();

    let mut tx = engine.begin_transaction();
    let retrieved = engine.get_record(&mut tx, "test_table", 1);
    assert!(retrieved.is_none());
}

#[test]
fn test_schema_validation() {
    let mut engine = setup_test_db("schema_validation", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    // Create table schema
    let mut fields = HashMap::new();
    fields.insert(
        "username".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: Some(3.0),
            max: Some(20.0),
            pattern: Some(r"^[a-zA-Z0-9_]+$".to_string()),
        },
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: true,
            min: None,
            max: None,
            pattern: None
        },
    );

    let schema = TableSchema {
        name: "users".to_string(),
        fields,
    };

    // Create table with schema
    engine.create_table_with_schema(&mut tx, "users", schema);
    engine.commit(tx).unwrap();

    // Test valid record
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(1);
    record.set_field("username", ValueType::Str("john_doe".to_string()));
    record.set_field("age", ValueType::Int(25));
    
    assert!(engine.insert_record(&mut tx, "users", record).is_ok());
    engine.commit(tx).unwrap();

    // Test invalid record (username too short)
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(2);
    record.set_field("username", ValueType::Str("ab".to_string()));
    record.set_field("age", ValueType::Int(25));
    
    assert!(engine.insert_record(&mut tx, "users", record).is_err());
}

#[test]
fn test_required_fields_validation() {
    let mut engine = setup_test_db("schema_required_fields", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    let mut fields = HashMap::new();
    fields.insert(
        "username".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None
        },
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: false, // Optional field
            min: Some(0.0),
            max: Some(150.0),
            pattern: None
        },
    );

    let schema = TableSchema {
        name: "users".to_string(),
        fields,
    };

    // Create table with schema
    engine.create_table_with_schema(&mut tx, "users", schema);
    engine.commit(tx).unwrap();

    // Test missing required field
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(1);
    record.set_field("age", ValueType::Int(25));
    
    assert!(
        engine.insert_record(&mut tx, "users", record).is_err(),
        "Should fail when required field 'username' is missing"
    );

    // Test unknown field
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(2);
    record.set_field("username", ValueType::Str("john".to_string()));
    record.set_field("unknown_field", ValueType::Int(42));
    
    assert!(
        engine.insert_record(&mut tx, "users", record).is_err(),
        "Should fail when unknown field is present"
    );

    // Test valid record with optional field omitted
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(3);
    record.set_field("username", ValueType::Str("john".to_string()));
    
    assert!(
        engine.insert_record(&mut tx, "users", record).is_ok(),
        "Should succeed with only required fields"
    );
}

#[test]
fn test_field_type_validation() {
    let mut engine = setup_test_db("schema_field_type_validation", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    let mut fields = HashMap::new();
    fields.insert(
        "string_field".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None
        },
    );
    fields.insert(
        "int_field".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: true,
            min: Some(0.0),
            max: Some(100.0),
            pattern: None
        },
    );

    let schema = TableSchema {
        name: "test_types".to_string(),
        fields,
    };

    engine.create_table_with_schema(&mut tx, "test_types", schema);
    engine.commit(tx).unwrap();

    // Test type mismatch (string in int field)
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(1);
    record.set_field("string_field", ValueType::Str("valid".to_string()));
    record.set_field("int_field", ValueType::Str("invalid".to_string()));
    
    assert!(
        engine.insert_record(&mut tx, "test_types", record).is_err(),
        "Should fail when field type doesn't match schema type"
    );

    // Test type mismatch (int in string field)
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(2);
    record.set_field("string_field", ValueType::Int(42));
    record.set_field("int_field", ValueType::Int(42));
    
    assert!(
        engine.insert_record(&mut tx, "test_types", record).is_err(),
        "Should fail when field type doesn't match schema type"
    );

    // Test correct types
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(3);
    record.set_field("string_field", ValueType::Str("valid".to_string()));
    record.set_field("int_field", ValueType::Int(42));
    
    assert!(
        engine.insert_record(&mut tx, "test_types", record).is_ok(),
        "Should succeed when field types match schema types"
    );
}
