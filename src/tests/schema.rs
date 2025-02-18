#![allow(unused_imports)]
use std::collections::HashMap;

use crate::{
    components::database::{
        record::Record,
        schema::{FieldConstraint, FieldType, TableSchema},
        value_type::ValueType,
    },
    tests::{create_test_record, setup_test_db},
    IsolationLevel,
};

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
            unique: false,
            default: None,
        },
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint::create_required(FieldType::Integer),
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
        FieldConstraint::create_required(FieldType::String),
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: false, // Optional field
            min: Some(0.0),
            max: Some(150.0),
            pattern: None,
            unique: false,
            default: None,
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
        FieldConstraint::create_required(FieldType::String),
    );
    fields.insert(
        "int_field".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: true,
            min: Some(0.0),
            max: Some(100.0),
            pattern: None,
            unique: false,
            default: None,
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

#[test]
fn test_unique_constraint() {
    let mut engine = setup_test_db("schema_unique_constraint", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    // Create table schema with a unique field
    let mut fields = HashMap::new();
    fields.insert(
        "username".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: true, // Make username unique
            default: None,
        },
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false, // Age doesn't need to be unique
            default: None,
        },
    );

    let schema = TableSchema {
        name: "users".to_string(),
        fields,
    };

    // Create table with schema
    engine.create_table_with_schema(&mut tx, "users", schema);
    engine.commit(tx).unwrap();

    // Test inserting first record (should succeed)
    let mut tx = engine.begin_transaction();
    let mut record1 = Record::new(1);
    record1.set_field("username", ValueType::Str("john_doe".to_string()));
    record1.set_field("age", ValueType::Int(25));

    assert!(engine.insert_record(&mut tx, "users", record1).is_ok());
    engine.commit(tx).unwrap();

    // Test inserting record with same username (should fail)
    let mut tx = engine.begin_transaction();
    let mut record2 = Record::new(2);
    record2.set_field("username", ValueType::Str("john_doe".to_string())); // Same username
    record2.set_field("age", ValueType::Int(30)); // Different age

    assert!(engine.insert_record(&mut tx, "users", record2).is_ok());
    assert!(engine.commit(tx).is_err());

    // Test inserting record with different username but same age (should succeed)
    let mut tx = engine.begin_transaction();
    let mut record3 = Record::new(3);
    record3.set_field("username", ValueType::Str("jane_doe".to_string())); // Different username
    record3.set_field("age", ValueType::Int(25)); // Same age as record1

    assert!(engine.insert_record(&mut tx, "users", record3).is_ok());
    engine.commit(tx).unwrap();
}

#[test]
fn test_default_values() {
    let mut engine = setup_test_db("schema_default_values", IsolationLevel::Serializable);
    let mut tx = engine.begin_transaction();

    // Create table schema with default values
    let mut fields = HashMap::new();
    fields.insert(
        "username".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: None, // username must be provided
        },
    );
    fields.insert(
        "active".to_string(),
        FieldConstraint {
            field_type: FieldType::Boolean,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: Some(ValueType::Bool(true)), // default to true
        },
    );
    fields.insert(
        "login_count".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: true,
            min: Some(0.0),
            max: None,
            pattern: None,
            unique: false,
            default: Some(ValueType::Int(0)), // default to 0
        },
    );

    let schema = TableSchema {
        name: "users".to_string(),
        fields,
    };

    // Create table with schema
    engine.create_table_with_schema(&mut tx, "users", schema);
    engine.commit(tx).unwrap();

    // Test inserting record with only required non-default field
    let mut tx = engine.begin_transaction();
    let mut record = Record::new(1);
    record.set_field("username", ValueType::Str("john_doe".to_string()));

    assert!(engine.insert_record(&mut tx, "users", record).is_ok());
    engine.commit(tx).unwrap();

    // Verify default values were set
    let mut tx = engine.begin_transaction();
    if let Some(record) = engine.get_record(&mut tx, "users", 1) {
        assert_eq!(record.get_field("active"), Some(&ValueType::Bool(true)));
        assert_eq!(record.get_field("login_count"), Some(&ValueType::Int(0)));
    } else {
        panic!("Record not found");
    }
}
