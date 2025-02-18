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
        version: 0,
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
        version: 0,
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
        version: 0,
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
        version: 0,
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
        version: 0,
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

#[test]
fn test_schema_updates() {
    let mut engine = setup_test_db("schema_updates", IsolationLevel::Serializable);

    // Create initial schema
    let mut fields = HashMap::new();
    fields.insert(
        "username".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: true,
            default: None,
        },
    );
    fields.insert(
        "age".to_string(),
        FieldConstraint {
            field_type: FieldType::Integer,
            required: false,
            min: Some(0.0),
            max: Some(150.0),
            pattern: None,
            unique: false,
            default: None,
        },
    );

    let initial_schema = TableSchema {
        name: "users".to_string(),
        fields,
        version: 1,
    };

    // Create table and insert initial records
    let mut tx = engine.begin_transaction();
    engine.create_table_with_schema(&mut tx, "users", initial_schema.clone());
    engine.commit(tx).unwrap();

    // Insert some test records
    let mut tx = engine.begin_transaction();
    let mut record1 = Record::new(1);
    record1.set_field("username", ValueType::Str("user1".to_string()));
    record1.set_field("age", ValueType::Int(25));
    engine.insert_record(&mut tx, "users", record1).unwrap();

    let mut record2 = Record::new(2);
    record2.set_field("username", ValueType::Str("user2".to_string()));
    // Note: age is optional, so we don't set it
    engine.insert_record(&mut tx, "users", record2).unwrap();
    engine.commit(tx).unwrap();

    // Test 1: Try to change field type (should fail)
    let mut tx = engine.begin_transaction();
    let mut bad_schema = initial_schema.clone();
    bad_schema.version = 2;
    if let Some(field) = bad_schema.fields.get_mut("age") {
        field.field_type = FieldType::String;
    }
    assert!(engine
        .update_table_schema(&mut tx, "users", bad_schema)
        .is_err());

    // Test 2: Try to make optional field required without default (should fail)
    let mut tx = engine.begin_transaction();
    let mut bad_schema = initial_schema.clone();
    bad_schema.version = 2;
    if let Some(field) = bad_schema.fields.get_mut("age") {
        field.required = true;
    }
    assert!(engine
        .update_table_schema(&mut tx, "users", bad_schema)
        .is_err());

    // Test 3: Add new optional field (should succeed)
    let mut tx = engine.begin_transaction();
    let mut new_schema = initial_schema.clone();
    new_schema.version = 2;
    new_schema.fields.insert(
        "email".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: false,
            min: None,
            max: None,
            pattern: Some(r"^[^@]+@[^@]+\.[^@]+$".to_string()),
            unique: true,
            default: None,
        },
    );
    assert!(engine
        .update_table_schema(&mut tx, "users", new_schema.clone())
        .is_ok());
    engine.commit(tx).unwrap();

    // Test 4: Add new required field with default (should succeed)
    let mut tx = engine.begin_transaction();
    let mut newer_schema = new_schema.clone();
    newer_schema.version = 3;
    newer_schema.fields.insert(
        "active".to_string(),
        FieldConstraint {
            field_type: FieldType::Boolean,
            required: true,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: Some(ValueType::Bool(true)),
        },
    );
    assert!(engine
        .update_table_schema(&mut tx, "users", newer_schema.clone())
        .is_ok());
    engine.commit(tx).unwrap();

    // Verify that existing records were properly migrated
    let mut tx = engine.begin_transaction();
    let record1 = engine.get_record(&mut tx, "users", 1).unwrap();
    let record2 = engine.get_record(&mut tx, "users", 2).unwrap();

    // Check that old fields are preserved
    assert_eq!(
        record1.get_field("username"),
        Some(&ValueType::Str("user1".to_string()))
    );
    assert_eq!(record1.get_field("age"), Some(&ValueType::Int(25)));
    assert_eq!(
        record2.get_field("username"),
        Some(&ValueType::Str("user2".to_string()))
    );
    assert_eq!(record2.get_field("age"), None);

    // Check that new default values were applied
    assert_eq!(record1.get_field("active"), Some(&ValueType::Bool(true)));
    assert_eq!(record2.get_field("active"), Some(&ValueType::Bool(true)));

    // Test 5: Try to update schema with lower version (should fail)
    let mut tx = engine.begin_transaction();
    let mut old_version_schema = newer_schema.clone();
    old_version_schema.version = 2;
    assert!(engine
        .update_table_schema(&mut tx, "users", old_version_schema)
        .is_err());

    // Test 6: Test unique constraint with default values
    let mut tx = engine.begin_transaction();
    let mut unique_schema = newer_schema;
    unique_schema.version = 4;
    if let Some(field) = unique_schema.fields.get_mut("active") {
        field.unique = true; // Try to make boolean field unique with same default (should fail)
    }
    assert!(engine
        .update_table_schema(&mut tx, "users", unique_schema)
        .is_err());
}
