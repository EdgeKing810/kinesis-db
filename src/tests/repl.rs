#![allow(unused_imports)]
use crate::{
    components::{
        repl::{OutputFormat, REPL},
        transaction::isolation_level::IsolationLevel,
    },
    tests::setup_test_db,
};

#[test]
fn test_repl_comprehensive() {
    let mut engine = setup_test_db("repl_comprehensive", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Test 1: Create table with schema
    let result = repl.execute(
        "CREATE_TABLE users name STRING --required --unique age INTEGER --required --min=0 --max=150 email STRING --pattern=^[^@]+@[^@]+\\.[^@]+$",
        None
    ).unwrap();
    assert!(result.contains("Created table 'users'"));

    // Test 2: Get table schema
    let result = repl.execute("GET_TABLE users", None).unwrap();
    assert!(result.contains("name"));
    assert!(result.contains("age"));
    assert!(result.contains("email"));

    // Test 3: Insert multiple records
    let result = repl
        .execute(
            r#"INSERT INTO users ID 1 SET name = "John Doe" age = 30 email = "john@example.com" AND
           INSERT INTO users ID 2 SET name = "Jane Doe" age = 25 email = "jane@example.com""#,
            None,
        )
        .unwrap();
    assert!(result.contains("Inserted record 1"));
    assert!(result.contains("Inserted record 2"));

    // Test 4: Get record
    let result = repl.execute("GET_RECORD FROM users 1", None).unwrap();
    assert!(result.contains("John Doe"));
    assert!(result.contains("30"));

    // Test 5: Update record
    let result = repl
        .execute(
            r#"UPDATE users ID 1 SET age = 31 email = "john.doe@example.com""#,
            None,
        )
        .unwrap();
    assert!(result.contains("Updated record 1"));

    // Test 6: Verify update
    let result = repl.execute("GET_RECORD FROM users 1", None).unwrap();
    assert!(result.contains("31"));
    assert!(result.contains("john.doe@example.com"));

    // Test 7: Test unique constraint violation
    let result = repl.execute(
        r#"INSERT INTO users ID 3 SET name = "Jane Doe" age = 28 email = "different@example.com""#,
        None,
    );
    assert!(result.is_err());

    // Test 8: Update schema
    let result = repl
        .execute(
            "UPDATE_SCHEMA users --version=2 active BOOLEAN --default=true",
            None,
        )
        .unwrap();
    assert!(result.contains("Updated schema"));

    // Test 9: Verify schema update applied defaults
    let result = repl.execute("GET_RECORD FROM users 1", None).unwrap();
    assert!(result.contains("true")); // Default value should be present

    // Test 10: Delete record
    let result = repl.execute("DELETE FROM users 2", None).unwrap();
    assert!(result.contains("Deleted record 2"));

    // Test 11: Verify deletion
    let result = repl.execute("GET_RECORD FROM users 2", None).unwrap();
    assert!(result.contains("not found"));

    // Test 12: Test invalid commands
    let result = repl.execute("INVALID_COMMAND", None);
    assert!(result.is_err());

    // Test 13: Test command chaining with mixed operations
    let result = repl
        .execute(
            r#"INSERT INTO users ID 4 SET name = "Alice" age = 28 email = "alice@example.com" AND
           UPDATE users ID 1 SET age = 29 AND
           GET_RECORD FROM users 1"#,
            None,
        )
        .unwrap();
    assert!(result.contains("Inserted record 4"));
    assert!(result.contains("Updated record 1"));
    assert!(result.contains("29")); // Verify the update was applied

    // Add tests for GET_RECORDS
    let records_result = repl
        .execute("GET_RECORDS FROM users", Some(OutputFormat::Table))
        .unwrap();
    assert!(records_result.contains("John Doe"));
    assert!(records_result.contains("29")); // Updated age value
    assert!(records_result.contains("alice@example.com"));

    // Test OutputFormat::JSON for GET_RECORDS
    let json_records = repl
        .execute("GET_RECORDS FROM users", Some(OutputFormat::JSON))
        .unwrap();
    assert!(json_records.contains("\"data\""));
    assert!(!json_records.contains("\"schema\"")); // JSON output shouldn't include schema

    // Test 14: Drop table
    let result = repl.execute("DROP_TABLE users", None).unwrap();
    assert!(result.contains("Dropped table 'users'"));

    // Test 15: Verify table is gone
    let result = repl.execute("GET_TABLE users", None).unwrap();
    assert!(result.contains("not found"));
}

#[test]
fn test_repl_output_formats() {
    let mut engine = setup_test_db("repl_output_formats", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Create test table and insert data
    let setup = r#"CREATE_TABLE users name STRING --required --unique age INTEGER --required --min=0 --max=150"#;
    repl.execute(setup, Some(OutputFormat::Standard)).unwrap();

    // Test standard output
    let result = repl
        .execute(
            r#"INSERT INTO users ID 1 SET name = "John Doe" age = 30"#,
            Some(OutputFormat::Standard),
        )
        .unwrap();
    assert!(result.contains("Inserted record 1"));

    // Test JSON output
    let result = repl
        .execute("GET_RECORD FROM users 1", Some(OutputFormat::JSON))
        .unwrap();
    assert!(result.contains("\"success\": true"));
    assert!(result.contains("John Doe"));

    // Test table output
    let result = repl
        .execute("GET_TABLE users", Some(OutputFormat::Table))
        .unwrap();
    assert!(result.contains("+-")); // Table border
    assert!(result.contains("name")); // Column header
}

#[test]
fn test_repl_table_output() {
    let mut engine = setup_test_db("repl_table_output", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Create test table with comprehensive schema
    let create_table = r#"
        CREATE_TABLE users 
            name STRING --required --unique --pattern=^[a-zA-Z]+$ 
            age INTEGER --required --min=0 --max=150 
            email STRING --pattern=^[^@]+@[^@]+\.[^@]+$ 
            active BOOLEAN --default=true
    "#;
    repl.execute(create_table, Some(OutputFormat::Standard))
        .unwrap();

    // Insert test data
    let insert_data = r#"
        INSERT INTO users ID 1 SET name = "John" age = 30 email = "john@example.com" AND
        INSERT INTO users ID 2 SET name = "Jane" age = 25 email = "jane@example.com"
    "#;
    repl.execute(insert_data, Some(OutputFormat::Standard))
        .unwrap();

    // Test table output format
    let table_result = repl
        .execute("GET_TABLE users", Some(OutputFormat::Table))
        .unwrap();
    assert!(table_result.contains("field"));
    assert!(table_result.contains("data_type"));
    assert!(table_result.contains("required"));
    assert!(table_result.contains("pattern"));

    // Test table data
    let records_result = repl
        .execute("GET_RECORDS FROM users", Some(OutputFormat::Table))
        .unwrap();
    assert!(records_result.contains("John"));
    assert!(records_result.contains("jane@example.com"));

    // Test JSON output format
    let json_result = repl
        .execute("GET_TABLE users", Some(OutputFormat::JSON))
        .unwrap();
    assert!(json_result.contains("\"fields\""));
    assert!(json_result.contains("\"data\""));

    // Test GET_TABLES command
    repl.execute(
        "CREATE_TABLE posts title STRING --required",
        Some(OutputFormat::Standard),
    )
    .unwrap();

    let tables_result = repl
        .execute("GET_TABLES", Some(OutputFormat::Table))
        .unwrap();
    assert!(tables_result.contains("users"));
    assert!(tables_result.contains("posts"));

    let json_tables = repl
        .execute("GET_TABLES", Some(OutputFormat::JSON))
        .unwrap();
    assert!(json_tables.contains("users"));
    assert!(json_tables.contains("posts"));
}

#[test]
fn test_repl_format_specific_behavior() {
    let mut engine = setup_test_db(
        "repl_format_specific_behavior",
        IsolationLevel::Serializable,
    );
    let mut repl = REPL::new(&mut engine);

    // Setup test data
    repl.execute(
        "CREATE_TABLE test name STRING --required age INTEGER active BOOLEAN",
        Some(OutputFormat::Standard),
    )
    .unwrap();

    repl.execute(
        r#"INSERT INTO test ID 1 SET name = "Test" age = 25 active = true"#,
        Some(OutputFormat::Standard),
    )
    .unwrap();

    // Test Table Format
    let table_output = repl
        .execute("GET_RECORDS FROM test", Some(OutputFormat::Table))
        .unwrap();
    assert!(table_output.contains("+-"));
    assert!(table_output.contains("| ID | active | age | name |"));
    assert!(table_output.contains("| 1  | true   | 25  | Test |"));

    // Test JSON Format
    let json_output = repl
        .execute("GET_RECORDS FROM test", Some(OutputFormat::JSON))
        .unwrap();
    assert!(json_output.contains("\"data\""));
    assert!(!json_output.contains("\"schema\"")); // Verify schema is not included
    assert!(json_output.contains("\"Test\""));
    assert!(json_output.contains("25"));
    assert!(json_output.contains("true"));

    // Test Standard Format
    let std_output = repl.execute("GET_RECORDS FROM test", None).unwrap();
    assert!(std_output.contains("Record 1:"));
    assert!(std_output.contains("name: Test"));
    assert!(std_output.contains("age: 25"));
}

#[test]
fn test_repl_error_handling() {
    let mut engine = setup_test_db("repl_error_handling", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Test schema validation errors
    repl.execute("CREATE_TABLE test age INTEGER --min=0", None)
        .unwrap();
    let result = repl.execute("INSERT INTO test ID 1 SET age = -1", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("below minimum"));

    // Test syntax errors
    let result = repl.execute("GET_RECORDS test", None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Invalid GET_RECORDS syntax"));

    // Test table existence errors
    let result = repl.execute("GET_RECORDS FROM nonexistent", None);
    assert!(result.unwrap().contains("not found"));
}

#[test]
fn test_repl_search_records() {
    let mut engine = setup_test_db("repl_search", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Create test table and insert data
    repl.execute(
        r#"
        CREATE_TABLE users 
            name STRING --required 
            role STRING --required
    "#,
        None,
    )
    .unwrap();

    // Insert test records
    repl.execute(
        r#"
        INSERT INTO users ID 1 SET name = "John Developer" role = "Software Engineer" AND
        INSERT INTO users ID 2 SET name = "Jane Manager" role = "Project Manager" AND
        INSERT INTO users ID 3 SET name = "Bob Developer" role = "Software Engineer"
    "#,
        None,
    )
    .unwrap();

    // Test search with standard output
    let result = repl
        .execute(
            r#"SEARCH_RECORDS FROM users MATCH Developer"#,
            Some(OutputFormat::Standard),
        )
        .unwrap();
    assert!(result.contains("John Developer"));
    assert!(result.contains("Bob Developer"));
    assert!(!result.contains("Jane Manager"));

    // Test search with JSON output
    let result = repl
        .execute(
            r#"SEARCH_RECORDS FROM users MATCH Manager"#,
            Some(OutputFormat::JSON),
        )
        .unwrap();
    assert!(result.contains("Jane Manager"));
    assert!(!result.contains("Developer"));
    assert!(result.contains("\"data\""));
    assert!(!result.contains("\"schema\""));

    // Test search with Table output
    let result = repl
        .execute(
            r#"SEARCH_RECORDS FROM users MATCH Engineer"#,
            Some(OutputFormat::Table),
        )
        .unwrap();
    println!("{}", result);
    assert!(result.contains("John Developer"));
    assert!(result.contains("Bob Developer"));
    assert!(result.contains("Software Engineer"));
    assert!(!result.contains("Manager"));

    // Test search with no matches
    let result = repl
        .execute(
            r#"SEARCH_RECORDS FROM users MATCH Designer"#,
            Some(OutputFormat::Standard),
        )
        .unwrap();
    assert!(!result.contains("Developer"));
    assert!(!result.contains("Manager"));
}

#[test]
fn test_repl_help() {
    let mut engine = setup_test_db("repl_help", IsolationLevel::Serializable);
    let mut repl = REPL::new(&mut engine);

    // Test general help
    let result = repl.execute("HELP", None).unwrap();
    assert!(result.contains("Available commands"));
    assert!(result.contains("CREATE_TABLE"));

    // Test specific command help
    let result = repl.execute("HELP CREATE_TABLE", None).unwrap();
    assert!(result.contains("Syntax:"));
    assert!(result.contains("Examples:"));
    assert!(result.contains("--required"));

    // Test JSON format
    let result = repl
        .execute("HELP CREATE_TABLE", Some(OutputFormat::JSON))
        .unwrap();
    assert!(result.contains("syntax"));
    assert!(result.contains("examples"));

    // Test table format
    let result = repl
        .execute("HELP CREATE_TABLE", Some(OutputFormat::Table))
        .unwrap();
    assert!(result.contains("Syntax"));
    assert!(result.contains("Description"));
    assert!(result.contains("Flags"));
    assert!(result.contains("Examples"));

    // Test invalid command
    let result = repl.execute("HELP INVALID_COMMAND", None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .contains("Use HELP to see available commands"));
}
