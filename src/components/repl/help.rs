use std::collections::HashMap;

use lazy_static::lazy_static;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct CommandHelp {
    pub syntax: String,
    pub description: String,
    pub examples: Vec<String>,
    pub flags: Vec<(String, String)>,
}

lazy_static! {
    pub static ref COMMAND_HELP: HashMap<&'static str, CommandHelp> = {
        let mut m = HashMap::new();

        m.insert(
            "CREATE_TABLE",
            CommandHelp {
                syntax: "CREATE_TABLE <name> [field type [--flags ...]] [...]".to_string(),
                description: "Creates a new table with optional schema definition".to_string(),
                examples: vec![
                    "CREATE_TABLE users".to_string(),
                    "CREATE_TABLE users name STRING --required --unique age INTEGER --min=0"
                        .to_string(),
                ],
                flags: vec![
                    (
                        "--required".to_string(),
                        "Makes the field mandatory".to_string(),
                    ),
                    (
                        "--unique".to_string(),
                        "Ensures field values are unique".to_string(),
                    ),
                    (
                        "--min=<value>".to_string(),
                        "Sets minimum value/length".to_string(),
                    ),
                    (
                        "--max=<value>".to_string(),
                        "Sets maximum value/length".to_string(),
                    ),
                    (
                        "--pattern=<regex>".to_string(),
                        "Sets regex pattern for strings".to_string(),
                    ),
                    (
                        "--default=<value>".to_string(),
                        "Sets default value".to_string(),
                    ),
                ],
            },
        );

        m.insert(
            "DROP_TABLE",
            CommandHelp {
                syntax: "DROP_TABLE <table>".to_string(),
                description: "Deletes an existing table and all its records".to_string(),
                examples: vec!["DROP_TABLE users".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "INSERT",
            CommandHelp {
                syntax: "INSERT INTO <table> ID <id> SET field1 = value1 [field2 = value2 ...]"
                    .to_string(),
                description: "Inserts a new record into a table".to_string(),
                examples: vec![
                    r#"INSERT INTO users ID 1 SET name = "John" age = 30"#.to_string(),
                    r#"INSERT INTO products ID 1 SET name = "Widget" price = 19.99 active = true"#
                        .to_string(),
                ],
                flags: vec![],
            },
        );

        m.insert(
            "UPDATE",
            CommandHelp {
                syntax: "UPDATE <table> ID <id> SET field1 = value1 [field2 = value2 ...]"
                    .to_string(),
                description: "Updates values in an existing record".to_string(),
                examples: vec![
                    r#"UPDATE users ID 1 SET age = 31"#.to_string(),
                    r#"UPDATE products ID 1 SET price = 24.99 active = false"#.to_string(),
                ],
                flags: vec![],
            },
        );

        m.insert(
            "DELETE",
            CommandHelp {
                syntax: "DELETE FROM <table> <id>".to_string(),
                description: "Deletes a record from a table".to_string(),
                examples: vec!["DELETE FROM users 1".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "GET_RECORD",
            CommandHelp {
                syntax: "GET_RECORD FROM <table> <id>".to_string(),
                description: "Retrieves a single record from a table".to_string(),
                examples: vec!["GET_RECORD FROM users 1".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "GET_RECORDS",
            CommandHelp {
                syntax: "GET_RECORDS FROM <table>".to_string(),
                description: "Retrieves all records from a table".to_string(),
                examples: vec!["GET_RECORDS FROM users".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "GET_TABLE",
            CommandHelp {
                syntax: "GET_TABLE <table>".to_string(),
                description: "Shows the schema information for a table".to_string(),
                examples: vec!["GET_TABLE users".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "GET_TABLES",
            CommandHelp {
                syntax: "GET_TABLES".to_string(),
                description: "Lists all tables in the database".to_string(),
                examples: vec!["GET_TABLES".to_string()],
                flags: vec![],
            },
        );

        m.insert(
            "SEARCH_RECORDS",
            CommandHelp {
                syntax: "SEARCH_RECORDS FROM <table> MATCH <query>".to_string(),
                description:
                    "Searches for records containing the specified text in any string field"
                        .to_string(),
                examples: vec![
                    r#"SEARCH_RECORDS FROM users MATCH "John""#.to_string(),
                    r#"SEARCH_RECORDS FROM products MATCH "active""#.to_string(),
                ],
                flags: vec![],
            },
        );

        m.insert(
            "UPDATE_SCHEMA",
            CommandHelp {
                syntax: "UPDATE_SCHEMA <table> [--version=<n>] field type [--flags ...] [...]"
                    .to_string(),
                description: "Updates an existing table's schema with new or modified fields"
                    .to_string(),
                examples: vec![
                    "UPDATE_SCHEMA users --version=2 active BOOLEAN --default=true".to_string(),
                    "UPDATE_SCHEMA products --version=3 category STRING --required".to_string(),
                ],
                flags: vec![
                    (
                        "--version=<n>".to_string(),
                        "Specifies the new schema version".to_string(),
                    ),
                    (
                        "--required".to_string(),
                        "Makes the field mandatory".to_string(),
                    ),
                    (
                        "--unique".to_string(),
                        "Ensures field values are unique".to_string(),
                    ),
                    (
                        "--min=<value>".to_string(),
                        "Sets minimum value/length".to_string(),
                    ),
                    (
                        "--max=<value>".to_string(),
                        "Sets maximum value/length".to_string(),
                    ),
                    (
                        "--pattern=<regex>".to_string(),
                        "Sets regex pattern for strings".to_string(),
                    ),
                    (
                        "--default=<value>".to_string(),
                        "Sets default value".to_string(),
                    ),
                ],
            },
        );

        m.insert(
            "HELP",
            CommandHelp {
                syntax: "HELP [command]".to_string(),
                description: "Shows help information for all commands or a specific command"
                    .to_string(),
                examples: vec!["HELP".to_string(), "HELP CREATE_TABLE".to_string()],
                flags: vec![],
            },
        );

        m
    };
}

pub fn get_general_help() -> String {
    "Available commands:\n\
     CREATE_TABLE   - Create a new table\n\
     DROP_TABLE     - Delete a table\n\
     INSERT         - Insert a new record\n\
     UPDATE         - Update an existing record\n\
     DELETE         - Delete a record\n\
     GET_RECORD     - Retrieve a single record\n\
     GET_RECORDS    - Retrieve all records from a table\n\
     GET_TABLE      - Show table schema\n\
     GET_TABLES     - List all tables\n\
     SEARCH_RECORDS - Search for records\n\
     UPDATE_SCHEMA  - Update table schema\n\
     \n\
     Use 'HELP <command>' for detailed information about a specific command."
        .to_string()
}
