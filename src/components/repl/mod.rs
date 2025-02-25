use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use tabled::{builder::Builder, settings::Style, Table, Tabled};

use crate::{
    components::database::{
        record::Record,
        schema::{FieldConstraint, FieldType, TableSchema},
        value_type::ValueType,
    },
    DBEngine,
};

mod help;
use help::{get_general_help, CommandHelp, COMMAND_HELP};

/// Represents different output format options for command results.
/// The format affects how command output is presented to the user.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Standard, // Plain text format with human-readable output
    JSON,     // Structured JSON format for programmatic consumption
    Table,    // ASCII table format for aligned columnar display
}

/// Represents all possible REPL commands with their parameters.
/// Each variant contains the necessary data for executing the command.
#[derive(Debug, Clone)]
pub enum Command {
    // Table Management Commands
    CreateTable {
        name: String,
    },
    CreateTableWithSchema {
        name: String,
        schema: TableSchema,
    },
    DropTable {
        name: String,
    },
    UpdateSchema {
        table: String,
        schema: TableSchema,
    },

    // Record Management Commands
    InsertRecord {
        table: String,
        id: u64,
        values: HashMap<String, ValueType>,
    },
    UpdateRecord {
        table: String,
        id: u64,
        values: HashMap<String, ValueType>,
    },
    DeleteRecord {
        table: String,
        id: u64,
    },

    // Query Commands
    GetRecord {
        table: String,
        id: u64,
    },
    GetRecords {
        table: String,
    },
    GetTable {
        name: String,
    },
    GetTables,
    SearchRecords {
        table: String,
        query: String,
    },

    // Help Command
    Help {
        command: Option<String>,
    },
}

/// Represents basic command types for output formatting decisions.
/// This enum is used internally to determine how to format command results.
#[derive(Serialize, Debug, PartialEq)]
enum CommandBasic {
    CreateTable,
    CreateTableWithSchema,
    DropTable,
    InsertRecord,
    UpdateRecord,
    DeleteRecord,
    UpdateSchema,
    GetRecord,
    GetTable,
    GetTables,
    GetRecords,
    SearchRecords,
    Help,
}

/// REPL (Read-Eval-Print Loop) implementation for database interaction.
/// Provides a command-line interface for executing database operations.
#[derive(Debug)]
pub struct REPL<'a> {
    engine: &'a mut DBEngine, // Reference to the underlying database engine
}

/// Represents the result of a command execution including metadata.
#[derive(Serialize, Debug)]
struct CommandResult {
    executed_command: CommandBasic,  // Type of command that was executed
    success: bool,                   // Whether the command succeeded
    message: String,                 // Human-readable result message
    data: Option<serde_json::Value>, // Additional data for formatting
    timestamp: String,               // ISO 8601 timestamp of execution
}

// Table structs for formatting different types of output
/// Used for displaying command execution status in table format
#[derive(Tabled)]
struct StatusTable {
    status: String,    // SUCCESS or ERROR
    message: String,   // Result or error message
    timestamp: String, // Execution timestamp
}

/// Used for displaying table names in table format
#[derive(Tabled)]
struct TableNameRecord {
    name: String, // Name of the table
}

/// Used for displaying table schema information in table format
#[derive(Tabled)]
struct TableSchemaRecord {
    field: String,     // Field name
    data_type: String, // Field type (STRING, INTEGER, etc.)
    required: String,  // Whether field is required
    unique: String,    // Whether field must be unique
    min: String,       // Minimum value/length if applicable
    max: String,       // Maximum value/length if applicable
    pattern: String,   // Regex pattern if applicable
    default: String,   // Default value if specified
}

/// Represents different types of field constraints for schema parsing
#[derive(Debug)]
enum ConstraintFlag {
    Required,           // Field must have a value
    Unique,             // Field value must be unique in table
    Min(f64),           // Minimum value or length
    Max(f64),           // Maximum value or length
    Pattern(String),    // Regex pattern for validation
    Default(ValueType), // Default value if none provided
}

impl REPL<'_> {
    /// Creates a new REPL instance with the given database engine
    pub fn new(engine: &mut DBEngine) -> REPL {
        REPL { engine }
    }

    /// Executes a command string and returns formatted output
    pub fn execute(&mut self, input: &str, format: Option<OutputFormat>) -> Result<String, String> {
        let format = format.unwrap_or(OutputFormat::Standard);

        let result = (|| {
            let commands = self.parse_commands(input)?;
            let mut tx = self.engine.begin_transaction();
            let mut results = Vec::new();

            for cmd in commands {
                let result = match cmd {
                    Command::CreateTable { name } => {
                        self.engine.create_table(&mut tx, &name);
                        CommandResult {
                            executed_command: CommandBasic::CreateTable,
                            success: true,
                            message: format!("Created table '{}'", name),
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::CreateTableWithSchema { name, schema } => {
                        self.engine.create_table_with_schema(&mut tx, &name, schema);
                        CommandResult {
                            executed_command: CommandBasic::CreateTableWithSchema,
                            success: true,
                            message: format!("Created table '{}' with schema", name),
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::DropTable { name } => {
                        if self.engine.drop_table(&mut tx, &name) {
                            CommandResult {
                                executed_command: CommandBasic::DropTable,
                                success: true,
                                message: format!("Dropped table '{}'", name),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::DropTable,
                                success: false,
                                message: format!("Table '{}' not found", name),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::InsertRecord { table, id, values } => {
                        let mut record = Record::new(id);
                        for (k, v) in values {
                            record.set_field(&k, v);
                        }
                        self.engine.insert_record(&mut tx, &table, record)?;
                        CommandResult {
                            executed_command: CommandBasic::InsertRecord,
                            success: true,
                            message: format!("Inserted record {} into table '{}'", id, table),
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::UpdateRecord { table, id, values } => {
                        self.engine.update_record(&mut tx, &table, id, values)?;
                        CommandResult {
                            executed_command: CommandBasic::UpdateRecord,
                            success: true,
                            message: format!("Updated record {} in table '{}'", id, table),
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::DeleteRecord { table, id } => {
                        if self.engine.delete_record(&mut tx, &table, id) {
                            CommandResult {
                                executed_command: CommandBasic::DeleteRecord,
                                success: true,
                                message: format!("Deleted record {} from table '{}'", id, table),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::DeleteRecord,
                                success: false,
                                message: format!("Record {} not found in table '{}'", id, table),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::UpdateSchema { table, schema } => {
                        self.engine.update_table_schema(&mut tx, &table, schema)?;
                        CommandResult {
                            executed_command: CommandBasic::UpdateSchema,
                            success: true,
                            message: format!("Updated schema for table '{}'", table),
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::GetRecord { table, id } => {
                        if let Some(record) = self.engine.get_record(&mut tx, &table, id) {
                            let formatted_record = match format {
                                OutputFormat::Standard => {
                                    let values = record
                                        .values
                                        .iter()
                                        .map(|(k, v)| format!("{}: {}", k, v))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    format!(
                                        "Record {} in table '{}' (version: {}, timestamp: {}): {{{}}}", 
                                        id, table, record.version, record.timestamp, values
                                    )
                                }
                                OutputFormat::Table => self.format_record_output(&record, id)?,
                                _ => format!("Record {} in table '{}'", id, table),
                            };

                            CommandResult {
                                executed_command: CommandBasic::GetRecord,
                                success: true,
                                message: formatted_record,
                                data: Some(json!(record)),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::GetRecord,
                                success: false,
                                message: format!("Record {} not found in table '{}'", id, table),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::GetTable { name } => {
                        if let Some(table) = self.engine.get_table(&name) {
                            let formatted_table = match format {
                                OutputFormat::Standard => {
                                    let schema_info = table.schema.fields.iter()
                                        .map(|(name, constraint)| {
                                            format!("{}: {:?} (required: {}, unique: {}, min: {}, max: {}, pattern: {}, default: {})", 
                                                name,
                                                constraint.field_type,
                                                constraint.required,
                                                constraint.unique,
                                                match constraint.min {
                                                    Some(min) => min.to_string(),
                                                    None => "None".to_string()
                                                },
                                                match constraint.max {
                                                    Some(max) => max.to_string(),
                                                    None => "None".to_string()
                                                },
                                                constraint.pattern.clone().unwrap_or_else(|| String::from("None")),
                                                match constraint.default.clone() {
                                                    Some(default) => default.to_string(),
                                                    None => "None".to_string()
                                                }
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n");

                                    format!("Table '{}'\nSchema:\n{}", name, schema_info)
                                }
                                _ => format!("Retrieved table '{}'", name),
                            };

                            CommandResult {
                                executed_command: CommandBasic::GetTable,
                                success: true,
                                message: formatted_table,
                                data: Some(json!({ "schema": table.schema })),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::GetTable,
                                success: false,
                                message: format!("Table '{}' not found", name),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::GetRecords { table } => {
                        if let Some(actual_table) = self.engine.get_table(&table) {
                            // Create a serializable data structure from the locked records
                            let mut records_data = HashMap::new();
                            for (id, record_lock) in &actual_table.data {
                                if let Ok(record) = record_lock.read() {
                                    records_data.insert(id.to_string(), record.clone());
                                }
                            }

                            let formatted_records = match format {
                                OutputFormat::Standard => records_data
                                    .iter()
                                    .map(|(id, record)| {
                                        let values = record
                                            .values
                                            .iter()
                                            .map(|(k, v)| format!("{}: {}", k, v))
                                            .collect::<Vec<_>>()
                                            .join(", ");
                                        format!("Record {}: {{{}}}", id, values)
                                    })
                                    .collect::<Vec<_>>()
                                    .join("\n"),
                                _ => format!("Retrieved records from table '{}'", table),
                            };

                            let data = match format {
                                OutputFormat::Table => json!({
                                    "data": records_data,
                                    "schema": actual_table.schema  // Include schema for table formatting
                                }),
                                _ => json!({ "data": records_data }), // JSON format only needs data
                            };

                            CommandResult {
                                executed_command: CommandBasic::GetRecords,
                                success: true,
                                message: formatted_records,
                                data: Some(data),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::GetRecords,
                                success: false,
                                message: format!("Table '{}' not found", table),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::GetTables => {
                        let tables = self.engine.get_tables();
                        let message = match format {
                            OutputFormat::Standard => format!(
                                "Found {} tables: {}",
                                tables.len(),
                                tables
                                    .iter()
                                    .map(|t| t.clone())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ),
                            OutputFormat::JSON => format!("Found {} tables", tables.len()),
                            _ => "".to_string(),
                        };

                        CommandResult {
                            executed_command: CommandBasic::GetTables,
                            success: true,
                            message,
                            data: Some(json!(tables)),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                    Command::SearchRecords { table, query } => {
                        if let Some(actual_table) = self.engine.get_table(&table) {
                            let search_results =
                                self.engine.search_records(&mut tx, &table, &query);

                            let mut records_data = HashMap::new();
                            for record in search_results {
                                records_data.insert(record.id.to_string(), record.clone());
                            }

                            let formatted_records = match format {
                                OutputFormat::Standard => records_data
                                    .iter()
                                    .map(|(id, record)| {
                                        let values = record
                                            .values
                                            .iter()
                                            .map(|(k, v)| format!("{}: {}", k, v))
                                            .collect::<Vec<_>>()
                                            .join(", ");
                                        format!("Record {}: {{{}}}", id, values)
                                    })
                                    .collect::<Vec<_>>()
                                    .join("\n"),
                                _ => format!(
                                    "Found {} matching records in table '{}'",
                                    records_data.len(),
                                    table
                                ),
                            };

                            let data = match format {
                                OutputFormat::Table => json!({
                                    "data": records_data,
                                    "schema": actual_table.schema  // Include schema for table formatting
                                }),
                                _ => json!({ "data": records_data }), // JSON format only needs data
                            };

                            CommandResult {
                                executed_command: CommandBasic::SearchRecords,
                                success: true,
                                message: formatted_records,
                                data: Some(data),
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        } else {
                            CommandResult {
                                executed_command: CommandBasic::SearchRecords,
                                success: false,
                                message: format!("Table '{}' not found", table),
                                data: None,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                            }
                        }
                    }
                    Command::Help { command } => {
                        let help_text = match command {
                            Some(cmd) => COMMAND_HELP
                                .get(cmd.as_str())
                                .map(|help| self.format_command_help(help, format))
                                .unwrap_or_else(|| {
                                    format!(
                                        "Unknown command: {}. Use HELP to see available commands.",
                                        cmd
                                    )
                                }),
                            None => self.format_general_help(format),
                        };

                        CommandResult {
                            executed_command: CommandBasic::Help,
                            success: true,
                            message: help_text,
                            data: None,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                        }
                    }
                };
                results.push(result);
            }

            self.engine.commit(tx)?;
            self.format_results(&results, format)
        })();

        match result {
            Ok(output) => Ok(output),
            Err(e) => match format {
                OutputFormat::Standard => Err(e),
                OutputFormat::JSON => Ok(json!({
                    "success": false,
                    "error": e,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                })
                .to_string()),
                OutputFormat::Table => {
                    let results = vec![StatusTable {
                        status: String::from("ERROR"),
                        message: e,
                        timestamp: chrono::Utc::now().to_rfc3339(),
                    }];

                    Ok(Table::new(results).with(Style::ascii()).to_string())
                }
            },
        }
    }

    /// Formats multiple command results according to the specified output format
    fn format_results(
        &self,
        results: &[CommandResult],
        format: OutputFormat,
    ) -> Result<String, String> {
        match format {
            OutputFormat::Standard => Ok(results
                .iter()
                .map(|r| r.message.clone())
                .collect::<Vec<_>>()
                .join("\n")),
            OutputFormat::JSON => Ok(serde_json::to_string_pretty(&json!({
                "results": results,
                "count": results.len(),
                "success": results.iter().all(|r| r.success),
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }))
            .unwrap()),
            OutputFormat::Table => match results.first() {
                Some(first) => {
                    if first.executed_command == CommandBasic::Help {
                        Ok(first.message.clone())
                    } else if let Some(table_data) = &first.data {
                        match first.executed_command {
                            CommandBasic::GetRecords | CommandBasic::SearchRecords if table_data.get("data").is_some() => self
                                .format_table_records(
                                    table_data.get("data").unwrap(),
                                    table_data.get("schema").unwrap_or(&json!({})),
                                ),
                            CommandBasic::GetTable if table_data.get("schema").is_some() => self
                                .format_table_schema(
                                    table_data.get("schema").unwrap_or(&json!({})),
                                ),
                            CommandBasic::GetRecord if table_data.get("values").is_some() => {
                                Ok(first.message.clone())
                            }
                            CommandBasic::GetTables if table_data.is_array() => {
                                let mut results = vec![];
                                for table_name in table_data.as_array().unwrap() {
                                    results.push(TableNameRecord {
                                        name: table_name.as_str().unwrap().to_string(),
                                    });
                                }
                                Ok(Table::new(results).with(Style::ascii()).to_string())
                            }
                            _ => Ok(self.format_status_table(results)),
                        }
                    } else {
                        Ok(self.format_status_table(results))
                    }
                }
                None => Ok("No results".to_string()),
            },
        }
    }

    /// Formats results as a status table showing success/failure of commands
    fn format_status_table(&self, results: &[CommandResult]) -> String {
        let mut status_results = vec![];

        for result in results {
            status_results.push(StatusTable {
                status: if result.success { "SUCCESS" } else { "ERROR" }.to_string(),
                message: result.message.clone(),
                timestamp: result.timestamp.clone(),
            });
        }

        Table::new(status_results).with(Style::ascii()).to_string()
    }

    /// Formats table schema information as a table
    fn format_table_schema(&self, schema: &serde_json::Value) -> Result<String, String> {
        let mut schema_results = vec![];

        if let Some(fields) = schema.get("fields").and_then(|f| f.as_object()) {
            for (field_name, constraints) in fields {
                let min_val = constraints
                    .get("min")
                    .and_then(|m| m.as_f64())
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "None".to_string());

                let max_val = constraints
                    .get("max")
                    .and_then(|m| m.as_f64())
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| "None".to_string());

                let default_val = constraints
                    .get("default")
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "None".to_string());

                schema_results.push(TableSchemaRecord {
                    field: field_name.to_string(),
                    data_type: constraints
                        .get("field_type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    required: constraints
                        .get("required")
                        .and_then(|r| r.as_bool())
                        .map_or("No", |r| if r { "Yes" } else { "No" })
                        .to_string(),
                    unique: constraints
                        .get("unique")
                        .and_then(|u| u.as_bool())
                        .map_or("No", |u| if u { "Yes" } else { "No" })
                        .to_string(),
                    min: min_val,
                    max: max_val,
                    pattern: constraints
                        .get("pattern")
                        .and_then(|p| p.as_str())
                        .unwrap_or("None")
                        .to_string(),
                    default: default_val,
                });
            }
        }

        let mut table = Table::new(schema_results);
        table.with(Style::ascii());
        Ok(table.to_string())
    }

    /// Formats record data as a table
    fn format_table_records(
        &self,
        data: &serde_json::Value,
        schema: &serde_json::Value,
    ) -> Result<String, String> {
        let mut builder = Builder::new();

        let field_names = self.extract_field_names(schema)?;
        builder.push_record(field_names.clone());

        if let Some(records) = data.as_object() {
            for (id, record) in records {
                let row = self.build_record_row(id, record, &field_names[1..])?;
                builder.push_record(row);
            }
        }

        let mut table = builder.build();
        table.with(Style::ascii());
        Ok(table.to_string())
    }

    /// Extracts field names from schema for table headers
    fn extract_field_names(&self, schema: &serde_json::Value) -> Result<Vec<String>, String> {
        let mut names = vec!["ID".to_string()];
        if let Some(fields) = schema.get("fields").and_then(|f| f.as_object()) {
            names.extend(fields.keys().cloned());
        }
        Ok(names)
    }

    /// Builds a row of record data for table output
    fn build_record_row(
        &self,
        id: &str,
        record: &serde_json::Value,
        field_names: &[String],
    ) -> Result<Vec<String>, String> {
        let mut row = vec![id.to_string()];

        if let Some(values) = record.get("values").and_then(|v| v.as_object()) {
            for field_name in field_names {
                let value = self.extract_value_from_record(values, field_name)?;
                row.push(value);
            }
        }

        Ok(row)
    }

    /// Extracts a value from a record's field
    fn extract_value_from_record(
        &self,
        values: &serde_json::Map<String, serde_json::Value>,
        field_name: &str,
    ) -> Result<String, String> {
        Ok(values
            .get(field_name)
            .and_then(|v| {
                if let Some(obj) = v.as_object() {
                    obj.values()
                        .next()
                        .map(|val| val.to_string().trim_matches('"').to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "NULL".to_string()))
    }

    /// Formats a single record as a table
    fn format_record_output(&self, record: &Record, id: u64) -> Result<String, String> {
        let mut builder = Builder::new();

        // Headers
        let mut headers = vec![
            "ID".to_string(),
            "Version".to_string(),
            "Timestamp".to_string(),
        ];
        headers.extend(record.values.keys().cloned());
        builder.push_record(headers);

        // Values
        let mut values = vec![
            id.to_string(),
            record.version.to_string(),
            record.timestamp.to_string(),
        ];
        values.extend(record.values.values().map(|v| v.to_string()));
        builder.push_record(values);

        let mut table_output = builder.build();
        table_output.with(Style::ascii());
        Ok(table_output.to_string())
    }

    /// Parses input string into a vector of commands
    fn parse_commands(&self, input: &str) -> Result<Vec<Command>, String> {
        let commands = input.split("AND").collect::<Vec<_>>();
        let mut parsed_commands = Vec::new();

        for cmd_str in commands {
            let tokens: Vec<&str> = cmd_str
                .trim_matches(|c: char| (c.is_whitespace() || c.is_control() || c == '\n'))
                .split_whitespace()
                .collect::<Vec<&str>>()
                .into_iter()
                .filter(|x| x.len() > 0)
                .collect();

            if tokens.is_empty() {
                continue;
            }

            let command = match tokens[0].to_uppercase().as_str() {
                "CREATE_TABLE" => self.parse_create_table(&tokens[1..])?,
                "DROP_TABLE" => self.parse_drop_table(&tokens[1..])?,
                "INSERT" => self.parse_insert(&tokens[1..])?,
                "UPDATE" => self.parse_update(&tokens[1..])?,
                "DELETE" => self.parse_delete(&tokens[1..])?,
                "UPDATE_SCHEMA" => self.parse_schema_update(&tokens[1..])?,
                "GET_RECORD" => self.parse_get_record(&tokens[1..])?,
                "GET_TABLE" => self.parse_get_table(&tokens[1..])?,
                "GET_TABLES" => self.parse_get_tables()?,
                "GET_RECORDS" => self.parse_get_records(&tokens[1..])?,
                "SEARCH_RECORDS" => self.parse_search_records(&tokens[1..])?,
                "HELP" => self.parse_help(if tokens.len() > 1 {
                    Some(tokens[1])
                } else {
                    None
                })?,
                _ => return Err(format!("Unknown command: {}", tokens[0])),
            };

            parsed_commands.push(command);
        }

        Ok(parsed_commands)
    }

    fn parse_create_table(&self, args: &[&str]) -> Result<Command, String> {
        if args.is_empty() {
            return Err("Missing table name".to_string());
        }

        let name = args[0].to_string();
        if args.len() == 1 {
            Ok(Command::CreateTable { name })
        } else {
            // Parse schema if provided
            let schema = self.parse_schema(&name, &args[1..])?;
            Ok(Command::CreateTableWithSchema { name, schema })
        }
    }

    fn parse_drop_table(&self, args: &[&str]) -> Result<Command, String> {
        if args.is_empty() {
            return Err("Invalid DROP_TABLE syntax. Expected: DROP_TABLE <table>".to_string());
        }
        Ok(Command::DropTable {
            name: args[0].to_string(),
        })
    }

    fn parse_insert(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 4 || args[0].to_uppercase() != "INTO" || args[2].to_uppercase() != "ID" {
            return Err("Invalid INSERT syntax. Expected: INSERT INTO <table> ID <id> SET <field=value ...>".to_string());
        }
        let table = args[1].to_string();
        let id = args[3].parse::<u64>().map_err(|_| {
            "Invalid INSERT syntax. Expected: INSERT INTO <table> ID <id> SET <field=value ...>"
                .to_string()
        })?;
        let values = self.parse_values(&args[4..])?;

        Ok(Command::InsertRecord { table, id, values })
    }

    fn parse_update(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 4 || args[1].to_uppercase() != "ID" {
            return Err(
                "Invalid UPDATE syntax. Expected: UPDATE <table> ID <id> SET <field=value ...>"
                    .to_string(),
            );
        }
        let table = args[0].to_string();
        let id = args[2].parse::<u64>().map_err(|_| {
            "Invalid UPDATE syntax. Expected: UPDATE <table> ID <id> SET <field=value ...>"
                .to_string()
        })?;
        let values = self.parse_values(&args[3..])?;

        Ok(Command::UpdateRecord { table, id, values })
    }

    fn parse_delete(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 3 || args[0].to_uppercase() != "FROM" {
            return Err("Invalid DELETE syntax. Expected: DELETE FROM <table> <id>".to_string());
        }
        let table = args[1].to_string();
        let id = args[2]
            .parse::<u64>()
            .map_err(|_| "Invalid DELETE syntax. Expected: DELETE FROM <table> <id>".to_string())?;

        Ok(Command::DeleteRecord { table, id })
    }

    fn parse_schema_update(&self, args: &[&str]) -> Result<Command, String> {
        if args.is_empty() {
            return Err("Invalid UPDATE_SCHEMA syntax. Expected: UPDATE_SCHEMA <table> [--version=<n>] <field type ...>".to_string());
        }
        let table = args[0].to_string();
        let schema = self.parse_schema(&table, &args[1..])?;

        Ok(Command::UpdateSchema { table, schema })
    }

    fn parse_get_record(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 3 || args[0].to_uppercase() != "FROM" {
            return Err(
                "Invalid GET_RECORD syntax. Expected: GET_RECORD FROM <table> <id>".to_string(),
            );
        }
        let table = args[1].to_string();
        let id = args[2].parse::<u64>().map_err(|_| {
            "Invalid GET_RECORD syntax. Expected: GET_RECORD FROM <table> <id>".to_string()
        })?;

        Ok(Command::GetRecord { table, id })
    }

    fn parse_get_table(&self, args: &[&str]) -> Result<Command, String> {
        if args.is_empty() {
            return Err("Invalid GET_TABLE syntax. Expected: GET_TABLE <table>".to_string());
        }
        Ok(Command::GetTable {
            name: args[0].to_string(),
        })
    }

    fn parse_get_tables(&self) -> Result<Command, String> {
        Ok(Command::GetTables)
    }

    fn parse_get_records(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 2 || args[0].to_uppercase() != "FROM" {
            return Err(
                "Invalid GET_RECORDS syntax. Expected: GET_RECORDS FROM <table>".to_string(),
            );
        }
        Ok(Command::GetRecords {
            table: args[1].to_string(),
        })
    }

    fn parse_search_records(&self, args: &[&str]) -> Result<Command, String> {
        if args.len() < 3 || args[0].to_uppercase() != "FROM" || args[2].to_uppercase() != "MATCH" {
            return Err("Invalid SEARCH_RECORDS syntax. Expected: SEARCH_RECORDS FROM <table> MATCH <query>".to_string());
        }
        Ok(Command::SearchRecords {
            table: args[1].to_string(),
            query: args[3..].join(" "),
        })
    }

    fn parse_help(&self, command: Option<&str>) -> Result<Command, String> {
        if let Some(cmd) = command {
            if !COMMAND_HELP.contains_key(cmd) {
                return Err(format!(
                    "Unknown command: {}. Use HELP to see available commands.",
                    cmd
                ));
            }
        }
        Ok(Command::Help {
            command: command.map(String::from),
        })
    }

    fn parse_schema(&self, table_name: &str, args: &[&str]) -> Result<TableSchema, String> {
        let current_schema: Option<TableSchema> = match self.engine.get_table(table_name) {
            Some(t) => Some(t.schema.clone()),
            _ => None,
        };

        let mut fields = match current_schema {
            Some(s) => s.fields,
            None => HashMap::new(),
        };

        let mut i = 0;
        let mut version = 1; // Default version

        while i < args.len() {
            if args[i].starts_with("--version=") {
                version = args[i][10..]
                    .parse::<u32>()
                    .map_err(|_| "Invalid schema version".to_string())?;
                i += 1;
                continue;
            }

            let field_name = args[i].to_string();
            i += 1;
            if i >= args.len() {
                break;
            }

            let (constraint, new_i) = self.parse_constraints(args, &field_name, &mut i)?;
            i = new_i;

            fields.insert(field_name, constraint);
        }

        Ok(TableSchema {
            name: table_name.to_string(),
            fields,
            version,
        })
    }

    fn parse_constraints(
        &self,
        args: &[&str],
        field_name: &str,
        i: &mut usize,
    ) -> Result<(FieldConstraint, usize), String> {
        let mut constraint = FieldConstraint {
            field_type: match args[*i].to_uppercase().as_str() {
                "STRING" => FieldType::String,
                "INTEGER" => FieldType::Integer,
                "FLOAT" => FieldType::Float,
                "BOOLEAN" => FieldType::Boolean,
                _ => return Err(format!("Invalid field type: {}", args[*i])),
            },
            required: false,
            min: None,
            max: None,
            pattern: None,
            unique: false,
            default: None,
        };

        *i += 1;
        while *i < args.len() {
            if !args[*i].starts_with("--") {
                break;
            }

            match self.parse_constraint_flag(args[*i], field_name)? {
                ConstraintFlag::Required => constraint.required = true,
                ConstraintFlag::Unique => constraint.unique = true,
                ConstraintFlag::Min(val) => constraint.min = Some(val),
                ConstraintFlag::Max(val) => constraint.max = Some(val),
                ConstraintFlag::Pattern(pat) => constraint.pattern = Some(pat),
                ConstraintFlag::Default(val) => constraint.default = Some(val),
            }

            *i += 1;
        }

        Ok((constraint, *i))
    }

    fn parse_constraint_flag(
        &self,
        flag: &str,
        field_name: &str,
    ) -> Result<ConstraintFlag, String> {
        match flag {
            "--required" => Ok(ConstraintFlag::Required),
            "--unique" => Ok(ConstraintFlag::Unique),
            _ if flag.starts_with("--min=") => {
                let val = flag[6..]
                    .parse()
                    .map_err(|_| format!("Invalid min value for field '{}'", field_name))?;
                Ok(ConstraintFlag::Min(val))
            }
            _ if flag.starts_with("--max=") => {
                let val = flag[6..]
                    .parse()
                    .map_err(|_| format!("Invalid max value for field '{}'", field_name))?;
                Ok(ConstraintFlag::Max(val))
            }
            _ if flag.starts_with("--pattern=") => {
                // Validate regex pattern
                let pattern = &flag[10..];
                regex::Regex::new(pattern)
                    .map_err(|_| format!("Invalid regex pattern for field '{}'", field_name))?;
                Ok(ConstraintFlag::Pattern(pattern.to_string()))
            }
            _ if flag.starts_with("--default=") => {
                let val = self
                    .parse_value(&flag[10..])
                    .map_err(|_| format!("Invalid default value for field '{}'", field_name))?;
                Ok(ConstraintFlag::Default(val))
            }
            _ => Err(format!(
                "Unknown constraint flag: {} for field '{}'",
                flag, field_name
            )),
        }
    }

    fn parse_values(&self, args: &[&str]) -> Result<HashMap<String, ValueType>, String> {
        let mut values = HashMap::new();
        let mut i = 0;

        while i < args.len() {
            if args[i] == "SET" {
                i += 1;
                continue;
            }

            if i + 2 >= args.len() {
                break;
            }

            let field = args[i].to_string();
            i += 1;

            if args[i] != "=" {
                return Err(format!("Expected '=', found '{}'", args[i]));
            }
            i += 1;

            // Handle quoted strings that may contain spaces
            if args[i].starts_with('"') {
                let mut full_value = args[i].to_string();

                // If the value doesn't end with a quote, keep adding tokens until we find the end quote
                if !args[i].ends_with('"') {
                    i += 1;
                    while i < args.len() {
                        full_value.push(' ');
                        full_value.push_str(args[i]);
                        if args[i].ends_with('"') {
                            break;
                        }
                        i += 1;
                    }
                }

                let value = self.parse_value(&full_value)?;
                values.insert(field, value);
            } else {
                // Handle non-string values normally
                let value = self.parse_value(args[i])?;
                values.insert(field, value);
            }

            i += 1;
        }

        Ok(values)
    }

    fn parse_value(&self, value: &str) -> Result<ValueType, String> {
        if value.starts_with('"') && value.ends_with('"') {
            Ok(ValueType::Str(value[1..value.len() - 1].to_string()))
        } else if value == "true" || value == "false" {
            Ok(ValueType::Bool(value == "true"))
        } else if let Ok(i) = value.parse::<i64>() {
            Ok(ValueType::Int(i))
        } else if let Ok(f) = value.parse::<f64>() {
            Ok(ValueType::Float(f))
        } else {
            Err(format!("Invalid value: {}", value))
        }
    }

    /// Formats help text according to the specified output format.
    /// Provides different layouts for standard, JSON, and table formats.
    fn format_command_help(&self, help: &CommandHelp, format: OutputFormat) -> String {
        match format {
            OutputFormat::Standard => format!(
                "Syntax: {}\n\nDescription:\n{}\n\nFlags:\n{}\n\nExamples:\n{}\n",
                help.syntax,
                help.description,
                help.flags
                    .iter()
                    .map(|(flag, desc)| format!("  {} - {}", flag, desc))
                    .collect::<Vec<_>>()
                    .join("\n"),
                help.examples
                    .iter()
                    .map(|ex| format!("  {}", ex))
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
            OutputFormat::JSON => serde_json::to_string_pretty(&help).unwrap(),
            OutputFormat::Table => {
                let mut builder = Builder::new();
                builder.push_record(["Syntax", &help.syntax]);
                builder.push_record(["Description", &help.description]);

                let flags = help
                    .flags
                    .iter()
                    .map(|(flag, desc)| format!("{}: {}", flag, desc))
                    .collect::<Vec<_>>()
                    .join("\n");
                builder.push_record(["Flags", &flags]);

                let examples = help.examples.join("\n");
                builder.push_record(["Examples", &examples]);

                builder.build().with(Style::ascii()).to_string()
            }
        }
    }

    /// Formats the general help menu that lists all available commands.
    /// Adapts the format based on the specified OutputFormat.
    fn format_general_help(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Standard => get_general_help(),
            OutputFormat::JSON => {
                let commands: Vec<&str> = COMMAND_HELP.keys().copied().collect();
                serde_json::to_string_pretty(&commands).unwrap()
            }
            OutputFormat::Table => {
                let mut builder = Builder::new();
                builder.push_record(["Command", "Description"]);
                for (cmd, help) in COMMAND_HELP.iter() {
                    builder.push_record([cmd, &*help.description]);
                }
                builder.build().with(Style::ascii()).to_string()
            }
        }
    }
}
