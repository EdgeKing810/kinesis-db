# Kinesis DB

A high-performance, ACID-compliant embedded database written in Rust with support for multiple storage engines and flexible schema management.

## Features

- **Multiple Storage Engines**

  - In-Memory: Fast, volatile storage for temporary data
  - On-Disk: Persistent storage with ACID guarantees
  - Hybrid: Intelligent caching with persistent backing

- **Schema Management**

  - Dynamic schema creation and updates
  - Strong type system with constraints
  - Support for required fields, unique constraints, and default values
  - Pattern matching for string fields
  - Min/max value constraints for numeric fields

- **Query Interface**

  - SQL-like command syntax for familiarity
  - Table operations (CREATE, DROP)
  - Record operations (INSERT, UPDATE, DELETE)
  - Search capabilities across string fields
  - Multiple output formats (Standard, JSON, Table)

- **Transaction Support**
  - ACID compliance
  - Configurable isolation levels
  - Deadlock detection
  - Write-Ahead Logging (WAL)

## REPL Command Reference

Use the built-in help system to learn about available commands:

```
-- Show all commands
HELP

-- Get details about a specific command
HELP CREATE_TABLE
```

Common commands:

```
-- Create a table
CREATE_TABLE users
    name STRING --required
    age INTEGER --min=0 --max=150

-- Insert a record
INSERT INTO users ID 1 SET
    name = "Alice"
    age = 25

-- Query records
GET_RECORDS FROM users
GET_RECORD FROM users 1
SEARCH_RECORDS FROM users MATCH Alice

-- Update a record
UPDATE users ID 1 SET age = 26

-- Delete a record
DELETE FROM users 1

-- Update schema
UPDATE_SCHEMA users --version=2
    active BOOLEAN --default=true
```

## Configuration Options

### Storage Engine Selection

- InMemory: Fastest performance, no persistence
- OnDisk: Full persistence with ACID guarantees
- Hybrid: Balanced performance with persistence

### Transaction Isolation Levels

- ReadUncommitted: Highest performance, lowest isolation
- ReadCommitted: Prevents dirty reads
- RepeatableRead: Prevents non-repeatable reads
- Serializable: Highest isolation, may impact performance

### Buffer Pool Configuration

- Adjust size based on available memory
- Consider workload patterns
- Monitor eviction rates

## Performance Considerations

- Choose appropriate storage engine for your use case
- Configure buffer pool size based on available memory
- Use appropriate isolation level for your requirements
- Consider batch operations for bulk data handling
- Index fields that are frequently searched

## Error Handling

The database provides detailed error messages for:

- Schema validation failures
- Constraint violations
- Deadlock detection
- Transaction conflicts
- I/O errors

## Contributing

Pull requests are welcome! Please ensure your changes include:

- Tests for new functionality
- Documentation updates
- Proper error handling
- Code formatting with `cargo fmt`
- Pass all checks with `cargo test`
