mod db;
mod btree;

use db::{Database, Field, FieldType, Record, StorageMode};

fn main() {
    let mut db = Database::new();
    db.create_table("users", vec![
        Field { name: "id".to_string(), field_type: FieldType::Integer },
        Field { name: "name".to_string(), field_type: FieldType::String },
        Field { name: "age".to_string(), field_type: FieldType::Integer }
    ], StorageMode::InMemory, None);
    
    db.insert("users", Record { id: 1, values: vec!["Alice".to_string(), "25".to_string()] });
    db.insert("users", Record { id: 2, values: vec!["Bob".to_string(), "30".to_string()] });
    
    if let Some(record) = db.search("users", 1) {
        println!("Found: {:?}", record);
    } else {
        println!("Not Found");
    }
    
    db.delete("users", 2);
}
