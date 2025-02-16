use std::collections::HashMap;
use std::fs::create_dir_all;

use components::database::db_type::DatabaseType;
use components::database::engine::DBEngine;
use components::database::record::Record;
use components::database::restore_policy::RestorePolicy;
use components::database::schema::{FieldConstraint, FieldType, TableSchema};
use components::database::value_type::ValueType;
use components::transaction::config::TransactionConfig;
use components::transaction::isolation_level::IsolationLevel;

mod components;
mod tests;

fn main() {
    create_dir_all("data").unwrap();
    let tx_config = TransactionConfig {
        timeout_secs: 60,                   // 1 minute timeout
        max_retries: 5,                     // More retries
        deadlock_detection_interval_ms: 50, // Faster detection
    };

    let mut engine = DBEngine::new(
        DatabaseType::Hybrid,
        RestorePolicy::RecoverPending,
        "data/test_db",
        "data/wal.log",
        Some(tx_config),
        IsolationLevel::Serializable,
    );

    println!("\n🚀 Initializing Database...");
    // Create data directory

    // Create table schemas
    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
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
        },
    );
    fields.insert(
        "role".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "salary".to_string(),
        FieldConstraint {
            field_type: FieldType::Float,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "senior".to_string(),
        FieldConstraint {
            field_type: FieldType::Boolean,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "department".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "work_mode".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );

    let users_schema = TableSchema {
        name: "users".to_string(),
        fields,
    };

    let mut fields = HashMap::new();
    fields.insert(
        "name".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "species".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
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
        },
    );
    fields.insert(
        "weight".to_string(),
        FieldConstraint {
            field_type: FieldType::Float,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "vaccinated".to_string(),
        FieldConstraint {
            field_type: FieldType::Boolean,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "breed".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "temperament".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "size".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "house_trained".to_string(),
        FieldConstraint {
            field_type: FieldType::Boolean,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );
    fields.insert(
        "origin".to_string(),
        FieldConstraint {
            field_type: FieldType::String,
            required: true,
            min: None,
            max: None,
            pattern: None,
        },
    );

    let animals_schema = TableSchema {
        name: "animals".to_string(),
        fields,
    };

    // Create tables
    let mut tx = engine.begin_transaction();
    engine.create_table_with_schema(&mut tx, "users", users_schema);
    engine.create_table_with_schema(&mut tx, "animals", animals_schema);
    engine.commit(tx).unwrap();
    println!("✅ Tables created successfully");

    // Insert users with more properties
    let mut tx = engine.begin_transaction();
    let users = vec![
        (
            1,
            "Alice",
            30,
            "Senior Developer",
            95000.0,
            true,
            "Engineering",
            "Remote",
        ),
        (
            2,
            "Bob",
            25,
            "UI Designer",
            75000.0,
            false,
            "Design",
            "Office",
        ),
        (
            3,
            "Charlie",
            35,
            "Product Manager",
            120000.0,
            true,
            "Management",
            "Hybrid",
        ),
        (
            4,
            "Diana",
            28,
            "Data Scientist",
            98000.0,
            true,
            "Analytics",
            "Remote",
        ),
        (
            5,
            "Eve",
            32,
            "DevOps Engineer",
            105000.0,
            true,
            "Infrastructure",
            "Remote",
        ),
        (6, "Frank", 41, "CTO", 180000.0, true, "Executive", "Office"),
        (
            7,
            "Grace",
            27,
            "QA Engineer",
            72000.0,
            false,
            "Quality",
            "Hybrid",
        ),
        (
            8,
            "Henry",
            38,
            "Solutions Architect",
            125000.0,
            true,
            "Engineering",
            "Remote",
        ),
    ];

    for (id, name, age, role, salary, senior, department, work_mode) in users {
        let mut record = Record::new(id);
        record.set_field("name", ValueType::Str(name.to_string()));
        record.set_field("age", ValueType::Int(age));
        record.set_field("role", ValueType::Str(role.to_string()));
        record.set_field("salary", ValueType::Float(salary));
        record.set_field("senior", ValueType::Bool(senior));
        record.set_field("department", ValueType::Str(department.to_string()));
        record.set_field("work_mode", ValueType::Str(work_mode.to_string()));

        engine.insert_record(&mut tx, "users", record).unwrap();
    }
    engine.commit(tx).unwrap();
    println!("✅ Users data inserted");

    // Insert diverse animals
    let mut tx = engine.begin_transaction();
    let animals = vec![
        // Dogs
        (
            1,
            "Max",
            "Dog",
            5,
            15.5,
            true,
            "Golden Retriever",
            "Friendly",
            "Medium",
            true,
            "USA",
        ),
        (
            2,
            "Luna",
            "Dog",
            3,
            30.2,
            true,
            "German Shepherd",
            "Protective",
            "Large",
            false,
            "Germany",
        ),
        (
            3,
            "Rocky",
            "Dog",
            2,
            8.5,
            true,
            "French Bulldog",
            "Playful",
            "Small",
            true,
            "France",
        ),
        // Cats
        (
            4, "Bella", "Cat", 1, 3.8, true, "Persian", "Lazy", "Medium", true, "Iran",
        ),
        (
            5,
            "Oliver",
            "Cat",
            4,
            4.2,
            true,
            "Maine Coon",
            "Independent",
            "Large",
            false,
            "USA",
        ),
        (
            6, "Lucy", "Cat", 2, 3.5, false, "Siamese", "Active", "Small", true, "Thailand",
        ),
        // Exotic Pets
        (
            7,
            "Ziggy",
            "Parrot",
            15,
            0.4,
            true,
            "African Grey",
            "Talkative",
            "Medium",
            false,
            "Congo",
        ),
        (
            8,
            "Monty",
            "Snake",
            3,
            2.0,
            false,
            "Ball Python",
            "Calm",
            "Medium",
            true,
            "Africa",
        ),
        (
            9,
            "Spike",
            "Lizard",
            2,
            0.3,
            true,
            "Bearded Dragon",
            "Friendly",
            "Small",
            true,
            "Australia",
        ),
        // Farm Animals
        (
            10, "Thunder", "Horse", 8, 450.0, true, "Arabian", "Spirited", "Large", false, "Arabia",
        ),
        (
            11,
            "Wooley",
            "Sheep",
            3,
            80.0,
            true,
            "Merino",
            "Gentle",
            "Medium",
            true,
            "Australia",
        ),
        (
            12,
            "Einstein",
            "Pig",
            1,
            120.0,
            true,
            "Vietnamese Pot-Belly",
            "Smart",
            "Medium",
            true,
            "Vietnam",
        ),
    ];

    for (
        id,
        name,
        species,
        age,
        weight,
        vaccinated,
        breed,
        temperament,
        size,
        house_trained,
        origin,
    ) in animals
    {
        let mut record = Record::new(id);
        record.set_field("name", ValueType::Str(name.to_string()));
        record.set_field("species", ValueType::Str(species.to_string()));
        record.set_field("age", ValueType::Int(age));
        record.set_field("weight", ValueType::Float(weight));
        record.set_field("vaccinated", ValueType::Bool(vaccinated));
        record.set_field("breed", ValueType::Str(breed.to_string()));
        record.set_field("temperament", ValueType::Str(temperament.to_string()));
        record.set_field("size", ValueType::Str(size.to_string()));
        record.set_field("house_trained", ValueType::Bool(house_trained));
        record.set_field("origin", ValueType::Str(origin.to_string()));

        engine.insert_record(&mut tx, "animals", record).unwrap();
    }
    engine.commit(tx).unwrap();
    println!("✅ Animals data inserted");

    // Advanced Queries
    println!("\n🔍 Running Queries...");

    let mut tx = engine.begin_transaction();

    // User queries
    println!("\n👥 User Statistics:");
    let remote_workers = engine.search_records(&mut tx, "users", "Remote");
    println!("Remote Workers: {}", remote_workers.len());

    let senior_staff = engine.search_records(&mut tx, "users", "Senior");
    println!("Senior Staff Members: {}", senior_staff.len());

    // Animal queries
    println!("\n🐾 Animal Statistics:");
    for species in [
        "Dog", "Cat", "Parrot", "Snake", "Lizard", "Horse", "Sheep", "Pig",
    ] {
        let animals = engine.search_records(&mut tx, "animals", species);
        println!("{} count: {}", species, animals.len());
    }

    // Test complex updates
    println!("\n✏️ Testing Updates...");
    let mut tx = engine.begin_transaction();

    // Promote an employee
    engine.delete_record(&mut tx, "users", 4);
    let mut promoted_user_record = Record::new(4);
    promoted_user_record.set_field("name", ValueType::Str("Diana".to_string()));
    promoted_user_record.set_field("age", ValueType::Int(29));
    promoted_user_record.set_field("role", ValueType::Str("Lead Data Scientist".to_string()));
    promoted_user_record.set_field("salary", ValueType::Float(120000.0));
    promoted_user_record.set_field("senior", ValueType::Bool(true));
    promoted_user_record.set_field("department", ValueType::Str("Analytics".to_string()));
    promoted_user_record.set_field("work_mode", ValueType::Str("Hybrid".to_string()));

    engine
        .insert_record(&mut tx, "users", promoted_user_record)
        .unwrap();

    // Update animal training status
    engine.delete_record(&mut tx, "animals", 6);
    let mut trained_cat_record = Record::new(6);
    trained_cat_record.set_field("name", ValueType::Str("Lucy".to_string()));
    trained_cat_record.set_field("species", ValueType::Str("Cat".to_string()));
    trained_cat_record.set_field("age", ValueType::Int(2));
    trained_cat_record.set_field("weight", ValueType::Float(3.5));
    trained_cat_record.set_field("vaccinated", ValueType::Bool(true));
    trained_cat_record.set_field("breed", ValueType::Str("Siamese".to_string()));
    trained_cat_record.set_field("temperament", ValueType::Str("Well-behaved".to_string()));
    trained_cat_record.set_field("size", ValueType::Str("Small".to_string()));
    trained_cat_record.set_field("house_trained", ValueType::Bool(true));
    trained_cat_record.set_field("origin", ValueType::Str("Thailand".to_string()));

    engine
        .insert_record(&mut tx, "animals", trained_cat_record)
        .unwrap();
    engine.commit(tx).unwrap();
    println!("✅ Updates completed successfully");

    // Final State Display
    println!("\n📊 Final Database State:");
    let mut final_tx = engine.begin_transaction();

    println!("\n👥 Users:");
    for id in 1..=8 {
        if let Some(user) = engine.get_record(&mut final_tx, "users", id) {
            println!(
                "ID {}: {} - {} ({})",
                id,
                match &user.get_field("name").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.get_field("role").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.get_field("department").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                }
            );
        }
    }

    println!("\n🐾 Animals:");
    for id in 1..=12 {
        if let Some(animal) = engine.get_record(&mut final_tx, "animals", id) {
            println!(
                "ID {}: {} - {} {} ({}, {})",
                id,
                match &animal.get_field("name").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.get_field("breed").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.get_field("species").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.get_field("temperament").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.get_field("origin").unwrap() {
                    ValueType::Str(s) => s,
                    _ => "?",
                }
            );
        }
    }

    println!("\n✨ Database operations completed successfully!");
}
