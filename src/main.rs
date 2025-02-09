use std::fs::create_dir_all;

use components::database::db_type::DatabaseType;
use components::database::engine::DBEngine;
use components::database::restore_policy::RestorePolicy;
use components::database::table::{Record, ValueType};
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

    let engine_ondisk = DBEngine::new(
        DatabaseType::OnDisk,
        RestorePolicy::RecoverPending,
        "data/test_db",
        "data/wal.log",
        Some(tx_config),
    );
    let mut engine = engine_ondisk;
    let isolation_level = IsolationLevel::Serializable;

    println!("\n🚀 Initializing Database...");
    // Create data directory

    // Create tables
    let mut tx = engine.begin_transaction(isolation_level);
    engine.create_table(&mut tx, "users");
    engine.create_table(&mut tx, "animals");
    engine.commit(tx).unwrap();
    println!("✅ Tables created successfully");

    // Insert users with more properties
    let mut tx = engine.begin_transaction(isolation_level);
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
        let record = Record {
            id,
            values: vec![
                ValueType::Str(name.to_string()),
                ValueType::Int(age),
                ValueType::Str(role.to_string()),
                ValueType::Float(salary),
                ValueType::Bool(senior),
                ValueType::Str(department.to_string()),
                ValueType::Str(work_mode.to_string()),
            ],
            version: 0,
            timestamp: 0,
        };
        engine.insert_record(&mut tx, "users", record);
    }
    engine.commit(tx).unwrap();
    println!("✅ Users data inserted");

    // Insert diverse animals
    let mut tx = engine.begin_transaction(isolation_level);
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
        let record = Record {
            id,
            values: vec![
                ValueType::Str(name.to_string()),
                ValueType::Str(species.to_string()),
                ValueType::Int(age),
                ValueType::Float(weight),
                ValueType::Bool(vaccinated),
                ValueType::Str(breed.to_string()),
                ValueType::Str(temperament.to_string()),
                ValueType::Str(size.to_string()),
                ValueType::Bool(house_trained),
                ValueType::Str(origin.to_string()),
            ],
            version: 0,
            timestamp: 0,
        };
        engine.insert_record(&mut tx, "animals", record);
    }
    engine.commit(tx).unwrap();
    println!("✅ Animals data inserted");

    // Advanced Queries
    println!("\n🔍 Running Queries...");

    let mut tx = engine.begin_transaction(isolation_level);

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
    let mut tx = engine.begin_transaction(isolation_level);

    // Promote an employee
    engine.delete_record(&mut tx, "users", 4);
    let promoted_user = Record {
        id: 4,
        values: vec![
            ValueType::Str("Diana".to_string()),
            ValueType::Int(29),
            ValueType::Str("Lead Data Scientist".to_string()),
            ValueType::Float(120000.0),
            ValueType::Bool(true),
            ValueType::Str("Analytics".to_string()),
            ValueType::Str("Hybrid".to_string()),
        ],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "users", promoted_user);

    // Update animal training status
    engine.delete_record(&mut tx, "animals", 6);
    let trained_cat = Record {
        id: 6,
        values: vec![
            ValueType::Str("Lucy".to_string()),
            ValueType::Str("Cat".to_string()),
            ValueType::Int(2),
            ValueType::Float(3.5),
            ValueType::Bool(true),
            ValueType::Str("Siamese".to_string()),
            ValueType::Str("Well-behaved".to_string()),
            ValueType::Str("Small".to_string()),
            ValueType::Bool(true),
            ValueType::Str("Thailand".to_string()),
        ],
        version: 0,
        timestamp: 0,
    };
    engine.insert_record(&mut tx, "animals", trained_cat);
    engine.commit(tx).unwrap();
    println!("✅ Updates completed successfully");

    // Final State Display
    println!("\n📊 Final Database State:");
    let mut final_tx = engine.begin_transaction(isolation_level);

    println!("\n👥 Users:");
    for id in 1..=8 {
        if let Some(user) = engine.get_record(&mut final_tx, "users", id) {
            println!(
                "ID {}: {} - {} ({})",
                id,
                match &user.values[0] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.values[2] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &user.values[5] {
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
                match &animal.values[0] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[5] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[1] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[6] {
                    ValueType::Str(s) => s,
                    _ => "?",
                },
                match &animal.values[9] {
                    ValueType::Str(s) => s,
                    _ => "?",
                }
            );
        }
    }

    println!("\n✨ Database operations completed successfully!");
}
