use std::fs::create_dir_all;

use components::database::db_type::DatabaseType;
use components::database::engine::DBEngine;
use components::database::record::Record;
use components::database::restore_policy::RestorePolicy;
use components::database::value_type::ValueType;
use components::repl::{OutputFormat, REPL};
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

    println!("\n🚀 Initializing Database...");

    let mut engine = DBEngine::new(
        DatabaseType::Hybrid,
        RestorePolicy::RecoverPending,
        "data/test_db",
        "data/wal.log",
        Some(tx_config),
        IsolationLevel::Serializable,
    );

    let mut repl = REPL::new(&mut engine);

    // Create table schemas
    let users_schema = r#"
        CREATE_TABLE users 
            name       STRING  --required
            age        INTEGER --required
            role       STRING  --required
            salary     FLOAT   --required
            senior     BOOLEAN --required
            department STRING  --required
            work_mode  STRING  --required
    "#;

    let animals_schema = r#"
        CREATE_TABLE animals 
            name          STRING  --required
            species       STRING  --required
            age           INTEGER --required
            weight        FLOAT   --required
            vaccinated    BOOLEAN --required
            breed         STRING  --required
            temperament   STRING  --required
            size          STRING  --required
            house_trained BOOLEAN --required
            origin        STRING  --required
    "#;

    // Create tables
    repl.execute(users_schema, Some(OutputFormat::Standard))
        .unwrap();
    repl.execute(animals_schema, Some(OutputFormat::Standard))
        .unwrap();
    println!("✅ Tables created successfully");

    // Insert users with more properties
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
        let record = format!(
            r#"
            INSERT INTO users ID {} SET
                name       = "{}"
                age        = {}
                role       = "{}"
                salary     = {}
                senior     = {}
                department = "{}"
                work_mode  = "{}"
        "#,
            id, name, age, role, salary, senior, department, work_mode
        );

        repl.execute(&record, Some(OutputFormat::Standard)).unwrap();
    }
    println!("✅ Users data inserted");

    // Insert diverse animals
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
        let record = format!(
            r#"
            INSERT INTO animals ID {} SET
                name          = "{}"
                species       = "{}"
                age           = {}
                weight        = {}
                vaccinated    = {}
                breed         = "{}"
                temperament   = "{}"
                size          = "{}"
                house_trained = {}
                origin        = "{}"
        "#,
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
            origin
        );

        repl.execute(&record, Some(OutputFormat::Standard)).unwrap();
    }
    println!("✅ Animals data inserted");

    // Advanced Queries
    println!("\n🔍 Running Queries...");

    // User queries
    println!("\n👥 User Statistics:");
    let remote_workers = repl
        .execute(
            "SEARCH_RECORDS FROM users MATCH Remote",
            Some(OutputFormat::Table),
        )
        .unwrap();
    println!(
        "Remote Workers: {}",
        (remote_workers.split('\n').count() - 3) / 2
    );

    let senior_staff = repl
        .execute(
            "SEARCH_RECORDS FROM users MATCH Senior",
            Some(OutputFormat::Table),
        )
        .unwrap();
    println!(
        "Senior Staff Members: {}",
        (senior_staff.split('\n').count() - 3) / 2
    );

    // Animal queries
    println!("\n🐾 Animal Statistics:");
    for species in [
        "Dog", "Cat", "Parrot", "Snake", "Lizard", "Horse", "Sheep", "Pig",
    ] {
        let query = format!("SEARCH_RECORDS FROM animals MATCH {}", species);
        let animals = repl.execute(&query, Some(OutputFormat::Table)).unwrap();
        println!(
            "{} count: {}",
            species,
            (animals.split('\n').count() - 3) / 2
        );
    }

    // Test complex updates
    println!("\n✏️ Testing Updates...");

    // Promote an employee
    let promoted_user_record = r#"
        UPDATE users ID 4 SET
            age        = 29
            role       = "Lead Data Scientist"
            salary     = 120000.0
            work_mode  = "Hybrid"
    "#;
    repl.execute(&promoted_user_record, Some(OutputFormat::Standard))
        .unwrap();

    // Update animal training status
    let trained_cat_record = r#"
        UPDATE animals ID 6 SET
            vaccinated    = true
            temperament   = "Well-behaved"
    "#;
    repl.execute(&trained_cat_record, Some(OutputFormat::Standard))
        .unwrap();

    println!("✅ Updates completed successfully");

    // Final State Display
    println!("\n📊 Final Database State:");

    println!("\n👥 Users:");
    let fetch_users = "GET_RECORDS FROM users";
    println!(
        "{}",
        repl.execute(fetch_users, Some(OutputFormat::Table))
            .unwrap()
    );

    println!("\n🐾 Animals:");
    let fetch_animals = "GET_RECORDS FROM animals";
    println!(
        "{}",
        repl.execute(fetch_animals, Some(OutputFormat::Table))
            .unwrap()
    );

    println!("\n✨ Database operations completed successfully!");
}
