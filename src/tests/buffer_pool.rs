#![allow(unused_imports)]
use crate::{
    components::{
        database::db_type::DatabaseType,
        storage::{
            buffer_pool::BufferPool,
            disk_manager::DiskManager,
            page::{PAGE_HEADER_SIZE, PAGE_SIZE},
            page_store::PageStore,
        },
    },
    tests::get_test_dir,
};

fn test_write_and_verify(
    test_data: Vec<u8>,
    page_store: &PageStore,
    buffer_pool: &mut BufferPool,
) -> Vec<u64> {
    let mut current_data = test_data.clone();
    let mut page_ids = Vec::new();

    // Write data across pages
    while !current_data.is_empty() {
        let page_id = page_store.allocate_page().unwrap();
        page_ids.push(page_id);

        let frame = buffer_pool.get_page(page_id, page_store);
        let remaining = {
            let mut page = frame.get_page().write().unwrap();
            let result = page.write_data(0, &current_data).unwrap();
            buffer_pool.unpin_page(page_id, true);
            result
        };

        current_data = remaining.unwrap_or_default();
    }

    // Flush changes
    buffer_pool.flush_all(page_store);

    // Read and verify
    let mut reconstructed_data = Vec::new();
    for &page_id in &page_ids {
        let frame = buffer_pool.get_page(page_id, page_store);
        let page = frame.get_page().read().unwrap();
        reconstructed_data.extend_from_slice(&page.data);
        buffer_pool.unpin_page(page_id, false);
    }

    reconstructed_data.truncate(test_data.len());
    assert_eq!(
        reconstructed_data,
        test_data,
        "Data verification failed - len: {}, expected len: {}",
        reconstructed_data.len(),
        test_data.len()
    );

    page_ids
}

#[test]
fn test_large_data_handling() {
    let test_dir = get_test_dir();
    let file_path = test_dir.join("test.db");
    let file_path_str = file_path.to_str().unwrap();

    // Create test data larger than PAGE_SIZE (16384 bytes)
    let large_data = vec![42u8; 40960]; // 40KB of data

    // Initialize components
    let page_store = PageStore::new(file_path_str).unwrap();
    let mut buffer_pool = BufferPool::new(10, DatabaseType::Hybrid);

    let page_ids = test_write_and_verify(large_data.clone(), &page_store, &mut buffer_pool);

    // Verify we used the expected number of pages
    let expected_pages = (large_data.len() + PAGE_SIZE - 1) / PAGE_SIZE;
    assert_eq!(
        page_ids.len(),
        expected_pages,
        "Should have used the correct number of pages"
    );

    // Clean up
    drop(buffer_pool);
    drop(page_store);
}

#[test]
fn test_edge_case_page_sizes() {
    let test_dir = get_test_dir();
    let file_path = test_dir.join("test.db");
    let file_path_str = file_path.to_str().unwrap();

    // Adjust test cases to account for page header size
    let test_cases = vec![
        PAGE_SIZE - 128,                        // Safely under page size
        PAGE_SIZE - 64,                         // Just under page size
        PAGE_SIZE - 32,                         // Very close to page size
        PAGE_SIZE - PAGE_HEADER_SIZE,           // Exact page size
        PAGE_SIZE + 1,                          // Just over page size
        PAGE_SIZE * 2 - 128,                    // Under two pages
        PAGE_SIZE * 2 - (PAGE_HEADER_SIZE * 2), // Exactly two pages
    ];

    for (index, size) in test_cases.iter().enumerate() {
        let test_data = vec![42u8; *size];
        let page_store = PageStore::new(file_path_str).unwrap();
        let mut buffer_pool = BufferPool::new(10, DatabaseType::Hybrid);

        let page_ids = test_write_and_verify(test_data.clone(), &page_store, &mut buffer_pool);

        let expected_pages = (*size + PAGE_SIZE - 1) / PAGE_SIZE;
        assert_eq!(
            page_ids.len(),
            expected_pages,
            "Case {}: Size {} bytes, got {} pages, expected {} pages",
            index,
            size,
            page_ids.len(),
            expected_pages
        );

        // Additional debugging for page utilization
        println!(
            "Case {}: Size {} bytes, using {} pages, last page utilization: {}%",
            index,
            size,
            page_ids.len(),
            (size % PAGE_SIZE) as f64 / PAGE_SIZE as f64 * 100.0
        );

        drop(buffer_pool);
        drop(page_store);
    }
}
