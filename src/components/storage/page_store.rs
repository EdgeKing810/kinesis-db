use std::{
    fs::{File, OpenOptions},
    io::{Read, Result, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};

use super::{
    disk_manager::DiskManager,
    page::{Page, PAGE_HEADER_SIZE, PAGE_SIZE},
};

#[derive(Debug, Clone)]
pub struct PageStore {
    file: Arc<Mutex<File>>,           // File handle for the page store
    page_count: Arc<Mutex<u64>>,      // Total number of pages in the store
    free_pages: Arc<Mutex<Vec<u64>>>, // List of free pages available for reuse
}

impl PageStore {
    pub fn new(path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let page_count = (file.metadata()?.len() / PAGE_SIZE as u64).max(1);

        let page_store = PageStore {
            file: Arc::new(Mutex::new(file)),
            page_count: Arc::new(Mutex::new(page_count)),
            free_pages: Arc::new(Mutex::new(Vec::new())),
        };

        // Initialize the first page if the file is empty
        if page_store.file.lock().unwrap().metadata()?.len() == 0 {
            let first_page = Page::new(0);
            page_store.write_page(&first_page)?;
        }

        Ok(page_store)
    }

    pub fn free_page(&self, page_id: u64) {
        self.free_pages.lock().unwrap().push(page_id);
    }
}

// Implement DiskManager trait for PageStore
impl DiskManager for PageStore {
    fn read_page(&self, page_id: u64) -> Result<Page> {
        let mut page = Page::new(page_id);
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        let mut buffer = vec![0; PAGE_SIZE - PAGE_HEADER_SIZE];
        file.read_exact(&mut buffer)?;
        page.data = buffer;
        Ok(page)
    }

    fn write_page(&self, page: &Page) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page.id * PAGE_SIZE as u64))?;
        file.write_all(&page.data)?;
        file.sync_data()?;
        Ok(())
    }

    fn allocate_page(&self) -> Result<u64> {
        // First try to reuse a freed page
        if let Some(page_id) = self.free_pages.lock().unwrap().pop() {
            return Ok(page_id);
        }

        // If no freed pages available, allocate a new one
        let mut page_count = self.page_count.lock().unwrap();
        let page_id = *page_count;
        *page_count += 1;

        // Extend the file if necessary
        let required_size = (page_id + 1) * PAGE_SIZE as u64;
        let mut file = self.file.lock().unwrap();
        let current_size = file.metadata()?.len();

        if required_size > current_size {
            file.seek(SeekFrom::Start(required_size - 1))?;
            file.write_all(&[0])?;
        }

        Ok(page_id)
    }
}
