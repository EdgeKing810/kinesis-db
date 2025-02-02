use crate::buffer::DiskManager;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

pub const PAGE_SIZE: usize = 16384; // 16KB pages
const PAGE_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub struct Page {
    pub id: u64,
    pub data: Vec<u8>,
    pub dirty: bool,
}

impl Page {
    pub fn new(id: u64) -> Self {
        Page {
            id,
            data: vec![0; PAGE_SIZE - PAGE_HEADER_SIZE],
            dirty: true,
        }
    }

    pub fn write_data(&mut self, offset: usize, data: &[u8]) -> io::Result<()> {
        if offset + data.len() > PAGE_SIZE - PAGE_HEADER_SIZE {
            // Instead of failing, split the data into multiple pages
            self.data.clear();
            let chunk_size = PAGE_SIZE - PAGE_HEADER_SIZE;
            let first_chunk = &data[..chunk_size.min(data.len())];
            self.data.extend_from_slice(first_chunk);
        } else {
            self.data.clear();
            self.data.extend_from_slice(data);
        }
        self.dirty = true;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PageStore {
    file: Arc<Mutex<File>>,
    page_count: Arc<Mutex<u64>>,
    free_pages: Arc<Mutex<Vec<u64>>>,
}

impl PageStore {
    pub fn new(path: &str) -> io::Result<Self> {
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
    fn read_page(&self, page_id: u64) -> io::Result<Page> {
        let mut page = Page::new(page_id);
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        let mut buffer = vec![0; PAGE_SIZE - PAGE_HEADER_SIZE];
        file.read_exact(&mut buffer)?;
        page.data = buffer;
        Ok(page)
    }

    fn write_page(&self, page: &Page) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(page.id * PAGE_SIZE as u64))?;
        file.write_all(&page.data)?;
        file.sync_data()?;
        Ok(())
    }

    fn allocate_page(&self) -> io::Result<u64> {
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
