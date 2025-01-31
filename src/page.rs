use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};

pub const PAGE_SIZE: usize = 4096;  // 4KB pages
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
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Data too large"));
        }
        if offset + data.len() > self.data.len() {
            self.data.resize(offset + data.len(), 0);
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
        self.dirty = true;
        Ok(())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut page_data = Vec::with_capacity(PAGE_SIZE);
        // Write header
        page_data.extend_from_slice(&self.id.to_le_bytes());
        page_data.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        page_data.extend_from_slice(&[0; PAGE_HEADER_SIZE - 12]); // Padding
        // Write data
        page_data.extend_from_slice(&self.data);
        // Pad to page size
        page_data.resize(PAGE_SIZE, 0);
        page_data
    }

    pub fn from_bytes(id: u64, bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < PAGE_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid page data"));
        }
        let data_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let mut page = Page::new(id);
        if data_len > 0 {
            page.data = bytes[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_len].to_vec();
        }
        page.dirty = false;
        Ok(page)
    }
}

#[derive(Debug)]
pub struct PageStore {
    file: File,
    page_count: u64,
    free_pages: Vec<u64>,
}

impl PageStore {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        let page_count = (file.metadata()?.len() / PAGE_SIZE as u64).max(1);
        
        // Initialize the first page if the file is empty
        if file.metadata()?.len() == 0 {
            let mut page_store = PageStore {
                file,
                page_count,
                free_pages: Vec::new(),
            };
            let first_page = Page::new(0);
            page_store.write_page(&first_page)?;
            return Ok(page_store);
        }

        Ok(PageStore {
            file,
            page_count,
            free_pages: Vec::new(),
        })
    }

    pub fn read_page(&mut self, page_id: u64) -> io::Result<Page> {
        let mut buffer = vec![0; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.read_exact(&mut buffer)?;
        Page::from_bytes(page_id, &buffer)
    }

    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        let page_data = page.to_bytes();
        self.file.seek(SeekFrom::Start(page.id * PAGE_SIZE as u64))?;
        self.file.write_all(&page_data)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> io::Result<u64> {
        let page_id = if let Some(id) = self.free_pages.pop() {
            id
        } else {
            let id = self.page_count;
            self.page_count += 1;
            // Extend file if needed
            if (id + 1) * PAGE_SIZE as u64 > self.file.metadata()?.len() {
                self.file.set_len((id + 1) * PAGE_SIZE as u64)?;
            }
            id
        };

        // Initialize new page
        let page = Page::new(page_id);
        self.write_page(&page)?;
        
        Ok(page_id)
    }

    pub fn free_page(&mut self, page_id: u64) {
        self.free_pages.push(page_id);
    }
}
