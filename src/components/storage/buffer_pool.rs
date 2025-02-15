use std::{
    collections::HashMap,
    io::Result,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use crate::components::database::db_type::DatabaseType;

use super::{buffer_frame::BufferFrame, disk_manager::DiskManager};

pub struct BufferPool {
    frames: HashMap<u64, Arc<BufferFrame>>, // Map of page id to buffer frame
    max_size: usize,                        // Maximum number of frames in the pool
    db_type: DatabaseType,                  // Type of database
}

impl BufferPool {
    pub fn new(max_size: usize, db_type: DatabaseType) -> Self {
        BufferPool {
            frames: HashMap::new(),
            max_size,
            db_type,
        }
    }

    pub fn get_page(&mut self, page_id: u64, disk_manager: &impl DiskManager) -> Arc<BufferFrame> {
        // Check if the page is already in the buffer pool
        if let Some(frame) = self.frames.get(&page_id) {
            let frame = frame.clone();
            let mut count = frame.pin_count.lock().unwrap();
            *count += 1;
            *frame.last_used.lock().unwrap() = Instant::now();
            return frame.clone();
        }

        // Evict a page if the buffer pool is full
        if self.frames.len() >= self.max_size {
            self.evict_page_lru(disk_manager);
        }

        // Read the page from disk and add it to the buffer pool
        let page = disk_manager.read_page(page_id).unwrap();
        let frame = Arc::new(BufferFrame {
            page: RwLock::new(page),
            dirty: Mutex::new(false),
            pin_count: Mutex::new(1),
            last_used: Mutex::new(Instant::now()),
        });

        self.frames.insert(page_id, frame.clone());
        frame
    }

    pub fn evict_page_lru(&mut self, disk_manager: &impl DiskManager) {
        // Adjust eviction thresholds
        let threshold = match self.db_type {
            DatabaseType::InMemory => self.max_size as f64 * 2.0,
            DatabaseType::Hybrid => self.max_size as f64 * 1.2, // More aggressive eviction
            DatabaseType::OnDisk => self.max_size as f64,
        };

        if (self.frames.len() as f64) < threshold {
            return;
        }

        // Try to evict up to 10% of pages at once for better performance
        let target_evictions = (self.max_size as f64 * 0.1) as usize;
        let mut evicted = 0;

        let frames = self.frames.clone();

        let candidates: Vec<_> = frames
            .iter()
            .filter(|(_, frame)| frame.get_pin_count() == 0)
            .collect();

        for (page_id, frame) in candidates {
            if evicted >= target_evictions {
                break;
            }

            if frame.is_dirty() {
                let page = frame.read_page();
                if disk_manager.write_page(&page).is_ok() {
                    self.frames.remove(page_id);
                    evicted += 1;
                }
            } else {
                self.frames.remove(page_id);
                evicted += 1;
            }
        }
    }

    pub fn unpin_page(&mut self, page_id: u64, is_dirty: bool) {
        // Decrement the pin count for the page and mark it as dirty if needed
        if let Some(frame) = self.frames.get(&page_id) {
            let mut count = frame.pin_count.lock().unwrap();
            if *count > 0 {
                *count -= 1;
            }
            if is_dirty {
                frame.set_dirty(true);
            }
        }
    }

    pub fn flush_all(&mut self, disk_manager: &impl DiskManager) {
        // Process frames directly without collecting them first
        for (_, frame) in self.frames.iter() {
            // First check if dirty without holding the lock long
            let is_dirty = frame.is_dirty();
            if !is_dirty {
                continue;
            }

            // Quick scope for page data access
            let page_data = {
                if let Ok(page) = frame.page.try_read() {
                    Some(page.clone())
                } else {
                    None
                }
            };

            // Write the page data if we got it
            if let Some(page) = page_data {
                if let Ok(()) = disk_manager.write_page(&page) {
                    frame.set_dirty(false);
                }
            }
        }
    }

    pub fn flush_page(&mut self, page_id: u64, disk: &impl DiskManager) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            if frame.is_dirty() {
                let page = frame.read_page();
                disk.write_page(&page)?;
                frame.set_dirty(false);
            }
        }
        Ok(())
    }
}
