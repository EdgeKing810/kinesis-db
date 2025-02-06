use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use super::page::Page;

#[derive(Debug)]
pub struct BufferFrame {
    pub page: RwLock<Page>, // Change to RwLock<Page>
    pub dirty: Mutex<bool>,
    pub pin_count: Mutex<u32>,
    pub last_used: Mutex<Instant>,
}

impl BufferFrame {
    pub fn read_page(&self) -> std::sync::RwLockReadGuard<'_, Page> {
        self.page.read().unwrap()
    }

    pub fn get_page(&self) -> &RwLock<Page> {
        &self.page
    }

    pub fn get_pin_count(&self) -> u32 {
        *self.pin_count.lock().unwrap()
    }

    pub fn is_dirty(&self) -> bool {
        *self.dirty.lock().unwrap()
    }

    pub fn set_dirty(&self, value: bool) {
        *self.dirty.lock().unwrap() = value;
    }
}

pub struct BufferPool {
    frames: HashMap<u64, Arc<BufferFrame>>,
    max_size: usize,
}

impl BufferPool {
    pub fn new(max_size: usize) -> Self {
        BufferPool {
            frames: HashMap::new(),
            max_size,
        }
    }

    pub fn get_page(&mut self, page_id: u64, disk_manager: &impl DiskManager) -> Arc<BufferFrame> {
        if let Some(frame) = self.frames.get(&page_id) {
            let frame = frame.clone();
            let mut count = frame.pin_count.lock().unwrap();
            *count += 1;
            *frame.last_used.lock().unwrap() = Instant::now();
            return frame.clone();
        }

        if self.frames.len() >= self.max_size {
            self.evict_page_lru(disk_manager);
        }

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

    fn evict_page_lru(&mut self, disk_manager: &impl DiskManager) {
        if let Some((page_id, frame)) = self.find_unpinned_page() {
            if frame.is_dirty() {
                let page = frame.read_page();
                let _ = disk_manager.write_page(&page);
            }
            self.frames.remove(&page_id);
        }
    }

    fn find_unpinned_page(&self) -> Option<(u64, Arc<BufferFrame>)> {
        self.frames
            .iter()
            .filter(|(_, frame)| frame.get_pin_count() == 0)
            .min_by_key(|(_, frame)| *frame.last_used.lock().unwrap())
            .map(|(&id, frame)| (id, frame.clone()))
    }

    pub fn unpin_page(&mut self, page_id: u64, is_dirty: bool) {
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
}

// Add Clone implementation for Page if not already present
impl Clone for Page {
    fn clone(&self) -> Self {
        Page {
            id: self.id,
            data: self.data.clone(),
            dirty: self.dirty,
        }
    }
}

pub trait DiskManager {
    fn read_page(&self, page_id: u64) -> std::io::Result<Page>;
    fn write_page(&self, page: &Page) -> std::io::Result<()>;
    fn allocate_page(&self) -> std::io::Result<u64>;
}
