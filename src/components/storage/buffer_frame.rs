use std::{
    sync::{Mutex, RwLock},
    time::Instant,
};

use super::page::Page;

#[derive(Debug)]
pub struct BufferFrame {
    pub page: RwLock<Page>,        // Page stored in the buffer frame
    pub dirty: Mutex<bool>,        // Flag to indicate if the page is dirty
    pub pin_count: Mutex<u32>,     // Number of times the page is pinned
    pub last_used: Mutex<Instant>, // Last time the page was accessed
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
