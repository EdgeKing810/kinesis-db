use std::io::Result;

use super::page::Page;

pub trait DiskManager {
    fn read_page(&self, page_id: u64) -> Result<Page>;
    fn write_page(&self, page: &Page) -> Result<()>;
    fn allocate_page(&self) -> Result<u64>;
}
