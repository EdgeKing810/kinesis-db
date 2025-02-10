use std::io::Result;

pub const PAGE_SIZE: usize = 16384; // 16 KB pages
pub const PAGE_HEADER_SIZE: usize = 16; // 16 bytes for page header

#[derive(Debug)]
pub struct Page {
    pub id: u64,       // Page ID
    pub data: Vec<u8>, // Page data (excluding header) as a byte vector
    pub dirty: bool,   // Dirty flag to track if the page has been modified
}

impl Page {
    /// Creates a new page with a specified ID.
    ///
    /// # Arguments
    ///
    /// * `id` - A unique identifier for the page
    ///
    /// # Returns
    ///
    /// Returns a new `Page` instance initialized with:
    /// * The specified ID
    /// * A zero-filled data vector of size `PAGE_SIZE - PAGE_HEADER_SIZE`
    /// * Dirty flag set to true indicating the page needs to be written to disk
    pub fn new(id: u64) -> Self {
        Page {
            id,
            data: vec![0; PAGE_SIZE - PAGE_HEADER_SIZE],
            dirty: true,
        }
    }

    /// Writes data to the page, replacing existing content.
    ///
    /// # Arguments
    ///
    /// * `offset` - The position in the page (currently unused)
    /// * `data` - The byte slice containing the data to be written
    ///
    /// # Returns
    ///
    /// Returns `io::Result<Option<Vec<u8>>>`:
    /// * `Ok(Some(vec))` - Contains remaining data that didn't fit in the page
    /// * `Ok(None)` - All data was written successfully
    ///
    /// # Behavior
    ///
    /// If the data exceeds the available page size:
    /// 1. Clears existing page data
    /// 2. Writes first PAGE_SIZE - PAGE_HEADER_SIZE bytes
    /// 3. Returns remaining data as Vec<u8>
    ///
    /// If the data fits in the page:
    /// 1. Clears existing page data
    /// 2. Writes entire data slice
    /// 3. Returns None
    ///
    /// In both cases, marks the page as dirty
    pub fn write_data(&mut self, offset: usize, data: &[u8]) -> Result<Option<Vec<u8>>> {
        if offset + data.len() > PAGE_SIZE - PAGE_HEADER_SIZE {
            self.data.clear();
            let chunk_size = PAGE_SIZE - PAGE_HEADER_SIZE;
            let first_chunk = &data[..chunk_size];
            self.data.extend_from_slice(first_chunk);
            self.dirty = true;

            // Return remaining data
            Ok(Some(data[chunk_size..].to_vec()))
        } else {
            self.data.clear();
            self.data.extend_from_slice(data);
            self.dirty = true;
            Ok(None)
        }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Page {
            id: self.id,
            data: self.data.clone(),
            dirty: self.dirty,
        }
    }
}
