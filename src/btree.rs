use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};
use memmap2::{MmapMut, MmapOptions};
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicU64, Ordering};

const ORDER: usize = 4; // B+Tree order
const NODE_SIZE: usize = 4096; // Node size for mmap optimization
const LOG_FILE: &str = "btree_log.dat"; // WAL log file

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum NodeType {
    Internal,
    Leaf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BTreeNode {
    node_type: NodeType,
    keys: Vec<u64>,
    children: Vec<u64>,
    records: Option<HashMap<u64, String>>, 
    next_leaf: Option<u64>,
}

impl BTreeNode {
    fn new(node_type: NodeType) -> Self {
        let is_leaf = node_type == NodeType::Leaf;

        BTreeNode {
            node_type,
            keys: Vec::new(),
            children: Vec::new(),
            records: if is_leaf { Some(HashMap::new()) } else { None },
            next_leaf: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTree {
    root_offset: Arc<AtomicU64>,
    file: Arc<RwLock<File>>, 
    mmap: Arc<RwLock<MmapMut>>, 
    log_file: Arc<RwLock<File>>,
}

impl BTree {
    pub fn new(filename: &str) -> Self {
        let path = Path::new(filename);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Failed to open file");
        file.set_len(1024 * 1024 * 10).unwrap(); 
        let mmap = unsafe { MmapOptions::new().len(1024 * 1024 * 10).map_mut(&file).unwrap() };
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(LOG_FILE)
            .expect("Failed to open WAL log file");
        
        // let root = BTreeNode::new(NodeType::Leaf);
        let root_offset = Arc::new(AtomicU64::new(0));
        
        let mut tree = BTree {
            root_offset: root_offset.clone(),
            file: Arc::new(RwLock::new(file)),
            mmap: Arc::new(RwLock::new(mmap)),
            log_file: Arc::new(RwLock::new(log_file)),
        };
        
        tree.replay_log();
        // tree.write_node(0, &root);
        tree
    }

    fn create_new_root(&mut self, key: u64, left_offset: u64, right_offset: u64) {
        let mut new_root = BTreeNode::new(NodeType::Internal);
        new_root.keys.push(key);
        new_root.children.push(left_offset);
        new_root.children.push(right_offset);

        let new_root_offset = self.root_offset.fetch_add(NODE_SIZE as u64, Ordering::SeqCst);
        self.write_node(new_root_offset, &new_root);
        self.root_offset.store(new_root_offset, Ordering::SeqCst);
    }

    fn read_node(&self, offset: u64) -> BTreeNode {
        let mmap = self.mmap.read().unwrap();
        let slice = &mmap[offset as usize..offset as usize + NODE_SIZE];
        bincode::deserialize(slice).unwrap()
    }

    fn write_node(&mut self, offset: u64, node: &BTreeNode) {
        let bytes = bincode::serialize(node).unwrap();
        let mut mmap = self.mmap.write().unwrap();
        let slice = &mut mmap[offset as usize..offset as usize + bytes.len()];
        slice.copy_from_slice(&bytes);
        mmap.flush().unwrap(); 
        self.write_node_to_disk(offset, node);
    }

    fn read_node_from_disk(&self, offset: u64) -> BTreeNode {
        let mut file = self.file.write().unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buffer = vec![0u8; NODE_SIZE];
        file.read_exact(&mut buffer).unwrap();
        bincode::deserialize(&buffer).unwrap()
    }

    fn write_node_to_disk(&self, offset: u64, node: &BTreeNode) {
        let mut file = self.file.write().unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let bytes = bincode::serialize(node).unwrap();
        file.write_all(&bytes).unwrap();
        file.flush().unwrap();
    }
    
    pub fn insert(&mut self, key: u64, value: String) {
        self.write_log("INSERT", key, Some(&value));
        self.insert_recursive(self.root_offset.load(Ordering::SeqCst), key, value);
    }
    
    fn insert_recursive(&mut self, offset: u64, key: u64, value: String) -> Option<(u64, u64)> {
        let mut node = self.read_node(offset);
        
        if node.node_type == NodeType::Leaf {
            node.records.as_mut().unwrap().insert(key, value);
            node.keys.push(key);
            node.keys.sort();
    
            if node.keys.len() > ORDER {
                let (new_offset, promoted_key) = self.split_leaf(offset, &mut node);
                if offset == self.root_offset.load(Ordering::SeqCst) {
                    self.create_new_root(promoted_key, offset, new_offset);
                }
                return Some((new_offset, promoted_key));
            }
    
            self.write_node(offset, &node);
            return None;
        } else {
            let child_index = node.keys.iter().position(|&k| key < k).unwrap_or(node.keys.len());
            let child_offset = node.children[child_index];
    
            if let Some((new_offset, promoted_key)) = self.insert_recursive(child_offset, key, value) {
                node.keys.insert(child_index, promoted_key);
                node.children.insert(child_index + 1, new_offset);
    
                if node.keys.len() > ORDER {
                    let (new_offset, promoted_key) = self.split_internal(offset, &mut node);
                    if offset == self.root_offset.load(Ordering::SeqCst) {
                        self.create_new_root(promoted_key, offset, new_offset);
                    }
                    return Some((new_offset, promoted_key));
                }
            }
    
            self.write_node(offset, &node);
        }
    
        None
    }

    pub fn search(&self, key: u64) -> Option<String> {
        let mut offset = self.root_offset.load(Ordering::SeqCst);
        while let Some(node) = Some(self.read_node(offset)) {
            if node.node_type == NodeType::Leaf {
                if let Some(value) = node.records.as_ref()?.get(&key) {
                    return Some(value.clone());
                }
                if let Some(next_offset) = node.next_leaf {
                    offset = next_offset;
                    continue;
                }
                return None;
            }
            let child_index = node.keys.iter().position(|&k| key < k).unwrap_or(node.keys.len());
            offset = node.children[child_index];
        }
        None
    }

    fn split_leaf(&mut self, offset: u64, node: &mut BTreeNode) -> (u64, u64) {
        let mid = node.keys.len() / 2;
        let promoted_key = node.keys[mid];
        
        let mut new_node = BTreeNode::new(NodeType::Leaf);
        new_node.keys = node.keys.split_off(mid);
        new_node.records = Some(node.records.as_ref().unwrap().iter()
        .filter(|(k, _)| new_node.keys.contains(k))
        .map(|(k, v)| (*k, v.clone()))
        .collect());
        new_node.next_leaf = node.next_leaf;
    
        let new_offset = self.root_offset.fetch_add(NODE_SIZE as u64, Ordering::SeqCst);
        node.next_leaf = Some(new_offset);
    
        self.write_node(offset, node);
        self.write_node(new_offset, &new_node);
    
        (new_offset, promoted_key)
    }

    fn split_internal(&mut self, offset: u64, node: &mut BTreeNode) -> (u64, u64) {
        let mid = node.keys.len() / 2;
        let promoted_key = node.keys[mid];
    
        let mut new_node = BTreeNode::new(NodeType::Internal);
        new_node.keys = node.keys.split_off(mid + 1);
        new_node.children = node.children.split_off(mid + 1);
    
        let new_offset = self.root_offset.fetch_add(NODE_SIZE as u64, Ordering::SeqCst);
        self.write_node(new_offset, &new_node);
    
        (new_offset, promoted_key)
    }
     
    pub fn delete(&mut self, key: u64) {
        self.write_log("DELETE", key, None);
        self.delete_recursive(self.root_offset.load(Ordering::SeqCst), key);
    }
    
    fn delete_recursive(&mut self, offset: u64, key: u64) {
        let mut node = self.read_node(offset);
        
        if node.node_type == NodeType::Leaf {
            if node.records.as_mut().unwrap().remove(&key).is_some() {
                node.keys.retain(|&k| k != key);
                if node.keys.is_empty() {
                    self.handle_underflow(offset);
                } else {
                    self.write_node(offset, &node);
                }
            }
            return;
        }

        let child_index = node.keys.iter().position(|&k| key < k).unwrap_or(node.keys.len());
        let child_offset = node.children[child_index];
        self.delete_recursive(child_offset, key);

        let child_node = self.read_node(child_offset);
        if child_node.keys.len() < ORDER / 2 {
            self.handle_underflow(child_offset);
        }
    }

    fn handle_underflow(&mut self, offset: u64) {
        let mut node = self.read_node(offset);
        
        if node.node_type == NodeType::Leaf {
            if let Some(next_offset) = node.next_leaf {
                let mut next_node = self.read_node(next_offset);
                if next_node.keys.len() > ORDER / 2 {
                    node.keys.push(next_node.keys.remove(0));
                    if let Some((first_key, first_value)) = next_node.records.as_mut().unwrap().drain().next() {
                        node.records.as_mut().unwrap().insert(first_key, first_value);
                    }
                    self.write_node(offset, &node);
                    self.write_node(next_offset, &next_node);
                } else {
                    node.keys.extend(next_node.keys);
                    node.records.as_mut().unwrap().extend(next_node.records.unwrap());
                    node.next_leaf = next_node.next_leaf;
                    self.write_node(offset, &node);
                }
            }
        } else {
            let parent_offset = self.find_parent_offset(offset);
            if let Some(parent_offset) = parent_offset {
                let mut parent = self.read_node(parent_offset);
                let index = parent.children.iter().position(|&child| child == offset).unwrap();
                
                if index > 0 {
                    let sibling_offset = parent.children[index - 1];
                    let mut sibling = self.read_node(sibling_offset);
                    
                    if sibling.keys.len() > ORDER / 2 {
                        node.keys.insert(0, parent.keys[index - 1]);
                        parent.keys[index - 1] = sibling.keys.pop().unwrap();
                        self.write_node(parent_offset, &parent);
                        self.write_node(offset, &node);
                        self.write_node(sibling_offset, &sibling);
                    } else {
                        sibling.keys.extend(node.keys);
                        sibling.children.extend(node.children);
                        parent.keys.remove(index - 1);
                        parent.children.remove(index);
                        self.write_node(parent_offset, &parent);
                        self.write_node(sibling_offset, &sibling);
                    }
                }
            }
        }
    }

    fn find_parent_offset(&self, child_offset: u64) -> Option<u64> {
        let mut offset = self.root_offset.load(Ordering::SeqCst);
        while let Some(node) = Some(self.read_node(offset)) {
            if node.children.contains(&child_offset) {
                return Some(offset);
            }
            let child_index = node.keys.iter().position(|&k| child_offset < k).unwrap_or(node.keys.len());
            offset = node.children[child_index];
        }
        None
    }

    fn replay_log(&mut self) {
        let log_entries: Vec<String> = {
            let log_file = self.log_file.read().unwrap();
            let reader = BufReader::new(&*log_file);
            reader.lines().flatten().collect()
        };
        
        for line in log_entries {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() < 2 {
                continue;
            }
            let action = parts[0];
            let key: u64 = parts[1].parse().unwrap();
            
            match action {
                "INSERT" if parts.len() == 3 => {
                    let value = parts[2].to_string();
                    self.insert(key, value);
                }
                "DELETE" => {
                    self.delete(key);
                }
                _ => {}
            }
        }
    }
    
    fn write_log(&self, action: &str, key: u64, value: Option<&String>) {
        let mut log_file = self.log_file.write().unwrap();
        let log_entry = match value {
            Some(v) => format!("{}:{}:{}\n", action, key, v),
            None => format!("{}:{}\n", action, key),
        };
        log_file.write_all(log_entry.as_bytes()).unwrap();
        log_file.flush().unwrap();
    }
}
