use crate::Result;

use super::{LogIterator, Range, Store};

use std::collections::{BTreeMap, HashMap};

pub struct Memory {
    log: BTreeMap<u64, Vec<u8>>,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            log: BTreeMap::new(),
            metadata: HashMap::new(),
        }
    }
}

impl Store for Memory {
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        Ok(self.log.get(&index).cloned())
    }

    fn insert(&mut self, index: u64, entry: Vec<u8>) -> Result<()> {
        self.log.insert(index, entry);
        Ok(())
    }

    fn scan(&self, range: Range) -> LogIterator {
        Box::new(self.log.range(range).map(|(_, v)| v).cloned().map(Ok))
    }

    fn remove(&mut self, index: u64) -> Result<()> {
        self.log.remove(&index);
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.log.clear();
        Ok(())
    }

    fn len(&self) -> usize {
        self.log.len()
    }

    fn truncate(&mut self, index: u64) -> Result<()> {
        self.log.split_off(&index);
        Ok(())
    }

    fn truncate_before(&mut self, index: u64) -> Result<()> {
        self.log = self.log.split_off(&index);
        Ok(())
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key.to_owned(), value);
        Ok(())
    }
}
