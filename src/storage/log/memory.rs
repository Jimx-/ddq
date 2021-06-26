use crate::Result;

use super::{LogIterator, Store};

use std::collections::BTreeMap;

pub struct Memory {
    log: BTreeMap<u64, Vec<u8>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            log: BTreeMap::new(),
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

    fn scan(&self, range: Option<(u64, u64)>) -> LogIterator {
        match range {
            Some((start, stop)) => {
                Box::new(self.log.range(start..stop).map(|(_, v)| v).cloned().map(Ok))
            }
            None => Box::new(self.log.values().cloned().map(Ok)),
        }
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
}
