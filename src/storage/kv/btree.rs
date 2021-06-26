use crate::Result;

use super::{KvIterator, Range, Store};

use std::collections::BTreeMap;

pub struct BTreeStore {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BTreeStore {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }
}

impl Store for BTreeStore {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.data.insert(key.to_owned(), value);
        Ok(())
    }

    fn scan(&self, range: Range) -> KvIterator {
        Box::new(
            self.data
                .range(range)
                .map(|(k, v)| Ok((k.clone(), v.clone())))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
