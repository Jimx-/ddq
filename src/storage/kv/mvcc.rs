use crate::Result;

use super::Store;

use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct MVCC {
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl MVCC {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self {
            store: Arc::new(RwLock::new(store)),
        }
    }

    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.store.clone())
    }
}

pub struct Transaction {
    store: Arc<RwLock<Box<dyn Store>>>,
    id: u64,
}

impl Transaction {
    fn begin(store: Arc<RwLock<Box<dyn Store>>>) -> Result<Self> {
        Ok(Self { store, id: 0 })
    }
}
