mod memory;

pub use memory::Memory;

use crate::Result;

pub trait Store: Send + Sync {
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>>;

    fn insert(&mut self, index: u64, entry: Vec<u8>) -> Result<()>;

    fn scan(&self, range: Option<(u64, u64)>) -> LogIterator;

    fn remove(&mut self, index: u64) -> Result<()>;

    fn clear(&mut self) -> Result<()>;

    fn len(&self) -> usize;

    fn truncate(&mut self, index: u64) -> Result<()>;
    fn truncate_before(&mut self, index: u64) -> Result<()>;
}

pub type LogIterator<'a> = Box<dyn DoubleEndedIterator<Item = Result<Vec<u8>>> + 'a>;
