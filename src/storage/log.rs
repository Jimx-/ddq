mod memory;

pub use memory::Memory;

use crate::Result;

use std::ops::{Bound, RangeBounds};

pub trait Store: Send + Sync {
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>>;

    fn insert(&mut self, index: u64, entry: Vec<u8>) -> Result<()>;

    fn scan(&self, range: Range) -> LogIterator;

    fn remove(&mut self, index: u64) -> Result<()>;

    fn clear(&mut self) -> Result<()>;

    fn len(&self) -> usize;

    fn truncate(&mut self, index: u64) -> Result<()>;
    fn truncate_before(&mut self, index: u64) -> Result<()>;

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}

pub struct Range {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl Range {
    pub fn from(range: impl RangeBounds<u64>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

impl RangeBounds<u64> for Range {
    fn start_bound(&self) -> Bound<&u64> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&u64> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

pub type LogIterator<'a> = Box<dyn DoubleEndedIterator<Item = Result<Vec<u8>>> + 'a>;
