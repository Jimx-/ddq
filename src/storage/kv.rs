mod btree;
mod encoding;
mod mvcc;

pub use btree::BTreeStore;
pub use mvcc::MVCC;

use crate::Result;

use std::ops::{Bound, RangeBounds};

pub trait Store: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    fn scan(&self, range: Range) -> KvIterator;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn flush(&mut self) -> Result<()>;
}

pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl Range {
    pub fn from(range: impl RangeBounds<Vec<u8>>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_owned()),
                Bound::Excluded(v) => Bound::Excluded(v.to_owned()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_owned()),
                Bound::Excluded(v) => Bound::Excluded(v.to_owned()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}

impl RangeBounds<Vec<u8>> for Range {
    fn start_bound(&self) -> Bound<&Vec<u8>> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Vec<u8>> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

pub type KvIterator = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;
