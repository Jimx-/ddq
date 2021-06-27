use crate::{Error, Result};

use super::{encoding, KvIterator, Range, Store};

use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::HashSet,
    iter::Peekable,
    ops::{Bound, RangeBounds},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

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

    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id)
    }
}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| Error::Internal(e.to_string()))
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    bincode::deserialize(bytes).map_err(|e| Error::Internal(e.to_string()))
}

pub struct Transaction {
    store: Arc<RwLock<Box<dyn Store>>>,
    id: u64,
    snapshot: Snapshot,
}

impl Transaction {
    fn begin(store: Arc<RwLock<Box<dyn Store>>>) -> Result<Self> {
        let (id, snapshot) = {
            let mut guard = store.write().unwrap();

            let id = match guard.get(&Key::TxnNext.encode())? {
                Some(ref v) => deserialize(v)?,
                None => 1,
            };
            guard.put(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
            guard.put(&Key::TxnActive(id).encode(), vec![])?;

            let snapshot = Snapshot::take(&mut guard, id)?;

            (id, snapshot)
        };

        Ok(Self {
            store,
            id,
            snapshot,
        })
    }

    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let snapshot = {
            let guard = store.read().unwrap();
            match guard.get(&Key::TxnActive(id).encode())? {
                None => {
                    return Err(Error::InvalidArgument(format!(
                        "No active transaction {}",
                        id
                    )))
                }
                _ => {}
            }
            Snapshot::restore(&guard, id)?
        };
        Ok(Self {
            store,
            id,
            snapshot,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn commit(self) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.delete(&Key::TxnActive(self.id).encode())?;
        store.delete(&Key::TxnSnapshot(self.id).encode())?;
        store.flush()
    }

    pub fn rollback(self) -> Result<()> {
        let mut guard = self.store.write().unwrap();
        let keys = {
            let mut keys = Vec::new();
            let mut iter = guard.scan(Range::from(
                Key::TxnUpdate(self.id, vec![].into()).encode()
                    ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = iter.next().transpose()? {
                match Key::decode(&key)? {
                    Key::TxnUpdate(_, key) => keys.push(key.into_owned()),
                    k => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", k))),
                };
                keys.push(key);
            }
            keys
        };
        for key in keys.into_iter() {
            guard.delete(&key)?;
        }

        guard.delete(&Key::TxnActive(self.id).encode())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self.store.read().unwrap();

        let mut iter = guard
            .scan(Range::from(
                Key::Record(key.into(), 0).encode()..=Key::Record(key.into(), self.id).encode(),
            ))
            .rev();
        while let Some((key, val)) = iter.next().transpose()? {
            match Key::decode(&key)? {
                Key::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&val);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            };
        }
        Ok(None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        let mut guard = self.store.write().unwrap();

        {
            let min = self
                .snapshot
                .active_txns
                .iter()
                .min()
                .cloned()
                .unwrap_or(self.id + 1);
            let mut iter = guard
                .scan(Range::from(
                    Key::Record(key.into(), min).encode()
                        ..=Key::Record(key.into(), std::u64::MAX).encode(),
                ))
                .rev();
            while let Some((key, _)) = iter.next().transpose()? {
                match Key::decode(&key)? {
                    Key::Record(_, version) => {
                        if !self.snapshot.is_visible(version) {
                            return Err(Error::WriteConflict);
                        }
                    }
                    k => return Err(Error::Internal(format!("Expected Record, got {:?}", k))),
                };
            }
        }

        guard.put(
            &Key::Record(key.into(), self.id).encode(),
            serialize(&value)?,
        )?;
        guard.put(&Key::TxnUpdate(self.id, key.into()).encode(), vec![])?;
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<KvIterator> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read().unwrap().scan(Range::from((start, end)));
        Ok(Box::new(MVCCIterator::new(scan, self.snapshot.clone())))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<KvIterator> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }
}

#[derive(Clone)]
struct Snapshot {
    version: u64,
    active_txns: HashSet<u64>,
}

impl Snapshot {
    fn take(store: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let snapshot = {
            let mut snapshot = Self {
                version,
                active_txns: HashSet::new(),
            };
            let mut iter = store.scan(Range::from(
                Key::TxnActive(0).encode()..Key::TxnActive(version).encode(),
            ));
            while let Some((key, _)) = iter.next().transpose()? {
                match Key::decode(&key)? {
                    Key::TxnActive(id) => snapshot.active_txns.insert(id),
                    k => return Err(Error::Internal(format!("Expected TxnActive, got {:?}", k))),
                };
            }
            snapshot
        };

        store.put(
            &Key::TxnSnapshot(version).encode(),
            serialize(&snapshot.active_txns)?,
        )?;
        Ok(snapshot)
    }

    fn restore(store: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match store.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref v) => Ok(Self {
                version,
                active_txns: deserialize(v)?,
            }),
            None => Err(Error::InvalidArgument(format!(
                "No snapshot for version {}",
                version
            ))),
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        version <= self.version && self.active_txns.get(&version).is_none()
    }
}

#[derive(Debug)]
enum Key<'a> {
    TxnNext,
    TxnActive(u64),
    TxnSnapshot(u64),
    TxnUpdate(u64, Cow<'a, [u8]>),
    Record(Cow<'a, [u8]>, u64),
}

impl<'a> Key<'a> {
    fn encode(self) -> Vec<u8> {
        use encoding::*;

        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => return Err(Error::Internal(format!("Unexpected key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal(
                "Unexpected data at the end of key".to_owned(),
            ));
        }
        Ok(key)
    }
}

pub struct MVCCIterator {
    inner: Peekable<KvIterator>,
    next_back_key: Option<Vec<u8>>,
}

impl MVCCIterator {
    fn new(inner: KvIterator, snapshot: Snapshot) -> Self {
        let inner: KvIterator = Box::new(inner.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(key, version) => {
                    if !snapshot.is_visible(version) {
                        Ok(None)
                    } else {
                        Ok(Some((key.into_owned(), v)))
                    }
                }
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self {
            inner: inner.peekable(),
            next_back_key: None,
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if match self.inner.peek() {
                Some(Ok((next_key, _))) if *next_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                _ => true,
            } {
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            if match &self.next_back_key {
                Some(next_key) if *next_key != key => true,
                Some(_) => false,
                _ => true,
            } {
                self.next_back_key = Some(key.clone());
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for MVCCIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for MVCCIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::BTreeStore;

    fn new_mvcc() -> MVCC {
        MVCC::new(Box::new(BTreeStore::new()))
    }

    #[test]
    fn test_begin() {
        let mvcc = new_mvcc();

        let txn = mvcc.begin().unwrap();
        assert_eq!(txn.id(), 1);
        txn.commit().unwrap();

        let txn = mvcc.begin().unwrap();
        assert_eq!(txn.id(), 2);
        txn.rollback().unwrap();
    }
}
