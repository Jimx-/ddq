use crate::{Error, Result};

use super::kv;

use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    sync::{Arc, Mutex},
};

const ROW_ID_PREALLOC_COUNT: usize = 32768;
const OID_PREALLOC_COUNT: usize = 8192;
const NORMAL_OID_START: u64 = 16384;

#[derive(Clone)]
pub struct Client {
    client: kv::Raft,
    oid_alloc: Arc<Mutex<Option<IdAlloc>>>,
}

impl Client {
    pub fn new(client: crate::Client) -> Self {
        let client = kv::Raft::new(client);
        let oid_alloc = Arc::new(Mutex::new(None));
        Self { client, oid_alloc }
    }

    fn alloc_id(&self, key: Key, count: usize, def: Option<u64>) -> Result<IdAlloc> {
        loop {
            let txn = self.client.begin()?;

            match {
                let k = key.clone().encode();
                let cur = match txn.get(&k)? {
                    Some(v) => deserialize(&v)?,
                    _ => {
                        if let Some(def) = def {
                            def
                        } else {
                            return Err(Error::InvalidArgument(format!(
                                "ID allocator {:?} not created",
                                k
                            )));
                        }
                    }
                };

                txn.put(&k, serialize(&(cur + count as u64))?)?;

                Ok(IdAlloc::new(cur, count))
            } {
                Err(Error::WriteConflict) => {
                    txn.rollback()?;
                    continue;
                }
                err @ Err(_) => {
                    txn.rollback()?;
                    return err;
                }
                res => {
                    txn.commit()?;
                    return res;
                }
            }
        }
    }

    pub fn get_next_oid(&self) -> Result<u64> {
        let mut guard = self.oid_alloc.lock().unwrap();

        match guard.as_mut().map(|alloc| alloc.next()) {
            Some(Some(id)) => Ok(id),
            _ => {
                *guard = Some(self.alloc_id(
                    Key::OidAlloc,
                    OID_PREALLOC_COUNT,
                    Some(NORMAL_OID_START),
                )?);
                Ok(guard.as_mut().unwrap().next().unwrap())
            }
        }
    }

    pub fn begin(&self) -> Result<Transaction> {
        Ok(Transaction::new(self.client.begin()?))
    }

    fn with_txn<F, R>(&self, f: F) -> Result<R>
    where
        F: Fn(&Transaction) -> Result<R>,
    {
        let txn = self.begin()?;
        match f(&txn) {
            err @ Err(_) => {
                txn.rollback()?;
                err
            }
            res => {
                txn.commit()?;
                res
            }
        }
    }

    pub fn create_table(&self, id: u64) -> Result<Table> {
        self.with_txn(|txn| txn.create_table(self.clone(), id))
    }

    pub fn open_table(&self, id: u64) -> Result<Option<Table>> {
        self.with_txn(|txn| txn.open_table(self.clone(), id))
    }

    pub fn create_index(&self, id: u64) -> Result<Index> {
        self.with_txn(|txn| txn.create_index(id))
    }

    pub fn open_index(&self, id: u64) -> Result<Option<Index>> {
        self.with_txn(|txn| txn.open_index(id))
    }
}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| Error::Internal(e.to_string()))
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    bincode::deserialize(bytes).map_err(|e| Error::Internal(e.to_string()))
}

struct IdAlloc {
    cur: u64,
    max: u64,
}

impl IdAlloc {
    fn new(start: u64, count: usize) -> Self {
        Self {
            cur: start,
            max: start + count as u64,
        }
    }
}

impl Iterator for IdAlloc {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.max {
            None
        } else {
            let id = self.cur;
            self.cur += 1;
            Some(id)
        }
    }
}

pub struct Transaction {
    inner: kv::Transaction,
}

impl Transaction {
    fn new(inner: kv::Transaction) -> Self {
        Self { inner }
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.inner.rollback()
    }

    fn open_table(&self, client: Client, id: u64) -> Result<Option<Table>> {
        Ok(self
            .inner
            .get(&Key::Table(Some(id)).encode())?
            .map(|_| Table::new(client, id)))
    }

    fn create_table(&self, client: Client, id: u64) -> Result<Table> {
        if self.open_table(client.clone(), id)?.is_some() {
            return Err(Error::InvalidArgument(format!(
                "Table {} already exists",
                id
            )));
        }
        self.inner
            .put(&Key::Table(Some(id)).encode(), serialize(&1u64)?)?;
        Ok(Table::new(client, id))
    }

    pub fn insert_tuple(&self, table: &Table, tuple: &[u8]) -> Result<u64> {
        let row_id = table.get_next_row_id()?;
        self.inner
            .put(&Key::Row(table.id(), Some(row_id)).encode(), tuple.into())?;
        Ok(row_id)
    }

    pub fn fetch_tuple(&self, table_id: u64, row_id: u64) -> Result<Option<Vec<u8>>> {
        Ok(self
            .inner
            .get(&Key::Row(table_id, Some(row_id)).encode())?
            .map(|v| v.into()))
    }

    pub fn scan_table(&self, table: &Table) -> Result<TableScan> {
        Ok(Box::new(
            self.inner
                .scan_prefix(&Key::Row(table.id(), None).encode())?
                .map(|r| r.and_then(|(_, v)| Ok(v.into()))),
        ))
    }

    fn open_index(&self, id: u64) -> Result<Option<Index>> {
        Ok(self
            .inner
            .get(&Key::Index(Some(id)).encode())?
            .map(|_| Index::new(id)))
    }

    fn create_index(&self, id: u64) -> Result<Index> {
        if self.open_index(id)?.is_some() {
            return Err(Error::InvalidArgument(format!(
                "Index {} already exists",
                id
            )));
        }
        self.inner.put(&Key::Index(Some(id)).encode(), vec![])?;
        Ok(Index::new(id))
    }

    pub fn insert_index(&self, index: &Index, value: &[u8], row_id: u64) -> Result<()> {
        self.inner.put(
            &Key::IndexRow(index.id(), Some(value.into()), Some(row_id)).encode(),
            vec![],
        )
    }

    pub fn scan_index<'a>(&'a self, index: &Index, table: &Table) -> Result<IndexScan<'a>> {
        Ok(IndexScan::new(table.id(), index.id(), &self))
    }

    fn scan_index_raw(&self, index_id: u64) -> Result<RawIndexScan> {
        Ok(Box::new(
            self.inner
                .scan_prefix(&Key::IndexRow(index_id, None, None).encode())?
                .map(|r| {
                    r.and_then(|(k, _)| match Key::decode(&k)? {
                        Key::IndexRow(_, Some(value), Some(row_id)) => {
                            Ok((value.into_owned(), row_id))
                        }
                        k => {
                            return Err(Error::Internal(format!("Expected IndexRow, got {:?}", k)))
                        }
                    })
                }),
        ))
    }
}

pub type TableScan = Box<dyn DoubleEndedIterator<Item = Result<Vec<u8>>> + Send>;

pub struct Table {
    id: u64,
    client: Client,
    row_id_alloc: Mutex<Option<IdAlloc>>,
}

impl Table {
    pub fn new(client: Client, id: u64) -> Self {
        let row_id_alloc = Mutex::new(None);
        Self {
            id,
            client,
            row_id_alloc,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    fn table_key(&self) -> Key {
        Key::Table(Some(self.id))
    }

    fn get_next_row_id(&self) -> Result<u64> {
        let mut guard = self.row_id_alloc.lock().unwrap();

        match guard.as_mut().map(|alloc| alloc.next()) {
            Some(Some(id)) => Ok(id),
            _ => {
                *guard = Some(self.client.alloc_id(
                    self.table_key(),
                    ROW_ID_PREALLOC_COUNT,
                    None,
                )?);
                Ok(guard.as_mut().unwrap().next().unwrap())
            }
        }
    }
}

pub struct Index {
    id: u64,
}

impl Index {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

type RawIndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, u64)>> + Send>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

pub struct IndexScan<'a> {
    table_id: u64,
    index_id: u64,
    txn: &'a Transaction,
    inner: Option<Box<dyn DoubleEndedIterator<Item = Result<u64>> + 'a>>,
}

impl<'a> IndexScan<'a> {
    fn new(table_id: u64, index_id: u64, txn: &'a Transaction) -> Self {
        IndexScan {
            table_id,
            index_id,
            txn,
            inner: None,
        }
    }

    pub fn rescan(&mut self, predicate: IndexScanPredicate<'a>) -> Result<()> {
        self.inner = Some(Box::new(
            self.txn
                .scan_index_raw(self.index_id)?
                .filter_map(move |r| match r {
                    Ok((val, row_id)) => match predicate(&val) {
                        Ok(true) => Some(Ok(row_id)),
                        Ok(false) => None,
                        Err(err) => Some(Err(err)),
                    },
                    Err(err) => Some(Err(err)),
                }),
        ));
        Ok(())
    }

    pub fn next(&mut self, dir: ScanDirection) -> Result<Option<Vec<u8>>> {
        if let Some(inner) = &mut self.inner {
            match match dir {
                ScanDirection::Forward => inner.next().transpose(),
                ScanDirection::Backward => inner.next_back().transpose(),
            }? {
                Some(row_id) => {
                    if let Some(row) = self.txn.fetch_tuple(self.table_id, row_id)? {
                        Ok(Some(row))
                    } else {
                        Err(Error::Internal(format!(
                            "Row {} not found in table {}",
                            row_id, self.table_id
                        )))
                    }
                }
                _ => Ok(None),
            }
        } else {
            Err(Error::InvalidState("Next without calling rescan".into()))
        }
    }
}

#[derive(Clone, Debug)]
enum Key<'a> {
    OidAlloc,
    Table(Option<u64>),
    Index(Option<u64>),
    Row(u64, Option<u64>),
    IndexRow(u64, Option<Cow<'a, [u8]>>, Option<u64>),
}

impl<'a> Key<'a> {
    fn encode(self) -> Vec<u8> {
        use crate::storage::kv::encoding::*;
        match self {
            Self::OidAlloc => vec![0x01],

            Self::Table(None) => vec![0x02],
            Self::Table(Some(id)) => [&[0x02][..], &encode_u64(id)].concat(),

            Self::Index(None) => vec![0x03],
            Self::Index(Some(id)) => [&[0x03][..], &encode_u64(id)].concat(),

            Self::Row(table_id, None) => [&[0x04][..], &encode_u64(table_id)].concat(),
            Self::Row(table_id, Some(row_id)) => {
                [&[0x04][..], &encode_u64(table_id), &encode_u64(row_id)].concat()
            }

            Self::IndexRow(index_id, None, None) => [&[0x05][..], &encode_u64(index_id)].concat(),
            Self::IndexRow(index_id, Some(value), None) => {
                [&[0x05][..], &encode_u64(index_id), &encode_bytes(&value)].concat()
            }
            Self::IndexRow(index_id, value, Some(row_id)) => [
                &[0x05][..],
                &encode_u64(index_id),
                &encode_bytes(&value.unwrap()),
                &encode_u64(row_id),
            ]
            .concat(),
        }
    }

    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use crate::storage::kv::encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::OidAlloc,
            0x02 => Self::Table(Some(take_u64(bytes)?)),
            0x03 => Self::Index(Some(take_u64(bytes)?)),
            0x04 => Self::Row(take_u64(bytes)?, Some(take_u64(bytes)?)),
            0x05 => Self::IndexRow(
                take_u64(bytes)?,
                Some(take_bytes(bytes)?.into()),
                Some(take_u64(bytes)?),
            ),
            b => return Err(Error::Internal(format!("Unknown SQL key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal("Unexpected data at the end of key".into()));
        }
        Ok(key)
    }
}

pub struct IndexScanPredicate<'a>(Box<dyn Fn(&[u8]) -> Result<bool> + 'a>);

impl<'a> IndexScanPredicate<'a> {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&[u8]) -> Result<bool> + 'a,
    {
        Self(Box::new(f))
    }
}

impl<'a> std::ops::Deref for IndexScanPredicate<'a> {
    type Target = Box<dyn Fn(&[u8]) -> Result<bool> + 'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
