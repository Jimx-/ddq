use crate::{raft, storage::kv, Client, Error, Result};

use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

#[derive(Clone, Serialize, Deserialize)]
enum Mutation {
    Begin,
    Commit(u64),
    Rollback(u64),

    Put(u64, Vec<u8>, Vec<u8>),
    Delete(u64, Vec<u8>),
}

#[derive(Clone, Serialize, Deserialize)]
enum Query {
    Get(u64, Vec<u8>),

    Scan(u64, kv::Range),
    ScanPrefix(u64, Vec<u8>),
}

#[derive(Clone)]
pub struct Raft {
    client: Client,
}

impl Raft {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn new_state(store: kv::MVCC) -> Result<State> {
        State::new(store)
    }

    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        bincode::serialize(value).map_err(|e| Error::Internal(e.to_string()))
    }

    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        bincode::deserialize(bytes).map_err(|e| Error::Internal(e.to_string()))
    }

    pub async fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.client.clone()).await
    }

    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.client.clone(), id)
    }
}

#[derive(Clone)]
pub struct Transaction {
    client: Client,
    id: u64,
}

impl Transaction {
    async fn begin(client: Client) -> Result<Self> {
        let id = Raft::deserialize(
            &client
                .raft_mutate(Raft::serialize(&Mutation::Begin)?)
                .await?,
        )?;
        Ok(Self { client, id })
    }

    fn resume(client: Client, id: u64) -> Result<Self> {
        Ok(Self { client, id })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    async fn mutate(&self, mutation: Mutation) -> Result<Vec<u8>> {
        self.client.raft_mutate(Raft::serialize(&mutation)?).await
    }

    async fn query(&self, query: Query) -> Result<Vec<u8>> {
        self.client.raft_query(Raft::serialize(&query)?).await
    }

    pub async fn commit(self) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Commit(self.id)).await?)
    }

    pub async fn rollback(self) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Rollback(self.id)).await?)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Raft::deserialize(&self.query(Query::Get(self.id, key.into())).await?)
    }

    pub async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        Raft::deserialize(
            &self
                .mutate(Mutation::Put(self.id, key.into(), value))
                .await?,
        )
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        Raft::deserialize(&self.mutate(Mutation::Delete(self.id, key.into())).await?)
    }

    pub async fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<kv::KvIterator> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(
                &self
                    .query(Query::Scan(self.id, kv::Range::from(range)))
                    .await?,
            )?
            .into_iter()
            .map(Ok),
        ))
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<kv::KvIterator> {
        Ok(Box::new(
            Raft::deserialize::<Vec<_>>(
                &self
                    .query(Query::ScanPrefix(self.id, prefix.into()))
                    .await?,
            )?
            .into_iter()
            .map(Ok),
        ))
    }
}

pub struct State {
    store: kv::MVCC,
    applied_index: u64,
}

impl State {
    pub fn new(store: kv::MVCC) -> Result<Self> {
        Ok(Self {
            store,
            applied_index: 0,
        })
    }

    fn apply(&mut self, mutation: Mutation) -> Result<Vec<u8>> {
        match mutation {
            Mutation::Begin => Raft::serialize(&self.store.begin()?.id()),
            Mutation::Commit(tid) => Raft::serialize(&self.store.resume(tid)?.commit()?),
            Mutation::Rollback(tid) => Raft::serialize(&self.store.resume(tid)?.rollback()?),

            Mutation::Put(tid, key, val) => {
                Raft::serialize(&self.store.resume(tid)?.put(&key, val)?)
            }
            Mutation::Delete(tid, key) => Raft::serialize(&self.store.resume(tid)?.delete(&key)?),
        }
    }
}

impl raft::State for State {
    fn applied_index(&self) -> u64 {
        self.applied_index
    }

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.apply(Raft::deserialize(&command)?) {
            err @ Err(Error::Internal(_)) => err,
            result => {
                self.applied_index = index;
                result
            }
        }
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match Raft::deserialize(&command)? {
            Query::Get(tid, key) => Raft::serialize(&self.store.resume(tid)?.get(&key)?),
            Query::Scan(tid, range) => Raft::serialize(
                &self
                    .store
                    .resume(tid)?
                    .scan(range)?
                    .collect::<Result<Vec<_>>>()?,
            ),
            Query::ScanPrefix(tid, prefix) => Raft::serialize(
                &self
                    .store
                    .resume(tid)?
                    .scan_prefix(&prefix)?
                    .collect::<Result<Vec<_>>>()?,
            ),
        }
    }

    fn get_snapshot(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn apply_snapshot(&mut self, _snapshot: &Vec<u8>) -> Result<()> {
        Ok(())
    }
}
