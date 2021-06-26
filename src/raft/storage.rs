use crate::{
    raft::{RaftRequest, RaftResponse, State},
    Error, NodeId,
};

use async_raft::{
    async_trait::async_trait,
    raft::{Entry, EntryPayload, MembershipConfig},
    storage::{CurrentSnapshotData, HardState, InitialState},
    RaftStorage,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Cursor};
use tokio::sync::RwLock;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub membership: MembershipConfig,
    pub data: Vec<u8>,
}

pub struct Storage {
    id: NodeId,
    log: RwLock<BTreeMap<u64, Entry<RaftRequest>>>,
    state: RwLock<Box<dyn State>>,
    hs: RwLock<Option<HardState>>,
    current_snapshot: RwLock<Option<Snapshot>>,
}

impl Storage {
    pub fn new(id: u64, state: Box<dyn State>) -> Self {
        let log = RwLock::new(BTreeMap::new());
        let state = RwLock::new(state);
        let hs = RwLock::new(None);
        let current_snapshot = RwLock::new(None);
        Self {
            id,
            log,
            state,
            hs,
            current_snapshot,
        }
    }
}

#[async_trait]
impl RaftStorage<RaftRequest, RaftResponse> for Storage {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = Error;

    async fn get_membership_config(&self) -> anyhow::Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    async fn get_initial_state(&self) -> anyhow::Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let state = self.state.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = state.applied_index();
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    async fn save_hard_state(&self, hs: &HardState) -> anyhow::Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    async fn get_log_entries(
        &self,
        start: u64,
        stop: u64,
    ) -> anyhow::Result<Vec<Entry<RaftRequest>>> {
        if start > stop {
            log::error!("Invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> anyhow::Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            log::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        log.split_off(&start);
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<RaftRequest>) -> anyhow::Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<RaftRequest>]) -> anyhow::Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &RaftRequest,
    ) -> anyhow::Result<RaftResponse> {
        let mut state = self.state.write().await;
        let response = state.mutate(*index, data.clone().into());
        response.map(RaftResponse::from).map_err(|e| e.into())
    }

    async fn replicate_to_state_machine(
        &self,
        entries: &[(&u64, &RaftRequest)],
    ) -> anyhow::Result<()> {
        let mut state = self.state.write().await;
        for (index, data) in entries {
            let data = data.clone();
            state.mutate(**index, data.clone().into())?;
        }
        Ok(())
    }

    async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            let state = self.state.read().await;
            data = state.get_snapshot()?;
            last_applied_log = state.applied_index();
        }

        let membership_config;
        {
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        }

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| Error::InconsistentLog)?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = Snapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = bincode::serialize(&snapshot)?;
            *current_snapshot = Some(snapshot);

            Ok(CurrentSnapshotData {
                term,
                index: last_applied_log,
                membership: membership_config.clone(),
                snapshot: Box::new(Cursor::new(snapshot_bytes)),
            })
        }
    }

    async fn create_snapshot(&self) -> anyhow::Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new()))))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> anyhow::Result<()> {
        let new_snapshot: Snapshot = bincode::deserialize(snapshot.get_ref().as_slice())?;

        {
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        {
            let mut state = self.state.write().await;
            state.apply_snapshot(&new_snapshot.data)?;
        }

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &self,
    ) -> anyhow::Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = bincode::serialize(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
