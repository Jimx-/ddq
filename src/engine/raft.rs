use crate::{raft, storage::kv, Result};

pub struct Raft {}

impl Raft {
    pub fn new_state(store: kv::MVCC) -> Result<State> {
        State::new(store)
    }
}

pub struct State {
    applied_index: u64,
}

impl State {
    pub fn new(store: kv::MVCC) -> Result<Self> {
        Ok(Self { applied_index: 0 })
    }
}

impl raft::State for State {
    fn applied_index(&self) -> u64 {
        self.applied_index
    }

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn get_snapshot(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn apply_snapshot(&mut self, snapshot: &Vec<u8>) -> Result<()> {
        Ok(())
    }
}
