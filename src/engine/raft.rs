use crate::{raft, Result};

pub struct Raft {}

impl Raft {
    pub fn new_state() -> Result<State> {
        State::new()
    }
}

pub struct State {}

impl State {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

impl raft::State for State {
    fn applied_index(&self) -> u64 {
        0
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
