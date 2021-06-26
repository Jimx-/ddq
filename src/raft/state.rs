use crate::Result;

pub trait State: Send + Sync {
    fn applied_index(&self) -> u64;

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;

    fn get_snapshot(&self) -> Result<Vec<u8>>;

    fn apply_snapshot(&mut self, snapshot: &Vec<u8>) -> Result<()>;
}
