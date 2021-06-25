mod client;
mod result;
mod server;

pub mod raft;

pub use self::{
    client::Client,
    result::{Error, Result},
    server::{Request, Response, Server},
};

pub type NodeId = async_raft::NodeId;
