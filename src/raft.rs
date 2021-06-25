use async_raft::Raft;
use memstore::{ClientRequest as MemClientRequest, ClientResponse as MemClientResponse, MemStore};

mod client;
mod message;
mod node;
mod router;
mod server;

pub use client::Client;
pub use message::{Address, Event, Message, Request, Response, RpcRequest, RpcResponse};
pub use node::Node;
pub use router::Router;
pub use server::Server;

pub type MemRaft = Raft<MemClientRequest, MemClientResponse, Router, MemStore>;
