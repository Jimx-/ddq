use crate::Result;

use async_raft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use memstore::ClientRequest as MemClientRequest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    Peer(u64),
    Local,
    Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub from: Address,
    pub to: Address,
    pub event: Event,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Event {
    ClientRequest {
        id: Vec<u8>,
        request: Request,
    },

    ClientResponse {
        id: Vec<u8>,
        response: Result<Response>,
    },

    RpcRequest {
        id: Vec<u8>,
        request: RpcRequest,
    },

    RpcResponse {
        id: Vec<u8>,
        response: RpcResponse,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Mutate(Vec<u8>),
    Query(Vec<u8>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    State(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRequest {
    AppendEntries(AppendEntriesRequest<MemClientRequest>),
    Vote(VoteRequest),
    InstallSnapshot(InstallSnapshotRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(AppendEntriesResponse),
    Vote(VoteResponse),
    InstallSnapshot(InstallSnapshotResponse),
}
