use crate::Result;

use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    AppData, AppDataResponse,
};
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
        response: Result<RpcResponse>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftRequest(Vec<u8>);

impl AppData for RaftRequest {}
impl From<Vec<u8>> for RaftRequest {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}
impl Into<Vec<u8>> for RaftRequest {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftResponse(Vec<u8>);

impl AppDataResponse for RaftResponse {}
impl From<Vec<u8>> for RaftResponse {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}
impl Into<Vec<u8>> for RaftResponse {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    Mutate(RaftRequest),
    Query(RaftRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    State(RaftResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRequest {
    AppendEntries(AppendEntriesRequest<RaftRequest>),
    Vote(VoteRequest),
    InstallSnapshot(InstallSnapshotRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(AppendEntriesResponse),
    Vote(VoteResponse),
    InstallSnapshot(InstallSnapshotResponse),
}
