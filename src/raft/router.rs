use crate::{
    raft::{RaftRequest, RpcRequest, RpcResponse},
    Error, Result,
};

use async_raft::{
    async_trait::async_trait,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    NodeId, RaftNetwork,
};
use tokio::sync::{mpsc, oneshot};

pub struct Router {
    rpc_tx: mpsc::UnboundedSender<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
}

impl Router {
    pub fn new(
        rpc_tx: mpsc::UnboundedSender<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
    ) -> Self {
        Self { rpc_tx }
    }

    async fn request(&self, target: NodeId, request: RpcRequest) -> Result<RpcResponse> {
        let (response_tx, response_rx) = oneshot::channel();
        self.rpc_tx.send((target, request, response_tx))?;
        response_rx.await?
    }
}

#[async_trait]
impl RaftNetwork<RaftRequest> for Router {
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<RaftRequest>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        match self.request(target, RpcRequest::AppendEntries(rpc)).await? {
            RpcResponse::AppendEntries(resp) => Ok(resp),
            resp => Err(Error::Internal(format!(
                "Unexpected response for append entries request: {:?}",
                resp
            ))
            .into()),
        }
    }

    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        match self
            .request(target, RpcRequest::InstallSnapshot(rpc))
            .await?
        {
            RpcResponse::InstallSnapshot(resp) => Ok(resp),
            resp => Err(Error::Internal(format!(
                "Unexpected response for install snapshot request: {:?}",
                resp
            ))
            .into()),
        }
    }

    async fn vote(&self, target: u64, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        match self.request(target, RpcRequest::Vote(rpc)).await? {
            RpcResponse::Vote(resp) => Ok(resp),
            resp => Err(Error::Internal(format!(
                "Unexpected response for vote request: {:?}",
                resp
            ))
            .into()),
        }
    }
}
