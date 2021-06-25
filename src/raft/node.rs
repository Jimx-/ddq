use crate::{
    raft::{Address, Event, MemRaft, Message, Router, RpcRequest, RpcResponse},
    Error, NodeId, Result,
};

use async_raft::Config;
use memstore::MemStore;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub struct Node {
    peers: Vec<u64>,
    raft: MemRaft,
    node_tx: mpsc::UnboundedSender<Message>,
}

impl Node {
    pub async fn new(
        id: u64,
        peers: Vec<u64>,
        rpc_tx: mpsc::UnboundedSender<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let config = Arc::new(Config::build("ddq".into()).validate().unwrap());
        let router = Arc::new(Router::new(rpc_tx));
        let storage = Arc::new(MemStore::new(id));
        let raft = MemRaft::new(id, config, router, storage);

        Ok(Self {
            peers,
            raft,
            node_tx,
        })
    }

    pub async fn initialize(&self) -> Result<()> {
        self.raft
            .initialize(self.peers.iter().cloned().collect())
            .await
            .map_err(|_| Error::Internal("Failed to initialize Raft".into()))
    }

    fn send(&self, to: Address, event: Event) -> Result<()> {
        let msg = Message {
            from: Address::Local,
            to,
            event,
        };
        Ok(self.node_tx.send(msg)?)
    }

    pub async fn step(&self, msg: Message) -> Result<()> {
        match msg.event {
            Event::RpcRequest { id, request } => {
                let response = self.handle_rpc(request).await?;
                self.send(msg.from, Event::RpcResponse { id, response })?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_rpc(&self, request: RpcRequest) -> Result<RpcResponse> {
        Ok(match request {
            RpcRequest::Vote(req) => RpcResponse::Vote(self.raft.vote(req).await?),
            RpcRequest::AppendEntries(req) => {
                RpcResponse::AppendEntries(self.raft.append_entries(req).await?)
            }
            RpcRequest::InstallSnapshot(req) => {
                RpcResponse::InstallSnapshot(self.raft.install_snapshot(req).await?)
            }
        })
    }
}
