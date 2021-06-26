use crate::{
    engine,
    raft::{
        Address, Event, Message, RaftNode, Request, Response, Router, RpcRequest, RpcResponse,
        Storage,
    },
    Error, NodeId, Result,
};

use async_raft::Config;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamExt};

pub struct Node {
    id: u64,
    peers: Vec<u64>,
    raft: RaftNode,
    node_tx: mpsc::UnboundedSender<Message>,
    queue_tx: mpsc::UnboundedSender<(Address, Event)>,
}

impl Node {
    pub async fn new(
        id: u64,
        peers: Vec<u64>,
        rpc_tx: mpsc::UnboundedSender<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let config = Arc::new(Config::build("ddq".into()).validate().unwrap());
        let heartbeat_interval = config.heartbeat_interval;
        let router = Arc::new(Router::new(rpc_tx));
        let state = Box::new(engine::Raft::new_state()?);
        let storage = Arc::new(Storage::new(id, state));
        let raft = RaftNode::new(id, config, router, storage);

        let (queue_tx, queue_rx) = mpsc::unbounded_channel();
        tokio::spawn(Self::forward_request(
            id,
            raft.clone(),
            heartbeat_interval,
            node_tx.clone(),
            queue_rx,
        ));

        Ok(Self {
            id,
            peers,
            raft,
            node_tx,
            queue_tx,
        })
    }

    async fn forward_request(
        id: u64,
        raft: RaftNode,
        ticks: u64,
        node_tx: mpsc::UnboundedSender<Message>,
        queue_rx: mpsc::UnboundedReceiver<(Address, Event)>,
    ) -> Result<()> {
        let mut queue_rx = UnboundedReceiverStream::new(queue_rx);
        let mut ticker = tokio::time::interval(Duration::from_millis(ticks));

        let mut queued_reqs = Vec::<(Address, Event)>::new();
        let mut proxied_reqs = HashMap::<Vec<u8>, Address>::new();
        let mut last_leader = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let current_leader = raft.current_leader().await;

                    if last_leader != current_leader {
                        /* Abort proxied requests in case of new leader. */
                        for(id, addr) in std::mem::replace(&mut proxied_reqs, HashMap::new()) {
                            Self::send(&node_tx, addr, Event::ClientResponse { id, response: Err(Error::RequestAborted) })?;
                        }
                    }

                    if let Some(leader) = current_leader {
                        if leader == id {
                            for (from, event) in std::mem::replace(&mut queued_reqs, Vec::new()) {
                                if let Event::ClientRequest { id, request } = event {
                                    let response =
                                        Self::handle_client_request(&raft, request).await;
                                    Self::send(
                                        &node_tx,
                                        from,
                                        Event::ClientResponse {
                                            id,
                                            response: response,
                                        },
                                    )?;
                                }
                            }
                        } else {
                            /* Forward queued requests to the new leader. */
                            if !queued_reqs.is_empty() {
                                for (from, event) in std::mem::replace(&mut queued_reqs, Vec::new()) {
                                    if let Event::ClientRequest { id, .. } = &event {
                                        proxied_reqs.insert(id.clone(), from.clone());
                                        node_tx.send(Message {
                                            from: match from {
                                                Address::Client => Address::Local,
                                                address => address,
                                            },
                                            to: Address::Peer(leader),
                                            event,
                                        })?;
                                    }
                                }

                                last_leader = current_leader;
                            }
                        }
                    }
                }

                Some((addr, event)) = queue_rx.next() => {
                    match event {
                        Event::ClientRequest { .. } => queued_reqs.push((addr, event)),
                        Event::ClientResponse { id, response } => {
                            proxied_reqs.remove(&id);
                            Self::send(&node_tx, Address::Client, Event::ClientResponse { id, response })?;
                        },
                        _ => {}
                    }
                }
            }
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        self.raft
            .initialize(self.peers.iter().cloned().collect())
            .await
            .map_err(|_| Error::Internal("Failed to initialize Raft".into()))
    }

    fn send(node_tx: &mpsc::UnboundedSender<Message>, to: Address, event: Event) -> Result<()> {
        let msg = Message {
            from: Address::Local,
            to,
            event,
        };
        Ok(node_tx.send(msg)?)
    }

    pub async fn step(&self, msg: Message) -> Result<()> {
        match msg.event {
            Event::RpcRequest { id, request } => match self.handle_rpc(request).await {
                Err(err @ Error::Internal(_)) => return Err(err),
                response => {
                    Self::send(&self.node_tx, msg.from, Event::RpcResponse { id, response })?;
                }
            },

            Event::ClientRequest { id, request } => {
                if Some(self.id) == self.raft.current_leader().await {
                    let response = Self::handle_client_request(&self.raft, request).await;
                    Self::send(
                        &self.node_tx,
                        msg.from,
                        Event::ClientResponse {
                            id,
                            response: response,
                        },
                    )?;
                } else {
                    self.queue_tx
                        .send((msg.from, Event::ClientRequest { id, request }))?;
                }
            }

            Event::ClientResponse { id, response } => {
                self.queue_tx
                    .send((Address::Client, Event::ClientResponse { id, response }))?;
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

    async fn handle_client_request(raft: &RaftNode, request: Request) -> Result<Response> {
        log::info!("Process client request {:?}", request);
        Err(Error::OutOfMemory)
    }
}
