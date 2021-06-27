use crate::{
    engine, raft,
    storage::{kv, log},
    Error, NodeId, Result,
};

use ::log::{error, info};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_stream::{wrappers::TcpListenerStream, StreamExt};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Raft(raft::Request),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Raft(raft::Response),
}

struct Session {
    client: raft::Client,
}

pub struct Server {
    raft: raft::Server,
    req_listener: Option<TcpListener>,
    raft_listener: Option<TcpListener>,
}

impl Session {
    pub fn new(
        request_tx: mpsc::UnboundedSender<(raft::Request, oneshot::Sender<Result<raft::Response>>)>,
    ) -> Result<Self> {
        Ok(Self {
            client: raft::Client::new(request_tx),
        })
    }

    async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut stream = tokio_serde::Framed::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::Bincode::default(),
        );

        while let Some(request) = stream.try_next().await? {
            let response = self.request(request).await;
            stream.send(response).await?;
        }
        Ok(())
    }

    async fn request(&mut self, request: Request) -> Result<Response> {
        Ok(match request {
            Request::Raft(raft) => Response::Raft(self.client.request(raft).await?),
        })
    }
}

impl Server {
    pub async fn new(
        id: NodeId,
        peers: HashMap<NodeId, String>,
        log: Box<dyn log::Store>,
        kv: Box<dyn kv::Store>,
    ) -> Result<Self> {
        Ok(Self {
            raft: raft::Server::new(
                id,
                peers,
                log,
                Box::new(engine::kv::Raft::new_state(kv::MVCC::new(kv))?),
            )
            .await?,
            req_listener: None,
            raft_listener: None,
        })
    }

    pub async fn listen(mut self, req_addr: &str, raft_addr: &str) -> Result<Self> {
        let req_listener = TcpListener::bind(req_addr).await?;
        let raft_listener = TcpListener::bind(raft_addr).await?;
        self.req_listener = Some(req_listener);
        self.raft_listener = Some(raft_listener);
        Ok(self)
    }

    pub async fn serve(self) -> Result<()> {
        let req_listener = self
            .req_listener
            .ok_or_else(|| Error::InvalidState("Must listen before serving".into()))?;
        let raft_listener = self
            .raft_listener
            .ok_or_else(|| Error::InvalidState("Must listen before serving".into()))?;
        let (raft_tx, raft_rx) = mpsc::unbounded_channel();

        tokio::try_join!(
            self.raft.serve(raft_listener, raft_rx),
            Self::serve_req(req_listener, raft_tx),
        )?;
        Ok(())
    }

    async fn serve_req(
        listener: TcpListener,
        raft_tx: mpsc::UnboundedSender<(raft::Request, oneshot::Sender<Result<raft::Response>>)>,
    ) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let session = Session::new(raft_tx.clone())?;
            tokio::spawn(async move {
                match session.handle(socket).await {
                    Ok(()) => info!("Client {} disconnected", peer),
                    Err(err) => error!("Client {} error: {}", peer, err),
                }
            });
        }
        Ok(())
    }
}
