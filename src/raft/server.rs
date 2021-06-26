use crate::{
    raft::{Address, Event, Message, Node, Request, Response, RpcRequest, RpcResponse},
    Error, NodeId, Result,
};

use futures::SinkExt;
use log::{debug, error};
use std::{collections::HashMap, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_stream::{
    wrappers::{TcpListenerStream, UnboundedReceiverStream},
    StreamExt,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

pub struct Server {
    id: NodeId,
    peers: HashMap<NodeId, String>,
    node: Node,
    node_rx: mpsc::UnboundedReceiver<Message>,
    rpc_rx: mpsc::UnboundedReceiver<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
}

impl Server {
    pub async fn new(id: NodeId, peers: HashMap<NodeId, String>) -> Result<Self> {
        let (rpc_tx, rpc_rx) =
            mpsc::unbounded_channel::<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>();

        let (node_tx, node_rx) = mpsc::unbounded_channel::<Message>();
        Ok(Self {
            id,
            node: Node::new(id, peers.keys().cloned().collect(), rpc_tx, node_tx).await?,
            peers,
            node_rx: node_rx,
            rpc_rx: rpc_rx,
        })
    }

    pub async fn serve(
        self,
        listener: TcpListener,
        client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Result<()> {
        let (in_tx, in_rx) = mpsc::unbounded_channel::<Message>();
        let (out_tx, out_rx) = mpsc::unbounded_channel::<Message>();

        tokio::try_join!(
            Self::tcp_receive(listener, in_tx),
            Self::tcp_send(self.id, self.peers.clone(), out_rx),
            Self::eventloop(
                self.node,
                self.node_rx,
                client_rx,
                self.rpc_rx,
                in_rx,
                out_tx
            ),
        )?;
        Ok(())
    }

    async fn eventloop(
        node: Node,
        node_rx: mpsc::UnboundedReceiver<Message>,
        client_rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response>>)>,
        rpc_rx: mpsc::UnboundedReceiver<(NodeId, RpcRequest, oneshot::Sender<Result<RpcResponse>>)>,
        in_rx: mpsc::UnboundedReceiver<Message>,
        out_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<()> {
        let mut node_rx = UnboundedReceiverStream::new(node_rx);
        let mut in_rx = UnboundedReceiverStream::new(in_rx);
        let mut client_rx = UnboundedReceiverStream::new(client_rx);
        let mut rpc_rx = UnboundedReceiverStream::new(rpc_rx);
        let mut requests = HashMap::<Vec<u8>, oneshot::Sender<Result<Response>>>::new();
        let mut rpc_requests =
            HashMap::<(NodeId, Vec<u8>), oneshot::Sender<Result<RpcResponse>>>::new();

        node.initialize().await?;

        loop {
            tokio::select! {
                Some(msg) = in_rx.next() => {
                    match msg {
                        Message { from: Address::Peer(src), event: Event::RpcResponse { id, response }, .. } => {
                            if let Some(response_tx) = rpc_requests.remove(&(src, id)) {
                                response_tx.send(response).map_err(|e| Error::Internal(format!("Failed to send RPC response {:?}", e)))?;
                            }
                        }

                        msg => node.step(msg).await?,
                    }
                }

                Some(msg) = node_rx.next()=> {
                    match msg {
                        Message{to: Address::Peer(_), ..} => out_tx.send(msg)?,
                        Message{to: Address::Client, event: Event::ClientResponse{id, response}, ..} => {
                            if let Some(response_tx) = requests.remove(&id) {
                                response_tx.send(response).map_err(|e| Error::Internal(format!("Failed to send response {:?}", e)))?;
                            }
                        },
                        _ => {},
                    }
                }

                Some((target, request, response_tx)) = rpc_rx.next() => {
                    let id = Uuid::new_v4().as_bytes().to_vec();
                    rpc_requests.insert((target, id.clone()), response_tx);
                    out_tx.send(Message {
                        from: Address::Local,
                        to: Address::Peer(target),
                        event: Event::RpcRequest{id ,request}
                    })?;
                }

                Some((request, response_tx)) = client_rx.next() => {
                    let id = Uuid::new_v4().as_bytes().to_vec();
                    requests.insert(id.clone(), response_tx);
                    node.step(Message {
                        from: Address::Client,
                        to: Address::Local,
                        event: Event::ClientRequest{id, request},
                    }).await?;
                }
            }
        }
    }

    async fn tcp_receive(
        listener: TcpListener,
        in_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<()> {
        let mut listener = TcpListenerStream::new(listener);
        while let Some(socket) = listener.try_next().await? {
            let peer = socket.peer_addr()?;
            let peer_in_tx = in_tx.clone();
            tokio::spawn(async move {
                match Self::tcp_receive_peer(socket, peer_in_tx).await {
                    Ok(()) => debug!("Raft peer {} disconnected", peer),
                    Err(err) => error!("Raft peer {} error: {}", peer, err.to_string()),
                };
            });
        }
        Ok(())
    }

    async fn tcp_receive_peer(
        socket: TcpStream,
        in_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = stream.try_next().await? {
            in_tx.send(message)?;
        }
        Ok(())
    }

    async fn tcp_send(
        id: NodeId,
        peers: HashMap<NodeId, String>,
        out_rx: mpsc::UnboundedReceiver<Message>,
    ) -> Result<()> {
        let mut out_rx = UnboundedReceiverStream::new(out_rx);
        let mut peer_txs: HashMap<NodeId, mpsc::UnboundedSender<Message>> = HashMap::new();

        for (id, addr) in peers.into_iter() {
            let (tx, rx) = mpsc::unbounded_channel::<Message>();
            peer_txs.insert(id, tx);
            tokio::spawn(Self::tcp_send_peer(addr, rx));
        }

        while let Some(mut msg) = out_rx.next().await {
            if msg.from == Address::Local {
                msg.from = Address::Peer(id);
            }

            let peer = match msg.to {
                Address::Peer(peer) => peer,
                _ => continue,
            };

            match peer_txs.get_mut(&peer) {
                Some(tx) => tx.send(msg)?,
                _ => error!("Unknown peer {} for send message", peer),
            }
        }

        Ok(())
    }

    async fn tcp_send_peer(addr: String, rx: mpsc::UnboundedReceiver<Message>) -> Result<()> {
        let mut rx = UnboundedReceiverStream::new(rx);
        loop {
            match TcpStream::connect(&addr).await {
                Ok(socket) => match Self::tcp_send_peer_session(socket, &mut rx).await {
                    Ok(()) => break,
                    Err(e) => error!("Failed to send to peer {}: {}", addr, e),
                },
                Err(e) => error!("Failed to connect to peer {}: {}", addr, e),
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        Ok(())
    }

    async fn tcp_send_peer_session(
        socket: TcpStream,
        rx: &mut UnboundedReceiverStream<Message>,
    ) -> Result<()> {
        let mut stream = tokio_serde::SymmetricallyFramed::<_, Message, _>::new(
            Framed::new(socket, LengthDelimitedCodec::new()),
            tokio_serde::formats::SymmetricalBincode::<Message>::default(),
        );
        while let Some(message) = rx.next().await {
            stream.send(message).await?;
        }
        Ok(())
    }
}
