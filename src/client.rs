use crate::{raft, Error, Request, Response, Result};

use futures::{sink::SinkExt, stream::TryStreamExt};
use std::sync::Arc;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type Connection = tokio_serde::Framed<
    Framed<TcpStream, LengthDelimitedCodec>,
    Result<Response>,
    Request,
    tokio_serde::formats::Bincode<Result<Response>, Request>,
>;

#[derive(Clone)]
pub struct Client {
    conn: Arc<Mutex<Connection>>,
}

impl Client {
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(tokio_serde::Framed::new(
                Framed::new(TcpStream::connect(addr).await?, LengthDelimitedCodec::new()),
                tokio_serde::formats::Bincode::default(),
            ))),
        })
    }

    async fn call_locked(
        &self,
        conn: &mut MutexGuard<'_, Connection>,
        request: Request,
    ) -> Result<Response> {
        conn.send(request).await?;
        match conn.try_next().await? {
            Some(res) => res,
            None => Err(Error::Internal("Server disconnected".into())),
        }
    }

    async fn raft_request(&self, request: raft::Request) -> Result<raft::Response> {
        let mut guard = self.conn.lock().await;
        match self.call_locked(&mut guard, Request::Raft(request)).await? {
            Response::Raft(res) => Ok(res),
            // resp => Err(Error::Internal(format!("Unexpected response: {:?}", resp))),
        }
    }

    pub async fn raft_mutate(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.raft_request(raft::Request::Mutate(command)).await? {
            raft::Response::State(resp) => Ok(resp),
            // resp => Err(Error::Internal(format!(
            //     "Unexpected Raft mutate response: {:?}",
            //     resp
            // ))),
        }
    }

    pub async fn raft_query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
        match self.raft_request(raft::Request::Query(command)).await? {
            raft::Response::State(resp) => Ok(resp),
            // resp => Err(Error::Internal(format!(
            //     "Unexpected Raft query response: {:?}",
            //     resp
            // ))),
        }
    }
}
