use crate::{
    raft::{Request, Response},
    Result,
};

use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
}

impl Client {
    pub fn new(
        request_tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response>>)>,
    ) -> Self {
        Self { request_tx }
    }

    pub async fn request(&self, request: Request) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx.send((request, response_tx))?;
        response_rx.await?
    }
}
