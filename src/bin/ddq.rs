use ddq::{storage, Error, NodeId, Result, Server};

use clap::{AppSettings, Clap};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::Level;
use tracing_subscriber;

#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(long, default_value = "ddq.yaml")]
    config: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    id: NodeId,
    peers: HashMap<NodeId, String>,
    listen_req: String,
    listen_raft: String,
    log_store: String,
    kv_store: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let mut c = config::Config::new();
        c.set_default("id", "0")?;
        c.set_default("peers", HashMap::<String, String>::new())?;
        c.set_default("listen_req", "0.0.0.0:9605")?;
        c.set_default("listen_raft", "0.0.0.0:9705")?;
        c.set_default("log_store", "memory")?;
        c.set_default("kv_store", "btree")?;
        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("DDQ"))?;
        Ok(c.try_into()?)
    }

    fn get_log_store(&self) -> Result<Box<dyn storage::log::Store>> {
        match self.log_store.as_str() {
            "memory" => Ok(Box::new(storage::log::Memory::new())),
            _ => Err(Error::InvalidArgument(format!(
                "Unknown Raft log storage engine {}",
                self.log_store
            ))),
        }
    }

    fn get_kv_store(&self) -> Result<Box<dyn storage::kv::Store>> {
        match self.kv_store.as_str() {
            "btree" => Ok(Box::new(storage::kv::BTreeStore::new())),
            _ => Err(Error::InvalidArgument(format!(
                "Unknown KV storage engine {}",
                self.log_store
            ))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("no global subscriber has been set");

    let opts = Opts::parse();

    let cfg = Config::new(&opts.config)?;

    Server::new(
        cfg.id,
        cfg.peers.clone(),
        cfg.get_log_store()?,
        cfg.get_kv_store()?,
    )
    .await?
    .listen(&cfg.listen_req, &cfg.listen_raft)
    .await?
    .serve()
    .await
}
