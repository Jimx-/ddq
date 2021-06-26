use ddq::{storage, Client, Error, NodeId, Result, Server};

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
    id: String,
    peers: HashMap<NodeId, String>,
    listen_req: String,
    listen_raft: String,
    log_store: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let mut c = config::Config::new();
        c.set_default("id", "ddq")?;
        c.set_default("peers", HashMap::<String, String>::new())?;
        c.set_default("listen_req", "0.0.0.0:9605")?;
        c.set_default("listen_raft", "0.0.0.0:9705")?;
        c.set_default("log_store", "memory")?;
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

    // let server = Server::new(&cfg.id, cfg.peers)
    //     .await?
    //     .listen(&cfg.listen_req, &cfg.listen_raft)
    //     .await?;

    // tokio::try_join!(server.serve(), client.raft_mutate(vec![]))?;

    let server1 = {
        let mut peers = HashMap::new();
        peers.insert(1, "0.0.0.0:8081".to_owned());
        peers.insert(2, "0.0.0.0:8082".to_owned());

        Server::new(0, peers.clone(), cfg.get_log_store()?)
            .await?
            .listen("0.0.0.0:8000", "0.0.0.0:8080")
            .await?
    };

    let server2 = {
        let mut peers = HashMap::new();
        peers.insert(0, "0.0.0.0:8080".to_owned());
        peers.insert(2, "0.0.0.0:8082".to_owned());
        Server::new(1, peers.clone(), cfg.get_log_store()?)
            .await?
            .listen("0.0.0.0:7001", "0.0.0.0:8081")
            .await?
    };

    let server3 = {
        let mut peers = HashMap::new();
        peers.insert(0, "0.0.0.0:8080".to_owned());
        peers.insert(1, "0.0.0.0:8081".to_owned());
        Server::new(2, peers.clone(), cfg.get_log_store()?)
            .await?
            .listen("0.0.0.0:7002", "0.0.0.0:8082")
            .await?
    };

    let client = Client::new("127.0.0.1:8000").await?;

    tokio::try_join!(
        server1.serve(),
        server2.serve(),
        server3.serve(),
        client.raft_mutate(vec![]),
    )?;

    Ok(())
}
