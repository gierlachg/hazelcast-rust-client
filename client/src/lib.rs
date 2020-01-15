#[macro_use]
extern crate hazelcast_rust_client_macros;

use std::{error::Error, sync::Arc};

use log::info;

pub use protocol::pn_counter::PnCounter;

use crate::remote::cluster::Cluster;

mod codec;
mod message;
mod protocol;
mod remote;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

trait TryFrom<T> {
    type Error;

    fn try_from(self) -> std::result::Result<T, Self::Error>;
}

pub struct HazelcastClient {
    cluster: Arc<Cluster>,
}

impl HazelcastClient {
    pub async fn new<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = &'a str>,
    {
        info!("HazelcastClient {} is STARTING", env!("CARGO_PKG_VERSION"));
        let cluster = Cluster::connect(endpoints, username, password).await?;
        info!("{}", cluster);
        info!("HazelcastClient is CONNECTED");
        info!("HazelcastClient is STARTED");

        Ok(HazelcastClient {
            cluster: Arc::new(cluster),
        })
    }

    pub fn pn_counter(&self, name: &str) -> PnCounter {
        PnCounter::new(name, self.cluster.clone())
    }
}
