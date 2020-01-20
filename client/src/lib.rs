#[macro_use]
extern crate hazelcast_rust_client_macros;

use std::{error, sync::Arc};

use log::info;
use thiserror::Error;

pub use protocol::pn_counter::PnCounter;

use crate::remote::cluster::Cluster;

mod codec;
mod messaging;
mod protocol;
mod remote;

#[derive(Error, Debug)]
pub enum HazelcastClientError {
    #[error("unable to authenticate")]
    InvalidCredentials,
    #[error("unable to communicate with any cluster member")]
    ClusterNonOperational,
    #[error("unable to communicate with the server ({0})")]
    CommunicationFailure(Box<dyn error::Error + Send + Sync>),
    #[error("server was unable to process messaging ({0})")]
    ServerFailure(Box<dyn error::Error + Send + Sync>),
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

type Result<T> = std::result::Result<T, HazelcastClientError>;

trait TryFrom<T> {
    type Error;

    fn try_from(self) -> std::result::Result<T, Self::Error>;
}
