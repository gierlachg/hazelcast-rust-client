use std::{error::Error, sync::Arc};

use log::info;

pub use protocol::pn_counter::PnCounter;

use crate::remote::cluster::Cluster;

mod bytes;
mod codec;
mod message;
mod protocol;
mod remote;

pub(crate) const CLIENT_TYPE: &str = "Rust";
pub(crate) const CLIENT_VERSION: &str = "0.1.0-SNAPSHOT";

pub(crate) type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) trait TryFrom<T> {
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
        info!("HazelcastClient {} is STARTING", CLIENT_VERSION);
        let cluster = Cluster::from(endpoints, username, password).await?;
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
