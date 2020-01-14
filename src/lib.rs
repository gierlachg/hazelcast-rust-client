use std::error::Error;
use std::sync::Arc;

pub use protocol::pn_counter::PnCounter as PnCounter;

use crate::remote::cluster::Cluster;

mod bytes;
mod codec;
mod message;
mod protocol;
mod remote;

pub(crate) type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) trait TryFrom<T> {
    type Error;

    fn try_from(self) -> std::result::Result<T, Self::Error>;
}

pub struct HazelcastClient {
    cluster: Arc<Cluster>,
}

impl HazelcastClient {
    pub async fn new<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self> where
        E: IntoIterator<Item=&'a str>
    {
        let cluster = Cluster::new(endpoints, username, password).await?;

        Ok(HazelcastClient {
            cluster: Arc::new(cluster),
        })
    }

    pub fn pn_counter(&self, name: &str) -> PnCounter {
        PnCounter::new(name, self.cluster.clone())
    }
}
