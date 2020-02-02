use std::sync::Arc;

use crate::messaging::pn_counter::{
    PnCounterAddRequest, PnCounterAddResponse, PnCounterGetReplicaCountRequest, PnCounterGetReplicaCountResponse,
    PnCounterGetRequest, PnCounterGetResponse,
};
use crate::{
    messaging::{Address, ReplicaTimestampEntry},
    remote::cluster::Cluster,
    Result,
};

pub struct PnCounter {
    name: String,
    cluster: Arc<Cluster>,

    address: Option<Address>,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounter {
    pub(crate) fn new(name: &str, cluster: Arc<Cluster>) -> Self {
        PnCounter {
            name: name.to_string(),
            cluster,
            address: None,
            replica_timestamps: vec![],
        }
    }

    pub async fn get(&mut self) -> Result<i64> {
        let address = self.cluster.address(self.address.take()).await?;
        let request = PnCounterGetRequest::new(&self.name, &self.replica_timestamps, &address);
        let response: PnCounterGetResponse = self.cluster.forward(request, &address).await?;
        self.address = Some(address);
        self.replica_timestamps = response.replica_timestamps().to_vec();
        Ok(response.value())
    }

    pub async fn get_and_add(&mut self, delta: i64) -> Result<i64> {
        self.add(delta, true).await
    }

    pub async fn add_and_get(&mut self, delta: i64) -> Result<i64> {
        self.add(delta, false).await
    }

    async fn add(&mut self, delta: i64, get_before_update: bool) -> Result<i64> {
        let address = self.cluster.address(self.address.take()).await?;
        let request =
            PnCounterAddRequest::new(&self.name, delta, get_before_update, &self.replica_timestamps, &address);
        let response: PnCounterAddResponse = self.cluster.forward(request, &address).await?;
        self.address = Some(address);
        self.replica_timestamps = response.replica_timestamps().to_vec();
        Ok(response.value())
    }

    pub async fn replica_count(&mut self) -> Result<u32> {
        let request = PnCounterGetReplicaCountRequest::new(&self.name);
        let response: PnCounterGetReplicaCountResponse = self.cluster.dispatch(request).await?;
        Ok(response.count())
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
