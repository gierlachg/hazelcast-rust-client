use std::sync::Arc;

use crate::{
    bytes::{Readable, Reader, Writeable, Writer},
    protocol::Address,
    remote::cluster::Cluster,
    Result,
};

pub struct PnCounter {
    name: String,
    cluster: Arc<Cluster>,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounter {
    pub(crate) fn new(name: &str, cluster: Arc<Cluster>) -> Self {
        PnCounter {
            name: name.to_string(),
            cluster,
            replica_timestamps: vec![],
        }
    }

    pub async fn get(&mut self) -> Result<i64> {
        let address = self.cluster.address().clone().expect("missing address!"); // TODO: not sure where address should come from, what is its purpose....

        let request = PnCounterGetRequest::new(&self.name, &self.replica_timestamps, &address);
        let response: PnCounterGetResponse = self.cluster.dispatch(request).await?;
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
        let address = self.cluster.address().clone().expect("missing address!"); // TODO: not sure where address should come from, what is its purpose....

        let request = PnCounterAddRequest::new(
            &self.name,
            delta,
            get_before_update,
            &self.replica_timestamps,
            &address,
        );
        let response: PnCounterAddResponse = self.cluster.dispatch(request).await?;
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

#[derive(Debug, Eq, PartialEq, Writer)]
pub(crate) struct PnCounterGetRequest<'a> {
    name: &'a str,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

#[allow(dead_code)]
impl<'a> PnCounterGetRequest<'a> {
    pub(crate) fn new(
        name: &'a str,
        replica_timestamps: &'a [ReplicaTimestampEntry],
        address: &'a Address,
    ) -> Self {
        PnCounterGetRequest {
            name,
            address,
            replica_timestamps,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.name
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        self.replica_timestamps
    }

    pub(crate) fn address(&self) -> &Address {
        self.address
    }
}

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct PnCounterGetResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetResponse {
    pub(crate) fn new(value: i64, replica_timestamps: Vec<ReplicaTimestampEntry>) -> Self {
        PnCounterGetResponse {
            value,
            replica_timestamps,
        }
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

#[derive(Debug, Eq, PartialEq, Writer)]
pub(crate) struct PnCounterAddRequest<'a> {
    name: &'a str,
    delta: i64,
    get_before_update: bool,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

#[allow(dead_code)]
impl<'a> PnCounterAddRequest<'a> {
    pub(crate) fn new(
        name: &'a str,
        delta: i64,
        get_before_update: bool,
        replica_timestamps: &'a [ReplicaTimestampEntry],
        address: &'a Address,
    ) -> Self {
        PnCounterAddRequest {
            name,
            address,
            delta,
            get_before_update,
            replica_timestamps,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.name
    }

    pub(crate) fn delta(&self) -> i64 {
        self.delta
    }

    pub(crate) fn get_before_update(&self) -> bool {
        self.get_before_update
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        self.replica_timestamps
    }

    pub(crate) fn address(&self) -> &Address {
        self.address
    }
}

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct PnCounterAddResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
    _replica_count: u32,
}

impl PnCounterAddResponse {
    pub(crate) fn new(
        value: i64,
        replica_timestamps: Vec<ReplicaTimestampEntry>,
        replica_count: u32,
    ) -> Self {
        PnCounterAddResponse {
            value,
            replica_timestamps,
            _replica_count: replica_count,
        }
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Writer, Reader)]
pub(crate) struct ReplicaTimestampEntry {
    key: String,
    value: i64,
}

#[allow(dead_code)]
impl ReplicaTimestampEntry {
    pub(crate) fn new(key: String, value: i64) -> Self {
        ReplicaTimestampEntry { key, value }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }
}

#[derive(Debug, Eq, PartialEq, Writer)]
pub(crate) struct PnCounterGetReplicaCountRequest<'a> {
    name: &'a str,
}

#[allow(dead_code)]
impl<'a> PnCounterGetReplicaCountRequest<'a> {
    pub(crate) fn new(name: &'a str) -> Self {
        PnCounterGetReplicaCountRequest { name }
    }

    pub(crate) fn name(&self) -> &str {
        self.name
    }
}

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct PnCounterGetReplicaCountResponse {
    count: u32,
}

impl PnCounterGetReplicaCountResponse {
    pub(crate) fn new(count: u32) -> Self {
        PnCounterGetReplicaCountResponse { count }
    }

    pub(crate) fn count(&self) -> u32 {
        self.count
    }
}
