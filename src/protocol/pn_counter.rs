use crate::protocol::Address;
use crate::remote::connection::Connection;
use crate::{Result, TryFrom};

pub struct PnCounter<'a> {
    name: String,
    connection: &'a mut Connection,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl<'a> PnCounter<'a> {
    pub(crate) fn new(name: &str, connection: &'a mut Connection) -> Self {
        PnCounter {
            name: name.to_string(),
            connection,
            replica_timestamps: vec![],
        }
    }

    pub async fn get(&mut self) -> Result<i64> {
        let address = self.connection.address().clone().expect("missing address!"); // TODO: not sure where address should come from, what is its purpose....

        let request =
            PnCounterGetRequest::new(&self.name, &address, &self.replica_timestamps).into();
        let response = self.connection.send(request).await?;

        match TryFrom::<PnCounterGetResponse>::try_from(response) {
            Ok(response) => {
                self.replica_timestamps = response.replica_timestamps().to_vec();
                Ok(response.value())
            }
            Err(exception) => {
                eprintln!("{}", exception);
                Err("Unable to fetch counter value.".into())
            }
        }
    }

    pub async fn get_and_add(&mut self, delta: i64) -> Result<i64> {
        self.add(delta, true).await
    }

    pub async fn add_and_get(&mut self, delta: i64) -> Result<i64> {
        self.add(delta, false).await
    }

    async fn add(&mut self, delta: i64, get_before_update: bool) -> Result<i64> {
        let address = self.connection.address().clone().expect("missing address!"); // TODO: not sure where address should come from, what is its purpose....

        let request = PnCounterAddRequest::new(
            &self.name,
            &address,
            delta,
            get_before_update,
            &self.replica_timestamps,
        )
        .into();
        let response = self.connection.send(request).await?;

        match TryFrom::<PnCounterAddResponse>::try_from(response) {
            Ok(response) => {
                self.replica_timestamps = response.replica_timestamps().to_vec();
                Ok(response.value())
            }
            Err(exception) => {
                eprintln!("{}", exception);
                Err("Unable to add to counter.".into())
            }
        }
    }

    pub async fn replica_count(&mut self) -> Result<u32> {
        let request = PnCounterGetReplicaCountRequest::new(&self.name).into();
        let response = self.connection.send(request).await?;

        match TryFrom::<PnCounterGetReplicaCountResponse>::try_from(response) {
            Ok(response) => Ok(response.count()),
            Err(exception) => {
                eprintln!("{}", exception);
                Err("Unable to fetch replica count for counter.".into())
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PnCounterGetRequest<'a> {
    name: &'a str,
    address: &'a Address,
    replica_timestamps: &'a [ReplicaTimestampEntry],
}

impl<'a> PnCounterGetRequest<'a> {
    pub(crate) fn new(
        name: &'a str,
        address: &'a Address,
        replica_timestamps: &'a [ReplicaTimestampEntry],
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

    pub(crate) fn address(&self) -> &Address {
        self.address
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        self.replica_timestamps
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PnCounterGetResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetResponse {
    pub(crate) fn new(value: i64, replica_timestamps: &[ReplicaTimestampEntry]) -> Self {
        PnCounterGetResponse {
            value,
            replica_timestamps: replica_timestamps.to_vec(),
        }
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PnCounterAddRequest<'a> {
    name: &'a str,
    address: &'a Address,
    get_before_update: bool,
    delta: i64,
    replica_timestamps: &'a [ReplicaTimestampEntry],
}

impl<'a> PnCounterAddRequest<'a> {
    pub(crate) fn new(
        name: &'a str,
        address: &'a Address,
        delta: i64,
        get_before_update: bool,
        replica_timestamps: &'a [ReplicaTimestampEntry],
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

    pub(crate) fn address(&self) -> &Address {
        self.address
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
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PnCounterAddResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
    _replica_count: u32,
}

impl PnCounterAddResponse {
    pub(crate) fn new(
        value: i64,
        replica_timestamps: &[ReplicaTimestampEntry],
        replica_count: u32,
    ) -> Self {
        PnCounterAddResponse {
            value,
            replica_timestamps: replica_timestamps.to_vec(),
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct ReplicaTimestampEntry {
    key: String,
    value: i64,
}

impl ReplicaTimestampEntry {
    pub(crate) fn new(key: &str, value: i64) -> Self {
        ReplicaTimestampEntry {
            key: key.to_string(),
            value,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PnCounterGetReplicaCountRequest<'a> {
    name: &'a str,
}

impl<'a> PnCounterGetReplicaCountRequest<'a> {
    pub(crate) fn new(name: &'a str) -> Self {
        PnCounterGetReplicaCountRequest { name }
    }

    pub(crate) fn name(&self) -> &str {
        self.name
    }
}

#[derive(Debug, Eq, PartialEq)]
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
