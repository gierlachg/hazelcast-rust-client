use crate::{Result, TryFrom};
use crate::protocol::Address;
use crate::remote::connection::Connection;

pub struct PnCounter<'a> {
    name: String,
    connection: &'a mut Connection,
    replica_timestamp_entries: Vec<ReplicaTimestampEntry>,
}

impl<'a> PnCounter<'a> {
    pub(crate) fn new(name: &str, connection: &'a mut Connection) -> Self {
        PnCounter {
            name: name.to_string(),
            connection,
            replica_timestamp_entries: vec![],
        }
    }

    pub async fn get(&mut self) -> Result<i64> {
        let address = self.connection.address().clone().expect("missing address!"); // TODO: not sure where address should come from, what is its purpose....
        
        let request =
            PnCounterGetRequest::new(&self.name, address, &self.replica_timestamp_entries).into();
        let response = self.connection.send(request).await?;

        match TryFrom::<PnCounterGetResponse>::try_from(response) {
            Ok(response) => {
                self.replica_timestamp_entries = response.replica_timestamp_entries().to_vec();
                Ok(response.value())
            }
            Err(exception) => {
                eprintln!("{}", exception);
                Err("Unable to crate connection.".into())
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PnCounterGetRequest {
    name: String,
    address: Address,
    replica_timestamp_entries: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetRequest {
    fn new(
        name: &str,
        address: Address,
        replica_timestamp_entries: &[ReplicaTimestampEntry],
    ) -> Self {
        PnCounterGetRequest {
            name: name.to_string(),
            address,
            replica_timestamp_entries: replica_timestamp_entries.to_vec(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn address(&self) -> &Address {
        &self.address
    }

    pub(crate) fn replica_timestamp_entries(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamp_entries
    }
}

#[derive(Debug)]
pub(crate) struct PnCounterGetResponse {
    value: i64,
    replica_timestamp_entries: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetResponse {
    pub(crate) fn new(value: i64, replica_timestamp_entries: &[ReplicaTimestampEntry]) -> Self {
        PnCounterGetResponse {
            value,
            replica_timestamp_entries: replica_timestamp_entries.to_vec(),
        }
    }

    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamp_entries(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamp_entries
    }
}

#[derive(Debug, Clone)]
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
