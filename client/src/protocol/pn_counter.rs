use std::sync::Arc;

use crate::{
    codec::{Readable, Reader, Writeable, Writer},
    messaging::{Request, Response},
    protocol::Address,
    remote::cluster::Cluster,
    Result,
};

const GET_REQUEST_MESSAGE_TYPE: u16 = 0x2001;
const GET_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

const ADD_REQUEST_MESSAGE_TYPE: u16 = 0x2002;
const ADD_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

const GET_REPLICA_COUNT_REQUEST_MESSAGE_TYPE: u16 = 0x2003;
const GET_REPLICA_COUNT_RESPONSE_MESSAGE_TYPE: u16 = 0x66;

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

        let request =
            PnCounterAddRequest::new(&self.name, delta, get_before_update, &self.replica_timestamps, &address);
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

#[derive(Writer, Eq, PartialEq, Debug)]
struct PnCounterGetRequest<'a> {
    name: &'a str,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

impl<'a> PnCounterGetRequest<'a> {
    fn new(name: &'a str, replica_timestamps: &'a [ReplicaTimestampEntry], address: &'a Address) -> Self {
        PnCounterGetRequest {
            name,
            address,
            replica_timestamps,
        }
    }
}

impl<'a> Request for PnCounterGetRequest<'a> {
    fn r#type() -> u16 {
        GET_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

#[derive(Reader, Eq, PartialEq, Debug)]
struct PnCounterGetResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetResponse {
    fn value(&self) -> i64 {
        self.value
    }

    fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

impl Response for PnCounterGetResponse {
    fn r#type() -> u16 {
        GET_RESPONSE_MESSAGE_TYPE
    }
}

#[derive(Writer, Eq, PartialEq, Debug)]
struct PnCounterAddRequest<'a> {
    name: &'a str,
    delta: i64,
    get_before_update: bool,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

impl<'a> PnCounterAddRequest<'a> {
    fn new(
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
}

impl<'a> Request for PnCounterAddRequest<'a> {
    fn r#type() -> u16 {
        ADD_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

#[derive(Reader, Eq, PartialEq, Debug)]
struct PnCounterAddResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
    _replica_count: u32,
}

impl PnCounterAddResponse {
    fn value(&self) -> i64 {
        self.value
    }

    fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

impl Response for PnCounterAddResponse {
    fn r#type() -> u16 {
        ADD_RESPONSE_MESSAGE_TYPE
    }
}

#[derive(Writer, Reader, Eq, PartialEq, Debug, Clone)]
struct ReplicaTimestampEntry {
    key: String,
    value: i64,
}

#[derive(Writer, Eq, PartialEq, Debug)]
struct PnCounterGetReplicaCountRequest<'a> {
    name: &'a str,
}

impl<'a> PnCounterGetReplicaCountRequest<'a> {
    fn new(name: &'a str) -> Self {
        PnCounterGetReplicaCountRequest { name }
    }
}

impl<'a> Request for PnCounterGetReplicaCountRequest<'a> {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

#[derive(Reader, Eq, PartialEq, Debug)]
struct PnCounterGetReplicaCountResponse {
    count: u32,
}

impl Response for PnCounterGetReplicaCountResponse {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_RESPONSE_MESSAGE_TYPE
    }
}

impl PnCounterGetReplicaCountResponse {
    fn count(&self) -> u32 {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use bytes::{Buf, BytesMut};

    use super::*;

    #[test]
    fn should_write_get_request() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };
        let replica_timestamps = &[ReplicaTimestampEntry {
            key: "key".to_string(),
            value: 69,
        }];
        let request = PnCounterGetRequest::new("counter-name", replica_timestamps, &address);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name);
        assert_eq!(
            Vec::<ReplicaTimestampEntry>::read_from(readable).deref(),
            replica_timestamps
        );
        assert_eq!(&Address::read_from(readable), request.address);
    }

    #[test]
    fn should_read_get_response() {
        let value = 12;
        let replica_timestamps = vec![ReplicaTimestampEntry {
            key: "key".to_string(),
            value: 69,
        }];

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        replica_timestamps.deref().write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterGetResponse::read_from(readable),
            PnCounterGetResponse {
                value,
                replica_timestamps,
            }
        );
    }

    #[test]
    fn should_write_add_request() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };
        let replica_timestamps = [ReplicaTimestampEntry {
            key: "key".to_string(),
            value: 69,
        }];
        let request = PnCounterAddRequest::new("counter-name", -13, true, &replica_timestamps, &address);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name);
        assert_eq!(i64::read_from(readable), request.delta);
        assert_eq!(bool::read_from(readable), request.get_before_update);
        assert_eq!(
            Vec::<ReplicaTimestampEntry>::read_from(readable).deref(),
            replica_timestamps
        );
        assert_eq!(&Address::read_from(readable), request.address);
    }

    #[test]
    fn should_read_add_response() {
        let value = 12;
        let replica_timestamps = vec![ReplicaTimestampEntry {
            key: "key".to_string(),
            value: 69,
        }];
        let replica_count = 3;

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        replica_timestamps.deref().write_to(writeable);
        replica_count.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterAddResponse::read_from(readable),
            PnCounterAddResponse {
                value,
                replica_timestamps,
                _replica_count: replica_count,
            }
        );
    }

    #[test]
    fn should_write_replica_timestamp_entry() {
        let replica_timestamp = ReplicaTimestampEntry {
            key: "key".to_string(),
            value: 69,
        };

        let writeable = &mut BytesMut::new();
        replica_timestamp.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), replica_timestamp.key);
        assert_eq!(i64::read_from(readable), replica_timestamp.value);
    }

    #[test]
    fn should_read_replica_timestamp_entry() {
        let key = "key";
        let value = 12;

        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        value.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            ReplicaTimestampEntry::read_from(readable),
            ReplicaTimestampEntry {
                key: key.to_string(),
                value,
            }
        );
    }

    #[test]
    fn should_write_replica_count_request() {
        let request = PnCounterGetReplicaCountRequest::new("counter-name");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name);
    }

    #[test]
    fn should_read_replica_count_response() {
        let count = 3;

        let writeable = &mut BytesMut::new();
        count.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterGetReplicaCountResponse::read_from(readable),
            PnCounterGetReplicaCountResponse { count }
        );
    }
}
