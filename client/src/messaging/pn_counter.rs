use crate::messaging::{Address, ReplicaTimestampEntry};

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x2001]
pub(crate) struct PnCounterGetRequest<'a> {
    name: &'a str,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

impl<'a> PnCounterGetRequest<'a> {
    pub(crate) fn new(name: &'a str, replica_timestamps: &'a [ReplicaTimestampEntry], address: &'a Address) -> Self {
        PnCounterGetRequest {
            name,
            address,
            replica_timestamps,
        }
    }
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x7F]
pub(crate) struct PnCounterGetResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
}

impl PnCounterGetResponse {
    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x2002]
pub(crate) struct PnCounterAddRequest<'a> {
    name: &'a str,
    delta: i64,
    get_before_update: bool,
    replica_timestamps: &'a [ReplicaTimestampEntry],
    address: &'a Address,
}

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
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x7F]
pub(crate) struct PnCounterAddResponse {
    value: i64,
    replica_timestamps: Vec<ReplicaTimestampEntry>,
    _replica_count: u32,
}

impl PnCounterAddResponse {
    pub(crate) fn value(&self) -> i64 {
        self.value
    }

    pub(crate) fn replica_timestamps(&self) -> &[ReplicaTimestampEntry] {
        &self.replica_timestamps
    }
}

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x2003]
pub(crate) struct PnCounterGetReplicaCountRequest<'a> {
    name: &'a str,
}

impl<'a> PnCounterGetReplicaCountRequest<'a> {
    pub(crate) fn new(name: &'a str) -> Self {
        PnCounterGetReplicaCountRequest { name }
    }
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x66]
pub(crate) struct PnCounterGetReplicaCountResponse {
    count: u32,
}

impl PnCounterGetReplicaCountResponse {
    pub(crate) fn count(&self) -> u32 {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

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
