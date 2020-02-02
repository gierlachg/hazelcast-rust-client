use std::net::SocketAddr;

use derive_more::Display;

use crate::codec::{Reader, Writer};

pub(crate) mod authentication;
pub(crate) mod error;
pub(crate) mod ping;
pub(crate) mod pn_counter;

pub(crate) trait Request: Writer {
    fn r#type() -> u16;

    fn partition_id(&self) -> i32 {
        -1
    }
}

pub(crate) trait Response: Reader {
    fn r#type() -> u16;
}

#[derive(Writer, Reader, Eq, PartialEq, Hash, Display, Debug, Clone)]
#[display(fmt = "{}:{}", host, port)]
pub(crate) struct Address {
    host: String,
    port: u32,
}

impl From<&std::net::SocketAddr> for Address {
    fn from(address: &SocketAddr) -> Self {
        Address {
            host: address.ip().to_string(),
            port: address.port() as u32,
        }
    }
}

#[derive(Reader, Eq, PartialEq, Debug)]
pub(crate) struct ClusterMember {
    address: Address,
    id: String,
    lite: bool,
    attributes: Vec<AttributeEntry>,
}

#[derive(Reader, Eq, PartialEq, Debug, Clone)]
pub(crate) struct AttributeEntry {
    _key: String,
    _value: String,
}

#[derive(Writer, Reader, Eq, PartialEq, Debug, Clone)]
pub(crate) struct ReplicaTimestampEntry {
    key: String,
    value: i64,
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

    use super::*;

    #[test]
    fn should_write_and_read_address() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };

        let mut writeable = BytesMut::new();
        address.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Address::read_from(readable), address);
    }

    #[test]
    fn should_read_cluster_member() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };
        let id = "id";
        let lite = true;

        let writeable = &mut BytesMut::new();
        address.write_to(writeable);
        id.write_to(writeable);
        lite.write_to(writeable);
        0u32.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            ClusterMember::read_from(readable),
            ClusterMember {
                address,
                id: id.to_string(),
                lite,
                attributes: vec!(),
            }
        );
    }

    #[test]
    fn should_read_attribute() {
        let key = "key";
        let value = "value";

        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        value.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            AttributeEntry::read_from(readable),
            AttributeEntry {
                _key: key.to_string(),
                _value: value.to_string(),
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
}
