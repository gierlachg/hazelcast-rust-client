use crate::{
    message::Payload,
    protocol::pn_counter::{
        PnCounterAddRequest, PnCounterAddResponse, PnCounterGetReplicaCountRequest,
        PnCounterGetReplicaCountResponse, PnCounterGetRequest, PnCounterGetResponse,
    },
};

const GET_REQUEST_MESSAGE_TYPE: u16 = 0x2001;
const GET_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

const ADD_REQUEST_MESSAGE_TYPE: u16 = 0x2002;
const ADD_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

const GET_REPLICA_COUNT_REQUEST_MESSAGE_TYPE: u16 = 0x2003;
const GET_REPLICA_COUNT_RESPONSE_MESSAGE_TYPE: u16 = 0x66;

impl<'a> Payload for PnCounterGetRequest<'a> {
    fn r#type() -> u16 {
        GET_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Payload for PnCounterGetResponse {
    fn r#type() -> u16 {
        GET_RESPONSE_MESSAGE_TYPE
    }
}

impl<'a> Payload for PnCounterAddRequest<'a> {
    fn r#type() -> u16 {
        ADD_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Payload for PnCounterAddResponse {
    fn r#type() -> u16 {
        ADD_RESPONSE_MESSAGE_TYPE
    }
}

impl<'a> Payload for PnCounterGetReplicaCountRequest<'a> {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Payload for PnCounterGetReplicaCountResponse {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_RESPONSE_MESSAGE_TYPE
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use bytes::{Buf, BytesMut};

    use crate::{
        bytes::{Reader, Writer},
        protocol::{pn_counter::ReplicaTimestampEntry, Address},
    };

    use super::*;

    #[test]
    fn should_write_get_request() {
        let address = Address::new("localhost".to_string(), 5701);
        let replica_timestamps = &[ReplicaTimestampEntry::new("key".to_string(), 69)];
        let request = PnCounterGetRequest::new("counter-name", replica_timestamps, &address);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name());
        assert_eq!(
            Vec::<ReplicaTimestampEntry>::read_from(readable).deref(),
            replica_timestamps
        );
        assert_eq!(&Address::read_from(readable), request.address());
    }

    #[test]
    fn should_read_get_response() {
        let value = 12;
        let replica_timestamps = vec![ReplicaTimestampEntry::new("key".to_string(), 69)];

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        replica_timestamps.deref().write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterGetResponse::read_from(readable),
            PnCounterGetResponse::new(value, replica_timestamps)
        );
    }

    #[test]
    fn should_write_add_request() {
        let address = Address::new("localhost".to_string(), 5701);
        let replica_timestamps = [ReplicaTimestampEntry::new("key".to_string(), 69)];
        let request =
            PnCounterAddRequest::new("counter-name", -13, true, &replica_timestamps, &address);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name());
        assert_eq!(i64::read_from(readable), request.delta());
        assert_eq!(bool::read_from(readable), request.get_before_update());
        assert_eq!(
            Vec::<ReplicaTimestampEntry>::read_from(readable).deref(),
            replica_timestamps
        );
        assert_eq!(&Address::read_from(readable), request.address());
    }

    #[test]
    fn should_read_add_response() {
        let value = 12;
        let replica_timestamps = vec![ReplicaTimestampEntry::new("key".to_string(), 69)];
        let replica_count = 3;

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        replica_timestamps.deref().write_to(writeable);
        replica_count.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterAddResponse::read_from(readable),
            PnCounterAddResponse::new(value, replica_timestamps, replica_count)
        );
    }

    #[test]
    fn should_write_replica_timestamp_entry() {
        let replica_timestamp = ReplicaTimestampEntry::new("key".to_string(), 69);

        let writeable = &mut BytesMut::new();
        replica_timestamp.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), replica_timestamp.key());
        assert_eq!(i64::read_from(readable), replica_timestamp.value());
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
            ReplicaTimestampEntry::new(key.to_string(), value)
        );
    }

    #[test]
    fn should_write_replica_count_request() {
        let request = PnCounterGetReplicaCountRequest::new("counter-name");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name());
    }

    #[test]
    fn should_read_replica_count_response() {
        let replica_count = 3;

        let writeable = &mut BytesMut::new();
        replica_count.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterGetReplicaCountResponse::read_from(readable),
            PnCounterGetReplicaCountResponse::new(replica_count)
        );
    }
}
