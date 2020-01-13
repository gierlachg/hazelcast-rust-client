use std::convert::TryInto;

use crate::bytes::{Readable, Reader, Writeable, Writer};
use crate::message::Payload;
use crate::protocol::pn_counter::{
    PnCounterAddRequest, PnCounterAddResponse, PnCounterGetReplicaCountRequest,
    PnCounterGetReplicaCountResponse, PnCounterGetRequest, PnCounterGetResponse,
    ReplicaTimestampEntry,
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

impl<'a> Writer for PnCounterGetRequest<'a> {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.name().write_to(writeable);
        let len: u32 = self
            .replica_timestamps()
            .len()
            .try_into()
            .expect("unable to convert!");
        len.write_to(writeable);
        for replica_timestamp in self.replica_timestamps() {
            replica_timestamp.key().write_to(writeable);
            replica_timestamp.value().write_to(writeable);
        }
        self.address().write_to(writeable);
    }
}

impl Payload for PnCounterGetResponse {
    fn r#type() -> u16 {
        GET_RESPONSE_MESSAGE_TYPE
    }
}

impl Reader for PnCounterGetResponse {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let value = i64::read_from(readable);

        let number_of_entries: usize = u32::read_from(readable)
            .try_into()
            .expect("unable to convert!");
        let mut replica_timestamp_entries = Vec::with_capacity(number_of_entries as usize);
        for _ in 0..number_of_entries {
            let key = String::read_from(readable);
            let value = i64::read_from(readable);
            replica_timestamp_entries.push(ReplicaTimestampEntry::new(&key, value));
        }

        PnCounterGetResponse::new(value, &replica_timestamp_entries)
    }
}

impl<'a> Payload for PnCounterAddRequest<'a> {
    fn r#type() -> u16 {
        ADD_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl<'a> Writer for PnCounterAddRequest<'a> {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.name().write_to(writeable);
        self.delta().write_to(writeable);
        self.get_before_update().write_to(writeable);
        let len: u32 = self
            .replica_timestamps()
            .len()
            .try_into()
            .expect("unable to convert!");
        len.write_to(writeable);
        for replica_timestamp in self.replica_timestamps() {
            replica_timestamp.key().write_to(writeable);
            replica_timestamp.value().write_to(writeable);
        }
        self.address().write_to(writeable);
    }
}

impl Payload for PnCounterAddResponse {
    fn r#type() -> u16 {
        ADD_RESPONSE_MESSAGE_TYPE
    }
}

impl Reader for PnCounterAddResponse {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let value = i64::read_from(readable);

        let number_of_entries: usize = u32::read_from(readable)
            .try_into()
            .expect("unable to convert!");
        let mut replica_timestamp_entries = Vec::with_capacity(number_of_entries as usize);
        for _ in 0..number_of_entries {
            let key = String::read_from(readable);
            let value = i64::read_from(readable);
            replica_timestamp_entries.push(ReplicaTimestampEntry::new(&key, value));
        }
        let replica_count = u32::read_from(readable);

        PnCounterAddResponse::new(value, &replica_timestamp_entries, replica_count)
    }
}

impl<'a> Payload for PnCounterGetReplicaCountRequest<'a> {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl<'a> Writer for PnCounterGetReplicaCountRequest<'a> {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.name().write_to(writeable);
    }
}

impl Payload for PnCounterGetReplicaCountResponse {
    fn r#type() -> u16 {
        GET_REPLICA_COUNT_RESPONSE_MESSAGE_TYPE
    }
}

impl Reader for PnCounterGetReplicaCountResponse {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let count = u32::read_from(readable);

        PnCounterGetReplicaCountResponse::new(count)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::protocol::Address;

    use super::*;

    #[test]
    fn should_write_get_request() {
        let address = Address::new("localhost", 5701);
        let replica_timestamps = &[ReplicaTimestampEntry::new("key", 69)];
        let request = PnCounterGetRequest::new("counter-name", &address, replica_timestamps);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name());
        assert_eq!(
            u32::read_from(readable),
            request.replica_timestamps().len() as u32
        );
        assert_eq!(
            String::read_from(readable),
            request.replica_timestamps()[0].key()
        );
        assert_eq!(
            i64::read_from(readable),
            request.replica_timestamps()[0].value()
        );
        assert_eq!(&Address::read_from(readable), request.address());
    }

    #[test]
    fn should_read_get_response() {
        let value = 12;
        let replica_timestamp_key = "replica-timestamp-key";
        let replica_timestamp_value = 69;

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        1u32.write_to(writeable);
        replica_timestamp_key.write_to(writeable);
        replica_timestamp_value.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterGetResponse::read_from(readable),
            PnCounterGetResponse::new(
                value,
                &[ReplicaTimestampEntry::new(
                    replica_timestamp_key,
                    replica_timestamp_value,
                )],
            )
        );
    }

    #[test]
    fn should_write_add_request() {
        let address = Address::new("localhost", 5701);
        let replica_timestamps = [ReplicaTimestampEntry::new("key", 69)];
        let request =
            PnCounterAddRequest::new("counter-name", &address, -13, true, &replica_timestamps);

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.name());
        assert_eq!(i64::read_from(readable), request.delta());
        assert_eq!(bool::read_from(readable), request.get_before_update());
        assert_eq!(
            u32::read_from(readable),
            request.replica_timestamps().len() as u32
        );
        assert_eq!(
            String::read_from(readable),
            request.replica_timestamps()[0].key()
        );
        assert_eq!(
            i64::read_from(readable),
            request.replica_timestamps()[0].value()
        );
        assert_eq!(&Address::read_from(readable), request.address());
    }

    #[test]
    fn should_read_add_response() {
        let value = 12;
        let replica_timestamp_key = "replica-timestamp-key";
        let replica_timestamp_value = 69;
        let replica_count = 3;

        let writeable = &mut BytesMut::new();
        value.write_to(writeable);
        1u32.write_to(writeable);
        replica_timestamp_key.write_to(writeable);
        replica_timestamp_value.write_to(writeable);
        replica_count.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            PnCounterAddResponse::read_from(readable),
            PnCounterAddResponse::new(
                value,
                &[ReplicaTimestampEntry::new(
                    replica_timestamp_key,
                    replica_timestamp_value,
                )],
                replica_count,
            )
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
