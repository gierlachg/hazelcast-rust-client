use std::convert::TryInto;

use crate::bytes::{Readable, Reader, Writeable, Writer};
use crate::message::Payload;
use crate::protocol::pn_counter::{
    PnCounterAddRequest, PnCounterAddResponse, PnCounterGetRequest, PnCounterGetResponse,
    ReplicaTimestampEntry,
};

const GET_REQUEST_MESSAGE_TYPE: u16 = 0x2001;
const GET_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

const ADD_REQUEST_MESSAGE_TYPE: u16 = 0x2002;
const ADD_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

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
