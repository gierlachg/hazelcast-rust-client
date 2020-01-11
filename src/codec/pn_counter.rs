use std::convert::TryInto;

use crate::bytes::{Readable, Reader, Writeable, Writer};
use crate::message::Payload;
use crate::protocol::pn_counter::{
    PnCounterGetRequest, PnCounterGetResponse, ReplicaTimestampEntry,
};

const GET_REQUEST_MESSAGE_TYPE: u16 = 0x2001;
const GET_RESPONSE_MESSAGE_TYPE: u16 = 0x7F;

impl Payload for PnCounterGetRequest {
    fn r#type() -> u16 {
        GET_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Writer for PnCounterGetRequest {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.name().write_to(writeable);
        let len: u32 = self
            .replica_timestamp_entries()
            .len()
            .try_into()
            .expect("unable to convert!");
        len.write_to(writeable);
        for replica_timestamp_entry in self.replica_timestamp_entries() {
            replica_timestamp_entry.key().write_to(writeable);
            replica_timestamp_entry.value().write_to(writeable);
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
