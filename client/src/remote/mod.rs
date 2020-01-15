use std::{collections::HashMap, convert::TryInto};

use crate::{
    codec::{Readable, Writeable, Writer},
    message::Message,
};

mod channel;
pub(crate) mod cluster;
mod member;

struct Correlator<T> {
    sequence: u64,
    correlations: HashMap<u64, T>,
}

impl<T> Correlator<T> {
    fn new() -> Self {
        Correlator {
            sequence: 0,
            correlations: HashMap::new(),
        }
    }

    fn set(&mut self, value: T) -> u64 {
        self.sequence += 1;
        self.correlations.insert(self.sequence, value);
        self.sequence
    }

    fn get(&mut self, sequence: &u64) -> Option<T> {
        self.correlations.remove(sequence)
    }
}

const PROTOCOL_SEQUENCE: [u8; 3] = [0x43, 0x42, 0x32];

const CLIENT_TYPE: &str = "Rust";
const CLIENT_VERSION: &'static str = env!("CARGO_PKG_VERSION");
const PROTOCOL_VERSION: u8 = 1;

const BEGIN_MESSAGE: u8 = 0x80;
const END_MESSAGE: u8 = 0x40;
const UNFRAGMENTED_MESSAGE: u8 = BEGIN_MESSAGE | END_MESSAGE;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;
const LENGTH_FIELD_ADJUSTMENT: isize = -4;
const HEADER_LENGTH: usize = 22;

struct MessageCodec {}

impl MessageCodec {
    fn encode(frame: &mut dyn Writeable, message: &Message, correlation_id: u64) {
        let data_offset: u16 = HEADER_LENGTH.try_into().expect("unable to convert");

        PROTOCOL_VERSION.write_to(frame);
        UNFRAGMENTED_MESSAGE.write_to(frame);
        message.message_type().write_to(frame);
        correlation_id.write_to(frame);
        message.partition_id().write_to(frame);
        data_offset.write_to(frame);
        message.payload().write_to(frame);
    }

    fn decode(frame: &mut dyn Readable) -> (Message, u64) {
        let _version = frame.read_u8();
        let _flags = frame.read_u8();
        let message_type = frame.read_u16();
        let correlation_id = frame.read_u64();
        let partition_id = frame.read_i32();

        let data_offset: usize = frame.read_u16().try_into().expect("unable to convert!");
        frame.skip(data_offset - HEADER_LENGTH);
        let payload = frame.read();

        (Message::new(message_type, partition_id, payload), correlation_id)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes, BytesMut};

    use super::*;

    #[test]
    fn should_not_retrieve_anything_for_unknown_id() {
        let mut correlator: Correlator<String> = Correlator::new();

        assert_eq!(correlator.get(&1), None);
    }

    #[test]
    fn should_retrieve_for_known_id() {
        let mut correlator = Correlator::new();

        let id = correlator.set("data".to_string());

        assert_eq!(correlator.get(&id), Some("data".to_string()));
    }

    #[test]
    fn should_generate_unique_ids() {
        let mut correlator = Correlator::new();

        assert_eq!(correlator.set("data".to_string()), 1);
        assert_eq!(correlator.set("data".to_string()), 2);
    }

    #[test]
    fn should_encode_and_decode_message() {
        let correlation_id = 13;
        let message = Message::new(1, 2, Bytes::from(vec![3]));

        let mut writeable = BytesMut::new();
        MessageCodec::encode(&mut writeable, &message, correlation_id);
        assert_eq!(
            writeable.bytes(),
            [
                1,   // version
                192, // flags
                1, 0, // message type
                13, 0, 0, 0, 0, 0, 0, 0, // correlation id
                2, 0, 0, 0, // partition id
                22, 0, // data offset
                3  // payload
            ]
        );

        let mut readable = writeable.to_bytes();
        assert_eq!(MessageCodec::decode(&mut readable), (message, correlation_id));
    }
}
