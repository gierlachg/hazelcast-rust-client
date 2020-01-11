use std::collections::HashMap;
use std::convert::TryInto;

use crate::bytes::{Readable, Writeable, Writer};
use crate::message::Message;

mod channel;
pub(crate) mod connection;

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
        self.sequence += self.sequence + 1;
        self.correlations.insert(self.sequence, value);
        self.sequence
    }

    fn get(&mut self, sequence: &u64) -> Option<T> {
        self.correlations.remove(sequence)
    }
}

const PROTOCOL_SEQUENCE: [u8; 3] = [0x43, 0x42, 0x32];
const PROTOCOL_VERSION: u8 = 1;

const BEGIN_MESSAGE: u8 = 0x80;
const END_MESSAGE: u8 = 0x40;
const UNFRAGMENTED_MESSAGE: u8 = BEGIN_MESSAGE | END_MESSAGE;

const LENGTH_FIELD_OFFSET: usize = 0;
const LENGTH_FIELD_LENGTH: usize = 4;
const LENGTH_FIELD_ADJUSTMENT: isize = -4;
const HEADER_LENGTH: usize = 22;

struct FrameCodec {}

impl FrameCodec {
    fn encode(message: Message, correlation_id: u64, frame: &mut dyn Writeable) {
        let data_offset: u16 = HEADER_LENGTH.try_into().expect("unable to convert");

        PROTOCOL_VERSION.write_to(frame);
        UNFRAGMENTED_MESSAGE.write_to(frame);
        message.message_type().write_to(frame);
        correlation_id.write_to(frame);
        message.partition_id().write_to(frame);
        data_offset.write_to(frame);
        message.payload().write_to(frame);
    }

    fn decode(frame: &mut dyn Readable, frame_length: usize) -> (Message, u64) {
        let _version = frame.read_u8();
        let _flags = frame.read_u8();
        let message_type = frame.read_u16();
        let correlation_id = frame.read_u64();
        let partition_id = frame.read_i32();

        let data_offset: usize = frame.read_u16().try_into().expect("unable to convert!");
        frame.skip(data_offset - HEADER_LENGTH);
        let payload = frame.read_slice(frame_length - data_offset);

        (
            Message::new(message_type, partition_id, payload),
            correlation_id,
        )
    }
}
