use std::{error::Error, fmt};

use bytes::{Buf, Bytes, BytesMut};

use crate::{
    codec::{Readable, Reader, Writer},
    TryFrom,
};

pub(crate) trait Payload {
    fn r#type() -> u16;

    fn partition_id(&self) -> i32 {
        -1
    }
}

#[derive(Eq, PartialEq)]
pub(crate) struct Message {
    // TODO: retry-able ???
    message_type: u16,
    partition_id: i32,
    payload: Bytes,
}

impl Message {
    pub(crate) fn new(message_type: u16, partition_id: i32, payload: Bytes) -> Self {
        Message {
            message_type,
            partition_id,
            payload,
        }
    }

    pub(crate) fn message_type(&self) -> u16 {
        self.message_type
    }

    pub(crate) fn partition_id(&self) -> i32 {
        self.partition_id
    }

    pub(crate) fn payload(&self) -> Bytes {
        self.payload.clone()
    }
}

impl fmt::Display for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Message (type: {}, partition id: {})",
            self.message_type, self.partition_id
        )
    }
}

impl<T> From<T> for Message
where
    T: Payload + Writer,
{
    fn from(payload: T) -> Self {
        let mut bytes = BytesMut::new();
        payload.write_to(&mut bytes);

        Message::new(T::r#type(), payload.partition_id(), bytes.to_bytes())
    }
}

impl<T> TryFrom<T> for Message
where
    T: Payload + Reader,
{
    type Error = Box<dyn Error + Send + Sync>;

    fn try_from(self) -> Result<T, Self::Error> {
        let readable = &mut self.payload();
        if self.message_type() == T::r#type() {
            Ok(T::read_from(readable))
        } else {
            assert_eq!(
                self.message_type(),
                Exception::r#type(),
                "unknown message type: {}, expected: {}",
                self.message_type(),
                T::r#type()
            );
            Err(Box::new(Exception::read_from(readable)))
        }
    }
}

const EXCEPTION_MESSAGE_TYPE: u16 = 0x6D;

#[derive(Reader)]
pub(crate) struct Exception {
    code: i32,
    class_name: String,
    message: Option<String>,
    stack_trace: Vec<StackTraceEntry>,
    cause_error_code: u32,
    cause_class_name: Option<String>,
}

impl Error for Exception {}

impl fmt::Display for Exception {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl fmt::Debug for Exception {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Error (code: {}, cause code: {}, cause class name: {:?}) {{\n",
            self.code, self.cause_error_code, self.cause_class_name
        )?;
        write!(
            formatter,
            "\t{}: {}\n",
            self.class_name,
            self.message.as_deref().unwrap_or("")
        )?;
        for stack_trace_entry in &self.stack_trace {
            write!(formatter, "\t\t{}\n", stack_trace_entry)?;
        }
        write!(formatter, "}}")
    }
}

impl Payload for Exception {
    fn r#type() -> u16 {
        EXCEPTION_MESSAGE_TYPE
    }
}

#[derive(Reader)]
pub(crate) struct StackTraceEntry {
    declaring_class: String,
    method_name: String,
    file_name: Option<String>,
    line_number: u32,
}

impl fmt::Display for StackTraceEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at {}.{}({}:{})",
            self.declaring_class,
            self.method_name,
            self.file_name.as_deref().unwrap_or(""),
            self.line_number
        )
    }
}
