use std::{convert::TryInto, fmt};

use bytes::{Buf, Bytes, BytesMut};

use crate::bytes::{Readable, Reader, Writer};
use crate::TryFrom;

pub(crate) trait Payload {
    fn r#type() -> u16;

    fn partition_id(&self) -> i32 {
        -1
    }
}

#[derive(Debug)]
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
    type Error = Exception;

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
            Err(Exception::read_from(readable))
        }
    }
}

pub(crate) struct Exception {
    code: i32,
    class_name: String,
    message: Option<String>,
    stack_trace: Vec<StackTraceEntry>,
    cause_error_code: u32,
    cause_class_name: Option<String>,
}

impl Exception {
    pub(crate) fn new(
        code: i32,
        class_name: &str,
        message: Option<String>,
        stack_trace: Vec<StackTraceEntry>,
        cause_error_code: u32,
        cause_class_name: Option<String>,
    ) -> Self {
        Exception {
            code,
            class_name: class_name.to_string(),
            message,
            stack_trace,
            cause_error_code,
            cause_class_name,
        }
    }
}

impl fmt::Display for Exception {
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

pub(crate) struct StackTraceEntry {
    declaring_class: String,
    method_name: String,
    file_name: Option<String>,
    line_number: u32,
}

impl StackTraceEntry {
    pub(crate) fn new(
        declaring_class: &str,
        method_name: &str,
        file_name: Option<String>,
        line_number: u32,
    ) -> Self {
        StackTraceEntry {
            declaring_class: declaring_class.to_string(),
            method_name: method_name.to_string(),
            file_name,
            line_number,
        }
    }
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

const EXCEPTION_MESSAGE_TYPE: u16 = 0x6D;

impl Payload for Exception {
    fn r#type() -> u16 {
        EXCEPTION_MESSAGE_TYPE
    }
}

impl Reader for Exception {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let code = i32::read_from(readable);
        let class_name = String::read_from(readable);

        let message = if !bool::read_from(readable) {
            Some(String::read_from(readable))
        } else {
            None
        };

        let number_of_entries = u32::read_from(readable)
            .try_into()
            .expect("unable to convert!");
        let mut stack_trace_entries = Vec::with_capacity(number_of_entries);
        for _ in 0..number_of_entries {
            let class = String::read_from(readable);
            let method = String::read_from(readable);

            let file_name = if !bool::read_from(readable) {
                Some(String::read_from(readable))
            } else {
                None
            };

            let line_number = u32::read_from(readable);

            stack_trace_entries.push(StackTraceEntry::new(
                &class,
                &method,
                file_name,
                line_number,
            ));
        }

        let cause_error_code = u32::read_from(readable);
        let cause_class_name = if !bool::read_from(readable) {
            Some(String::read_from(readable))
        } else {
            None
        };

        Exception::new(
            code,
            &class_name,
            message,
            stack_trace_entries,
            cause_error_code,
            cause_class_name,
        )
    }
}
