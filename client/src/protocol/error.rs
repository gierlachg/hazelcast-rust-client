use std::{error::Error, fmt};

#[derive(Response, Eq, PartialEq)]
#[r#type = 0x6D]
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

#[derive(Reader, Eq, PartialEq, Debug)]
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

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

    use super::*;

    #[test]
    fn should_read_exception() {
        let code = 128;
        let class_name = "NullPointerException";
        let message = Some("null");
        let cause_error_code = 321;
        let cause_class_name = Some("CauseClassName");

        let writeable = &mut BytesMut::new();
        code.write_to(writeable);
        class_name.write_to(writeable);
        message.write_to(writeable);
        0u32.write_to(writeable);
        cause_error_code.write_to(writeable);
        cause_class_name.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            Exception::read_from(readable),
            Exception {
                code,
                class_name: class_name.to_string(),
                message: message.map(str::to_string),
                stack_trace: vec!(),
                cause_error_code,
                cause_class_name: cause_class_name.map(str::to_string),
            }
        );
    }

    #[test]
    fn should_read_stack_trace_entry() {
        let declaring_class = "NullPointerException";
        let method_name = "some-method";
        let file_name = Some("NullPointerException.java");
        let line_number = 999;

        let writeable = &mut BytesMut::new();
        declaring_class.write_to(writeable);
        method_name.write_to(writeable);
        file_name.write_to(writeable);
        line_number.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            StackTraceEntry::read_from(readable),
            StackTraceEntry {
                declaring_class: declaring_class.to_string(),
                method_name: method_name.to_string(),
                file_name: file_name.map(str::to_string),
                line_number,
            }
        );
    }
}
