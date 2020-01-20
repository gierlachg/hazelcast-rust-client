use crate::codec::{Reader, Writer};

pub(crate) trait Request: Writer {
    fn r#type() -> u16;

    fn partition_id(&self) -> i32 {
        -1
    }
}

pub(crate) trait Response: Reader {
    fn r#type() -> u16;
}
