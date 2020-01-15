use crate::bytes::{Readable, Reader, Writeable, Writer};

pub(crate) mod authentication;
pub mod pn_counter;

#[derive(Debug, Eq, PartialEq, Clone, Writer, Reader)]
pub(crate) struct Address {
    host: String,
    port: u32,
}

impl Address {
    pub(crate) fn new(host: String, port: u32) -> Self {
        Address { host, port }
    }
}
