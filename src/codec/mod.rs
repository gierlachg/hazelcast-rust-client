use crate::bytes::{Readable, Reader, Writeable, Writer};
use crate::protocol::Address;

mod authentication;
mod pn_counter;

impl Writer for Address {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.host().write_to(writeable);
        self.port().write_to(writeable);
    }
}

impl Reader for Address {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let host = String::read_from(readable);
        let port = u32::read_from(readable);

        Address::new(&host, port)
    }
}
