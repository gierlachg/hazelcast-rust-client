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

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::protocol::Address;

    use super::*;

    #[test]
    fn should_write_and_read_address() {
        let address = Address::new("localhost", 5701);

        let mut writeable = BytesMut::new();
        address.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Address::read_from(readable), address);
    }
}
