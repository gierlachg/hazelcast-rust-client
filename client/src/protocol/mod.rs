use crate::codec::{Readable, Reader, Writeable, Writer};

pub(crate) mod authentication;
pub mod pn_counter;

#[derive(Writer, Reader, Eq, PartialEq, Debug, Clone)]
pub(crate) struct Address {
    host: String,
    port: u32,
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use super::*;

    #[test]
    fn should_write_and_read_address() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };

        let mut writeable = BytesMut::new();
        address.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Address::read_from(readable), address);
    }
}
