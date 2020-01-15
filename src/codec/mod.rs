use std::convert::TryInto;

use crate::{
    bytes::{Readable, Reader, Writeable, Writer},
    protocol::Address,
};

mod authentication;
mod pn_counter;

impl Writer for &str {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len = self.len().try_into().expect("unable to convert!");
        writeable.write_u32(len);
        writeable.write_slice(self.as_bytes());
    }
}

impl Reader for String {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let len = readable.read_u32().try_into().expect("unable to convert!");
        std::str::from_utf8(&readable.read_slice(len))
            .expect("unable to parse utf8 string!")
            .to_string()
    }
}

impl<T: Writer> Writer for Option<T> {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        match self {
            Some(value) => {
                false.write_to(writeable);
                value.write_to(writeable);
            }
            None => true.write_to(writeable),
        }
    }
}

impl<T: Reader> Reader for Option<T> {
    fn read_from(readable: &mut dyn Readable) -> Self {
        if !bool::read_from(readable) {
            Some(T::read_from(readable))
        } else {
            None
        }
    }
}

impl<T: Writer> Writer for &[T] {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len: u32 = self.len().try_into().expect("unable to convert!");
        len.write_to(writeable);
        for item in self.iter() {
            item.write_to(writeable);
        }
    }
}

impl<T: Reader> Reader for Vec<T> {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let len = u32::read_from(readable)
            .try_into()
            .expect("unable to convert!");
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::read_from(readable));
        }
        items
    }
}

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
    use std::ops::Deref;

    #[test]
    fn should_write_and_read_str() {
        let writeable = &mut BytesMut::new();
        "10".write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), "10");
    }

    #[test]
    fn should_write_and_read_option() {
        let writeable = &mut BytesMut::new();
        Some(1u32).write_to(writeable);
        Option::<u32>::None.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Option::read_from(readable), Some(1u32));
        assert_eq!(Option::<u32>::read_from(readable), None);
    }

    #[test]
    fn should_write_and_read_vec() {
        let writeable = &mut BytesMut::new();
        vec![1u32].deref().write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Vec::<u32>::read_from(readable), vec!(1u32));
    }

    #[test]
    fn should_write_and_read_address() {
        let address = Address::new("localhost", 5701);

        let mut writeable = BytesMut::new();
        address.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(Address::read_from(readable), address);
    }
}
