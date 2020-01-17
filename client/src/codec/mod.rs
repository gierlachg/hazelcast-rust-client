use std::{convert::TryInto, mem};

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) trait Writer {
    fn length(&self) -> usize;

    fn write_to(&self, writeable: &mut dyn Writeable);
}

pub(crate) trait Writeable {
    fn write_bool(&mut self, value: bool);

    fn write_u8(&mut self, value: u8);

    fn write_u16(&mut self, value: u16);

    fn write_i32(&mut self, value: i32);

    fn write_u32(&mut self, value: u32);

    fn write_i64(&mut self, value: i64);

    fn write_u64(&mut self, value: u64);

    fn write_slice(&mut self, value: &[u8]);
}

pub(crate) trait Reader {
    fn read_from(readable: &mut dyn Readable) -> Self;
}

pub(crate) trait Readable {
    fn read_bool(&mut self) -> bool;

    fn read_u8(&mut self) -> u8;

    fn read_u16(&mut self) -> u16;

    fn read_i32(&mut self) -> i32;

    fn read_u32(&mut self) -> u32;

    fn read_i64(&mut self) -> i64;

    fn read_u64(&mut self) -> u64;

    fn read_slice(&mut self, len: usize) -> Bytes;

    fn skip(&mut self, len: usize);
}

impl Writer for bool {
    fn length(&self) -> usize {
        mem::size_of::<u8>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_bool(*self);
    }
}

impl Writer for u8 {
    fn length(&self) -> usize {
        mem::size_of::<u8>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u8(*self);
    }
}

impl Writer for u16 {
    fn length(&self) -> usize {
        mem::size_of::<u16>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u16(*self);
    }
}

impl Writer for i32 {
    fn length(&self) -> usize {
        mem::size_of::<i32>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_i32(*self);
    }
}

impl Writer for u32 {
    fn length(&self) -> usize {
        mem::size_of::<u32>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u32(*self);
    }
}

impl Writer for i64 {
    fn length(&self) -> usize {
        mem::size_of::<i64>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_i64(*self);
    }
}

impl Writer for u64 {
    fn length(&self) -> usize {
        mem::size_of::<u64>()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u64(*self);
    }
}

impl Writer for [u8] {
    fn length(&self) -> usize {
        self.len()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_slice(self);
    }
}

impl Writer for &str {
    fn length(&self) -> usize {
        mem::size_of::<u32>() + self.len()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len: u32 = self.len().try_into().expect("unable to convert!");
        len.write_to(writeable);
        self.as_bytes().write_to(writeable);
    }
}

impl Writer for String {
    fn length(&self) -> usize {
        mem::size_of::<u32>() + self.len()
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len: u32 = self.len().try_into().expect("unable to convert!");
        len.write_to(writeable);
        self.as_bytes().write_to(writeable);
    }
}

impl<T: Writer> Writer for Option<T> {
    fn length(&self) -> usize {
        mem::size_of::<u8>() + self.as_ref().map(|v| v.length()).unwrap_or(0)
    }

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

impl<T: Writer> Writer for &[T] {
    fn length(&self) -> usize {
        mem::size_of::<u32>() + self.len() * self.first().map(|v| v.length()).unwrap_or(0)
    }

    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len: u32 = self.len().try_into().expect("unable to convert!");
        len.write_to(writeable);
        for item in self.iter() {
            item.write_to(writeable);
        }
    }
}

impl Reader for bool {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_bool()
    }
}

impl Reader for u8 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_u8()
    }
}

impl Reader for u16 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_u16()
    }
}

impl Reader for i32 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_i32()
    }
}

impl Reader for u32 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_u32()
    }
}

impl Reader for i64 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_i64()
    }
}

impl Reader for u64 {
    fn read_from(readable: &mut dyn Readable) -> Self {
        readable.read_u64()
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

impl<T: Reader> Reader for Option<T> {
    fn read_from(readable: &mut dyn Readable) -> Self {
        if !bool::read_from(readable) {
            Some(T::read_from(readable))
        } else {
            None
        }
    }
}

impl<T: Reader> Reader for Vec<T> {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let len = u32::read_from(readable).try_into().expect("unable to convert!");
        let mut items = Vec::with_capacity(len);
        for _ in 0..len {
            items.push(T::read_from(readable));
        }
        items
    }
}

impl Writeable for BytesMut {
    fn write_bool(&mut self, value: bool) {
        if value {
            self.put_u8(1);
        } else {
            self.put_u8(0);
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.put_u8(value);
    }

    fn write_u16(&mut self, value: u16) {
        self.put_u16_le(value);
    }

    fn write_i32(&mut self, value: i32) {
        self.put_i32_le(value);
    }

    fn write_u32(&mut self, value: u32) {
        self.put_u32_le(value);
    }

    fn write_i64(&mut self, value: i64) {
        self.put_i64_le(value);
    }

    fn write_u64(&mut self, value: u64) {
        self.put_u64_le(value);
    }

    fn write_slice(&mut self, value: &[u8]) {
        self.put(value);
    }
}

impl Readable for Bytes {
    fn read_bool(&mut self) -> bool {
        self.read_u8() > 0
    }

    fn read_u8(&mut self) -> u8 {
        self.get_u8()
    }

    fn read_u16(&mut self) -> u16 {
        self.get_u16_le()
    }

    fn read_i32(&mut self) -> i32 {
        self.get_i32_le()
    }

    fn read_u32(&mut self) -> u32 {
        self.get_u32_le()
    }

    fn read_i64(&mut self) -> i64 {
        self.get_i64_le()
    }

    fn read_u64(&mut self) -> u64 {
        self.get_u64_le()
    }

    fn read_slice(&mut self, len: usize) -> Bytes {
        self.split_to(len)
    }

    fn skip(&mut self, len: usize) {
        self.advance(len);
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use bytes::{Buf, BytesMut};

    use super::*;

    #[test]
    fn should_write_and_read_bool() {
        let writeable = &mut BytesMut::new();
        true.write_to(writeable);
        false.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), false);
    }

    #[test]
    fn should_write_and_read_u8() {
        let writeable = &mut BytesMut::new();
        1u8.write_to(writeable);
        0u8.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(u8::read_from(readable), 1);
        assert_eq!(u8::read_from(readable), 0);
    }

    #[test]
    fn should_write_and_read_u16() {
        let writeable = &mut BytesMut::new();
        1u16.write_to(writeable);
        0u16.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(u16::read_from(readable), 1);
        assert_eq!(u16::read_from(readable), 0);
    }

    #[test]
    fn should_write_and_read_i32() {
        let writeable = &mut BytesMut::new();
        (-1i32).write_to(writeable);
        1i32.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(i32::read_from(readable), -1);
        assert_eq!(i32::read_from(readable), 1);
    }

    #[test]
    fn should_write_and_read_u32() {
        let writeable = &mut BytesMut::new();
        1u32.write_to(writeable);
        0u32.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(u32::read_from(readable), 1);
        assert_eq!(u32::read_from(readable), 0);
    }

    #[test]
    fn should_write_and_read_i64() {
        let writeable = &mut BytesMut::new();
        (-1i64).write_to(writeable);
        1i64.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(i64::read_from(readable), -1);
        assert_eq!(i64::read_from(readable), 1);
    }

    #[test]
    fn should_write_and_read_u64() {
        let writeable = &mut BytesMut::new();
        1u64.write_to(writeable);
        0u64.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(u64::read_from(readable), 1);
        assert_eq!(u64::read_from(readable), 0);
    }

    #[test]
    fn should_write_and_read_slice() {
        let writeable = &mut BytesMut::new();
        [1, 0].write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(readable.read_slice(1)[..], [1]);
        assert_eq!(readable.read_slice(1)[..], [0]);
    }

    #[test]
    fn should_skip() {
        let writeable = &mut BytesMut::new();
        [1, 0, 1].write_to(writeable);

        let readable = &mut writeable.to_bytes();
        readable.skip(1);
        assert_eq!(readable.read_slice(2)[..], [0, 1]);
    }

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
}
