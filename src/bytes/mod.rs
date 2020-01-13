use std::convert::TryInto;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) trait Writer {
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

    fn read(&mut self) -> Bytes;

    fn skip(&mut self, len: usize);
}

impl Writer for bool {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_bool(*self);
    }
}

impl Writer for u8 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u8(*self);
    }
}

impl Writer for u16 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u16(*self);
    }
}

impl Writer for i32 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_i32(*self);
    }
}

impl Writer for u32 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u32(*self);
    }
}

impl Writer for i64 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_i64(*self);
    }
}

impl Writer for u64 {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_u64(*self);
    }
}

impl Writer for [u8] {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        writeable.write_slice(self);
    }
}

impl Writer for &str {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        let len = self.len().try_into().expect("unable to convert!");
        writeable.write_u32(len);
        writeable.write_slice(self.as_bytes());
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

    fn read(&mut self) -> Bytes {
        self.split_to(self.len())
    }

    fn skip(&mut self, len: usize) {
        let _ = self.split_to(len);
    }
}
