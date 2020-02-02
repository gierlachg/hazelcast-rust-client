#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0xf]
pub(crate) struct PingRequest {}

impl PingRequest {
    pub(crate) fn new() -> Self {
        PingRequest {}
    }
}

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x64]
pub(crate) struct PingResponse {}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use crate::codec::{Reader, Writer};

    use super::*;

    #[test]
    fn should_write_ping_request() {
        let request = PingRequest::new();

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(readable.bytes(), []);
    }

    #[test]
    fn should_read_ping_response() {
        let readable = &mut BytesMut::new().to_bytes();
        assert_eq!(PingResponse::read_from(readable), PingResponse {});
    }
}
