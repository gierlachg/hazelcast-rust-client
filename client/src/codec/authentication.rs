use crate::{
    message::Payload,
    protocol::authentication::{AuthenticationRequest, AuthenticationResponse},
};

const AUTHENTICATION_REQUEST_MESSAGE_TYPE: u16 = 0x2;
const AUTHENTICATION_RESPONSE_MESSAGE_TYPE: u16 = 0x6B;

impl<'a> Payload for AuthenticationRequest<'a> {
    fn r#type() -> u16 {
        AUTHENTICATION_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Payload for AuthenticationResponse {
    fn r#type() -> u16 {
        AUTHENTICATION_RESPONSE_MESSAGE_TYPE
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use super::*;
    use crate::{
        bytes::{Reader, Writer},
        protocol::{
            authentication::{AttributeEntry, ClusterMember},
            Address,
        },
    };

    #[test]
    fn should_write_authentication_request() {
        let request = AuthenticationRequest::new("username", "password", "Rust", 1, "1.0.0");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.username());
        assert_eq!(String::read_from(readable), request.password());
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(String::read_from(readable), request.client_type());
        assert_eq!(u8::read_from(readable), request.serialization_version());
        assert_eq!(String::read_from(readable), request.client_version());
    }

    #[test]
    fn should_read_authentication_response() {
        let failure = false;
        let address = Some(Address::new("localhost".to_string(), 5701));
        let id = Some("id");
        let owner_id = Some("owner-id");
        let protocol_version = 1;

        let writeable = &mut BytesMut::new();
        failure.write_to(writeable);
        address.write_to(writeable);
        id.write_to(writeable);
        owner_id.write_to(writeable);
        protocol_version.write_to(writeable);
        true.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            AuthenticationResponse::read_from(readable),
            AuthenticationResponse::new(
                failure,
                address,
                id.map(str::to_string),
                owner_id.map(str::to_string),
                protocol_version,
                None,
            )
        );
    }

    #[test]
    fn should_read_cluster_member() {
        let address = Address::new("localhost".to_string(), 5701);
        let id = "id";
        let lite = true;

        let writeable = &mut BytesMut::new();
        address.write_to(writeable);
        id.write_to(writeable);
        lite.write_to(writeable);
        0u32.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            ClusterMember::read_from(readable),
            ClusterMember::new(address, id.to_string(), lite, vec!())
        );
    }

    #[test]
    fn should_read_attribute() {
        let key = "key";
        let value = "value";

        let writeable = &mut BytesMut::new();
        key.write_to(writeable);
        value.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            AttributeEntry::read_from(readable),
            AttributeEntry::new(key.to_string(), value.to_string())
        );
    }
}
