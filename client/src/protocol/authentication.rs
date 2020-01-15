use crate::{
    codec::{Readable, Reader, Writeable, Writer},
    message::Payload,
    protocol::Address,
};

const AUTHENTICATION_REQUEST_MESSAGE_TYPE: u16 = 0x2;
const AUTHENTICATION_RESPONSE_MESSAGE_TYPE: u16 = 0x6B;

#[derive(Debug, Eq, PartialEq, Writer)]
pub(crate) struct AuthenticationRequest<'a> {
    username: &'a str,
    password: &'a str,
    id: Option<&'a str>,
    owner_id: Option<&'a str>,
    owner_connection: bool,
    client_type: &'a str,
    serialization_version: u8,
    client_version: &'a str,
}

impl<'a> AuthenticationRequest<'a> {
    pub(crate) fn new(
        username: &'a str,
        password: &'a str,
        client_type: &'a str,
        serialization_version: u8,
        client_version: &'a str,
    ) -> Self {
        AuthenticationRequest {
            username,
            password,
            id: None,
            owner_id: None,
            owner_connection: true,
            client_type,
            serialization_version,
            client_version,
        }
    }
}

impl<'a> Payload for AuthenticationRequest<'a> {
    fn r#type() -> u16 {
        AUTHENTICATION_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct AuthenticationResponse {
    failure: bool,
    address: Option<Address>,
    id: Option<String>,
    owner_id: Option<String>,
    _serialization_version: u8,
    _unregistered_cluster_members: Option<Vec<ClusterMember>>,
}

impl AuthenticationResponse {
    pub(crate) fn failure(&self) -> bool {
        self.failure
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.address
    }

    pub(crate) fn id(&self) -> &Option<String> {
        &self.id
    }

    pub(crate) fn owner_id(&self) -> &Option<String> {
        &self.owner_id
    }
}

impl Payload for AuthenticationResponse {
    fn r#type() -> u16 {
        AUTHENTICATION_RESPONSE_MESSAGE_TYPE
    }
}

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct ClusterMember {
    address: Address,
    id: String,
    lite: bool,
    attributes: Vec<AttributeEntry>,
}

#[derive(Debug, Eq, PartialEq, Clone, Reader)]
pub(crate) struct AttributeEntry {
    _key: String,
    _value: String,
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use super::*;

    #[test]
    fn should_write_authentication_request() {
        let request = AuthenticationRequest::new("username", "password", "Rust", 1, "1.0.0");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.username);
        assert_eq!(String::read_from(readable), request.password);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(String::read_from(readable), request.client_type);
        assert_eq!(u8::read_from(readable), request.serialization_version);
        assert_eq!(String::read_from(readable), request.client_version);
    }

    #[test]
    fn should_read_authentication_response() {
        let failure = false;
        let address = Some(Address {
            host: "localhost".to_string(),
            port: 5701,
        });
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
            AuthenticationResponse {
                failure,
                address,
                id: id.map(str::to_string),
                owner_id: owner_id.map(str::to_string),
                _serialization_version: protocol_version,
                _unregistered_cluster_members: None,
            }
        );
    }

    #[test]
    fn should_read_cluster_member() {
        let address = Address {
            host: "localhost".to_string(),
            port: 5701,
        };
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
            ClusterMember {
                address,
                id: id.to_string(),
                lite,
                attributes: vec!(),
            }
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
            AttributeEntry {
                _key: key.to_string(),
                _value: value.to_string(),
            }
        );
    }
}
