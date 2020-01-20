use crate::{
    codec::{Readable, Reader, Writeable, Writer},
    messaging::{Request, Response},
    protocol::{Address, ClusterMember},
};

#[derive(Request, Eq, PartialEq, Debug)]
#[r#type = 0x2]
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

#[derive(Response, Eq, PartialEq, Debug)]
#[r#type = 0x6B]
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
}
