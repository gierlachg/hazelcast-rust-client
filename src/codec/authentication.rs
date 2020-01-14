use std::convert::TryInto;

use crate::{
    bytes::{Readable, Reader, Writeable, Writer},
    message::Payload,
    protocol::{
        authentication::{
            AttributeEntry, AuthenticationRequest, AuthenticationResponse, ClusterMember,
        },
        Address,
    },
};

const AUTHENTICATION_REQUEST_MESSAGE_TYPE: u16 = 0x2;
const AUTHENTICATION_RESPONSE_MESSAGE_TYPE: u16 = 0x6B;

impl<'a> Payload for AuthenticationRequest<'a> {
    fn r#type() -> u16 {
        AUTHENTICATION_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl<'a> Writer for AuthenticationRequest<'a> {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.username().write_to(writeable);
        self.password().write_to(writeable);

        match self.id().as_deref() {
            Some(s) => {
                false.write_to(writeable);
                s.write_to(writeable);
            }
            None => true.write_to(writeable),
        }
        match self.owner_id().as_deref() {
            Some(s) => {
                false.write_to(writeable);
                s.write_to(writeable);
            }
            None => true.write_to(writeable),
        }

        self.owner_connection().write_to(writeable);
        self.client_type().write_to(writeable);
        self.serialization_version().write_to(writeable);
        self.client_version().write_to(writeable);
    }
}

impl Payload for AuthenticationResponse {
    fn r#type() -> u16 {
        AUTHENTICATION_RESPONSE_MESSAGE_TYPE
    }
}

impl Reader for AuthenticationResponse {
    fn read_from(readable: &mut dyn Readable) -> Self {
        let status = u8::read_from(readable);
        let address = if !bool::read_from(readable) {
            Some(Address::read_from(readable))
        } else {
            None
        };
        let id = if !bool::read_from(readable) {
            Some(String::read_from(readable))
        } else {
            None
        };
        let owner_id = if !bool::read_from(readable) {
            Some(String::read_from(readable))
        } else {
            None
        };
        let serialization_version = u8::read_from(readable);

        let unregistered_cluster_member_entries = if !bool::read_from(readable) {
            let number_of_entries = u32::read_from(readable)
                .try_into()
                .expect("unable to convert!");
            let mut cluster_member_entries = Vec::with_capacity(number_of_entries);
            for _ in 0..number_of_entries {
                let address = Address::read_from(readable);
                let id = String::read_from(readable);
                let lite = bool::read_from(readable);

                let number_of_attributes = u32::read_from(readable)
                    .try_into()
                    .expect("unable to convert!");
                let mut attributes = Vec::with_capacity(number_of_attributes);
                for _ in 0..number_of_attributes {
                    let key = String::read_from(readable);
                    let value = String::read_from(readable);

                    attributes.push(AttributeEntry::new(&key, &value));
                }

                cluster_member_entries.push(ClusterMember::new(&address, &id, lite, &attributes));
            }

            Some(cluster_member_entries)
        } else {
            None
        };

        AuthenticationResponse::new(
            status,
            address,
            id,
            owner_id,
            serialization_version,
            unregistered_cluster_member_entries,
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};

    use super::*;
    use crate::{protocol::authentication::SERIALIZATION_VERSION, CLIENT_TYPE, CLIENT_VERSION};

    #[test]
    fn should_write_authentication_request() {
        let request = AuthenticationRequest::new("username", "password");

        let mut writeable = BytesMut::new();
        request.write_to(&mut writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(String::read_from(readable), request.username());
        assert_eq!(String::read_from(readable), request.password());
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(bool::read_from(readable), true);
        assert_eq!(String::read_from(readable), CLIENT_TYPE);
        assert_eq!(u8::read_from(readable), SERIALIZATION_VERSION);
        assert_eq!(String::read_from(readable), CLIENT_VERSION);
    }

    #[test]
    fn should_read_authentication_response() {
        let status: u8 = 1;
        let address = Address::new("localhost", 5701);
        let id = "id";
        let owner_id = "owner-id";

        let writeable = &mut BytesMut::new();
        status.write_to(writeable);
        false.write_to(writeable);
        address.write_to(writeable);
        false.write_to(writeable);
        id.write_to(writeable);
        false.write_to(writeable);
        owner_id.write_to(writeable);
        SERIALIZATION_VERSION.write_to(writeable);
        true.write_to(writeable);

        let readable = &mut writeable.to_bytes();
        assert_eq!(
            AuthenticationResponse::read_from(readable),
            AuthenticationResponse::new(
                status,
                Some(address),
                Some(id.to_string()),
                Some(owner_id.to_string()),
                SERIALIZATION_VERSION,
                None
            )
        );
    }
}
