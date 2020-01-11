use std::convert::TryInto;

use crate::bytes::{Readable, Reader, Writeable, Writer};
use crate::message::Payload;
use crate::protocol::{
    authentication::{
        AttributeEntry, AuthenticationRequest, AuthenticationResponse, ClusterMember,
    },
    Address,
};

const AUTHENTICATION_REQUEST_MESSAGE_TYPE: u16 = 0x2;
const AUTHENTICATION_RESPONSE_MESSAGE_TYPE: u16 = 0x6B;

impl Payload for AuthenticationRequest {
    fn r#type() -> u16 {
        AUTHENTICATION_REQUEST_MESSAGE_TYPE
    }

    // TODO: partition
}

impl Writer for AuthenticationRequest {
    fn write_to(&self, writeable: &mut dyn Writeable) {
        self.username().write_to(writeable);
        self.password().write_to(writeable);

        match self.id().as_deref() {
            Some(s) => {
                true.write_to(writeable);
                s.write_to(writeable);
            }
            None => false.write_to(writeable),
        }
        match self.owner_id().as_deref() {
            Some(s) => {
                true.write_to(writeable);
                s.write_to(writeable);
            }
            None => false.write_to(writeable),
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
