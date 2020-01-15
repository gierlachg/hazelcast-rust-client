use crate::{
    bytes::{Readable, Reader, Writeable, Writer},
    protocol::Address,
};

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

#[allow(dead_code)]
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

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    pub(crate) fn id(&self) -> Option<&'a str> {
        self.id
    }

    pub(crate) fn owner_id(&self) -> Option<&'a str> {
        self.owner_id
    }

    pub(crate) fn owner_connection(&self) -> bool {
        self.owner_connection
    }

    pub(crate) fn client_type(&self) -> &str {
        &self.client_type
    }

    pub(crate) fn serialization_version(&self) -> u8 {
        self.serialization_version
    }

    pub(crate) fn client_version(&self) -> &str {
        &self.client_version
    }
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
    pub(crate) fn new(
        failure: bool,
        address: Option<Address>,
        id: Option<String>,
        owner_id: Option<String>,
        serialization_version: u8,
        unregistered_cluster_members: Option<Vec<ClusterMember>>,
    ) -> Self {
        AuthenticationResponse {
            failure,
            address,
            id,
            owner_id,
            _serialization_version: serialization_version,
            _unregistered_cluster_members: unregistered_cluster_members,
        }
    }

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

#[derive(Debug, Eq, PartialEq, Reader)]
pub(crate) struct ClusterMember {
    address: Address,
    id: String,
    lite: bool,
    attributes: Vec<AttributeEntry>,
}

impl ClusterMember {
    pub(crate) fn new(
        address: Address,
        id: String,
        lite: bool,
        attributes: Vec<AttributeEntry>,
    ) -> Self {
        ClusterMember {
            address,
            id,
            lite,
            attributes,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Reader)]
pub(crate) struct AttributeEntry {
    _key: String,
    _value: String,
}

impl AttributeEntry {
    pub(crate) fn new(key: String, value: String) -> Self {
        AttributeEntry {
            _key: key,
            _value: value,
        }
    }
}
