use crate::protocol::Address;

pub(crate) const CLIENT_TYPE: &str = "Rust";
pub(crate) const CLIENT_VERSION: &str = "0.1.0";
pub(crate) const SERIALIZATION_VERSION: u8 = 1;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AuthenticationRequest<'a> {
    username: &'a str,
    password: &'a str,
    id: Option<&'a str>,
    owner_id: Option<&'a str>,
    owner_connection: bool,
}

impl<'a> AuthenticationRequest<'a> {
    pub(crate) fn new(username: &'a str, password: &'a str) -> Self {
        AuthenticationRequest {
            username,
            password,
            id: None,
            owner_id: None,
            owner_connection: true,
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
        CLIENT_TYPE
    }

    pub(crate) fn serialization_version(&self) -> u8 {
        SERIALIZATION_VERSION
    }

    pub(crate) fn client_version(&self) -> &str {
        &CLIENT_VERSION
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AuthenticationResponse {
    _status: u8,
    address: Option<Address>,
    id: Option<String>,
    owner_id: Option<String>,
    _serialization_version: u8,
    _unregistered_cluster_members: Option<Vec<ClusterMember>>,
}

impl AuthenticationResponse {
    pub(crate) fn new(
        status: u8,
        address: Option<Address>,
        id: Option<String>,
        owner_id: Option<String>,
        serialization_version: u8,
        unregistered_cluster_members: Option<Vec<ClusterMember>>,
    ) -> Self {
        AuthenticationResponse {
            _status: status,
            address,
            id,
            owner_id,
            _serialization_version: serialization_version,
            _unregistered_cluster_members: unregistered_cluster_members,
        }
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

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ClusterMember {
    address: Address,
    id: String,
    lite: bool,
    attributes: Vec<AttributeEntry>,
}

impl ClusterMember {
    pub(crate) fn new(
        address: &Address,
        id: &str,
        lite: bool,
        attributes: &[AttributeEntry],
    ) -> Self {
        ClusterMember {
            address: address.clone(),
            id: id.to_string(),
            lite,
            attributes: attributes.to_vec(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct AttributeEntry {
    _key: String,
    _value: String,
}

impl AttributeEntry {
    pub(crate) fn new(key: &str, value: &str) -> Self {
        AttributeEntry {
            _key: key.to_string(),
            _value: value.to_string(),
        }
    }
}
