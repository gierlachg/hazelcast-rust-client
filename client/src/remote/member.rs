use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    messaging::{Request, Response},
    // TODO: remove dependency to protocol ???
    protocol::Address,
    remote::{channel::Channel, CLIENT_TYPE, CLIENT_VERSION, PROTOCOL_VERSION},
    HazelcastClientError::{CommunicationFailure, InvalidCredentials},
    {Result, TryFrom},
};

pub(in crate::remote) struct Member {
    // TODO: what is the purpose of it ???
    _id: Option<String>,
    // TODO: what is the purpose of it ???
    owner_id: Option<String>,
    // TODO: what is the purpose of it ???
    address: Option<Address>,

    endpoint: String,

    sequencer: AtomicUsize,
    channel: Channel,
}

impl Member {
    pub(in crate::remote) async fn connect(endpoint: &str, username: &str, password: &str) -> Result<Self> {
        // TODO: remove dependency to protocol ???
        use crate::protocol::authentication::{AuthenticationRequest, AuthenticationResponse};

        let channel = match Channel::connect(endpoint).await {
            Ok(channel) => channel,
            Err(e) => return Err(CommunicationFailure(e)),
        };

        let request = AuthenticationRequest::new(username, password, CLIENT_TYPE, PROTOCOL_VERSION, CLIENT_VERSION);
        match channel.send((0, request).into()).await {
            Ok(response) => {
                let response = TryFrom::<AuthenticationResponse>::try_from(response)?;
                if response.failure() {
                    Err(InvalidCredentials)
                } else {
                    Ok(Member {
                        _id: response.id().clone(),
                        owner_id: response.owner_id().clone(),
                        address: response.address().clone(), // TODO: is it the same as endpoint ???
                        endpoint: endpoint.to_string(),
                        sequencer: AtomicUsize::new(1),
                        channel,
                    })
                }
            }
            Err(e) => Err(CommunicationFailure(e)),
        }
    }

    pub(in crate::remote) async fn send<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
        use std::convert::TryInto;

        let id: u64 = self
            .sequencer
            .fetch_add(1, Ordering::SeqCst)
            .try_into()
            .expect("unable to convert!");
        let message = (id, request).into();

        match self.channel.send(message).await {
            Ok(message) => TryFrom::<RS>::try_from(message),
            Err(e) => Err(CommunicationFailure(e)),
        }
    }

    pub(in crate::remote) fn address(&self) -> &Option<Address> {
        &self.address
    }
}

impl fmt::Display for Member {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Member {} - {:?}", self.endpoint, self.owner_id)
    }
}
