use std::sync::atomic::{AtomicUsize, Ordering};

use derive_more::Display;

// TODO: remove dependency to protocol ???
use crate::{
    messaging::{Request, Response},
    protocol::{authentication::AuthenticationStatus, Address},
    remote::{channel::Channel, CLIENT_TYPE, CLIENT_VERSION, PROTOCOL_VERSION},
    HazelcastClientError::{AuthenticationFailure, CommunicationFailure},
    {Result, TryFrom},
};

#[derive(Display)]
#[display(fmt = "Member {} - {:?}", address, owner_id)]
pub(in crate::remote) struct Member {
    _id: String,
    owner_id: String,
    address: Address,

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
                match AuthenticationResponse::status(&response) {
                    AuthenticationStatus::Authenticated => Ok(Member {
                        _id: response.id().as_ref().expect("missing id!").clone(),
                        owner_id: response.owner_id().as_ref().expect("missing owner id!").clone(),
                        address: response.address().as_ref().expect("missing address!").clone(),
                        sequencer: AtomicUsize::new(1),
                        channel,
                    }),
                    status => Err(AuthenticationFailure(status.to_string())),
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

    pub(in crate::remote) fn address(&self) -> &Address {
        &self.address
    }
}
