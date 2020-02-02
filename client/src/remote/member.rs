use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use derive_more::Display;

use crate::{
    messaging::{Address, Request, Response},
    remote::{channel::Channel, CLIENT_TYPE, CLIENT_VERSION, PROTOCOL_VERSION},
    HazelcastClientError::{AuthenticationFailure, CommunicationFailure},
    {Result, TryFrom},
};

#[derive(Display)]
#[display(fmt = "{} - {:?}", address, owner_id)]
pub(in crate::remote) struct Member {
    _id: String,
    owner_id: String,
    address: Address,

    sender: Sender,
}

impl Member {
    pub(in crate::remote) async fn connect(endpoint: &SocketAddr, username: &str, password: &str) -> Result<Self> {
        use crate::messaging::authentication::{AuthenticationRequest, AuthenticationResponse, AuthenticationStatus};

        let channel = match Channel::connect(endpoint).await {
            Ok(channel) => channel,
            Err(e) => return Err(CommunicationFailure(e)),
        };
        let sender = Sender::new(channel);

        let request = AuthenticationRequest::new(username, password, CLIENT_TYPE, PROTOCOL_VERSION, CLIENT_VERSION);
        let response: AuthenticationResponse = sender.send(request).await?;
        match AuthenticationResponse::status(&response) {
            AuthenticationStatus::Authenticated => Ok(Member {
                _id: response.id().as_ref().expect("missing id!").clone(),
                owner_id: response.owner_id().as_ref().expect("missing owner id!").clone(),
                address: response.address().as_ref().expect("missing address!").clone(),
                sender,
            }),
            status => Err(AuthenticationFailure(status.to_string())),
        }
    }

    pub(in crate::remote) async fn send<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
        self.sender.send(request).await
    }

    pub(in crate::remote) fn address(&self) -> &Address {
        &self.address
    }
}

struct Sender {
    sequencer: AtomicUsize,
    channel: Channel,
}

impl Sender {
    fn new(channel: Channel) -> Self {
        Sender {
            sequencer: AtomicUsize::new(0),
            channel,
        }
    }

    async fn send<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
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
}
