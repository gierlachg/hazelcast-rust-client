use crate::{Result, TryFrom};
use crate::message::Message;
// TODO: remove dependency to protocol ???
use crate::protocol::{
    Address,
    authentication::{AuthenticationRequest, AuthenticationResponse},
};
use crate::remote::channel::Channel;

pub(crate) struct Connection {
    // TODO: what is the purpose of it ???
    _id: Option<String>,
    // TODO: what is the purpose of it ???
    _owner_id: Option<String>,
    // TODO: what is the purpose of it ???
    address: Option<Address>,

    channel: Channel,
}

impl Connection {
    pub(crate) async fn new(endpoint: &str, username: &str, password: &str) -> Result<Self> {
        let channel = Channel::connect(endpoint).await?;

        let request = AuthenticationRequest::new(username, password).into();
        let response = channel.send(request).await?;

        match TryFrom::<AuthenticationResponse>::try_from(response) {
            Ok(response) => {
                // TODO: check status & serialization version ???
                Ok(Connection {
                    _id: response.id().clone(),
                    _owner_id: response.owner_id().clone(),
                    address: response.address().clone(),
                    channel,
                })
            }
            Err(exception) => {
                eprintln!("{}", exception); // TODO: propagate ???
                Err("Unable to create connection.".into())
            }
        }
    }

    pub(crate) async fn send(&self, message: Message) -> Result<Message> {
        self.channel.send(message).await
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.address
    }
}
