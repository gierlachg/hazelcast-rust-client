use std::fmt::{self, Display, Formatter};

use crate::{
    // TODO: remove dependency to protocol ???
    message::Message,
    protocol::{
        authentication::{AuthenticationRequest, AuthenticationResponse},
        Address,
    },
    remote::channel::Channel,
    {Result, TryFrom},
};

pub(crate) struct Member {
    // TODO: what is the purpose of it ???
    _id: Option<String>,
    // TODO: what is the purpose of it ???
    owner_id: Option<String>,
    // TODO: what is the purpose of it ???
    address: Option<Address>,

    endpoint: String,
    channel: Channel,
}

impl Member {
    pub(crate) async fn connect(endpoint: &str, username: &str, password: &str) -> Result<Self> {
        let channel = Channel::connect(endpoint).await?;

        let request = AuthenticationRequest::new(username, password).into();
        let response = channel.send(request).await?;

        match TryFrom::<AuthenticationResponse>::try_from(response) {
            Ok(response) => {
                // TODO: check status & serialization version ???
                Ok(Member {
                    _id: response.id().clone(),
                    owner_id: response.owner_id().clone(),
                    address: response.address().clone(), // TODO: is it the same as endpoint ???
                    endpoint: endpoint.to_string(),
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

impl Display for Member {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Member {} - {:?}", self.endpoint, self.owner_id)
    }
}
