use crate::message::Message;
use crate::{Result, TryFrom};

// TODO: remove dependency to protocol ???
use crate::protocol::{
    authentication::{AuthenticationRequest, AuthenticationResponse},
    Address,
};
use crate::remote::channel::Channel;

pub(crate) struct Connection {
    // TODO: what is the purpose of it ???
    _id: Option<String>,
    // TODO: what is the purpose of it ???
    _owner_id: Option<String>,
    // TODO: what is the purpose of it ???
    address: Option<Address>,

    broker: Channel,
}

impl Connection {
    pub(crate) async fn create(address: &str, username: &str, password: &str) -> Result<Self> {
        let mut broker = Channel::connect(address).await?;

        let request = AuthenticationRequest::new(username, password).into();
        let response = broker.send(request).await?;

        match TryFrom::<AuthenticationResponse>::try_from(response) {
            Ok(response) => {
                // TODO: check status & serialization version ???
                Ok(Connection {
                    _id: response.id().clone(),
                    _owner_id: response.owner_id().clone(),
                    address: response.address().clone(),
                    broker,
                })
            }
            Err(exception) => {
                eprintln!("{}", exception); // TODO: propagate
                Err("Unable to create connection.".into())
            }
        }
    }

    pub(crate) async fn send(&mut self, message: Message) -> Result<Message> {
        Ok(self.broker.send(message).await?)
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.address
    }
}
