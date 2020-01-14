use std::sync::atomic::{AtomicUsize, Ordering};

use crate::message::Message;
use crate::protocol::Address;
use crate::remote::connection::Connection;
use crate::Result;

pub(crate) struct Cluster {
    counter: AtomicUsize,
    connections: Vec<Connection>,
}

impl Cluster {
    pub(crate) async fn new<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
        where E: IntoIterator<Item=&'a str>
    {
        let mut connections = vec!();
        for endpoint in endpoints {
            match Connection::new(endpoint, username, password).await {
                Ok(connection) => connections.push(connection),
                Err(_) => {} // TODO: log ???
            }
        }

        if connections.is_empty() {
            Err("Unable to connect any member.".into())
        } else {
            Ok(Cluster {
                counter: AtomicUsize::new(0),
                connections,
            })
        }
    }

    pub(crate) async fn dispatch(&self, message: Message) -> Result<Message> { // TODO: accepting & dispatching by address ???
        let value = self.counter.fetch_add(1, Ordering::SeqCst);
        self.connections[value % self.connections.len()].send(message).await
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.connections[0].address() // TODO: ???
    }
}