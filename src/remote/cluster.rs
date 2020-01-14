use std::{
    fmt::{self, Display, Formatter},
    sync::atomic::{AtomicUsize, Ordering},
};

use log::{error, info};

use crate::{message::Message, protocol::Address, remote::member::Member, Result};

pub(crate) struct Cluster {
    counter: AtomicUsize,
    members: Vec<Member>,
}

impl Cluster {
    pub(crate) async fn from<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = &'a str>,
    {
        let mut members = vec![];
        for endpoint in endpoints {
            info!("Trying to connect to {} as owner member.", endpoint);
            match Member::connect(endpoint, username, password).await {
                Ok(member) => members.push(member),
                Err(e) => error!("Failed to connect to {} - {}", endpoint, e),
            }
        }

        if members.is_empty() {
            Err("Unable to connect any member.".into())
        } else {
            Ok(Cluster {
                counter: AtomicUsize::new(0),
                members,
            })
        }
    }

    pub(crate) async fn dispatch(&self, message: Message) -> Result<Message> {
        // TODO: accepting & dispatching by address ???
        let value = self.counter.fetch_add(1, Ordering::SeqCst);
        self.members[value % self.members.len()].send(message).await
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.members[0].address() // TODO: ???
    }
}

impl Display for Cluster {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "\n\nMembers {{size: {}}} [\n",
            self.members.len(),
        )?;
        for member in &self.members {
            write!(formatter, "\t{}\n", member)?;
        }
        write!(formatter, "]\n")
    }
}
