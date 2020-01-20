use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

use log::{error, info};

use crate::{
    HazelcastClientError::ClusterNonOperational,
    messaging::{Message, Response},
    // TODO: remove dependency to protocol ???
    protocol::Address,
    remote::member::Member,
    Result,
    TryFrom,
};

pub(crate) struct Cluster {
    members: Members,
}

impl Cluster {
    pub(crate) async fn connect<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
        where
            E: IntoIterator<Item=&'a str>,
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
            Err(ClusterNonOperational)
        } else {
            Ok(Cluster { members: Members::new(members) })
        }
    }

    pub(crate) async fn dispatch<I, O>(&self, message: I) -> Result<O>
        where
            I: Into<Message>,
            O: Response,
    {
        match self.members.next() {
            Some(member) => {
                match member.send(message.into()).await {
                    Ok(message) => TryFrom::<O>::try_from(message),
                    Err(e) => Err(e),
                }
            }
            None => Err(ClusterNonOperational)
        }
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.members.members[0].address() // TODO: !?!?!?
    }
}

impl fmt::Display for Cluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "\n\n{}\n", self.members)
    }
}

struct Members {
    sequence: AtomicUsize,
    members: Vec<Member>,
}

impl Members {
    fn new(members: Vec<Member>) -> Self {
        Members { sequence: AtomicUsize::new(1), members }
    }

    fn next(&self) -> Option<&Member> {
        if self.members.is_empty() {
            None
        } else {
            let index = self.sequence.fetch_add(1, Ordering::SeqCst) % self.members.len();
            Some(&self.members[index])
        }
    }
}

impl fmt::Display for Members {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Members {{size: {}}} [\n", self.members.len(), )?;
        for member in &self.members {
            write!(formatter, "\t{}\n", member)?;
        }
        write!(formatter, "]")
    }
}