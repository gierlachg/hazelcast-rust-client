use std::{
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{error, info};

use crate::{
    messaging::{Request, Response},
    // TODO: remove dependency to protocol ???
    protocol::Address,
    remote::member::Member,
    HazelcastClientError::ClusterNonOperational,
    Result,
};

pub(crate) struct Cluster {
    members: Arc<Members>,
}

impl Cluster {
    pub(crate) async fn connect<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
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
            Err(ClusterNonOperational)
        } else {
            let members = Arc::new(Members::new(members));

            let pinger = Pinger::new(members.clone());
            tokio::spawn(async move { pinger.run().await }); // TODO: cancel on drop

            Ok(Cluster { members })
        }
    }

    // TODO: dispatch based on address ???
    pub(crate) async fn dispatch<RQ: Request, RS: Response>(&self, request: RQ) -> Result<RS> {
        match self.members.next() {
            Some(member) => member.send(request).await,
            None => Err(ClusterNonOperational),
        }
    }

    pub(crate) fn address(&self) -> &Option<Address> {
        &self.members.connected().next().unwrap().address() // TODO: !?!?!?
    }
}

impl fmt::Display for Cluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "\n\n{}\n", self.members)
    }
}

struct Members {
    sequence: AtomicUsize,
    connected: Vec<Member>,
}

impl Members {
    fn new(members: Vec<Member>) -> Self {
        Members {
            sequence: AtomicUsize::new(1),
            connected: members,
        }
    }

    fn next(&self) -> Option<&Member> {
        let connected = &self.connected;
        if connected.is_empty() {
            None
        } else {
            let index = self.sequence.fetch_add(1, Ordering::SeqCst) % connected.len();
            Some(&connected[index])
        }
    }

    fn connected(&self) -> impl Iterator<Item = &Member> {
        self.connected.iter()
    }
}

impl fmt::Display for Members {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connected = &self.connected;
        write!(formatter, "Members {{size: {}}} [\n", connected.len(),)?;
        for member in connected {
            write!(formatter, "\t{}\n", member)?;
        }
        write!(formatter, "]")
    }
}

const PING_INTERVAL: Duration = Duration::from_secs(300);

struct Pinger {
    members: Arc<Members>,
}

impl Pinger {
    fn new(members: Arc<Members>) -> Self {
        Pinger { members }
    }

    async fn run(&self) {
        use tokio::stream::StreamExt;
        // TODO: remove dependency to protocol ???
        use crate::protocol::ping::{PingRequest, PingResponse};

        let mut interval = tokio::time::interval(PING_INTERVAL);
        loop {
            interval.next().await;
            for member in self.members.connected() {
                let _ = member.send::<PingRequest, PingResponse>(PingRequest::new()).await;
            }
        }
    }
}
