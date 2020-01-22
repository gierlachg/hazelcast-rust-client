use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{error, info};

// TODO: remove dependency to protocol ???
use crate::{
    messaging::{Request, Response},
    protocol::Address,
    remote::member::Member,
    HazelcastClientError::{ClusterNonOperational, NodeNonOperational},
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
        let mut connected = HashMap::new();
        let mut disconnected = HashSet::new();
        for endpoint in endpoints.into_iter().map(|e| e.into()).collect::<HashSet<String>>() {
            info!("Trying to connect to {} as owner member.", endpoint);
            match Member::connect(&endpoint, username, password).await {
                Ok(member) => {
                    connected.insert(member.address().clone(), member);
                }
                Err(e) => {
                    error!("Failed to connect to {} - {}", endpoint, e);
                    disconnected.insert(endpoint);
                }
            }
        }

        if connected.is_empty() {
            Err(ClusterNonOperational)
        } else {
            let members = Arc::new(Members::new(connected, disconnected));

            let pinger = Pinger::new(members.clone());
            tokio::spawn(async move { pinger.run().await }); // TODO: cancel on drop

            Ok(Cluster { members })
        }
    }

    pub(crate) async fn dispatch<RQ, RS>(&self, request: RQ) -> Result<RS>
    where
        RQ: Request,
        RS: Response,
    {
        match self.members.get() {
            Some(member) => member.send(request).await,
            None => Err(ClusterNonOperational),
        }
    }

    pub(crate) async fn forward<RQ, RS>(&self, request: RQ, address: &Address) -> Result<RS>
    where
        RQ: Request,
        RS: Response,
    {
        match self.members.get_by_address(address) {
            Some(member) => member.send(request).await,
            None => Err(NodeNonOperational),
        }
    }

    pub(crate) fn address(&self, address: Option<Address>) -> Result<Address> {
        address
            .map(|address| self.members.get_by_address(&address).map(|_| address))
            .unwrap_or_else(|| self.members.get().map(|m| m.address().clone()))
            .ok_or(ClusterNonOperational)
    }
}

impl fmt::Display for Cluster {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "\n\n{}\n", self.members)
    }
}

struct Members {
    connected: HashMap<Address, Member>,
    _disconnected: HashSet<String>,

    sequencer: AtomicUsize,
}

impl Members {
    fn new(connected: HashMap<Address, Member>, disconnected: HashSet<String>) -> Self {
        Members {
            connected,
            _disconnected: disconnected,
            sequencer: AtomicUsize::new(0),
        }
    }

    fn get(&self) -> Option<&Member> {
        if self.connected.is_empty() {
            None
        } else {
            let nth = self.sequencer.fetch_add(1, Ordering::SeqCst) % self.connected.len();
            self.connected.values().nth(nth)
        }
    }

    fn get_by_address(&self, address: &Address) -> Option<&Member> {
        self.connected.get(address)
    }

    fn get_all(&self) -> Vec<&Member> {
        self.connected.values().collect()
    }
}

impl fmt::Display for Members {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connected = &self.connected;
        write!(formatter, "Members {{size: {}}} [\n", connected.len(),)?;
        for member in connected.values() {
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
            for member in self.members.get_all() {
                let _ = member.send::<PingRequest, PingResponse>(PingRequest::new()).await;
            }
        }
    }
}
