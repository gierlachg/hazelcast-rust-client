use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use log::{error, info};
use tokio::sync::Mutex;

// TODO: remove dependency to protocol ???
use crate::{
    HazelcastClientError::{ClusterNonOperational, NodeNonOperational},
    messaging::{Request, Response},
    protocol::Address,
    remote::member::Member,
    Result,
};

pub(crate) struct Cluster {
    members: Arc<Members>,
}

impl Cluster {
    pub(crate) async fn connect<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
        where
            E: IntoIterator<Item=&'a str>,
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
        info!("\n\n{}\n", Cluster::format(&connected));

        if connected.is_empty() {
            Err(ClusterNonOperational)
        } else {
            let members = Arc::new(Members::new(connected, disconnected));

            // TODO: reconnecting...,
            let pinger = Pinger::new(members.clone());
            tokio::spawn(async move { pinger.run().await }); // TODO: cancel on drop

            Ok(Cluster { members })
        }
    }

    fn format(members: &HashMap<Address, Member>) -> String {
        let mut s = String::new();
        s.push_str(&format!("Members {{size: {}}} [\n", members.len()));
        for member in members.values() {
            s.push_str(&format!("\t{}\n", member));
        }
        s.push_str("]");
        s
    }

    pub(crate) async fn dispatch<RQ, RS>(&self, request: RQ) -> Result<RS>
        where
            RQ: Request,
            RS: Response,
    {
        match self.members.get().await {
            Some(member) => member.send(request).await,
            None => Err(ClusterNonOperational),
        }
    }

    pub(crate) async fn forward<RQ, RS>(&self, request: RQ, address: &Address) -> Result<RS>
        where
            RQ: Request,
            RS: Response,
    {
        match self.members.get_by_address(address).await {
            Some(member) => member.send(request).await,
            None => Err(NodeNonOperational),
        }
    }

    pub(crate) async fn address(&self, address: Option<Address>) -> Result<Address> {
        match match address {
            Some(address) => self.members.get_by_address(&address).await.map(|_| address),
            None => self.members.get().await.map(|m| m.address().clone()),
        } {
            Some(address) => Ok(address),
            None => Err(ClusterNonOperational),
        }
    }
}

// TODO: replace mutex with something like evmap ???
struct Members {
    inner: Mutex<MembersInner>,
}

impl Members {
    fn new(connected: HashMap<Address, Member>, disconnected: HashSet<String>) -> Self {
        Members {
            inner: Mutex::new(MembersInner::new(connected, disconnected)),
        }
    }

    async fn get(&self) -> Option<Arc<Member>> {
        self.inner.lock().await.get()
    }

    async fn get_by_address(&self, address: &Address) -> Option<Arc<Member>> {
        self.inner.lock().await.get_by_address(address).await
    }

    async fn get_all(&self) -> Vec<Arc<Member>> {
        self.inner.lock().await.get_all()
    }

    async fn disable(&self, address: &Address) {
        self.inner.lock().await.disable(address)
    }
}

struct MembersInner {
    connected: HashMap<Address, Arc<Member>>,
    disconnected: HashSet<String>,
    sequencer: usize,
}

impl MembersInner {
    fn new(connected: HashMap<Address, Member>, disconnected: HashSet<String>) -> Self {
        MembersInner {
            connected: connected.into_iter().map(|e| (e.0, Arc::new(e.1))).collect(),
            disconnected,
            sequencer: 0,
        }
    }

    fn get(&mut self) -> Option<Arc<Member>> {
        if self.connected.is_empty() {
            None
        } else {
            self.sequencer += 1;
            self.connected
                .values()
                .nth(self.sequencer % self.connected.len())
                .map(Arc::clone) // TODO: O(1) !?
        }
    }

    async fn get_by_address(&self, address: &Address) -> Option<Arc<Member>> {
        self.connected.get(address).map(Arc::clone)
    }

    fn get_all(&self) -> Vec<Arc<Member>> {
        self.connected.values().map(Arc::clone).collect()
    }

    fn disable(&mut self, address: &Address) {
        self.connected.remove(address);
        self.disconnected.insert(address.to_string());
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
            for member in self.members.get_all().await {
                if let Err(_) = member.send::<PingRequest, PingResponse>(PingRequest::new()).await {
                    self.members.disable(member.address()).await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_get_none_for_empty_members() {
        let connected = HashMap::new();
        let disconnected = HashSet::new();
        let members = Members::new(connected, disconnected);

        assert!(members.get().await.is_none());
        assert!(members.get_by_address(&Address::new("localhost", 5701)).await.is_none());
        assert!(members.get_all().await.is_empty());
    }

    // TODO: more tests
}
