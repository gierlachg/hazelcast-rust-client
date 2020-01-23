use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{error, info};
use tokio::sync::RwLock;

// TODO: remove dependency to protocol ???
use crate::{
    messaging::{Request, Response},
    protocol::Address,
    remote::member::Member,
    HazelcastClientError::{ClusterNonOperational, NodeNonOperational},
    Result,
};

pub(crate) struct Cluster {
    members: Arc<Registry<Address, Member>>,
}

impl Cluster {
    pub(crate) async fn connect<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = &'a SocketAddr>,
    {
        let mut enabled = HashMap::new();
        let mut disabled = HashSet::new();
        for endpoint in endpoints.into_iter().collect::<HashSet<&SocketAddr>>() {
            info!("Trying to connect to {} as owner member.", endpoint);
            match Member::connect(&endpoint, username, password).await {
                Ok(member) => {
                    enabled.insert(member.address().clone(), member);
                }
                Err(e) => {
                    error!("Failed to connect to {} - {}", endpoint, e);
                    disabled.insert(endpoint.into());
                }
            }
        }
        info!("\n\n{}\n", Cluster::format(&enabled));

        if enabled.is_empty() {
            Err(ClusterNonOperational)
        } else {
            let members = Arc::new(Registry::new(enabled, disabled));

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
        match self.members.get_by(address).await {
            Some(member) => member.send(request).await,
            None => Err(NodeNonOperational),
        }
    }

    pub(crate) async fn address(&self, address: Option<Address>) -> Result<Address> {
        match match match address {
            Some(address) => self.members.get_by(&address).await.map(|_| address),
            None => None,
        } {
            Some(address) => Some(address),
            None => self.members.get().await.map(|member| member.address().clone()),
        } {
            Some(address) => Ok(address),
            None => Err(ClusterNonOperational),
        }
    }
}

struct Registry<K, V> {
    inner: RwLock<RegistryInner<K, V>>,
}

impl<K, V> Registry<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(enabled: HashMap<K, V>, disabled: HashSet<K>) -> Self {
        Registry {
            inner: RwLock::new(RegistryInner::new(enabled, disabled)),
        }
    }

    async fn get(&self) -> Option<Arc<V>> {
        self.inner.read().await.get()
    }

    async fn get_by(&self, key: &K) -> Option<Arc<V>> {
        self.inner.read().await.get_by(key).await
    }

    async fn get_all(&self) -> Vec<Arc<V>> {
        self.inner.read().await.get_all()
    }

    async fn disable(&self, key: &K) {
        self.inner.write().await.disable(key)
    }
}

struct RegistryInner<K, V> {
    vec: Vec<(K, Arc<V>)>,
    map: HashMap<K, Arc<V>>,
    disabled: HashSet<K>,
    sequencer: AtomicUsize,
}

impl<K, V> RegistryInner<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(enabled: HashMap<K, V>, disabled: HashSet<K>) -> Self {
        let vec: Vec<(K, Arc<V>)> = enabled.into_iter().map(|(k, v)| (k.clone(), Arc::new(v))).collect();
        let map = vec.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        RegistryInner {
            vec,
            map,
            disabled,
            sequencer: AtomicUsize::new(0),
        }
    }

    fn get(&self) -> Option<Arc<V>> {
        if self.vec.is_empty() {
            None
        } else {
            let sequence = self.sequencer.fetch_add(1, Ordering::SeqCst);
            Some(self.vec[sequence % self.vec.len()].1.clone())
        }
    }

    async fn get_by(&self, key: &K) -> Option<Arc<V>> {
        self.map.get(key).map(Arc::clone)
    }

    fn get_all(&self) -> Vec<Arc<V>> {
        self.vec.iter().map(|(_, v)| v.clone()).collect()
    }

    fn disable(&mut self, key: &K) {
        self.vec.iter().position(|(k, _)| k == key).map(|i| self.vec.remove(i));
        self.map.remove(key);
        self.disabled.insert(key.clone());
    }
}

const PING_INTERVAL: Duration = Duration::from_secs(300);

struct Pinger {
    members: Arc<Registry<Address, Member>>,
}

impl Pinger {
    fn new(members: Arc<Registry<Address, Member>>) -> Self {
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
    async fn should_get_none_for_empty_registry() {
        let enabled: HashMap<&str, &str> = HashMap::new();
        let disabled = HashSet::new();
        let registry = Registry::new(enabled, disabled);

        assert!(registry.get().await.is_none());
        assert!(registry.get_by(&"some-key").await.is_none());
        assert!(registry.get_all().await.is_empty());
    }

    #[tokio::test]
    async fn should_get_none_for_all_disabled_in_registry() {
        let key = "some-key";

        let enabled: HashMap<&str, &str> = HashMap::new();
        let mut disabled = HashSet::new();
        disabled.insert(key);
        let registry = Registry::new(enabled, disabled);

        assert!(registry.get().await.is_none());
        assert!(registry.get_by(&key).await.is_none());
        assert!(registry.get_all().await.is_empty());
    }

    #[tokio::test]
    async fn should_get_some_from_registry() {
        let key = "some-key";
        let value = "some-value";

        let mut enabled = HashMap::new();
        enabled.insert(key, value);
        let disabled = HashSet::new();
        let registry = Registry::new(enabled, disabled);

        assert_eq!(*registry.get().await.unwrap(), value);
        assert_eq!(*registry.get_by(&key).await.unwrap(), value);
        assert_eq!(*registry.get_all().await[0], "some-value");
    }

    #[tokio::test]
    async fn should_get_none_after_disabling_from_registry() {
        let key = "some-key";
        let value = "some-value";

        let mut enabled = HashMap::new();
        enabled.insert(key, value);
        let disabled = HashSet::new();
        let registry = Registry::new(enabled, disabled);

        registry.disable(&"some-key").await;

        assert!(registry.get().await.is_none());
        assert!(registry.get_by(&key).await.is_none());
        assert!(registry.get_all().await.is_empty());
    }
}
