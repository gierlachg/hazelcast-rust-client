use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use log::{error, info};
use tokio::{
    stream::{Stream, StreamExt},
    sync::{oneshot, RwLock},
    time::Interval,
};

use crate::{
    messaging::{Address, Request, Response},
    remote::member::Member,
    HazelcastClientError::{ClusterNonOperational, NodeNonOperational},
    Result,
};

const PING_INTERVAL: Duration = Duration::from_secs(300);

pub(crate) struct Cluster {
    members: Arc<Members>,
    _ping_handle: oneshot::Sender<()>,
}

impl Cluster {
    pub(crate) async fn init<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = &'a SocketAddr>,
    {
        let members = Arc::new(Members::from(endpoints, username, password).await?);

        let (ping_handle, receiver) = oneshot::channel();
        Cluster::ping(members.clone(), receiver);

        // TODO: reconnecting...

        Ok(Cluster {
            members,
            _ping_handle: ping_handle,
        })
    }

    fn ping(members: Arc<Members>, receiver: oneshot::Receiver<()>) {
        use crate::messaging::ping::{PingRequest, PingResponse};

        tokio::spawn(async move {
            let mut ticks = Ticks::new(PING_INTERVAL, receiver);
            while let Some(_) = ticks.next().await {
                for member in members.get_all().await {
                    if let Err(_) = member.send::<PingRequest, PingResponse>(PingRequest::new()).await {
                        error!("Pinging {} failed.", member);
                        members.disable(member.address()).await
                    }
                }
            }
        });
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

    pub(crate) async fn to_string(&self) -> String {
        let members = self.members.get_all().await;

        let mut formatted = String::new();
        formatted.push_str(&format!("\n\nMembers {{size: {}}} [\n", members.len()));
        for member in members {
            formatted.push_str(&format!("\tMember {}\n", member));
        }
        formatted.push_str("]\n");
        formatted
    }
}

struct Members {
    registry: RwLock<Registry<Address, Member>>,
}

impl Members {
    async fn from<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = &'a SocketAddr>,
    {
        let mut connected = HashMap::new();
        let mut disconnected = HashSet::new();
        for endpoint in endpoints.into_iter().collect::<HashSet<&SocketAddr>>() {
            info!("Trying to connect to {} as owner member.", endpoint);
            match Member::connect(&endpoint, username, password).await {
                Ok(member) => {
                    connected.insert(member.address().clone(), member);
                }
                Err(e) => {
                    error!("Failed to connect to {} - {}", endpoint, e);
                    disconnected.insert(endpoint.into());
                }
            }
        }

        Ok(Members {
            registry: RwLock::new(Registry::new(connected, disconnected)),
        })
    }

    async fn get(&self) -> Option<Arc<Member>> {
        self.registry.read().await.get()
    }

    async fn get_by(&self, address: &Address) -> Option<Arc<Member>> {
        self.registry.read().await.get_by(address)
    }

    async fn get_all(&self) -> Vec<Arc<Member>> {
        self.registry.read().await.get_all()
    }

    async fn disable(&self, key: &Address) {
        self.registry.write().await.disable(key)
    }
}

struct Registry<K, V> {
    vec: Vec<(K, Arc<V>)>,
    map: HashMap<K, Arc<V>>,
    disabled: HashSet<K>,
    sequencer: AtomicUsize,
}

impl<K, V> Registry<K, V>
where
    K: Eq + Hash + Clone,
{
    fn new(enabled: HashMap<K, V>, disabled: HashSet<K>) -> Self {
        let vec: Vec<(K, Arc<V>)> = enabled.into_iter().map(|(k, v)| (k.clone(), Arc::new(v))).collect();
        let map = vec.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Registry {
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
            self.vec.get(sequence % self.vec.len()).as_ref().map(|e| e.1.clone())
        }
    }

    fn get_by(&self, key: &K) -> Option<Arc<V>> {
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

struct Ticks {
    interval: Interval,
    receiver: oneshot::Receiver<()>,
}

impl Ticks {
    fn new(interval: Duration, receiver: oneshot::Receiver<()>) -> Self {
        Ticks {
            interval: tokio::time::interval(interval),
            receiver,
        }
    }
}

impl Stream for Ticks {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;

        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Pending => {}
            _ => return Poll::Ready(None),
        }

        Poll::Ready(match futures::ready!(Pin::new(&mut self.interval).poll_next(cx)) {
            None => None,
            _ => Some(()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_get_none_for_empty_registry() {
        let enabled: HashMap<&str, &str> = HashMap::new();
        let disabled = HashSet::new();
        let registry = Registry::new(enabled, disabled);

        assert!(registry.get().is_none());
        assert!(registry.get_by(&"some-key").is_none());
        assert!(registry.get_all().is_empty());
    }

    #[test]
    fn should_get_none_for_all_disabled_in_registry() {
        let key = "some-key";

        let enabled: HashMap<&str, &str> = HashMap::new();
        let mut disabled = HashSet::new();
        disabled.insert(key);
        let registry = Registry::new(enabled, disabled);

        assert!(registry.get().is_none());
        assert!(registry.get_by(&key).is_none());
        assert!(registry.get_all().is_empty());
    }

    #[test]
    fn should_get_some_from_registry() {
        let key = "some-key";
        let value = "some-value";

        let mut enabled = HashMap::new();
        enabled.insert(key, value);
        let disabled = HashSet::new();
        let registry = Registry::new(enabled, disabled);

        assert_eq!(*registry.get().unwrap(), value);
        assert_eq!(*registry.get_by(&key).unwrap(), value);
        assert_eq!(*registry.get_all()[0], "some-value");
    }

    #[test]
    fn should_get_none_after_disabling_from_registry() {
        let key = "some-key";
        let value = "some-value";

        let mut enabled = HashMap::new();
        enabled.insert(key, value);
        let disabled = HashSet::new();
        let mut registry = Registry::new(enabled, disabled);

        registry.disable(&"some-key");

        assert!(registry.get().is_none());
        assert!(registry.get_by(&key).is_none());
        assert!(registry.get_all().is_empty());
    }
}
