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

pub(crate) struct Cluster {
    members: Arc<Members>,
    _pinger: Pinger,
}

impl Cluster {
    pub(crate) async fn init<E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        let members = Arc::new(Members::from(endpoints, username, password).await?);
        let pinger = Pinger::ping(members.clone());
        // TODO: reconnector...

        Ok(Cluster {
            members,
            _pinger: pinger,
        })
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

const PING_INTERVAL: Duration = Duration::from_secs(300);

struct Pinger {
    _handle: oneshot::Sender<()>,
}

impl Pinger {
    fn ping(members: Arc<Members>) -> Self {
        use crate::messaging::ping::{PingRequest, PingResponse};

        let (handle, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let mut ticks = Ticks::new(PING_INTERVAL, receiver);
            while let Some(_) = ticks.next().await {
                for member in members.get_all().await {
                    if let Err(_) = member.send::<PingRequest, PingResponse>(PingRequest::new()).await {
                        error!("Pinging {} failed.", member);
                        members.disable(&*member).await
                    }
                }
            }
        });

        Pinger { _handle: handle }
    }
}

struct Members {
    registry: RwLock<Registry<Address, Member>>,
}

impl Members {
    async fn from<'a, E>(endpoints: E, username: &str, password: &str) -> Result<Self>
    where
        E: IntoIterator<Item = SocketAddr>,
    {
        let mut registry = Registry::new();
        for endpoint in endpoints.into_iter().collect::<HashSet<SocketAddr>>() {
            info!("Trying to connect to {} as owner member.", endpoint);
            match Member::connect(&endpoint, username, password).await {
                Ok(member) => registry.enable(member.address().clone(), member),
                Err(e) => error!("Failed to connect to {} - {}", endpoint, e),
            }
        }

        Ok(Members {
            registry: RwLock::new(registry),
        })
    }

    /*async fn enable(&self, address: Address, member: Member) {
        self.registry.write().await.enable(address, member)
    }*/

    async fn get(&self) -> Option<Arc<Member>> {
        self.registry.read().await.get()
    }

    async fn get_by(&self, address: &Address) -> Option<Arc<Member>> {
        self.registry.read().await.get_by(address)
    }

    async fn get_all(&self) -> Vec<Arc<Member>> {
        self.registry.read().await.get_all()
    }

    async fn disable(&self, member: &Member) {
        self.registry.write().await.disable(member)
    }
}

struct Registry<K, V> {
    enabled: Vec<Arc<V>>,
    enabled_by_key: HashMap<K, Arc<V>>,
    disabled: HashSet<K>,
    sequencer: AtomicUsize,
}

impl<K, V> Registry<K, V>
where
    K: Eq + Hash + Clone,
    V: Eq,
{
    fn new() -> Self {
        Registry {
            enabled: Vec::new(),
            enabled_by_key: HashMap::new(),
            disabled: HashSet::new(),
            sequencer: AtomicUsize::new(0),
        }
    }

    fn enable(&mut self, key: K, value: V) {
        self.disabled.remove(&key);
        let value = Arc::new(value);
        self.enabled.push(value.clone());
        self.enabled_by_key.insert(key, value);
    }

    fn get(&self) -> Option<Arc<V>> {
        if self.enabled.is_empty() {
            None
        } else {
            let sequence = self.sequencer.fetch_add(1, Ordering::SeqCst);
            self.enabled.get(sequence % self.enabled.len()).map(Arc::clone)
        }
    }

    fn get_by(&self, key: &K) -> Option<Arc<V>> {
        self.enabled_by_key.get(key).map(Arc::clone)
    }

    fn get_all(&self) -> Vec<Arc<V>> {
        self.enabled.iter().map(Arc::clone).collect()
    }

    fn disable(&mut self, value: &V) {
        self.enabled
            .iter()
            .position(|v| **v == *value)
            .map(|i| self.enabled.remove(i));
        if let Some(key) = self
            .enabled_by_key
            .iter()
            .filter(|(_, v)| ***v == *value)
            .nth(0)
            .map(|(k, _)| k.clone())
        {
            self.enabled_by_key.remove(&key);
            self.disabled.insert(key);
        }
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
        let registry: Registry<&str, &str> = Registry::new();

        assert!(registry.get().is_none());
        assert!(registry.get_by(&"some-key").is_none());
        assert!(registry.get_all().is_empty());
    }

    #[test]
    fn should_get_some_after_enable() {
        let mut registry = Registry::new();

        let key = "some-key";
        let value = "some=value";

        registry.enable(key, value);

        assert_eq!(*registry.get().unwrap(), value);
        assert_eq!(*registry.get_by(&key).unwrap(), value);
        assert_eq!(*registry.get_all()[0], value);
    }

    #[test]
    fn should_get_none_after_disable() {
        let mut registry = Registry::new();

        let key = "some-key";
        let value = "some=value";

        registry.enable(key, value);
        registry.disable(&value);

        assert!(registry.get().is_none());
        assert!(registry.get_by(&key).is_none());
        assert!(registry.get_all().is_empty());
    }
}
