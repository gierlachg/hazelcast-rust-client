pub(crate) mod authentication;

pub mod pn_counter;

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct Address {
    host: String,
    port: u32,
}

impl Address {
    pub(crate) fn new(host: &str, port: u32) -> Self {
        Address {
            host: host.to_string(),
            port,
        }
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> u32 {
        self.port
    }
}
