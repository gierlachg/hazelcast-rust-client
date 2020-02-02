use std::error::Error;

use log::{info, LevelFilter};
use log4rs::{
    append::console::ConsoleAppender,
    config::{Appender, Config, Root},
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio;

use hazelcast_rust_client::HazelcastClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_logger();

    let client = HazelcastClient::new(vec!["127.0.0.1:5701".parse().unwrap()], "dev", "dev-pass").await?;

    let mut counter = client.pn_counter(&counter_name());

    assert_eq!(counter.get().await?, 0);
    assert_eq!(counter.get_and_add(1).await?, 0);
    assert_eq!(counter.get_and_add(2).await?, 1);
    assert_eq!(counter.add_and_get(-1).await?, 2);
    assert_eq!(counter.get().await?, 2);

    let replica_count = counter.replica_count().await?;
    info!("Replica count for {}: {}", counter.name(), replica_count);

    Ok(())
}

fn counter_name() -> String {
    format!(
        "my-counter-{}",
        thread_rng().sample_iter(&Alphanumeric).take(8).collect::<String>()
    )
}

fn init_logger() {
    let _ = log4rs::init_config(
        Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(ConsoleAppender::builder().build())))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .unwrap(),
    )
    .unwrap();
}
