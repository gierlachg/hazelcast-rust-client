use std::error::Error;

use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use tokio;

use hazelcast_rust_client::HazelcastClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = HazelcastClient::new(vec!["127.0.0.1:5701"], "dev", "dev-pass").await?;

    let name = &format!(
        "my-counter-{}",
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .collect::<String>()
    );
    let mut counter = client.pn_counter(name);

    assert_eq!(counter.get().await?, 0);
    assert_eq!(counter.get_and_add(1).await?, 0);
    assert_eq!(counter.get_and_add(2).await?, 1);
    assert_eq!(counter.add_and_get(-1).await?, 2);
    assert_eq!(counter.get().await?, 2);

    println!(
        "Replica count for {}: {}",
        name,
        counter.replica_count().await?
    );

    Ok(())
}
