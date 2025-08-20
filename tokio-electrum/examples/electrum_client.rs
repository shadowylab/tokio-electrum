use std::str::FromStr;
use std::time::Duration;

use bitcoin::{Address, Network};
use tokio_electrum::address::ElectrumServerAddress;
use tokio_electrum::client::ElectrumClient;
use tokio_electrum::notification::ElectrumNotification;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let addr = ElectrumServerAddress::parse("ssl://electrum.blockstream.info:50002").unwrap();

    // Create a new electrum client instance
    let client = ElectrumClient::new(addr);

    // Connect and keep connection alive
    client.connect();

    // Get history for an address
    let address = Address::from_str("1DWYVT2Db2ct7dG4Wf6bBD9yVhvsWhJWYj").unwrap();
    let address = address.require_network(Network::Bitcoin).unwrap();
    let script = address.script_pubkey();
    let txs = client.script_get_history(&script).await.unwrap();
    println!("{:?}", txs);

    // Subscribe to notifications
    let mut notification = client.notifications();

    // Subscribe to block headers
    client.subscribe_headers().unwrap();

    // Handle notifications
    while let Ok(notification) = notification.recv().await {
        match notification {
            ElectrumNotification::ConnectionStatusChanged(status) => {
                println!("Connection status changed: {:?}", status)
            }
            ElectrumNotification::BlockHeader { height, header } => println!(
                "Received new block header: {:?} at height {}.",
                header, height
            ),
            ElectrumNotification::ScriptHash { hash, status } => {
                println!("Script hash status: {:?} {:?}", hash, status);
            }
            ElectrumNotification::Shutdown => break,
        }
    }
}
