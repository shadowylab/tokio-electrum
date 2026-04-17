use std::str::FromStr;

use bitcoin::{Address, Network};
use futures_util::StreamExt;
use tokio_electrum::prelude::*;

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
    let script_hash = ElectrumScriptHash::new(script);
    let txs = client.script_get_history(script_hash).await.unwrap();
    println!("{:?}", txs);

    // Subscribe to notifications
    let mut notification = client.notifications();

    // Subscribe to block headers
    client.block_headers_subscribe().await.unwrap();

    // Handle notifications
    while let Some(notification) = notification.next().await {
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
