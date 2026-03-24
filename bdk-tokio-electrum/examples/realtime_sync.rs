use std::time::Duration;

use bdk_core::bitcoin::Network;
use bdk_tokio_electrum::BdkElectrumClient;
use bdk_wallet::Wallet;
use futures::StreamExt;
use tokio_electrum::prelude::*;

#[tokio::main]
async fn main() {
    // Initialize tracing for better diagnostics
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Connect to Electrum server
    let addr = ElectrumServerAddress::parse("ssl://mempool.space:40002").unwrap();
    let electrum_client = ElectrumClient::new(addr);
    let client = BdkElectrumClient::new(electrum_client);

    // Connect
    client.connect();

    // Wait for connection
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Connection status: {:?}", client.status());

    let mut wallet = Wallet::create_from_two_path_descriptor("wpkh(tpubDDks68wKK1xKaVVVbNmXUAx68K1K817M6KwjvjEyCrjdU7xMvjKnfYAtZjfZcrfPfGFzqmibuVqMzKJGbBnK7mo7WSJri8Y9QgM7aNQ3fCp/<0;1>/*)").network(Network::Testnet4).create_wallet_no_persist().unwrap();

    let request = wallet.start_full_scan().build();

    println!("Starting full scan with subscriptions");

    // Initial scan with subscriptions
    let (initial_response, mut update_stream) = client
        .full_scan_and_subscribe(request, 50, 5, false)
        .await
        .unwrap();

    println!("Full scan completed");

    // Apply initial scan to your wallet
    wallet.apply_update(initial_response).unwrap();

    println!("Listening for updates");

    // Listen for real-time updates
    while let Some(update_result) = update_stream.next().await {
        match update_result {
            Ok(update) => {
                println!("Received tx update: {:?}", update);

                // Apply update to wallet
                wallet.apply_update(update).unwrap();
            }
            Err(e) => eprintln!("Update error: {}", e),
        }
    }
}
