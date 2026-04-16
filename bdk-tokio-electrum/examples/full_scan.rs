use std::sync::Arc;
use std::time::Duration;

use bdk_core::bitcoin::Network;
use bdk_tokio_electrum::BdkElectrumClient;
use bdk_wallet::Wallet;
use tokio_electrum::prelude::*;

#[tokio::main]
async fn main() {
    // Initialize tracing for better diagnostics
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    // Connect to Electrum server
    let addr = ElectrumServerAddress::parse("ssl://mempool.space:40002").unwrap();
    let client = ElectrumClient::new(addr);
    let client = BdkElectrumClient::new(Arc::new(client));

    // Connect
    client.connect();

    // Wait for connection
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Connection status: {:?}", client.status());

    let mut wallet = Wallet::create_from_two_path_descriptor("wpkh(tpubDDks68wKK1xKaVVVbNmXUAx68K1K817M6KwjvjEyCrjdU7xMvjKnfYAtZjfZcrfPfGFzqmibuVqMzKJGbBnK7mo7WSJri8Y9QgM7aNQ3fCp/<0;1>/*)").network(Network::Testnet4).create_wallet_no_persist().unwrap();

    let request = wallet.start_full_scan().build();
    let update = client.full_scan(request, 20, 20, false).await.unwrap();
    wallet.apply_update(update).unwrap();
    println!("Initial full scan completed.");
}
