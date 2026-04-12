use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use bdk_core::bitcoin::{Amount, ScriptBuf};
use bdk_core::spk_client::FullScanRequest;
use bdk_tokio_electrum::{BdkElectrumClient, SubscribeEvent};
use futures_util::StreamExt;
use testenv::TestEnv;
use tokio::time::{sleep, timeout};
use tokio_electrum::address::ElectrumServerAddress;
use tokio_electrum::builder::ElectrumClientBuilder;

fn ensure_funded_wallet(env: &TestEnv) {
    let reward_addr = env.bitcoind.client.new_address().unwrap();
    env.bitcoind
        .client
        .generate_to_address(101, &reward_addr)
        .unwrap();
    let tip_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
    env.electrsd.wait_height(tip_height);
}

fn build_external_request(spks: &[(u32, ScriptBuf)]) -> FullScanRequest<String> {
    FullScanRequest::<String>::builder_at(0)
        .spks_for_keychain("external".to_string(), spks.to_vec())
        .build()
}

async fn wait_connected(client: &BdkElectrumClient) {
    let connected = timeout(Duration::from_secs(20), async {
        loop {
            if client.status().is_connected() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        connected.is_ok(),
        "timed out waiting for electrum connection"
    );
}

async fn connected_bdk_client(env: &TestEnv) -> BdkElectrumClient {
    let addr = ElectrumServerAddress::parse(&format!("tcp://{}", env.electrsd.electrum_url))
        .expect("valid test electrum address");
    let client = ElectrumClientBuilder::new(addr)
        .request_timeout(Duration::from_secs(2))
        .build();
    let client = BdkElectrumClient::new(Arc::new(client));
    client.connect();
    wait_connected(&client).await;
    client
}

#[tokio::test]
async fn repeated_disconnect_recovery_with_large_wallet_does_not_miss_confirmed_tx() {
    let env = TestEnv::new();
    ensure_funded_wallet(&env);

    let addresses = (0..320)
        .map(|_| env.bitcoind.client.new_address().unwrap())
        .collect::<Vec<_>>();
    let spks = addresses
        .iter()
        .enumerate()
        .map(|(i, addr)| (i as u32, addr.script_pubkey()))
        .collect::<Vec<_>>();
    let tracked_index = 280_usize;

    // Seed sparse activity so stop-gap scanning reaches high indices in this large wallet.
    for index in (0..=tracked_index).step_by(15) {
        let txid = env
            .bitcoind
            .client
            .send_to_address(&addresses[index], Amount::from_sat(10_000 + index as u64))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);
    }

    // Simulate an app retry loop where sync streams are repeatedly interrupted.
    for attempt in 0..3_u8 {
        let client = connected_bdk_client(&env).await;
        let mut stream = client
            .sync(build_external_request(&spks))
            .stop_gap(NonZeroU32::new(20).unwrap())
            .batch_size(NonZeroU32::new(20).unwrap())
            .await
            .expect("sync stream should start");

        let initial = timeout(Duration::from_secs(90), stream.next())
            .await
            .expect("timed out waiting for initial event")
            .expect("stream ended before initial event")
            .expect("initial event should not be an error");
        assert!(
            matches!(initial, SubscribeEvent::Initial(_)),
            "attempt {attempt}: first event must be initial"
        );

        client.disconnect();
        let interrupted = timeout(Duration::from_secs(20), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SubscribeEvent::Disconnected) | Err(_) => return true,
                    Ok(_) => {}
                }
            }
            true
        })
        .await
        .unwrap_or(false);
        assert!(
            interrupted,
            "attempt {attempt}: stream should be interrupted after forced disconnect"
        );
    }

    let txid = env
        .bitcoind
        .client
        .send_to_address(&addresses[tracked_index], Amount::from_sat(44_000))
        .unwrap()
        .txid()
        .unwrap();
    env.electrsd.wait_tx(&txid);
    env.bitcoind
        .client
        .generate_to_address(1, &env.bitcoind.client.new_address().unwrap())
        .unwrap();
    let confirmed_height = env.bitcoind.client.get_blockchain_info().unwrap().blocks as usize;
    env.electrsd.wait_height(confirmed_height);

    let client = connected_bdk_client(&env).await;
    let mut stream = client
        .sync(build_external_request(&spks))
        .stop_gap(NonZeroU32::new(20).unwrap())
        .batch_size(NonZeroU32::new(20).unwrap())
        .await
        .expect("recovery sync stream should start");

    let first = timeout(Duration::from_secs(90), stream.next())
        .await
        .expect("timed out waiting for recovery initial event")
        .expect("stream ended before recovery initial event")
        .expect("recovery initial event should not be an error");
    match first {
        SubscribeEvent::Initial(initial) => {
            assert!(
                initial
                    .tx_update
                    .txs
                    .iter()
                    .any(|tx| tx.compute_txid() == txid),
                "final recovery scan must include transaction received while offline"
            );
            assert!(
                initial
                    .tx_update
                    .anchors
                    .iter()
                    .any(|(_, anchor_txid)| *anchor_txid == txid),
                "final recovery scan must include confirmation anchor for offline tx"
            );
        }
        other => panic!("expected initial event, got: {:?}", other),
    }
}

#[tokio::test]
async fn stop_gap_edge_activity_extends_tracking_window_for_future_indices() {
    let env = TestEnv::new();
    ensure_funded_wallet(&env);
    let client = connected_bdk_client(&env).await;

    let addresses = (0..90)
        .map(|_| env.bitcoind.client.new_address().unwrap())
        .collect::<Vec<_>>();
    let spks = addresses
        .iter()
        .enumerate()
        .map(|(i, addr)| (i as u32, addr.script_pubkey()))
        .collect::<Vec<_>>();

    // Seed activity at index 0 and 20 so initial stop-gap window reaches index 40.
    for index in [0_usize, 20_usize] {
        let txid = env
            .bitcoind
            .client
            .send_to_address(&addresses[index], Amount::from_sat(30_000 + index as u64))
            .unwrap()
            .txid()
            .unwrap();
        env.electrsd.wait_tx(&txid);
    }

    let mut stream = client
        .sync(build_external_request(&spks))
        .stop_gap(NonZeroU32::new(20).unwrap())
        .batch_size(NonZeroU32::new(20).unwrap())
        .await
        .expect("sync stream should start");
    let first = timeout(Duration::from_secs(90), stream.next())
        .await
        .expect("timed out waiting for initial event")
        .expect("stream ended before initial event")
        .expect("initial event should not be an error");
    assert!(
        matches!(first, SubscribeEvent::Initial(_)),
        "first event must be initial"
    );

    let edge_txid = env
        .bitcoind
        .client
        .send_to_address(&addresses[40], Amount::from_sat(41_000))
        .unwrap()
        .txid()
        .unwrap();
    env.electrsd.wait_tx(&edge_txid);

    let saw_edge_tx = timeout(Duration::from_secs(30), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(SubscribeEvent::Update(update))
                    if update
                        .tx_update
                        .txs
                        .iter()
                        .any(|tx| tx.compute_txid() == edge_txid) =>
                {
                    return true;
                }
                Ok(SubscribeEvent::Disconnected) => return false,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        saw_edge_tx,
        "activity at the stop-gap edge must be reported to the stream"
    );

    let far_txid = env
        .bitcoind
        .client
        .send_to_address(&addresses[55], Amount::from_sat(55_000))
        .unwrap()
        .txid()
        .unwrap();
    env.electrsd.wait_tx(&far_txid);

    let saw_far_tx = timeout(Duration::from_secs(30), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(SubscribeEvent::Update(update))
                    if update
                        .tx_update
                        .txs
                        .iter()
                        .any(|tx| tx.compute_txid() == far_txid) =>
                {
                    return true;
                }
                Ok(SubscribeEvent::Disconnected) => return false,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        saw_far_tx,
        "edge activity must extend tracking so farther addresses are not missed"
    );
}
