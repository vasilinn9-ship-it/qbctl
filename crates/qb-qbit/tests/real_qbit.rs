use std::{env, time::Duration};

use qb_application::torrent::{AddTorrentRequest, EffectAttempt, TorrentClient};
use qb_domain::torrent::{TorrentId, TorrentState};
use qb_qbit::{QbitClient, QbitCredentials};

#[tokio::test]
async fn disposable_qbittorrent_authenticates_and_probes() {
    if env::var("QBCTL_REAL_QBIT").as_deref() != Ok("1") {
        return;
    }

    let url = env::var("QBCTL_QBIT_URL").expect("QBCTL_QBIT_URL");
    let username = env::var("QBCTL_QBIT_USERNAME").expect("QBCTL_QBIT_USERNAME");
    let password = env::var("QBCTL_QBIT_PASSWORD").expect("QBCTL_QBIT_PASSWORD");

    let client = QbitClient::new(
        &url,
        QbitCredentials::new(username, password),
        Duration::from_secs(10),
    )
    .expect("real qBittorrent client");

    let probe = client.probe().await.expect("real qBittorrent probe");
    assert!(
        probe.application_version.starts_with("v5."),
        "unexpected qBittorrent version: {}",
        probe.application_version
    );
    assert!(
        probe.webapi_version.starts_with("2."),
        "unexpected WebAPI version: {}",
        probe.webapi_version
    );
    assert!(
        probe.mutation_ready,
        "pinned real qBittorrent must be mutation-ready: app={}, webapi={}",
        probe.application_version, probe.webapi_version
    );

    let torrents = client.list().await.expect("real torrent list");
    assert!(
        torrents.is_empty(),
        "disposable qBittorrent profile should start empty"
    );

    client
        .transfer_info()
        .await
        .expect("real transfer observation");
    client
        .queue_settings()
        .await
        .expect("real queue observation");
    let network = client
        .network_preferences()
        .await
        .expect("real network preferences");
    assert!(network.listen_port > 0, "real listen port must be non-zero");

    let mut metainfo =
        b"d4:infod6:lengthi4e4:name8:file.bin12:piece lengthi16384e6:pieces20:".to_vec();
    metainfo.extend_from_slice(&[0_u8; 20]);
    metainfo.extend_from_slice(b"ee");
    let request = AddTorrentRequest {
        metainfo,
        save_path: "/downloads".into(),
        stopped: true,
    };
    assert!(matches!(
        client.add_torrent(&request).await,
        EffectAttempt::Accepted
    ));

    let id =
        TorrentId::new("9a3b4b94ae398193bcc849dd8b2024f607c5c27a").expect("fixture torrent id");
    let mut last_observed = None;
    for _ in 0..50 {
        if let Some(torrent) = client.get(&id).await.expect("observe added torrent") {
            let save_path_matches = torrent.save_path.trim_end_matches('/') == "/downloads";
            let stopped = torrent.state == TorrentState::Stopped;
            last_observed = Some(torrent);
            if stopped && save_path_matches {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let observed = last_observed.expect("added torrent must become observable");
    assert_eq!(observed.id, id);
    assert_eq!(
        observed.state,
        TorrentState::Stopped,
        "added torrent did not settle into stopped state"
    );
    assert_eq!(
        observed.save_path.trim_end_matches('/'),
        "/downloads",
        "added torrent did not settle on the managed save path"
    );
}
