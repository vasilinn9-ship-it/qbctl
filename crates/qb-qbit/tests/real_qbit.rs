use std::{env, time::Duration};

use qb_application::torrent::TorrentClient;
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
        probe.application_version,
        probe.webapi_version
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
}
