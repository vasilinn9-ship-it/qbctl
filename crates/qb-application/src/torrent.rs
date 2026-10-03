use std::{future::Future, pin::Pin};

use qb_domain::torrent::{TorrentId, TorrentMetainfo, TorrentState};

use crate::PortError;

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, PortError>> + Send + 'a>>;
pub type EffectFuture<'a> = Pin<Box<dyn Future<Output = EffectAttempt> + Send + 'a>>;

#[derive(Debug)]
pub enum EffectAttempt {
    NotSent(PortError),
    Accepted,
    Rejected(PortError),
    Uncertain(PortError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QbitProbe {
    pub application_version: String,
    pub webapi_version: String,
    pub mutation_ready: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Firewalled,
    Disconnected,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferInfo {
    pub download_rate_bps: u64,
    pub upload_rate_bps: u64,
    pub download_limit_bps: u64,
    pub upload_limit_bps: u64,
    pub dht_nodes: u64,
    pub connection_status: ConnectionStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueSettings {
    pub queueing_enabled: bool,
    pub max_active_downloads: i64,
    pub max_active_torrents: i64,
    pub dont_count_slow_torrents: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkPreferences {
    pub listen_port: u16,
    pub upnp: bool,
    pub dht: bool,
    pub pex: bool,
    pub lsd: bool,
    pub current_network_interface: String,
    pub current_interface_address: String,
    pub max_connections: i64,
    pub max_connections_per_torrent: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TorrentView {
    pub id: TorrentId,
    pub name: String,
    pub state: TorrentState,
    pub total_bytes: u64,
    pub remaining_bytes: u64,
    pub download_rate_bps: u64,
    pub upload_rate_bps: u64,
    pub progress_ppm: u32,
    pub availability: Option<f64>,
    pub peers_connected: u64,
    pub peers_known: u64,
    pub seeds_connected: u64,
    pub seeds_known: u64,
}

impl TorrentView {
    pub const fn is_complete(&self) -> bool {
        self.remaining_bytes == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrackerStatus {
    Disabled,
    NotContacted,
    Working,
    Updating,
    Error,
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileObservation {
    pub index: u32,
    pub path: String,
    pub size: u64,
    pub progress_ppm: u32,
    pub selected: bool,
    pub is_seed: bool,
    pub availability: Option<f64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackerEvidence {
    pub identity: String,
    pub status: TrackerStatus,
    pub peers: i64,
    pub seeds: i64,
    pub leeches: i64,
    pub message: String,
}

pub trait MetainfoReader: Send + Sync {
    fn parse(&self, bytes: &[u8]) -> Result<TorrentMetainfo, PortError>;
}

pub trait TorrentClient: Send + Sync {
    fn probe(&self) -> PortFuture<'_, QbitProbe>;
    fn list(&self) -> PortFuture<'_, Vec<TorrentView>>;
    fn get<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Option<TorrentView>>;
    fn transfer_info(&self) -> PortFuture<'_, TransferInfo>;
    fn queue_settings(&self) -> PortFuture<'_, QueueSettings>;
    fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences>;
    fn trackers<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Vec<TrackerEvidence>>;
    fn files<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Vec<FileObservation>>;
    fn stop<'a>(&'a self, id: &'a TorrentId) -> EffectFuture<'a>;
    fn start<'a>(&'a self, id: &'a TorrentId) -> EffectFuture<'a>;
    fn set_active_downloads(&self, value: u32) -> EffectFuture<'_>;
    fn set_download_limit(&self, bytes_per_sec: u64) -> EffectFuture<'_>;
    fn set_upload_limit(&self, bytes_per_sec: u64) -> EffectFuture<'_>;
}

pub struct TorrentService {
    client: std::sync::Arc<dyn TorrentClient>,
}

impl TorrentService {
    pub fn new(client: std::sync::Arc<dyn TorrentClient>) -> Self {
        Self { client }
    }

    pub async fn probe(&self) -> Result<QbitProbe, PortError> {
        self.client.probe().await
    }

    pub async fn list(&self) -> Result<Vec<TorrentView>, PortError> {
        self.client.list().await
    }

    pub async fn get(&self, id: &TorrentId) -> Result<Option<TorrentView>, PortError> {
        self.client.get(id).await
    }

    pub async fn transfer_info(&self) -> Result<TransferInfo, PortError> {
        self.client.transfer_info().await
    }

    pub async fn queue_settings(&self) -> Result<QueueSettings, PortError> {
        self.client.queue_settings().await
    }

    pub async fn network_preferences(&self) -> Result<NetworkPreferences, PortError> {
        self.client.network_preferences().await
    }

    pub async fn trackers(&self, id: &TorrentId) -> Result<Vec<TrackerEvidence>, PortError> {
        self.client.trackers(id).await
    }

    pub async fn files(&self, id: &TorrentId) -> Result<Vec<FileObservation>, PortError> {
        self.client.files(id).await
    }

    pub async fn stop(&self, id: &TorrentId) -> EffectAttempt {
        self.client.stop(id).await
    }

    pub async fn start(&self, id: &TorrentId) -> EffectAttempt {
        self.client.start(id).await
    }

    pub async fn set_active_downloads(&self, value: u32) -> EffectAttempt {
        self.client.set_active_downloads(value).await
    }

    pub async fn set_download_limit(&self, bytes_per_sec: u64) -> EffectAttempt {
        self.client.set_download_limit(bytes_per_sec).await
    }

    pub async fn set_upload_limit(&self, bytes_per_sec: u64) -> EffectAttempt {
        self.client.set_upload_limit(bytes_per_sec).await
    }
}
