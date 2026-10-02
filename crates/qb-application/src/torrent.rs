use std::{future::Future, pin::Pin};

use qb_domain::torrent::{TorrentId, TorrentState};

use crate::PortError;

pub type PortFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, PortError>> + Send + 'a>>;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackerEvidence {
    pub identity: String,
    pub status: TrackerStatus,
    pub peers: i64,
    pub seeds: i64,
    pub leeches: i64,
    pub message: String,
}

pub trait TorrentClient: Send + Sync {
    fn probe(&self) -> PortFuture<'_, QbitProbe>;
    fn list(&self) -> PortFuture<'_, Vec<TorrentView>>;
    fn get(&self, id: &TorrentId) -> PortFuture<'_, Option<TorrentView>>;
    fn transfer_info(&self) -> PortFuture<'_, TransferInfo>;
    fn queue_settings(&self) -> PortFuture<'_, QueueSettings>;
    fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences>;
    fn trackers(&self, id: &TorrentId) -> PortFuture<'_, Vec<TrackerEvidence>>;
}
