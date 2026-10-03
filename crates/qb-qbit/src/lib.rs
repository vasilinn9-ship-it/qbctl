use std::{net::IpAddr, str::FromStr, time::Duration};

use qb_application::{
    torrent::{
        ConnectionStatus, EffectAttempt, EffectFuture, FileObservation, NetworkPreferences,
        PortFuture, QbitProbe, QueueSettings, TorrentClient, TorrentView, TrackerEvidence,
        TrackerStatus, TransferInfo,
    },
    PortError,
};
use qb_domain::torrent::{TorrentId, TorrentState};
use reqwest::{
    header::{COOKIE, ORIGIN, SET_COOKIE},
    redirect::Policy,
    Response, StatusCode, Url,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::RwLock;

pub const ADAPTER_NAME: &str = "qBittorrent WebUI API";
pub const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

pub struct QbitCredentials {
    username: String,
    password: String,
}

impl QbitCredentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

pub struct QbitClient {
    http: reqwest::Client,
    base_url: Url,
    origin: String,
    credentials: QbitCredentials,
    sid: RwLock<Option<String>>,
}

#[derive(Debug, Error)]
pub enum QbitBuildError {
    #[error("invalid qBittorrent URL: {0}")]
    InvalidUrl(String),
    #[error("failed to build qBittorrent HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
enum QbitError {
    #[error("qBittorrent is unavailable: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("qBittorrent authentication failed")]
    Authentication,
    #[error("qBittorrent returned HTTP {0}")]
    HttpStatus(u16),
    #[error("qBittorrent response exceeded {MAX_RESPONSE_BYTES} bytes")]
    ResponseTooLarge,
    #[error("qBittorrent returned invalid data: {0}")]
    InvalidResponse(String),
}

impl QbitClient {
    pub fn new(
        base_url: &str,
        credentials: QbitCredentials,
        timeout: Duration,
    ) -> Result<Self, QbitBuildError> {
        let base_url = validate_base_url(base_url)?;
        let origin = base_url.origin().ascii_serialization();

        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(timeout)
            .timeout(timeout)
            .user_agent(concat!("qbctl/", env!("CARGO_PKG_VERSION")))
            .build()?;

        Ok(Self {
            http,
            base_url,
            origin,
            credentials,
            sid: RwLock::new(None),
        })
    }

    async fn probe_inner(&self) -> Result<QbitProbe, QbitError> {
        let application_version = self.get_text("app/version", &[]).await?;
        let webapi_version = self.get_text("app/webapiVersion", &[]).await?;

        Ok(QbitProbe {
            mutation_ready: is_supported_mutation_version(&application_version, &webapi_version),
            application_version,
            webapi_version,
        })
    }

    async fn list_inner(&self) -> Result<Vec<TorrentView>, QbitError> {
        let rows: Vec<TorrentDto> = self.get_json("torrents/info", &[]).await?;
        rows.into_iter().map(map_torrent).collect()
    }

    async fn get_inner(&self, id: &TorrentId) -> Result<Option<TorrentView>, QbitError> {
        let query = [("hashes", id.as_str().to_string())];
        let rows: Vec<TorrentDto> = self.get_json("torrents/info", &query).await?;

        match rows.len() {
            0 => Ok(None),
            1 => map_torrent(rows.into_iter().next().expect("one row")).map(Some),
            count => Err(QbitError::InvalidResponse(format!(
                "exact torrent lookup returned {count} rows"
            ))),
        }
    }

    async fn transfer_info_inner(&self) -> Result<TransferInfo, QbitError> {
        let value: TransferInfoDto = self.get_json("transfer/info", &[]).await?;
        Ok(TransferInfo {
            download_rate_bps: nonnegative(value.dl_info_speed),
            upload_rate_bps: nonnegative(value.up_info_speed),
            download_limit_bps: nonnegative(value.dl_rate_limit),
            upload_limit_bps: nonnegative(value.up_rate_limit),
            dht_nodes: nonnegative(value.dht_nodes),
            connection_status: match value.connection_status.as_str() {
                "connected" => ConnectionStatus::Connected,
                "firewalled" => ConnectionStatus::Firewalled,
                "disconnected" => ConnectionStatus::Disconnected,
                _ => ConnectionStatus::Unknown,
            },
        })
    }

    async fn queue_settings_inner(&self) -> Result<QueueSettings, QbitError> {
        let value = self.preferences().await?;
        Ok(QueueSettings {
            queueing_enabled: value.queueing_enabled,
            max_active_downloads: value.max_active_downloads,
            max_active_torrents: value.max_active_torrents,
            dont_count_slow_torrents: value.dont_count_slow_torrents,
        })
    }

    async fn network_preferences_inner(&self) -> Result<NetworkPreferences, QbitError> {
        let value = self.preferences().await?;
        let listen_port = u16::try_from(value.listen_port)
            .map_err(|_| QbitError::InvalidResponse("listen_port is outside u16 range".into()))?;

        Ok(NetworkPreferences {
            listen_port,
            upnp: value.upnp,
            dht: value.dht,
            pex: value.pex,
            lsd: value.lsd,
            current_network_interface: value.current_network_interface,
            current_interface_address: value.current_interface_address,
            max_connections: value.max_connec,
            max_connections_per_torrent: value.max_connec_per_torrent,
        })
    }

    async fn files_inner(&self, id: &TorrentId) -> Result<Vec<FileObservation>, QbitError> {
        let query = [("hash", id.as_str().to_string())];
        let rows: Vec<FileDto> = self.get_json("torrents/files", &query).await?;
        rows.into_iter().map(map_file).collect()
    }

    async fn trackers_inner(&self, id: &TorrentId) -> Result<Vec<TrackerEvidence>, QbitError> {
        let query = [("hash", id.as_str().to_string())];
        let rows: Vec<TrackerDto> = self.get_json("torrents/trackers", &query).await?;

        Ok(rows
            .into_iter()
            .map(|row| TrackerEvidence {
                identity: redact_tracker_identity(&row.url),
                status: match row.status {
                    0 => TrackerStatus::Disabled,
                    1 => TrackerStatus::NotContacted,
                    2 => TrackerStatus::Working,
                    3 => TrackerStatus::Updating,
                    4 => TrackerStatus::Error,
                    _ => TrackerStatus::Unknown,
                },
                peers: row.num_peers,
                seeds: row.num_seeds,
                leeches: row.num_leeches,
                message: sanitize_untrusted_text(&row.msg, 512),
            })
            .collect())
    }

    async fn preferences(&self) -> Result<PreferencesDto, QbitError> {
        self.get_json("app/preferences", &[]).await
    }

    async fn get_text(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<String, QbitError> {
        let response = self.authenticated_get(endpoint, query).await?;
        let bytes = read_bounded_body(response).await?;
        String::from_utf8(bytes)
            .map(|value| value.trim().to_string())
            .map_err(|error| QbitError::InvalidResponse(error.to_string()))
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<T, QbitError> {
        let response = self.authenticated_get(endpoint, query).await?;
        let bytes = read_bounded_body(response).await?;
        serde_json::from_slice(&bytes)
            .map_err(|error| QbitError::InvalidResponse(error.to_string()))
    }

    async fn authenticated_get(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
    ) -> Result<Response, QbitError> {
        for attempt in 0..2 {
            let sid = self.ensure_session().await?;
            let response = self
                .http
                .get(self.endpoint(endpoint))
                .header(ORIGIN, &self.origin)
                .header(COOKIE, format!("SID={sid}"))
                .query(query)
                .send()
                .await?;

            if response.status() == StatusCode::FORBIDDEN && attempt == 0 {
                *self.sid.write().await = None;
                continue;
            }

            return classify_response(response);
        }

        Err(QbitError::Authentication)
    }

    async fn mutation_post_form(&self, endpoint: &str, form: &[(&str, String)]) -> EffectAttempt {
        for attempt in 0..2 {
            let sid = match self.ensure_session().await {
                Ok(sid) => sid,
                Err(error) => return EffectAttempt::NotSent(map_port_error(error)),
            };

            let response = self
                .http
                .post(self.endpoint(endpoint))
                .header(ORIGIN, &self.origin)
                .header(COOKIE, format!("SID={sid}"))
                .form(form)
                .send()
                .await;

            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    return EffectAttempt::Uncertain(PortError::new(
                        "QBIT_MUTATION_UNCERTAIN",
                        error.to_string(),
                    ));
                }
            };

            if response.status() == StatusCode::FORBIDDEN && attempt == 0 {
                *self.sid.write().await = None;
                continue;
            }

            if response.status().is_success() {
                return EffectAttempt::Accepted;
            }

            if response.status().is_server_error() {
                return EffectAttempt::Uncertain(PortError::new(
                    "QBIT_MUTATION_UNCERTAIN",
                    format!("qBittorrent returned HTTP {}", response.status()),
                ));
            }

            let code = if response.status() == StatusCode::FORBIDDEN {
                "QBIT_AUTH_FAILED"
            } else {
                "QBIT_MUTATION_REJECTED"
            };
            return EffectAttempt::Rejected(PortError::new(
                code,
                format!("qBittorrent returned HTTP {}", response.status()),
            ));
        }

        EffectAttempt::Rejected(PortError::new(
            "QBIT_AUTH_FAILED",
            "qBittorrent authentication failed",
        ))
    }

    async fn stop_inner(&self, id: &TorrentId) -> EffectAttempt {
        let form = [("hashes", id.as_str().to_string())];
        self.mutation_post_form("torrents/stop", &form).await
    }

    async fn start_inner(&self, id: &TorrentId) -> EffectAttempt {
        let form = [("hashes", id.as_str().to_string())];
        self.mutation_post_form("torrents/start", &form).await
    }

    async fn set_active_downloads_inner(&self, value: u32) -> EffectAttempt {
        let json = serde_json::json!({ "max_active_downloads": value }).to_string();
        let form = [("json", json)];
        self.mutation_post_form("app/setPreferences", &form).await
    }

    async fn set_download_limit_inner(&self, bytes_per_sec: u64) -> EffectAttempt {
        let form = [("limit", bytes_per_sec.to_string())];
        self.mutation_post_form("transfer/setDownloadLimit", &form)
            .await
    }

    async fn set_upload_limit_inner(&self, bytes_per_sec: u64) -> EffectAttempt {
        let form = [("limit", bytes_per_sec.to_string())];
        self.mutation_post_form("transfer/setUploadLimit", &form)
            .await
    }

    async fn ensure_session(&self) -> Result<String, QbitError> {
        if let Some(sid) = self.sid.read().await.clone() {
            return Ok(sid);
        }

        let mut guard = self.sid.write().await;
        if let Some(sid) = guard.clone() {
            return Ok(sid);
        }

        let response = self
            .http
            .post(self.endpoint("auth/login"))
            .header(ORIGIN, &self.origin)
            .form(&[
                ("username", self.credentials.username.as_str()),
                ("password", self.credentials.password.as_str()),
            ])
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(QbitError::Authentication);
        }

        let sid = response
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .find_map(extract_sid)
            .ok_or(QbitError::Authentication)?;

        let body = read_bounded_body(response).await?;
        if body != b"Ok." {
            return Err(QbitError::Authentication);
        }

        *guard = Some(sid.clone());
        Ok(sid)
    }

    fn endpoint(&self, endpoint: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("/api/v2/{endpoint}"));
        url.set_query(None);
        url.set_fragment(None);
        url
    }
}

impl TorrentClient for QbitClient {
    fn probe(&self) -> PortFuture<'_, QbitProbe> {
        Box::pin(async move { self.probe_inner().await.map_err(map_port_error) })
    }

    fn list(&self) -> PortFuture<'_, Vec<TorrentView>> {
        Box::pin(async move { self.list_inner().await.map_err(map_port_error) })
    }

    fn get<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Option<TorrentView>> {
        Box::pin(async move { self.get_inner(id).await.map_err(map_port_error) })
    }

    fn transfer_info(&self) -> PortFuture<'_, TransferInfo> {
        Box::pin(async move { self.transfer_info_inner().await.map_err(map_port_error) })
    }

    fn queue_settings(&self) -> PortFuture<'_, QueueSettings> {
        Box::pin(async move { self.queue_settings_inner().await.map_err(map_port_error) })
    }

    fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences> {
        Box::pin(async move {
            self.network_preferences_inner()
                .await
                .map_err(map_port_error)
        })
    }

    fn trackers<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Vec<TrackerEvidence>> {
        Box::pin(async move { self.trackers_inner(id).await.map_err(map_port_error) })
    }

    fn files<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Vec<FileObservation>> {
        Box::pin(async move { self.files_inner(id).await.map_err(map_files_error) })
    }

    fn stop<'a>(&'a self, id: &'a TorrentId) -> EffectFuture<'a> {
        Box::pin(async move { self.stop_inner(id).await })
    }

    fn start<'a>(&'a self, id: &'a TorrentId) -> EffectFuture<'a> {
        Box::pin(async move { self.start_inner(id).await })
    }

    fn set_active_downloads(&self, value: u32) -> EffectFuture<'_> {
        Box::pin(async move { self.set_active_downloads_inner(value).await })
    }

    fn set_download_limit(&self, bytes_per_sec: u64) -> EffectFuture<'_> {
        Box::pin(async move { self.set_download_limit_inner(bytes_per_sec).await })
    }

    fn set_upload_limit(&self, bytes_per_sec: u64) -> EffectFuture<'_> {
        Box::pin(async move { self.set_upload_limit_inner(bytes_per_sec).await })
    }
}

fn validate_base_url(value: &str) -> Result<Url, QbitBuildError> {
    let mut url =
        Url::parse(value).map_err(|error| QbitBuildError::InvalidUrl(error.to_string()))?;

    if url.scheme() != "http"
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(QbitBuildError::InvalidUrl(
            "URL must be plain loopback HTTP origin with no credentials/path/query/fragment".into(),
        ));
    }

    let host = url
        .host_str()
        .ok_or_else(|| QbitBuildError::InvalidUrl("host is required".into()))?;
    let ip_host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let loopback = host.eq_ignore_ascii_case("localhost")
        || IpAddr::from_str(ip_host).is_ok_and(|address| address.is_loopback());
    if !loopback {
        return Err(QbitBuildError::InvalidUrl(
            "qBittorrent URL must use a loopback host".into(),
        ));
    }

    url.set_path("/");
    Ok(url)
}

fn classify_response(response: Response) -> Result<Response, QbitError> {
    if response.status().is_success() {
        Ok(response)
    } else if response.status() == StatusCode::FORBIDDEN {
        Err(QbitError::Authentication)
    } else {
        Err(QbitError::HttpStatus(response.status().as_u16()))
    }
}

async fn read_bounded_body(mut response: Response) -> Result<Vec<u8>, QbitError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(QbitError::ResponseTooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(QbitError::ResponseTooLarge)?;
        if next_len > MAX_RESPONSE_BYTES {
            return Err(QbitError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn extract_sid(header: &str) -> Option<String> {
    header.split(';').find_map(|part| {
        part.trim()
            .strip_prefix("SID=")
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn is_supported_mutation_version(application: &str, webapi: &str) -> bool {
    let app_v5 = application
        .strip_prefix('v')
        .unwrap_or(application)
        .starts_with("5.");
    let mut parts = webapi.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());

    app_v5 && major == Some(2) && minor.is_some_and(|minor| (13..=16).contains(&minor))
}

fn map_torrent(row: TorrentDto) -> Result<TorrentView, QbitError> {
    let id =
        TorrentId::new(&row.hash).map_err(|error| QbitError::InvalidResponse(error.to_string()))?;
    let progress = if row.progress.is_finite() {
        row.progress.clamp(0.0, 1.0)
    } else {
        0.0
    };

    Ok(TorrentView {
        id,
        name: sanitize_untrusted_text(&row.name, 1024),
        state: map_state(&row.state),
        total_bytes: nonnegative(row.total_size),
        remaining_bytes: nonnegative(row.amount_left),
        download_rate_bps: nonnegative(row.dlspeed),
        upload_rate_bps: nonnegative(row.upspeed),
        progress_ppm: (progress * 1_000_000.0).round() as u32,
        availability: row
            .availability
            .filter(|value| value.is_finite() && *value >= 0.0),
        peers_connected: nonnegative(row.peers),
        peers_known: nonnegative(row.peers_total),
        seeds_connected: nonnegative(row.seeds),
        seeds_known: nonnegative(row.seeds_total),
    })
}

fn map_file(row: FileDto) -> Result<FileObservation, QbitError> {
    let index = u32::try_from(row.index)
        .map_err(|_| QbitError::InvalidResponse("file index is outside u32 range".into()))?;
    if row.name.is_empty() || row.name.chars().any(char::is_control) {
        return Err(QbitError::InvalidResponse(
            "file name is empty or contains control characters".into(),
        ));
    }
    let progress = if row.progress.is_finite() {
        row.progress.clamp(0.0, 1.0)
    } else {
        return Err(QbitError::InvalidResponse(
            "file progress is not finite".into(),
        ));
    };

    Ok(FileObservation {
        index,
        path: row.name,
        size: nonnegative(row.size),
        progress_ppm: (progress * 1_000_000.0).round() as u32,
        selected: row.priority != 0,
        is_seed: row.is_seed,
        availability: row
            .availability
            .filter(|value| value.is_finite() && *value >= 0.0),
    })
}

fn map_state(value: &str) -> TorrentState {
    match value {
        "downloading" | "forcedDL" | "metaDL" | "allocating" | "moving" => {
            TorrentState::Downloading
        }
        "stalledDL" => TorrentState::StalledDownloading,
        "queuedDL" => TorrentState::QueuedDownloading,
        "pausedDL" | "pausedUP" => TorrentState::Stopped,
        "uploading" | "forcedUP" => TorrentState::Uploading,
        "stalledUP" => TorrentState::StalledUploading,
        "queuedUP" => TorrentState::QueuedUploading,
        "checkingUP" | "checkingDL" | "checkingResumeData" => TorrentState::Checking,
        "error" | "missingFiles" => TorrentState::Error,
        _ => TorrentState::Unknown,
    }
}

fn redact_tracker_identity(value: &str) -> String {
    match Url::parse(value) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("<unknown>");
            match url.port() {
                Some(port) => format!("{}://{}:{port}", url.scheme(), host),
                None => format!("{}://{}", url.scheme(), host),
            }
        }
        Err(_) => sanitize_untrusted_text(value, 128),
    }
}

fn sanitize_untrusted_text(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control())
        .take(max_chars)
        .collect()
}

fn nonnegative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn map_files_error(error: QbitError) -> PortError {
    match error {
        QbitError::HttpStatus(404) => PortError::new("TORRENT_NOT_FOUND", "torrent was not found"),
        other => map_port_error(other),
    }
}

fn map_port_error(error: QbitError) -> PortError {
    match error {
        QbitError::Authentication => PortError::new("QBIT_AUTH_FAILED", error.to_string()),
        QbitError::InvalidResponse(_) | QbitError::ResponseTooLarge => {
            PortError::new("QBIT_RESPONSE_INVALID", error.to_string())
        }
        QbitError::Transport(_) | QbitError::HttpStatus(_) => {
            PortError::new("QBIT_UNAVAILABLE", error.to_string())
        }
    }
}

#[derive(Debug, Deserialize)]
struct TorrentDto {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    total_size: i64,
    #[serde(default)]
    amount_left: i64,
    #[serde(default)]
    dlspeed: i64,
    #[serde(default)]
    upspeed: i64,
    #[serde(default)]
    progress: f64,
    #[serde(default)]
    availability: Option<f64>,
    #[serde(default)]
    peers: i64,
    #[serde(default)]
    peers_total: i64,
    #[serde(default)]
    seeds: i64,
    #[serde(default)]
    seeds_total: i64,
}

#[derive(Debug, Deserialize)]
struct FileDto {
    index: i64,
    name: String,
    size: i64,
    progress: f64,
    priority: i64,
    #[serde(default)]
    is_seed: bool,
    #[serde(default)]
    availability: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TransferInfoDto {
    #[serde(default)]
    dl_info_speed: i64,
    #[serde(default)]
    up_info_speed: i64,
    #[serde(default)]
    dl_rate_limit: i64,
    #[serde(default)]
    up_rate_limit: i64,
    #[serde(default)]
    dht_nodes: i64,
    #[serde(default)]
    connection_status: String,
}

#[derive(Debug, Deserialize)]
struct PreferencesDto {
    #[serde(default)]
    queueing_enabled: bool,
    #[serde(default)]
    max_active_downloads: i64,
    #[serde(default)]
    max_active_torrents: i64,
    #[serde(default)]
    dont_count_slow_torrents: bool,
    #[serde(default)]
    listen_port: i64,
    #[serde(default)]
    upnp: bool,
    #[serde(default)]
    dht: bool,
    #[serde(default)]
    pex: bool,
    #[serde(default)]
    lsd: bool,
    #[serde(default)]
    current_network_interface: String,
    #[serde(default)]
    current_interface_address: String,
    #[serde(default)]
    max_connec: i64,
    #[serde(default)]
    max_connec_per_torrent: i64,
}

#[derive(Debug, Deserialize)]
struct TrackerDto {
    #[serde(default)]
    url: String,
    #[serde(default)]
    status: i64,
    #[serde(default = "minus_one")]
    num_peers: i64,
    #[serde(default = "minus_one")]
    num_seeds: i64,
    #[serde(default = "minus_one")]
    num_leeches: i64,
    #[serde(default)]
    msg: String,
}

const fn minus_one() -> i64 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_and_url_credentials() {
        assert!(matches!(
            validate_base_url("http://192.0.2.10:8080"),
            Err(QbitBuildError::InvalidUrl(_))
        ));
        assert!(matches!(
            validate_base_url("http://user:secret@127.0.0.1:8080"),
            Err(QbitBuildError::InvalidUrl(_))
        ));
    }

    #[test]
    fn accepts_ipv4_ipv6_and_localhost_loopback() {
        assert!(validate_base_url("http://127.0.0.1:8080").is_ok());
        assert!(validate_base_url("http://localhost:8080").is_ok());
        assert!(validate_base_url("http://[::1]:8080").is_ok());
    }

    #[test]
    fn mutation_version_matrix_is_fail_closed() {
        assert!(is_supported_mutation_version("v5.0.0", "2.13.0"));
        assert!(is_supported_mutation_version("v5.1.2", "2.16.2"));
        assert!(!is_supported_mutation_version("v5.1.2", "2.17.0"));
        assert!(!is_supported_mutation_version("v4.6.7", "2.13.0"));
        assert!(!is_supported_mutation_version("v5.1.2", "2.12.0"));
    }

    #[test]
    fn tracker_identity_removes_path_and_passkey() {
        assert_eq!(
            redact_tracker_identity("https://tracker.example:443/announce/secret-passkey"),
            "https://tracker.example"
        );
        assert_eq!(
            redact_tracker_identity("https://tracker.example:8443/announce/secret-passkey"),
            "https://tracker.example:8443"
        );
    }

    #[test]
    fn qbit_state_mapping_is_normalized() {
        assert_eq!(map_state("pausedDL"), TorrentState::Stopped);
        assert_eq!(map_state("stalledDL"), TorrentState::StalledDownloading);
        assert_eq!(map_state("future-state"), TorrentState::Unknown);
    }

    #[tokio::test]
    async fn probe_authenticates_and_reads_versions() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test-session; HttpOnly"),
            FakeResponse::ok("v5.2.4"),
            FakeResponse::ok("2.16.2"),
        ])
        .await;

        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");

        let probe = client.probe_inner().await.expect("probe");
        assert_eq!(probe.application_version, "v5.2.4");
        assert_eq!(probe.webapi_version, "2.16.2");
        assert!(probe.mutation_ready);

        let requests = server.finish().await;
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("POST /api/v2/auth/login HTTP/1.1"));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("origin: http://127.0.0.1:"));
        assert!(requests[0].contains("username=admin"));
        assert!(requests[0].contains("password=secret"));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("cookie: sid=test-session"));
        assert!(requests[2]
            .to_ascii_lowercase()
            .contains("cookie: sid=test-session"));
    }

    #[tokio::test]
    async fn forbidden_read_reauthenticates_once() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=first; HttpOnly"),
            FakeResponse::status("403 Forbidden", ""),
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=second; HttpOnly"),
            FakeResponse::ok(
                r#"{"dl_info_speed":1,"up_info_speed":2,"dl_rate_limit":3,"up_rate_limit":4,"dht_nodes":5,"connection_status":"connected"}"#,
            ),
        ])
        .await;

        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");

        let info = client.transfer_info_inner().await.expect("transfer info");
        assert_eq!(info.download_rate_bps, 1);
        assert_eq!(info.upload_rate_bps, 2);
        assert_eq!(info.connection_status, ConnectionStatus::Connected);

        let requests = server.finish().await;
        assert_eq!(requests.len(), 4);
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("cookie: sid=first"));
        assert!(requests[3]
            .to_ascii_lowercase()
            .contains("cookie: sid=second"));
    }

    #[tokio::test]
    async fn torrent_read_normalizes_unknown_state() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test; HttpOnly"),
            FakeResponse::ok(
                r#"[{"hash":"abcdef0123456789abcdef0123456789abcdef01","name":"sample","state":"futureState","total_size":100,"amount_left":25,"dlspeed":7,"upspeed":3,"progress":0.75,"availability":1.5,"peers":2,"peers_total":4,"seeds":1,"seeds_total":3}]"#,
            ),
        ])
        .await;

        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");

        let torrents = client.list_inner().await.expect("torrent list");
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].state, TorrentState::Unknown);
        assert_eq!(torrents[0].progress_ppm, 750_000);
        assert_eq!(torrents[0].peers_connected, 2);

        server.finish().await;
    }

    #[tokio::test]
    async fn file_observation_preserves_selection_and_incomplete_name() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test; HttpOnly"),
            FakeResponse::ok(
                r#"[{"index":0,"name":"dir/file.bin.!qB","size":100,"progress":0.5,"priority":0,"is_seed":false,"availability":0.75},{"index":1,"name":"dir/ready.bin","size":200,"progress":1.0,"priority":1,"is_seed":true,"availability":1.0}]"#,
            ),
        ])
        .await;
        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");
        let id = TorrentId::new("abcdef0123456789abcdef0123456789abcdef01").expect("torrent id");

        let files = client.files_inner(&id).await.expect("files");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "dir/file.bin.!qB");
        assert_eq!(files[0].progress_ppm, 500_000);
        assert!(!files[0].selected);
        assert!(files[1].selected);
        assert!(files[1].is_seed);

        let requests = server.finish().await;
        assert!(requests[1].to_ascii_lowercase().starts_with(
            "get /api/v2/torrents/files?hash=abcdef0123456789abcdef0123456789abcdef01 http/1.1"
        ));
    }

    #[tokio::test]
    async fn stop_posts_exact_hash_once() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test; HttpOnly"),
            FakeResponse::ok(""),
        ])
        .await;
        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");
        let id = TorrentId::new("abcdef0123456789abcdef0123456789abcdef01").expect("torrent id");

        assert!(matches!(
            client.stop_inner(&id).await,
            EffectAttempt::Accepted
        ));

        let requests = server.finish().await;
        assert_eq!(requests.len(), 2);
        let request = requests[1].to_ascii_lowercase();
        assert!(request.starts_with("post /api/v2/torrents/stop http/1.1"));
        assert!(request.contains("hashes=abcdef0123456789abcdef0123456789abcdef01"));
    }

    #[tokio::test]
    async fn mutation_reauthenticates_only_after_explicit_forbidden() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=first; HttpOnly"),
            FakeResponse::status("403 Forbidden", ""),
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=second; HttpOnly"),
            FakeResponse::ok(""),
        ])
        .await;
        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");
        let id = TorrentId::new("abcdef0123456789abcdef0123456789abcdef01").expect("torrent id");

        assert!(matches!(
            client.start_inner(&id).await,
            EffectAttempt::Accepted
        ));

        let requests = server.finish().await;
        assert_eq!(requests.len(), 4);
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("cookie: sid=first"));
        assert!(requests[3]
            .to_ascii_lowercase()
            .contains("cookie: sid=second"));
    }

    #[tokio::test]
    async fn dropped_mutation_response_is_uncertain() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test; HttpOnly"),
            FakeResponse::drop_connection(),
        ])
        .await;
        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");
        let id = TorrentId::new("abcdef0123456789abcdef0123456789abcdef01").expect("torrent id");

        let result = client.stop_inner(&id).await;
        assert!(matches!(
            result,
            EffectAttempt::Uncertain(ref error) if error.code == "QBIT_MUTATION_UNCERTAIN"
        ));

        server.finish().await;
    }

    #[tokio::test]
    async fn queue_and_transfer_mutations_use_narrow_endpoints() {
        let server = FakeHttpServer::spawn(vec![
            FakeResponse::ok("Ok.").with_header("Set-Cookie", "SID=test; HttpOnly"),
            FakeResponse::ok(""),
            FakeResponse::ok(""),
        ])
        .await;
        let client = QbitClient::new(
            &server.url,
            QbitCredentials::new("admin", "secret"),
            Duration::from_secs(2),
        )
        .expect("client");

        assert!(matches!(
            client.set_active_downloads_inner(10).await,
            EffectAttempt::Accepted
        ));
        assert!(matches!(
            client.set_download_limit_inner(24_000_000).await,
            EffectAttempt::Accepted
        ));

        let requests = server.finish().await;
        let queue_request = requests[1].to_ascii_lowercase();
        let limit_request = requests[2].to_ascii_lowercase();
        assert!(queue_request.starts_with("post /api/v2/app/setpreferences http/1.1"));
        assert!(queue_request.contains("max_active_downloads"));
        assert!(limit_request.starts_with("post /api/v2/transfer/setdownloadlimit http/1.1"));
        assert!(limit_request.contains("limit=24000000"));
    }

    use std::sync::{Arc, Mutex};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    struct FakeResponse {
        status: &'static str,
        headers: Vec<(&'static str, &'static str)>,
        body: &'static str,
        drop_connection: bool,
    }

    impl FakeResponse {
        fn ok(body: &'static str) -> Self {
            Self::status("200 OK", body)
        }

        fn status(status: &'static str, body: &'static str) -> Self {
            Self {
                status,
                headers: Vec::new(),
                body,
                drop_connection: false,
            }
        }

        fn drop_connection() -> Self {
            Self {
                status: "200 OK",
                headers: Vec::new(),
                body: "",
                drop_connection: true,
            }
        }

        fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
            self.headers.push((name, value));
            self
        }
    }

    struct FakeHttpServer {
        url: String,
        requests: Arc<Mutex<Vec<String>>>,
        task: JoinHandle<()>,
    }

    impl FakeHttpServer {
        async fn spawn(responses: Vec<FakeResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let address = listener.local_addr().expect("local address");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&requests);

            let task = tokio::spawn(async move {
                for response in responses {
                    let (mut socket, _) = listener.accept().await.expect("accept");
                    let request = read_http_request(&mut socket).await;
                    recorded.lock().expect("requests mutex").push(request);

                    if response.drop_connection {
                        drop(socket);
                        continue;
                    }

                    let mut headers = String::new();
                    for (name, value) in response.headers {
                        headers.push_str(name);
                        headers.push_str(": ");
                        headers.push_str(value);
                        headers.push_str("\r\n");
                    }
                    let wire = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                        response.status,
                        response.body.len(),
                        headers,
                        response.body
                    );
                    socket
                        .write_all(wire.as_bytes())
                        .await
                        .expect("write response");
                    socket.shutdown().await.expect("shutdown");
                }
            });

            Self {
                url: format!("http://{address}"),
                requests,
                task,
            }
        }

        async fn finish(self) -> Vec<String> {
            self.task.await.expect("server task");
            Arc::try_unwrap(self.requests)
                .expect("no request references")
                .into_inner()
                .expect("requests mutex")
        }
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut data = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end;

        loop {
            let read = socket.read(&mut buffer).await.expect("read request");
            assert!(read > 0, "connection closed before headers");
            data.extend_from_slice(&buffer[..read]);
            if let Some(position) = find_bytes(&data, b"\r\n\r\n") {
                header_end = position + 4;
                break;
            }
            assert!(data.len() < 64 * 1024, "request headers too large");
        }

        let header_text = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
        let content_length = header_text
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);

        while data.len() < header_end + content_length {
            let read = socket.read(&mut buffer).await.expect("read request body");
            assert!(read > 0, "connection closed before request body");
            data.extend_from_slice(&buffer[..read]);
        }

        String::from_utf8_lossy(&data).into_owned()
    }

    fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }
}
