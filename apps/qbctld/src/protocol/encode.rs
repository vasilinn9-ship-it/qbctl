use qb_application::{
    system::{
        DaemonPhase, DoctorReport as ApplicationDoctorReport,
        SystemStatus as ApplicationSystemStatus,
    },
    torrent::{
        QbitProbe, QueueSettings, TorrentView, TransferInfo,
    },
};
use qb_domain::torrent::TorrentState;
use qb_proto::v1::{
    DaemonState, DoctorCheck, DoctorResponse, QbitProbeResponse, QueueSettingsResponse,
    StatusResponse, TorrentListResponse, TorrentStateView, TorrentSummary, TransferLimitsResponse,
};

pub fn daemon_state(phase: DaemonPhase) -> DaemonState {
    match phase {
        DaemonPhase::Starting => DaemonState::Starting,
        DaemonPhase::Migrating => DaemonState::Migrating,
        DaemonPhase::Recovering => DaemonState::Recovering,
        DaemonPhase::Degraded => DaemonState::Degraded,
        DaemonPhase::Ready => DaemonState::Ready,
        DaemonPhase::Draining => DaemonState::Draining,
        DaemonPhase::Stopped => DaemonState::Stopped,
        DaemonPhase::Failed => DaemonState::Failed,
    }
}

pub fn system_status(status: ApplicationSystemStatus) -> StatusResponse {
    StatusResponse {
        daemon_state: daemon_state(status.phase) as i32,
        instance_id: status.instance_id,
        schema_version: status.schema_version,
        mutation_admission_enabled: status.mutation_admission_enabled,
    }
}

pub fn doctor(report: ApplicationDoctorReport) -> DoctorResponse {
    DoctorResponse {
        checks: report
            .checks
            .into_iter()
            .map(|check| DoctorCheck {
                name: check.name,
                ok: check.ok,
                message: check.message,
            })
            .collect(),
    }
}


pub fn torrent_summary(value: TorrentView) -> TorrentSummary {
    TorrentSummary {
        id: value.id.to_string(),
        name: value.name,
        state: torrent_state(value.state) as i32,
        total_bytes: value.total_bytes,
        remaining_bytes: value.remaining_bytes,
        download_rate_bps: value.download_rate_bps,
        upload_rate_bps: value.upload_rate_bps,
        progress_ppm: value.progress_ppm,
        availability: value.availability,
        peers_connected: value.peers_connected,
        peers_known: value.peers_known,
        seeds_connected: value.seeds_connected,
        seeds_known: value.seeds_known,
    }
}

pub fn torrent_list(values: Vec<TorrentView>) -> TorrentListResponse {
    TorrentListResponse {
        torrents: values.into_iter().map(torrent_summary).collect(),
    }
}

pub fn queue_settings(value: QueueSettings) -> QueueSettingsResponse {
    QueueSettingsResponse {
        queueing_enabled: value.queueing_enabled,
        max_active_downloads: value.max_active_downloads,
        max_active_torrents: value.max_active_torrents,
        dont_count_slow_torrents: value.dont_count_slow_torrents,
    }
}

pub fn transfer_limits(value: TransferInfo) -> TransferLimitsResponse {
    TransferLimitsResponse {
        download_limit_bps: value.download_limit_bps,
        upload_limit_bps: value.upload_limit_bps,
        observed_download_rate_bps: value.download_rate_bps,
        observed_upload_rate_bps: value.upload_rate_bps,
    }
}

pub fn qbit_probe(value: QbitProbe) -> QbitProbeResponse {
    QbitProbeResponse {
        application_version: value.application_version,
        webapi_version: value.webapi_version,
        mutation_ready: value.mutation_ready,
    }
}

fn torrent_state(value: TorrentState) -> TorrentStateView {
    match value {
        TorrentState::Downloading => TorrentStateView::Downloading,
        TorrentState::StalledDownloading => TorrentStateView::StalledDownloading,
        TorrentState::QueuedDownloading => TorrentStateView::QueuedDownloading,
        TorrentState::Checking => TorrentStateView::Checking,
        TorrentState::Stopped => TorrentStateView::Stopped,
        TorrentState::Uploading => TorrentStateView::Uploading,
        TorrentState::StalledUploading => TorrentStateView::StalledUploading,
        TorrentState::QueuedUploading => TorrentStateView::QueuedUploading,
        TorrentState::Error => TorrentStateView::Error,
        TorrentState::Unknown => TorrentStateView::Unknown,
    }
}
