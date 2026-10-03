use qb_application::{
    completion::{
        CompletionFileState, CompletionHandoffStrategy, CompletionRecord, CompletionState,
    },
    storage::{ManagedRoot, ManagedRootStatus, StorageStatusSnapshot},
    system::{
        DaemonPhase, DoctorReport as ApplicationDoctorReport,
        SystemStatus as ApplicationSystemStatus,
    },
    torrent::{
        QbitProbe, QueueSettings, TorrentView, TrackerEvidence, TrackerStatus, TransferInfo,
    },
};
use qb_domain::torrent::TorrentState;
use qb_proto::v1::{
    DaemonState, DoctorCheck, DoctorResponse, IncomingStatusEntry, ManagedRootView,
    OperationFileView, OperationSummary, OperationView, QbitProbeResponse, QueueSettingsResponse,
    StatusResponse, StorageListResponse, StorageRootView, StorageStatusResponse, TorrentListResponse,
    TorrentStateView, TorrentSummary,
    TrackerEvidenceView, TrackerStatusView, TransferLimitsResponse,
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

pub fn operation_summary(record: &CompletionRecord) -> OperationSummary {
    OperationSummary {
        operation_id: record.operation_id.to_string(),
        request_id: record.request_id.to_string(),
        kind: "completion".into(),
        state: completion_state_name(record.state).into(),
        registry_id: record.registry_id.clone(),
        torrent_id: record.torrent_id.to_string(),
        problem_code: record.problem_code.clone(),
        revision: record.revision,
        files_total: saturating_u32(record.files.len()),
        files_moved_and_receipted: saturating_u32(
            record
                .files
                .iter()
                .filter(|file| file.state == CompletionFileState::HandedOff)
                .count(),
        ),
        archive_receipted: record.archive_destination_evidence.is_some()
            && record.archive_sha256.is_some(),
        post_handoff_content_audited: false,
    }
}

pub fn operation_view(record: CompletionRecord) -> OperationView {
    let summary = operation_summary(&record);
    let files = record
        .files
        .into_iter()
        .map(|file| OperationFileView {
            index: file.index,
            relative_path: file.relative_path,
            size: file.size,
            strategy: completion_strategy_name(file.strategy).into(),
            state: completion_file_state_name(file.state).into(),
            destination_receipted: file.destination_evidence.is_some(),
            problem_code: file.problem_code,
            revision: file.revision,
        })
        .collect();

    OperationView {
        summary: Some(summary),
        source_relative: record.source_relative,
        files,
    }
}

pub fn completion_execution_status_name(
    status: qb_application::completion::CompletionExecutionStatus,
) -> &'static str {
    use qb_application::completion::CompletionExecutionStatus;

    match status {
        CompletionExecutionStatus::Stopped => "stopped",
        CompletionExecutionStatus::ArchivePending => "archive_pending",
        CompletionExecutionStatus::PayloadPending => "payload_pending",
        CompletionExecutionStatus::RemoveRecordPending => "remove_record_pending",
        CompletionExecutionStatus::Finished => "finished",
        CompletionExecutionStatus::Blocked => "blocked",
        CompletionExecutionStatus::UnknownStop => "unknown_stop",
        CompletionExecutionStatus::UnknownArchive => "unknown_archive",
        CompletionExecutionStatus::UnknownMove => "unknown_move",
        CompletionExecutionStatus::UnknownSourceDelete => "unknown_source_delete",
        CompletionExecutionStatus::UnknownRemoveRecord => "unknown_remove_record",
        CompletionExecutionStatus::Failed => "failed",
    }
}

fn completion_state_name(state: CompletionState) -> &'static str {
    match state {
        CompletionState::Prepared => "prepared",
        CompletionState::StopPending => "stop_pending",
        CompletionState::UnknownStop => "unknown_stop",
        CompletionState::Stopped => "stopped",
        CompletionState::ArchivePending => "archive_pending",
        CompletionState::UnknownArchive => "unknown_archive",
        CompletionState::PayloadPending => "payload_pending",
        CompletionState::RemoveRecordPending => "remove_record_pending",
        CompletionState::UnknownRemoveRecord => "unknown_remove_record",
        CompletionState::Finished => "finished",
        CompletionState::Blocked => "blocked",
        CompletionState::Failed => "failed",
    }
}

fn completion_file_state_name(state: CompletionFileState) -> &'static str {
    match state {
        CompletionFileState::Prepared => "prepared",
        CompletionFileState::MovePending => "move_pending",
        CompletionFileState::UnknownMove => "unknown_move",
        CompletionFileState::DestinationReceipted => "destination_receipted",
        CompletionFileState::SourceDeletePending => "source_delete_pending",
        CompletionFileState::UnknownSourceDelete => "unknown_source_delete",
        CompletionFileState::HandedOff => "handed_off",
        CompletionFileState::Blocked => "blocked",
        CompletionFileState::Failed => "failed",
    }
}

fn completion_strategy_name(strategy: CompletionHandoffStrategy) -> &'static str {
    match strategy {
        CompletionHandoffStrategy::SameVolume => "same_volume",
        CompletionHandoffStrategy::CrossVolume => "cross_volume",
    }
}

pub fn storage_list(roots: Vec<ManagedRootStatus>) -> StorageListResponse {
    StorageListResponse {
        roots: roots.into_iter().map(storage_root).collect(),
    }
}

pub fn storage_status(status: StorageStatusSnapshot) -> StorageStatusResponse {
    let eligible_count = saturating_u32(status.incoming.eligible.len());
    let already_processed_count = saturating_u32(status.incoming.already_processed.len());
    let redundant_identical_count = saturating_u32(status.incoming.redundant_identical.len());
    let rejected_count = saturating_u32(status.incoming.rejected.len());

    let mut incoming = Vec::with_capacity(
        status.incoming.eligible.len()
            + status.incoming.already_processed.len()
            + status.incoming.redundant_identical.len()
            + status.incoming.rejected.len(),
    );

    incoming.extend(
        status
            .incoming
            .eligible
            .into_iter()
            .map(|entry| IncomingStatusEntry {
                relative_path: entry.relative_path,
                classification: "eligible".into(),
                registry_id: None,
                problem_code: None,
                detail: None,
            }),
    );
    incoming.extend(status.incoming.already_processed.into_iter().map(|entry| {
        IncomingStatusEntry {
            relative_path: entry.relative_path,
            classification: "already_processed".into(),
            registry_id: Some(entry.registry_id),
            problem_code: None,
            detail: Some(format!(
                "state={:?}; source={}",
                entry.registry_state, entry.registered_source_relative
            )),
        }
    }));
    incoming.extend(
        status
            .incoming
            .redundant_identical
            .into_iter()
            .map(|entry| IncomingStatusEntry {
                relative_path: entry.redundant_path,
                classification: "redundant_identical".into(),
                registry_id: None,
                problem_code: None,
                detail: Some(format!("canonical={}", entry.canonical_path)),
            }),
    );
    incoming.extend(
        status
            .incoming
            .rejected
            .into_iter()
            .map(|entry| IncomingStatusEntry {
                relative_path: entry.relative_path,
                classification: "rejected".into(),
                registry_id: None,
                problem_code: Some(entry.problem_code.into()),
                detail: Some(entry.message),
            }),
    );

    StorageStatusResponse {
        roots: status.roots.into_iter().map(storage_root).collect(),
        eligible_count,
        already_processed_count,
        redundant_identical_count,
        rejected_count,
        incoming,
    }
}

fn storage_root(value: ManagedRootStatus) -> StorageRootView {
    StorageRootView {
        root: managed_root(value.root) as i32,
        path: value.path,
        volume_id: value.volume_id,
        free_bytes: value.free_bytes,
        total_bytes: value.total_bytes,
    }
}

fn managed_root(root: ManagedRoot) -> ManagedRootView {
    match root {
        ManagedRoot::Incoming => ManagedRootView::Incoming,
        ManagedRoot::Archive => ManagedRootView::Archive,
        ManagedRoot::Working => ManagedRootView::Working,
        ManagedRoot::Completed => ManagedRootView::Completed,
        ManagedRoot::Runtime => ManagedRootView::Unspecified,
    }
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
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

pub fn tracker_evidence(value: TrackerEvidence) -> TrackerEvidenceView {
    TrackerEvidenceView {
        identity: value.identity,
        status: tracker_status(value.status) as i32,
        peers: value.peers,
        seeds: value.seeds,
        leeches: value.leeches,
        message: value.message,
    }
}

fn tracker_status(value: TrackerStatus) -> TrackerStatusView {
    match value {
        TrackerStatus::Disabled => TrackerStatusView::Disabled,
        TrackerStatus::NotContacted => TrackerStatusView::NotContacted,
        TrackerStatus::Working => TrackerStatusView::Working,
        TrackerStatus::Updating => TrackerStatusView::Updating,
        TrackerStatus::Error => TrackerStatusView::Error,
        TrackerStatus::Unknown => TrackerStatusView::Unknown,
    }
}
