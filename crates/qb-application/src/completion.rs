use std::{collections::BTreeMap, sync::Arc, time::Duration};

use qb_domain::{
    torrent::{TorrentId, TorrentIdentity, TorrentMetainfo},
    OperationId, RequestId,
};
use sha2::{Digest, Sha256};

use crate::{
    registry::{RegistryState, TorrentRegistry},
    storage::{
        FileEvidence, ManagedDeleteOutcome, ManagedRoot, SameVolumeMoveOutcome, Storage,
        VerifiedCopyOutcome,
    },
    torrent::{EffectAttempt, FileObservation, MetainfoReader, TorrentClient, TorrentView},
    PortError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionRequest {
    pub request_id: RequestId,
    pub registry_id: String,
}

pub const COMPLETION_FINGERPRINT_VERSION: u32 = 1;

impl CompletionRequest {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-completion-fingerprint-v1\0");
        digest.update(self.registry_id.as_bytes());
        digest.finalize().into()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionHandoffStrategy {
    SameVolume,
    CrossVolume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionState {
    Prepared,
    StopPending,
    UnknownStop,
    Stopped,
    ArchivePending,
    UnknownArchive,
    PayloadPending,
    RemoveRecordPending,
    UnknownRemoveRecord,
    Finished,
    Blocked,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionFileState {
    Prepared,
    MovePending,
    UnknownMove,
    DestinationReceipted,
    SourceDeletePending,
    UnknownSourceDelete,
    HandedOff,
    Blocked,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionFileRecord {
    pub index: u32,
    pub relative_path: String,
    pub size: u64,
    pub source_evidence: FileEvidence,
    pub strategy: CompletionHandoffStrategy,
    pub state: CompletionFileState,
    pub destination_evidence: Option<FileEvidence>,
    pub destination_sha256: Option<[u8; 32]>,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionRecord {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub registry_id: String,
    pub torrent_id: TorrentId,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub completed_volume_id: u64,
    pub archive_volume_id: u64,
    pub working_save_path: String,
    pub total_bytes: u64,
    pub archive_destination_evidence: Option<FileEvidence>,
    pub archive_sha256: Option<[u8; 32]>,
    pub state: CompletionState,
    pub problem_code: Option<String>,
    pub revision: u64,
    pub files: Vec<CompletionFileRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionReservation {
    New(CompletionRecord),
    Replay(CompletionRecord),
    Conflict { operation_id: OperationId },
    ActiveConflict { operation_id: OperationId },
}

pub trait CompletionJournal: Send + Sync {
    fn lookup_completion_request(
        &self,
        request: &CompletionRequest,
    ) -> Result<Option<CompletionReservation>, PortError>;

    fn reserve_completion(
        &self,
        preflight: &CompletionPreflight,
    ) -> Result<CompletionReservation, PortError>;

    fn get_completion(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CompletionRecord>, PortError>;

    fn list_recoverable_completions(&self) -> Result<Vec<CompletionRecord>, PortError>;

    fn mark_stop_pending(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError>;

    fn mark_unknown_stop(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn retry_stop(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError>;

    fn mark_stopped(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError>;

    fn mark_completion_blocked(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_completion_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_archive_pending(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_unknown_archive(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn retry_archive(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError>;

    fn mark_archive_destination_receipted(
        &self,
        operation_id: &OperationId,
        destination: &FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError>;

    fn mark_archive_receipted(
        &self,
        operation_id: &OperationId,
        destination: &FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_move_pending(
        &self,
        operation_id: &OperationId,
        file_index: u32,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_unknown_move(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_destination_receipted(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        destination: &FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_source_delete_pending(
        &self,
        operation_id: &OperationId,
        file_index: u32,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_unknown_source_delete(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_handed_off(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        destination: &FileEvidence,
        destination_sha256: Option<[u8; 32]>,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_file_blocked(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_payload_handed_off(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError>;

    fn mark_unknown_remove_record(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError>;

    fn retry_remove_record(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError>;

    fn finish_completion(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionFilePlan {
    pub relative_path: String,
    pub size: u64,
    pub source_evidence: FileEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionPreflight {
    pub request_id: RequestId,
    pub registry_id: String,
    pub torrent_id: TorrentId,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub completed_volume_id: u64,
    pub archive_volume_id: u64,
    pub working_save_path: String,
    pub files: Vec<CompletionFilePlan>,
    pub total_bytes: u64,
}

impl CompletionPreflight {
    pub fn fingerprint(&self) -> [u8; 32] {
        let request = CompletionRequest {
            request_id: self.request_id.clone(),
            registry_id: self.registry_id.clone(),
        };
        request.fingerprint()
    }

    pub const fn payload_strategy(&self) -> CompletionHandoffStrategy {
        if self.working_volume_id == self.completed_volume_id {
            CompletionHandoffStrategy::SameVolume
        } else {
            CompletionHandoffStrategy::CrossVolume
        }
    }

    pub const fn archive_strategy(&self) -> CompletionHandoffStrategy {
        if self.source_evidence.identity.volume_id == self.archive_volume_id {
            CompletionHandoffStrategy::SameVolume
        } else {
            CompletionHandoffStrategy::CrossVolume
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionExecutionStatus {
    Stopped,
    ArchivePending,
    PayloadPending,
    RemoveRecordPending,
    Finished,
    Blocked,
    UnknownStop,
    UnknownArchive,
    UnknownMove,
    UnknownSourceDelete,
    UnknownRemoveRecord,
    Failed,
}

#[derive(Debug)]
pub struct CompletionExecution {
    pub status: CompletionExecutionStatus,
    pub record: CompletionRecord,
    pub problem: Option<PortError>,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum CompletionExecutionResult {
    Execution(Box<CompletionExecution>),
    Conflict { operation_id: OperationId },
    ActiveConflict { operation_id: OperationId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopObservation {
    Stopped,
    Running,
}

enum HandoffProgress {
    Continue(CompletionRecord),
    Halt {
        status: CompletionExecutionStatus,
        record: CompletionRecord,
        problem: Option<PortError>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SameVolumeObservation {
    SourceReady,
    Applied(FileEvidence),
}

pub struct CompletionPreflightService {
    registry: Arc<dyn TorrentRegistry>,
    storage: Arc<dyn Storage>,
    metainfo: Arc<dyn MetainfoReader>,
    client: Arc<dyn TorrentClient>,
    max_metainfo_bytes: usize,
}

impl CompletionPreflightService {
    pub fn new(
        registry: Arc<dyn TorrentRegistry>,
        storage: Arc<dyn Storage>,
        metainfo: Arc<dyn MetainfoReader>,
        client: Arc<dyn TorrentClient>,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            registry,
            storage,
            metainfo,
            client,
            max_metainfo_bytes,
        }
    }

    pub async fn preflight(
        &self,
        request: &CompletionRequest,
    ) -> Result<CompletionPreflight, PortError> {
        let registry = self
            .registry
            .get_by_id(&request.registry_id)?
            .ok_or_else(|| PortError::new("REGISTRY_NOT_FOUND", "registry record was not found"))?;
        if registry.state != RegistryState::Processing {
            return Err(PortError::new(
                "COMPLETION_NOT_PROCESSING",
                "completion requires a registry record in Processing state",
            ));
        }
        if registry.archive_ref.is_some() || registry.handoff_receipt_count != 0 {
            return Err(PortError::new(
                "COMPLETION_ALREADY_STARTED",
                "completion handoff evidence already exists for this registry record",
            ));
        }

        let source = self
            .storage
            .read_incoming(&registry.source_relative, self.max_metainfo_bytes)?;
        let digest: [u8; 32] = Sha256::digest(&source.bytes).into();
        if digest != registry.source_metainfo_digest {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "Incoming metainfo bytes no longer match the registry digest",
            ));
        }
        let metainfo = self.metainfo.parse(&source.bytes)?;
        if metainfo.identity != registry.identity {
            return Err(PortError::new(
                "IDENTITY_CONFLICT",
                "Incoming metainfo identity no longer matches the registry",
            ));
        }

        let torrent = self.observe_unique_target(&metainfo.identity).await?;
        if !torrent.is_complete() {
            return Err(PortError::new(
                "TORRENT_NOT_COMPLETE",
                "qBittorrent does not report the target torrent as complete",
            ));
        }
        if !self
            .storage
            .matches_root_path(ManagedRoot::Working, &torrent.save_path)?
        {
            return Err(PortError::new(
                "WORKING_OWNERSHIP_MISMATCH",
                "qBittorrent save path is not the managed Working root",
            ));
        }

        let qbit_files = self.client.files(&torrent.id).await?;
        let files = self.validate_manifest_files(&metainfo, qbit_files)?;

        let working = self.storage.volume_status(ManagedRoot::Working)?;
        let completed = self.storage.volume_status(ManagedRoot::Completed)?;
        let archive = self.storage.volume_status(ManagedRoot::Archive)?;

        if completed.volume_id != working.volume_id
            && completed.free_bytes < metainfo.manifest.total_size
        {
            return Err(PortError::new(
                "INSUFFICIENT_CAPACITY",
                format!(
                    "Completed volume has {} free bytes but {} bytes are required for cross-volume handoff",
                    completed.free_bytes, metainfo.manifest.total_size
                ),
            ));
        }
        if archive.volume_id != source.evidence.identity.volume_id
            && archive.free_bytes < source.evidence.size
        {
            return Err(PortError::new(
                "INSUFFICIENT_CAPACITY",
                format!(
                    "Archive volume has {} free bytes but {} bytes are required for metainfo handoff",
                    archive.free_bytes, source.evidence.size
                ),
            ));
        }
        if self
            .storage
            .observe_file(ManagedRoot::Archive, &registry.source_relative)?
            .is_some()
        {
            return Err(PortError::new(
                "DESTINATION_CONFLICT",
                "Archive destination already exists",
            ));
        }

        let mut planned = Vec::with_capacity(files.len());
        for file in files {
            let source_evidence = self
                .storage
                .observe_file(ManagedRoot::Working, &file.path)?
                .ok_or_else(|| {
                    PortError::new(
                        "WORKING_SOURCE_MISSING",
                        format!("Working payload file is missing: {}", file.path),
                    )
                })?;
            if source_evidence.size != file.size {
                return Err(PortError::new(
                    "WORKING_SOURCE_CHANGED",
                    format!(
                        "Working payload size for {} is {} but manifest requires {}",
                        file.path, source_evidence.size, file.size
                    ),
                ));
            }
            if self
                .storage
                .observe_file(ManagedRoot::Completed, &file.path)?
                .is_some()
            {
                return Err(PortError::new(
                    "DESTINATION_CONFLICT",
                    format!("Completed destination already exists: {}", file.path),
                ));
            }
            planned.push(CompletionFilePlan {
                relative_path: file.path,
                size: file.size,
                source_evidence,
            });
        }

        Ok(CompletionPreflight {
            request_id: request.request_id.clone(),
            registry_id: registry.registry_id,
            torrent_id: torrent.id,
            identity: metainfo.identity,
            source_relative: source.relative_path,
            source_evidence: source.evidence,
            source_metainfo_digest: digest,
            working_volume_id: working.volume_id,
            completed_volume_id: completed.volume_id,
            archive_volume_id: archive.volume_id,
            working_save_path: torrent.save_path,
            total_bytes: metainfo.manifest.total_size,
            files: planned,
        })
    }

    async fn observe_unique_target(
        &self,
        identity: &TorrentIdentity,
    ) -> Result<TorrentView, PortError> {
        let mut found: Option<TorrentView> = None;
        for selector in identity.qbit_selector_ids() {
            let Some(observed) = self.client.get(&selector).await? else {
                continue;
            };
            if let Some(existing) = found.as_ref() {
                if existing.id != observed.id {
                    return Err(PortError::new(
                        "IDENTITY_CONFLICT",
                        "torrent identity aliases resolve to different qBittorrent records",
                    ));
                }
            } else {
                found = Some(observed);
            }
        }
        found.ok_or_else(|| {
            PortError::new(
                "TORRENT_NOT_FOUND",
                "qBittorrent does not contain the registry torrent",
            )
        })
    }

    fn validate_manifest_files(
        &self,
        metainfo: &TorrentMetainfo,
        qbit_files: Vec<FileObservation>,
    ) -> Result<Vec<ValidatedFile>, PortError> {
        let mut observed = BTreeMap::new();
        for file in qbit_files {
            let normalized = normalize_qbit_path(&file.path)?;
            if observed.insert(normalized, file).is_some() {
                return Err(PortError::new(
                    "MANIFEST_MISMATCH",
                    "qBittorrent returned duplicate logical file paths",
                ));
            }
        }

        let mut files = Vec::with_capacity(metainfo.manifest.files.len());
        for expected in &metainfo.manifest.files {
            let key = normalize_qbit_path(&expected.path)?;
            let actual = observed.remove(&key).ok_or_else(|| {
                PortError::new(
                    "MANIFEST_MISMATCH",
                    format!("qBittorrent is missing manifest file {}", expected.path),
                )
            })?;
            if actual.path.ends_with(".!qB") || actual.path.ends_with(".!qb") {
                return Err(PortError::new(
                    "INCOMPLETE_FILE_PRESENT",
                    format!(
                        "qBittorrent incomplete temporary name cannot satisfy completion: {}",
                        actual.path
                    ),
                ));
            }
            if actual.size != expected.size {
                return Err(PortError::new(
                    "MANIFEST_MISMATCH",
                    format!(
                        "qBittorrent reports {} bytes for {} but manifest requires {}",
                        actual.size, expected.path, expected.size
                    ),
                ));
            }
            if !actual.selected {
                return Err(PortError::new(
                    "FILE_NOT_SELECTED",
                    format!("completion requires selected file {}", expected.path),
                ));
            }
            if actual.progress_ppm != 1_000_000 {
                return Err(PortError::new(
                    "FILE_NOT_COMPLETE",
                    format!(
                        "completion requires fully downloaded file {} but progress is {} ppm",
                        expected.path, actual.progress_ppm
                    ),
                ));
            }
            files.push(ValidatedFile {
                path: expected.path.clone(),
                size: expected.size,
            });
        }

        if !observed.is_empty() {
            return Err(PortError::new(
                "MANIFEST_MISMATCH",
                "qBittorrent contains files not present in the metainfo manifest",
            ));
        }

        Ok(files)
    }
}

struct ValidatedFile {
    path: String,
    size: u64,
}

fn normalize_qbit_path(value: &str) -> Result<String, PortError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(PortError::new(
            "MANIFEST_MISMATCH",
            "qBittorrent returned an invalid file path",
        ));
    }
    let normalized = value.replace('\\', "/");
    if normalized
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(PortError::new(
            "MANIFEST_MISMATCH",
            "qBittorrent returned a non-canonical file path",
        ));
    }
    Ok(normalized)
}

pub struct CompletionService {
    preflight: CompletionPreflightService,
    journal: Arc<dyn CompletionJournal>,
    storage: Arc<dyn Storage>,
    client: Arc<dyn TorrentClient>,
    observation_attempts: usize,
    observation_delay: Duration,
    lane: tokio::sync::Mutex<()>,
}

impl CompletionService {
    pub fn new(
        journal: Arc<dyn CompletionJournal>,
        registry: Arc<dyn TorrentRegistry>,
        storage: Arc<dyn Storage>,
        metainfo: Arc<dyn MetainfoReader>,
        client: Arc<dyn TorrentClient>,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            preflight: CompletionPreflightService::new(
                registry,
                storage.clone(),
                metainfo,
                client.clone(),
                max_metainfo_bytes,
            ),
            journal,
            storage,
            client,
            observation_attempts: 30,
            observation_delay: Duration::from_millis(100),
            lane: tokio::sync::Mutex::new(()),
        }
    }

    pub fn with_observation_policy(mut self, attempts: usize, delay: Duration) -> Self {
        self.observation_attempts = attempts.max(1);
        self.observation_delay = delay;
        self
    }

    pub async fn execute(
        &self,
        request: &CompletionRequest,
    ) -> Result<CompletionExecutionResult, PortError> {
        let _guard = self.lane.lock().await;

        if let Some(reservation) = self.journal.lookup_completion_request(request)? {
            return self.advance_reservation(reservation, true, true).await;
        }

        let preflight = self.preflight.preflight(request).await?;
        self.advance_reservation(self.journal.reserve_completion(&preflight)?, false, true)
            .await
    }

    pub async fn recover_all(&self) -> Result<Vec<CompletionExecution>, PortError> {
        let _guard = self.lane.lock().await;
        let records = self.journal.list_recoverable_completions()?;
        let mut executions = Vec::with_capacity(records.len());
        for record in records {
            executions.push(self.advance(record, true, false).await?);
        }
        Ok(executions)
    }

    async fn advance_reservation(
        &self,
        reservation: CompletionReservation,
        replayed: bool,
        explicit_request: bool,
    ) -> Result<CompletionExecutionResult, PortError> {
        match reservation {
            CompletionReservation::New(record) | CompletionReservation::Replay(record) => self
                .advance(record, replayed, explicit_request)
                .await
                .map(|execution| CompletionExecutionResult::Execution(Box::new(execution))),
            CompletionReservation::Conflict { operation_id } => {
                Ok(CompletionExecutionResult::Conflict { operation_id })
            }
            CompletionReservation::ActiveConflict { operation_id } => {
                Ok(CompletionExecutionResult::ActiveConflict { operation_id })
            }
        }
    }

    async fn advance(
        &self,
        mut record: CompletionRecord,
        replayed: bool,
        explicit_request: bool,
    ) -> Result<CompletionExecution, PortError> {
        match record.state {
            CompletionState::Blocked => {
                return Ok(completion_execution(
                    CompletionExecutionStatus::Blocked,
                    record,
                    Some(PortError::new(
                        "COMPLETION_BLOCKED",
                        "completion is blocked",
                    )),
                    replayed,
                ));
            }
            CompletionState::Failed => {
                return Ok(completion_execution(
                    CompletionExecutionStatus::Failed,
                    record,
                    Some(PortError::new("COMPLETION_FAILED", "completion failed")),
                    replayed,
                ));
            }
            CompletionState::Stopped
            | CompletionState::ArchivePending
            | CompletionState::UnknownArchive
            | CompletionState::PayloadPending
            | CompletionState::RemoveRecordPending
            | CompletionState::UnknownRemoveRecord
            | CompletionState::Finished => {
                return self
                    .advance_handoff(record, replayed, explicit_request)
                    .await;
            }
            CompletionState::StopPending | CompletionState::UnknownStop => {
                match self.observe_stop_bounded(&record).await {
                    Ok(StopObservation::Stopped) => {
                        let stopped = self.journal.mark_stopped(&record.operation_id)?;
                        return self
                            .advance_handoff(stopped, replayed, explicit_request)
                            .await;
                    }
                    Ok(StopObservation::Running) if !explicit_request => {
                        let unknown = if record.state == CompletionState::UnknownStop {
                            record
                        } else {
                            self.journal
                                .mark_unknown_stop(&record.operation_id, "QBIT_STOP_UNCERTAIN")?
                        };
                        return Ok(completion_execution(
                            CompletionExecutionStatus::UnknownStop,
                            unknown,
                            Some(PortError::new(
                                "QBIT_STOP_UNCERTAIN",
                                "completion stop remains unconfirmed after recovery observation; explicit replay is required before another stop request",
                            )),
                            replayed,
                        ));
                    }
                    Ok(StopObservation::Running) => {
                        record = self.journal.retry_stop(&record.operation_id)?;
                    }
                    Err(problem) => {
                        let unknown = if record.state == CompletionState::UnknownStop {
                            record
                        } else {
                            self.journal
                                .mark_unknown_stop(&record.operation_id, "QBIT_STOP_UNCERTAIN")?
                        };
                        return Ok(completion_execution(
                            CompletionExecutionStatus::UnknownStop,
                            unknown,
                            Some(problem),
                            replayed,
                        ));
                    }
                }
            }
            CompletionState::Prepared => {}
        }

        if let Err(problem) = self.revalidate_record(&record).await {
            let blocked = self
                .journal
                .mark_completion_blocked(&record.operation_id, problem.code)?;
            return Ok(completion_execution(
                CompletionExecutionStatus::Blocked,
                blocked,
                Some(problem),
                replayed,
            ));
        }

        let pending = self.journal.mark_stop_pending(&record.operation_id)?;
        match self.observe_stop_once(&pending).await {
            Ok(StopObservation::Stopped) => {
                let stopped = self.journal.mark_stopped(&pending.operation_id)?;
                return self
                    .advance_handoff(stopped, replayed, explicit_request)
                    .await;
            }
            Ok(StopObservation::Running) => {}
            Err(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_stop(&pending.operation_id, "QBIT_STOP_UNCERTAIN")?;
                return Ok(completion_execution(
                    CompletionExecutionStatus::UnknownStop,
                    unknown,
                    Some(problem),
                    replayed,
                ));
            }
        }

        match self.client.stop(&pending.torrent_id).await {
            EffectAttempt::NotSent(problem) => {
                let prepared = self.journal.retry_stop(&pending.operation_id)?;
                Ok(completion_execution(
                    CompletionExecutionStatus::Blocked,
                    prepared,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Rejected(problem) => {
                let failed = self
                    .journal
                    .mark_completion_failed(&pending.operation_id, problem.code)?;
                Ok(completion_execution(
                    CompletionExecutionStatus::Failed,
                    failed,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Uncertain(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_stop(&pending.operation_id, "QBIT_STOP_UNCERTAIN")?;
                Ok(completion_execution(
                    CompletionExecutionStatus::UnknownStop,
                    unknown,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Accepted => match self.observe_stop_bounded(&pending).await {
                Ok(StopObservation::Stopped) => {
                    let stopped = self.journal.mark_stopped(&pending.operation_id)?;
                    self.advance_handoff(stopped, replayed, explicit_request)
                        .await
                }
                Ok(StopObservation::Running) => {
                    let unknown = self.journal.mark_unknown_stop(
                        &pending.operation_id,
                        "QBIT_STOP_POSTCONDITION_UNCONFIRMED",
                    )?;
                    Ok(completion_execution(
                        CompletionExecutionStatus::UnknownStop,
                        unknown,
                        Some(PortError::new(
                            "QBIT_STOP_POSTCONDITION_UNCONFIRMED",
                            "qBittorrent accepted stop but fresh bounded observation did not confirm stopped state",
                        )),
                        replayed,
                    ))
                }
                Err(problem) => {
                    let unknown = self
                        .journal
                        .mark_unknown_stop(&pending.operation_id, "QBIT_STOP_UNCERTAIN")?;
                    Ok(completion_execution(
                        CompletionExecutionStatus::UnknownStop,
                        unknown,
                        Some(problem),
                        replayed,
                    ))
                }
            },
        }
    }

    async fn advance_handoff(
        &self,
        mut record: CompletionRecord,
        replayed: bool,
        explicit_request: bool,
    ) -> Result<CompletionExecution, PortError> {
        loop {
            let progress = match record.state {
                CompletionState::Stopped
                | CompletionState::ArchivePending
                | CompletionState::UnknownArchive => {
                    self.advance_archive(record, explicit_request).await?
                }
                CompletionState::PayloadPending => {
                    self.advance_payload(record, explicit_request).await?
                }
                CompletionState::RemoveRecordPending | CompletionState::UnknownRemoveRecord => {
                    self.advance_remove_record(record, explicit_request).await?
                }
                CompletionState::Finished => {
                    return Ok(completion_execution(
                        CompletionExecutionStatus::Finished,
                        record,
                        None,
                        replayed,
                    ));
                }
                CompletionState::Blocked => {
                    return Ok(completion_execution(
                        CompletionExecutionStatus::Blocked,
                        record,
                        Some(PortError::new(
                            "COMPLETION_BLOCKED",
                            "completion handoff is blocked",
                        )),
                        replayed,
                    ));
                }
                CompletionState::Failed => {
                    return Ok(completion_execution(
                        CompletionExecutionStatus::Failed,
                        record,
                        Some(PortError::new(
                            "COMPLETION_FAILED",
                            "completion handoff failed",
                        )),
                        replayed,
                    ));
                }
                other => {
                    return Err(PortError::new(
                        "OPERATION_TRANSITION_INVALID",
                        format!("unexpected completion handoff state {other:?}"),
                    ));
                }
            };

            match progress {
                HandoffProgress::Continue(next) => record = next,
                HandoffProgress::Halt {
                    status,
                    record,
                    problem,
                } => return Ok(completion_execution(status, record, problem, replayed)),
            }
        }
    }

    async fn advance_archive(
        &self,
        mut record: CompletionRecord,
        explicit_request: bool,
    ) -> Result<HandoffProgress, PortError> {
        if record.source_evidence.identity.volume_id != record.archive_volume_id {
            return self.advance_cross_volume_archive(record, explicit_request);
        }

        let observation = self.observe_same_volume_handoff(
            ManagedRoot::Incoming,
            ManagedRoot::Archive,
            &record.source_relative,
            &record.source_evidence,
        );

        match record.state {
            CompletionState::Stopped => match observation {
                Ok(SameVolumeObservation::SourceReady) => {
                    record = self.journal.mark_archive_pending(&record.operation_id)?;
                }
                Ok(SameVolumeObservation::Applied(_)) => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "ARCHIVE_EFFECT_WITHOUT_INTENT",
                            "Archive destination contains the source file but no archive intent is durable",
                        ),
                    );
                }
                Err(problem) => return self.block_handoff(record, problem),
            },
            CompletionState::ArchivePending | CompletionState::UnknownArchive => {
                match observation {
                    Ok(SameVolumeObservation::Applied(destination)) => {
                        let receipted = self.journal.mark_archive_receipted(
                            &record.operation_id,
                            &destination,
                            record.source_metainfo_digest,
                        )?;
                        return Ok(HandoffProgress::Continue(receipted));
                    }
                    Ok(SameVolumeObservation::SourceReady) => {
                        if record.state == CompletionState::UnknownArchive {
                            record = self.journal.retry_archive(&record.operation_id)?;
                            record = self.journal.mark_archive_pending(&record.operation_id)?;
                        }
                    }
                    Err(problem) => return self.block_handoff(record, problem),
                }
            }
            _ => {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "archive handoff requires Stopped, ArchivePending or UnknownArchive",
                ));
            }
        }

        match self.storage.move_same_volume_no_replace(
            ManagedRoot::Incoming,
            &record.source_relative,
            ManagedRoot::Archive,
            &record.source_relative,
            &record.source_evidence,
        ) {
            Ok(SameVolumeMoveOutcome::Moved { destination }) => {
                let receipted = self.journal.mark_archive_receipted(
                    &record.operation_id,
                    &destination,
                    record.source_metainfo_digest,
                )?;
                Ok(HandoffProgress::Continue(receipted))
            }
            Ok(SameVolumeMoveOutcome::SourceMissing) => {
                match self.observe_same_volume_handoff(
                    ManagedRoot::Incoming,
                    ManagedRoot::Archive,
                    &record.source_relative,
                    &record.source_evidence,
                ) {
                    Ok(SameVolumeObservation::Applied(destination)) => {
                        let receipted = self.journal.mark_archive_receipted(
                            &record.operation_id,
                            &destination,
                            record.source_metainfo_digest,
                        )?;
                        Ok(HandoffProgress::Continue(receipted))
                    }
                    Ok(SameVolumeObservation::SourceReady) => {
                        let unknown = self
                            .journal
                            .mark_unknown_archive(&record.operation_id, "ARCHIVE_MOVE_UNCERTAIN")?;
                        Ok(HandoffProgress::Halt {
                            status: CompletionExecutionStatus::UnknownArchive,
                            record: unknown,
                            problem: Some(PortError::new(
                                "ARCHIVE_MOVE_UNCERTAIN",
                                "Archive move returned source-missing but evidence still shows the source",
                            )),
                        })
                    }
                    Err(problem) => self.block_handoff(record, problem),
                }
            }
            Ok(SameVolumeMoveOutcome::SourceChanged { .. }) => self.block_handoff(
                record,
                PortError::new(
                    "HANDOFF_SOURCE_CHANGED",
                    "Incoming metainfo changed before archive move",
                ),
            ),
            Ok(SameVolumeMoveOutcome::DestinationExists { .. }) => self.block_handoff(
                record,
                PortError::new(
                    "DESTINATION_CONFLICT",
                    "Archive destination appeared before no-replace move",
                ),
            ),
            Err(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_archive(&record.operation_id, problem.code)?;
                Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownArchive,
                    record: unknown,
                    problem: Some(problem),
                })
            }
        }
    }

    fn advance_cross_volume_archive(
        &self,
        mut record: CompletionRecord,
        explicit_request: bool,
    ) -> Result<HandoffProgress, PortError> {
        let temp_relative = Self::completion_archive_temp_relative(&record.operation_id);

        if record.state == CompletionState::Stopped {
            match self
                .storage
                .observe_file(ManagedRoot::Incoming, &record.source_relative)?
            {
                Some(source) if source == record.source_evidence => {}
                Some(_) => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "HANDOFF_SOURCE_CHANGED",
                            "Incoming metainfo changed before cross-volume Archive handoff",
                        ),
                    );
                }
                None => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "ARCHIVE_SOURCE_MISSING",
                            "Incoming metainfo is missing before durable Archive intent",
                        ),
                    );
                }
            }
            if self
                .storage
                .observe_file(ManagedRoot::Archive, &record.source_relative)?
                .is_some()
                || self
                    .storage
                    .observe_file(ManagedRoot::Archive, &temp_relative)?
                    .is_some()
            {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_EFFECT_WITHOUT_INTENT",
                        "Archive destination/temp exists before durable Archive intent",
                    ),
                );
            }
            record = self.journal.mark_archive_pending(&record.operation_id)?;
        }

        if !matches!(
            record.state,
            CompletionState::ArchivePending | CompletionState::UnknownArchive
        ) {
            return Err(PortError::new(
                "OPERATION_TRANSITION_INVALID",
                "cross-volume Archive requires ArchivePending or UnknownArchive",
            ));
        }

        if let (Some(destination), Some(sha256)) = (
            record.archive_destination_evidence.clone(),
            record.archive_sha256,
        ) {
            if sha256 != record.source_metainfo_digest {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_DIGEST_MISMATCH",
                        "durable Archive receipt digest does not match retained metainfo digest",
                    ),
                );
            }
            if self
                .storage
                .observe_file(ManagedRoot::Archive, &record.source_relative)?
                .as_ref()
                != Some(&destination)
            {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_DESTINATION_CHANGED",
                        "Archive destination changed after durable receipt",
                    ),
                );
            }

            match self
                .storage
                .observe_file(ManagedRoot::Incoming, &record.source_relative)?
            {
                None => {
                    let receipted = self.journal.mark_archive_receipted(
                        &record.operation_id,
                        &destination,
                        sha256,
                    )?;
                    return Ok(HandoffProgress::Continue(receipted));
                }
                Some(source) if source == record.source_evidence => {}
                Some(_) => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "HANDOFF_SOURCE_CHANGED",
                            "Incoming metainfo changed before exact source delete",
                        ),
                    );
                }
            }

            if record.state == CompletionState::UnknownArchive && !explicit_request {
                return Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownArchive,
                    record,
                    problem: Some(PortError::new(
                        "ARCHIVE_DELETE_UNCERTAIN",
                        "Archive source delete remains uncertain; explicit replay is required before another delete",
                    )),
                });
            }
            if record.state == CompletionState::UnknownArchive {
                record = self.journal.retry_archive(&record.operation_id)?;
                record = self.journal.mark_archive_pending(&record.operation_id)?;
            }

            match self.storage.delete_managed_exact(
                ManagedRoot::Incoming,
                &record.source_relative,
                &record.source_evidence,
                &sha256,
            ) {
                Ok(ManagedDeleteOutcome::Deleted | ManagedDeleteOutcome::Missing) => {
                    if self
                        .storage
                        .observe_file(ManagedRoot::Incoming, &record.source_relative)?
                        .is_none()
                    {
                        let receipted = self.journal.mark_archive_receipted(
                            &record.operation_id,
                            &destination,
                            sha256,
                        )?;
                        return Ok(HandoffProgress::Continue(receipted));
                    }
                    let unknown = self
                        .journal
                        .mark_unknown_archive(&record.operation_id, "ARCHIVE_DELETE_UNCERTAIN")?;
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::UnknownArchive,
                        record: unknown,
                        problem: Some(PortError::new(
                            "ARCHIVE_DELETE_UNCERTAIN",
                            "Archive source delete did not produce an absent source observation",
                        )),
                    });
                }
                Ok(ManagedDeleteOutcome::Changed) => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "HANDOFF_SOURCE_CHANGED",
                            "Incoming metainfo changed at exact-delete boundary",
                        ),
                    );
                }
                Err(problem) => {
                    let unknown = self
                        .journal
                        .mark_unknown_archive(&record.operation_id, problem.code)?;
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::UnknownArchive,
                        record: unknown,
                        problem: Some(problem),
                    });
                }
            }
        }

        if record.state == CompletionState::UnknownArchive && !explicit_request {
            return Ok(HandoffProgress::Halt {
                status: CompletionExecutionStatus::UnknownArchive,
                record,
                problem: Some(PortError::new(
                    "ARCHIVE_HANDOFF_UNCERTAIN",
                    "cross-volume Archive handoff remains uncertain; explicit replay is required before another copy/publish attempt",
                )),
            });
        }
        if record.state == CompletionState::UnknownArchive {
            record = self.journal.retry_archive(&record.operation_id)?;
            record = self.journal.mark_archive_pending(&record.operation_id)?;
        }

        if self
            .storage
            .observe_file(ManagedRoot::Archive, &record.source_relative)?
            .is_some()
        {
            match self.storage.copy_to_temp_verified(
                ManagedRoot::Incoming,
                &record.source_relative,
                ManagedRoot::Archive,
                &record.source_relative,
                &record.source_evidence,
            )? {
                VerifiedCopyOutcome::Verified {
                    temp: destination,
                    sha256,
                    ..
                } if sha256 == record.source_metainfo_digest => {
                    record = self.journal.mark_archive_destination_receipted(
                        &record.operation_id,
                        &destination,
                        sha256,
                    )?;
                    return self.advance_cross_volume_archive(record, explicit_request);
                }
                VerifiedCopyOutcome::Verified { .. } => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "ARCHIVE_DIGEST_MISMATCH",
                            "published Archive destination digest does not match retained metainfo",
                        ),
                    );
                }
                _ => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "DESTINATION_CONFLICT",
                            "Archive destination exists but cannot be verified against retained metainfo",
                        ),
                    );
                }
            }
        }

        let (temp, sha256) = match self.storage.copy_to_temp_verified(
            ManagedRoot::Incoming,
            &record.source_relative,
            ManagedRoot::Archive,
            &temp_relative,
            &record.source_evidence,
        ) {
            Ok(VerifiedCopyOutcome::Verified { temp, sha256, .. }) => (temp, sha256),
            Ok(VerifiedCopyOutcome::SourceMissing) => {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_SOURCE_MISSING",
                        "Incoming metainfo disappeared during verified Archive copy",
                    ),
                );
            }
            Ok(VerifiedCopyOutcome::SourceChanged { .. }) => {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "HANDOFF_SOURCE_CHANGED",
                        "Incoming metainfo changed during verified Archive copy",
                    ),
                );
            }
            Ok(VerifiedCopyOutcome::TempConflict { .. }) => {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_TEMP_CONFLICT",
                        "operation-owned Archive temp conflicts with retained metainfo",
                    ),
                );
            }
            Err(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_archive(&record.operation_id, problem.code)?;
                return Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownArchive,
                    record: unknown,
                    problem: Some(problem),
                });
            }
        };
        if sha256 != record.source_metainfo_digest {
            return self.block_handoff(
                record,
                PortError::new(
                    "ARCHIVE_DIGEST_MISMATCH",
                    "verified Archive copy digest does not match retained metainfo digest",
                ),
            );
        }

        let destination = match self.storage.move_same_volume_no_replace(
            ManagedRoot::Archive,
            &temp_relative,
            ManagedRoot::Archive,
            &record.source_relative,
            &temp,
        ) {
            Ok(SameVolumeMoveOutcome::Moved { destination }) => destination,
            Ok(SameVolumeMoveOutcome::DestinationExists { .. })
            | Ok(SameVolumeMoveOutcome::SourceMissing) => {
                match self.storage.copy_to_temp_verified(
                    ManagedRoot::Incoming,
                    &record.source_relative,
                    ManagedRoot::Archive,
                    &record.source_relative,
                    &record.source_evidence,
                )? {
                    VerifiedCopyOutcome::Verified {
                        temp: destination,
                        sha256: published_sha256,
                        ..
                    } if published_sha256 == record.source_metainfo_digest => destination,
                    _ => {
                        let unknown = self.journal.mark_unknown_archive(
                            &record.operation_id,
                            "ARCHIVE_PUBLISH_UNCERTAIN",
                        )?;
                        return Ok(HandoffProgress::Halt {
                            status: CompletionExecutionStatus::UnknownArchive,
                            record: unknown,
                            problem: Some(PortError::new(
                                "ARCHIVE_PUBLISH_UNCERTAIN",
                                "Archive publish result could not be verified",
                            )),
                        });
                    }
                }
            }
            Ok(SameVolumeMoveOutcome::SourceChanged { .. }) => {
                return self.block_handoff(
                    record,
                    PortError::new(
                        "ARCHIVE_TEMP_CHANGED",
                        "operation-owned Archive temp changed before publish",
                    ),
                );
            }
            Err(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_archive(&record.operation_id, problem.code)?;
                return Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownArchive,
                    record: unknown,
                    problem: Some(problem),
                });
            }
        };

        record = self.journal.mark_archive_destination_receipted(
            &record.operation_id,
            &destination,
            sha256,
        )?;
        self.advance_cross_volume_archive(record, explicit_request)
    }

    fn completion_archive_temp_relative(operation_id: &OperationId) -> String {
        let payload = Self::completion_temp_relative(operation_id, u32::MAX);
        payload.replace("4294967295.part", "archive.torrent")
    }

    async fn advance_payload(
        &self,
        mut record: CompletionRecord,
        explicit_request: bool,
    ) -> Result<HandoffProgress, PortError> {
        for index in 0..record.files.len() {
            let file = record.files[index].clone();
            if file.state == CompletionFileState::HandedOff {
                continue;
            }
            if file.strategy == CompletionHandoffStrategy::CrossVolume {
                match self.advance_cross_volume_file(record, &file, explicit_request)? {
                    HandoffProgress::Continue(next) => {
                        record = next;
                        continue;
                    }
                    halt @ HandoffProgress::Halt { .. } => return Ok(halt),
                }
            }

            let observation = self.observe_same_volume_handoff(
                ManagedRoot::Working,
                ManagedRoot::Completed,
                &file.relative_path,
                &file.source_evidence,
            );

            match file.state {
                CompletionFileState::Prepared => match observation {
                    Ok(SameVolumeObservation::SourceReady) => {
                        record = self
                            .journal
                            .mark_file_move_pending(&record.operation_id, file.index)?;
                    }
                    Ok(SameVolumeObservation::Applied(_)) => {
                        return self.block_file_handoff(
                            record,
                            &file,
                            PortError::new(
                                "HANDOFF_EFFECT_WITHOUT_INTENT",
                                format!(
                                    "Completed contains {} but no file move intent is durable",
                                    file.relative_path
                                ),
                            ),
                        );
                    }
                    Err(problem) => return self.block_file_handoff(record, &file, problem),
                },
                CompletionFileState::MovePending | CompletionFileState::UnknownMove => {
                    match observation {
                        Ok(SameVolumeObservation::Applied(destination)) => {
                            record = self.journal.mark_file_handed_off(
                                &record.operation_id,
                                file.index,
                                &destination,
                                None,
                            )?;
                            continue;
                        }
                        Ok(SameVolumeObservation::SourceReady) => {
                            if file.state == CompletionFileState::UnknownMove {
                                record = self
                                    .journal
                                    .mark_file_move_pending(&record.operation_id, file.index)?;
                            }
                        }
                        Err(problem) => return self.block_file_handoff(record, &file, problem),
                    }
                }
                CompletionFileState::Blocked => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "HANDOFF_FILE_BLOCKED",
                            format!("file handoff is blocked: {}", file.relative_path),
                        ),
                    );
                }
                CompletionFileState::Failed => {
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::Failed,
                        record,
                        problem: Some(PortError::new(
                            "HANDOFF_FILE_FAILED",
                            format!("file handoff failed: {}", file.relative_path),
                        )),
                    });
                }
                CompletionFileState::DestinationReceipted
                | CompletionFileState::SourceDeletePending
                | CompletionFileState::UnknownSourceDelete => {
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::PayloadPending,
                        record,
                        problem: Some(PortError::new(
                            "CROSS_VOLUME_PAYLOAD_PENDING",
                            format!(
                                "cross-volume source-delete stage remains pending for {}",
                                file.relative_path
                            ),
                        )),
                    });
                }
                CompletionFileState::HandedOff => continue,
            }

            match self.storage.move_same_volume_no_replace(
                ManagedRoot::Working,
                &file.relative_path,
                ManagedRoot::Completed,
                &file.relative_path,
                &file.source_evidence,
            ) {
                Ok(SameVolumeMoveOutcome::Moved { destination }) => {
                    record = self.journal.mark_file_handed_off(
                        &record.operation_id,
                        file.index,
                        &destination,
                        None,
                    )?;
                }
                Ok(SameVolumeMoveOutcome::SourceMissing) => {
                    match self.observe_same_volume_handoff(
                        ManagedRoot::Working,
                        ManagedRoot::Completed,
                        &file.relative_path,
                        &file.source_evidence,
                    ) {
                        Ok(SameVolumeObservation::Applied(destination)) => {
                            record = self.journal.mark_file_handed_off(
                                &record.operation_id,
                                file.index,
                                &destination,
                                None,
                            )?;
                        }
                        Ok(SameVolumeObservation::SourceReady) => {
                            record = self.journal.mark_file_unknown_move(
                                &record.operation_id,
                                file.index,
                                "HANDOFF_MOVE_UNCERTAIN",
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownMove,
                                record,
                                problem: Some(PortError::new(
                                    "HANDOFF_MOVE_UNCERTAIN",
                                    format!("move result is uncertain for {}", file.relative_path),
                                )),
                            });
                        }
                        Err(problem) => return self.block_file_handoff(record, &file, problem),
                    }
                }
                Ok(SameVolumeMoveOutcome::SourceChanged { .. }) => {
                    return self.block_file_handoff(
                        record,
                        &file,
                        PortError::new(
                            "HANDOFF_SOURCE_CHANGED",
                            format!("Working source changed: {}", file.relative_path),
                        ),
                    );
                }
                Ok(SameVolumeMoveOutcome::DestinationExists { .. }) => {
                    return self.block_file_handoff(
                        record,
                        &file,
                        PortError::new(
                            "DESTINATION_CONFLICT",
                            format!(
                                "Completed destination appeared before no-replace move: {}",
                                file.relative_path
                            ),
                        ),
                    );
                }
                Err(problem) => {
                    record = self.journal.mark_file_unknown_move(
                        &record.operation_id,
                        file.index,
                        problem.code,
                    )?;
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::UnknownMove,
                        record,
                        problem: Some(problem),
                    });
                }
            }
        }

        let ready = self.journal.mark_payload_handed_off(&record.operation_id)?;
        Ok(HandoffProgress::Continue(ready))
    }

    fn advance_cross_volume_file(
        &self,
        mut record: CompletionRecord,
        original_file: &CompletionFileRecord,
        explicit_request: bool,
    ) -> Result<HandoffProgress, PortError> {
        let temp_relative =
            Self::completion_temp_relative(&record.operation_id, original_file.index);
        let mut delete_intent_created_now = false;

        loop {
            let file = record
                .files
                .iter()
                .find(|file| file.index == original_file.index)
                .cloned()
                .ok_or_else(|| {
                    PortError::new("COMPLETION_FILE_NOT_FOUND", original_file.index.to_string())
                })?;

            match file.state {
                CompletionFileState::Prepared => {
                    let source = self
                        .storage
                        .observe_file(ManagedRoot::Working, &file.relative_path)?;
                    match source {
                        Some(source) if source == file.source_evidence => {}
                        Some(_) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!("Working source changed: {}", file.relative_path),
                                ),
                            );
                        }
                        None => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_MISSING",
                                    format!("Working source is missing: {}", file.relative_path),
                                ),
                            );
                        }
                    }

                    if self
                        .storage
                        .observe_file(ManagedRoot::Completed, &file.relative_path)?
                        .is_some()
                    {
                        return self.block_file_handoff(
                            record,
                            &file,
                            PortError::new(
                                "HANDOFF_EFFECT_WITHOUT_INTENT",
                                format!(
                                    "Completed contains {} before a durable move intent",
                                    file.relative_path
                                ),
                            ),
                        );
                    }
                    if self
                        .storage
                        .observe_file(ManagedRoot::Completed, &temp_relative)?
                        .is_some()
                    {
                        return self.block_file_handoff(
                            record,
                            &file,
                            PortError::new(
                                "HANDOFF_TEMP_WITHOUT_INTENT",
                                format!(
                                    "operation temp exists before a durable move intent: {temp_relative}"
                                ),
                            ),
                        );
                    }

                    record = self
                        .journal
                        .mark_file_move_pending(&record.operation_id, file.index)?;
                }
                CompletionFileState::MovePending | CompletionFileState::UnknownMove => {
                    let source = self
                        .storage
                        .observe_file(ManagedRoot::Working, &file.relative_path)?;
                    match source {
                        Some(source) if source == file.source_evidence => {}
                        Some(_) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!("Working source changed: {}", file.relative_path),
                                ),
                            );
                        }
                        None => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_MISSING",
                                    format!(
                                        "Working source disappeared before destination receipt: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                    }

                    if self
                        .storage
                        .observe_file(ManagedRoot::Completed, &file.relative_path)?
                        .is_some()
                    {
                        match self.storage.copy_to_temp_verified(
                            ManagedRoot::Working,
                            &file.relative_path,
                            ManagedRoot::Completed,
                            &file.relative_path,
                            &file.source_evidence,
                        )? {
                            VerifiedCopyOutcome::Verified {
                                temp: destination,
                                sha256,
                                ..
                            } => {
                                record = self.journal.mark_file_destination_receipted(
                                    &record.operation_id,
                                    file.index,
                                    &destination,
                                    sha256,
                                )?;
                                continue;
                            }
                            VerifiedCopyOutcome::SourceMissing => {
                                return self.block_file_handoff(
                                    record,
                                    &file,
                                    PortError::new(
                                        "HANDOFF_SOURCE_MISSING",
                                        format!(
                                            "Working source disappeared while verifying published destination: {}",
                                            file.relative_path
                                        ),
                                    ),
                                );
                            }
                            VerifiedCopyOutcome::SourceChanged { .. } => {
                                return self.block_file_handoff(
                                    record,
                                    &file,
                                    PortError::new(
                                        "HANDOFF_SOURCE_CHANGED",
                                        format!(
                                            "Working source changed while verifying published destination: {}",
                                            file.relative_path
                                        ),
                                    ),
                                );
                            }
                            VerifiedCopyOutcome::TempConflict { .. } => {
                                return self.block_file_handoff(
                                    record,
                                    &file,
                                    PortError::new(
                                        "DESTINATION_CONFLICT",
                                        format!(
                                            "Completed destination does not match Working source: {}",
                                            file.relative_path
                                        ),
                                    ),
                                );
                            }
                        }
                    }

                    let verified = match self.storage.copy_to_temp_verified(
                        ManagedRoot::Working,
                        &file.relative_path,
                        ManagedRoot::Completed,
                        &temp_relative,
                        &file.source_evidence,
                    ) {
                        Ok(VerifiedCopyOutcome::Verified { temp, sha256, .. }) => (temp, sha256),
                        Ok(VerifiedCopyOutcome::SourceMissing) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_MISSING",
                                    format!(
                                        "Working source disappeared during verified copy: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                        Ok(VerifiedCopyOutcome::SourceChanged { .. }) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!(
                                        "Working source changed during verified copy: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                        Ok(VerifiedCopyOutcome::TempConflict { .. }) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_TEMP_CONFLICT",
                                    format!(
                                        "operation temp does not match Working source: {temp_relative}"
                                    ),
                                ),
                            );
                        }
                        Err(problem) => {
                            record = self.journal.mark_file_unknown_move(
                                &record.operation_id,
                                file.index,
                                problem.code,
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownMove,
                                record,
                                problem: Some(problem),
                            });
                        }
                    };

                    match self.storage.move_same_volume_no_replace(
                        ManagedRoot::Completed,
                        &temp_relative,
                        ManagedRoot::Completed,
                        &file.relative_path,
                        &verified.0,
                    ) {
                        Ok(SameVolumeMoveOutcome::Moved { destination }) => {
                            record = self.journal.mark_file_destination_receipted(
                                &record.operation_id,
                                file.index,
                                &destination,
                                verified.1,
                            )?;
                        }
                        Ok(SameVolumeMoveOutcome::DestinationExists { .. })
                        | Ok(SameVolumeMoveOutcome::SourceMissing) => {
                            match self.storage.copy_to_temp_verified(
                                ManagedRoot::Working,
                                &file.relative_path,
                                ManagedRoot::Completed,
                                &file.relative_path,
                                &file.source_evidence,
                            )? {
                                VerifiedCopyOutcome::Verified {
                                    temp: destination,
                                    sha256,
                                    ..
                                } => {
                                    record = self.journal.mark_file_destination_receipted(
                                        &record.operation_id,
                                        file.index,
                                        &destination,
                                        sha256,
                                    )?;
                                }
                                _ => {
                                    record = self.journal.mark_file_unknown_move(
                                        &record.operation_id,
                                        file.index,
                                        "HANDOFF_PUBLISH_UNCERTAIN",
                                    )?;
                                    return Ok(HandoffProgress::Halt {
                                        status: CompletionExecutionStatus::UnknownMove,
                                        record,
                                        problem: Some(PortError::new(
                                            "HANDOFF_PUBLISH_UNCERTAIN",
                                            format!(
                                                "published destination could not be verified for {}",
                                                file.relative_path
                                            ),
                                        )),
                                    });
                                }
                            }
                        }
                        Ok(SameVolumeMoveOutcome::SourceChanged { .. }) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_TEMP_CHANGED",
                                    format!(
                                        "operation temp changed before publish: {temp_relative}"
                                    ),
                                ),
                            );
                        }
                        Err(problem) => {
                            record = self.journal.mark_file_unknown_move(
                                &record.operation_id,
                                file.index,
                                problem.code,
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownMove,
                                record,
                                problem: Some(problem),
                            });
                        }
                    }
                }
                CompletionFileState::DestinationReceipted => {
                    let destination = file.destination_evidence.as_ref().ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "destination-receipted file has no destination evidence",
                        )
                    })?;
                    let sha256 = file.destination_sha256.ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "destination-receipted file has no destination digest",
                        )
                    })?;
                    if self
                        .storage
                        .observe_file(ManagedRoot::Completed, &file.relative_path)?
                        .as_ref()
                        != Some(destination)
                    {
                        return self.block_file_handoff(
                            record,
                            &file,
                            PortError::new(
                                "DESTINATION_CHANGED",
                                format!(
                                    "Completed destination changed after receipt: {}",
                                    file.relative_path
                                ),
                            ),
                        );
                    }
                    match self
                        .storage
                        .observe_file(ManagedRoot::Working, &file.relative_path)?
                    {
                        Some(source) if source == file.source_evidence => {}
                        Some(_) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!(
                                        "Working source changed after destination receipt: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                        None => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_DELETE_WITHOUT_INTENT",
                                    format!(
                                        "Working source disappeared before source-delete intent: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                    }
                    let _ = sha256;
                    record = self
                        .journal
                        .mark_file_source_delete_pending(&record.operation_id, file.index)?;
                    delete_intent_created_now = true;
                }
                CompletionFileState::SourceDeletePending
                | CompletionFileState::UnknownSourceDelete => {
                    let destination = file.destination_evidence.as_ref().ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "source-delete file has no destination evidence",
                        )
                    })?;
                    let sha256 = file.destination_sha256.ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "source-delete file has no destination digest",
                        )
                    })?;
                    if self
                        .storage
                        .observe_file(ManagedRoot::Completed, &file.relative_path)?
                        .as_ref()
                        != Some(destination)
                    {
                        return self.block_file_handoff(
                            record,
                            &file,
                            PortError::new(
                                "DESTINATION_CHANGED",
                                format!(
                                    "Completed destination changed before source delete: {}",
                                    file.relative_path
                                ),
                            ),
                        );
                    }

                    match self
                        .storage
                        .observe_file(ManagedRoot::Working, &file.relative_path)?
                    {
                        None => {
                            record = self.journal.mark_file_handed_off(
                                &record.operation_id,
                                file.index,
                                destination,
                                Some(sha256),
                            )?;
                            return Ok(HandoffProgress::Continue(record));
                        }
                        Some(source) if source == file.source_evidence => {}
                        Some(_) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!(
                                        "Working source changed before exact delete: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                    }

                    if file.state == CompletionFileState::UnknownSourceDelete && !explicit_request {
                        return Ok(HandoffProgress::Halt {
                            status: CompletionExecutionStatus::UnknownSourceDelete,
                            record,
                            problem: Some(PortError::new(
                                "HANDOFF_SOURCE_DELETE_UNCERTAIN",
                                format!(
                                    "source delete remains uncertain for {}; explicit replay is required",
                                    file.relative_path
                                ),
                            )),
                        });
                    }
                    if file.state == CompletionFileState::SourceDeletePending
                        && !delete_intent_created_now
                        && !explicit_request
                    {
                        record = self.journal.mark_file_unknown_source_delete(
                            &record.operation_id,
                            file.index,
                            "HANDOFF_SOURCE_DELETE_UNCERTAIN",
                        )?;
                        return Ok(HandoffProgress::Halt {
                            status: CompletionExecutionStatus::UnknownSourceDelete,
                            record,
                            problem: Some(PortError::new(
                                "HANDOFF_SOURCE_DELETE_UNCERTAIN",
                                format!(
                                    "restart observed source still present after durable delete intent for {}",
                                    file.relative_path
                                ),
                            )),
                        });
                    }
                    if file.state == CompletionFileState::UnknownSourceDelete {
                        record = self
                            .journal
                            .mark_file_source_delete_pending(&record.operation_id, file.index)?;
                    }

                    match self.storage.delete_managed_exact(
                        ManagedRoot::Working,
                        &file.relative_path,
                        &file.source_evidence,
                        &sha256,
                    ) {
                        Ok(ManagedDeleteOutcome::Deleted | ManagedDeleteOutcome::Missing) => {
                            if self
                                .storage
                                .observe_file(ManagedRoot::Working, &file.relative_path)?
                                .is_none()
                            {
                                record = self.journal.mark_file_handed_off(
                                    &record.operation_id,
                                    file.index,
                                    destination,
                                    Some(sha256),
                                )?;
                                return Ok(HandoffProgress::Continue(record));
                            }
                            record = self.journal.mark_file_unknown_source_delete(
                                &record.operation_id,
                                file.index,
                                "HANDOFF_SOURCE_DELETE_UNCERTAIN",
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownSourceDelete,
                                record,
                                problem: Some(PortError::new(
                                    "HANDOFF_SOURCE_DELETE_UNCERTAIN",
                                    format!(
                                        "source delete did not produce an absent source observation for {}",
                                        file.relative_path
                                    ),
                                )),
                            });
                        }
                        Ok(ManagedDeleteOutcome::Changed) => {
                            return self.block_file_handoff(
                                record,
                                &file,
                                PortError::new(
                                    "HANDOFF_SOURCE_CHANGED",
                                    format!(
                                        "Working source changed at exact-delete boundary: {}",
                                        file.relative_path
                                    ),
                                ),
                            );
                        }
                        Err(problem) => {
                            record = self.journal.mark_file_unknown_source_delete(
                                &record.operation_id,
                                file.index,
                                problem.code,
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownSourceDelete,
                                record,
                                problem: Some(problem),
                            });
                        }
                    }
                }
                CompletionFileState::HandedOff => return Ok(HandoffProgress::Continue(record)),
                CompletionFileState::Blocked => {
                    return self.block_handoff(
                        record,
                        PortError::new(
                            "HANDOFF_FILE_BLOCKED",
                            format!("file handoff is blocked: {}", file.relative_path),
                        ),
                    );
                }
                CompletionFileState::Failed => {
                    return Ok(HandoffProgress::Halt {
                        status: CompletionExecutionStatus::Failed,
                        record,
                        problem: Some(PortError::new(
                            "HANDOFF_FILE_FAILED",
                            format!("file handoff failed: {}", file.relative_path),
                        )),
                    });
                }
            }
        }
    }

    fn completion_temp_relative(operation_id: &OperationId, file_index: u32) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = Sha256::digest(operation_id.as_str().as_bytes());
        let mut token = String::with_capacity(32);
        for byte in &digest[..16] {
            token.push(char::from(HEX[usize::from(byte >> 4)]));
            token.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        format!("_qbctl_tmp/{token}/{file_index}.part")
    }

    async fn advance_remove_record(
        &self,
        mut record: CompletionRecord,
        explicit_request: bool,
    ) -> Result<HandoffProgress, PortError> {
        match self.client.get(&record.torrent_id).await {
            Ok(None) => {
                let finished = self.journal.finish_completion(&record.operation_id)?;
                return Ok(HandoffProgress::Continue(finished));
            }
            Ok(Some(_)) => {}
            Err(problem) => {
                if record.state == CompletionState::RemoveRecordPending {
                    record = self.journal.mark_unknown_remove_record(
                        &record.operation_id,
                        "QBIT_REMOVE_UNCERTAIN",
                    )?;
                }
                return Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownRemoveRecord,
                    record,
                    problem: Some(problem),
                });
            }
        }

        if !explicit_request {
            if record.state == CompletionState::RemoveRecordPending {
                record = self
                    .journal
                    .mark_unknown_remove_record(&record.operation_id, "QBIT_REMOVE_UNCERTAIN")?;
            }
            return Ok(HandoffProgress::Halt {
                status: CompletionExecutionStatus::UnknownRemoveRecord,
                record,
                problem: Some(PortError::new(
                    "QBIT_REMOVE_UNCERTAIN",
                    "qBittorrent record is still present after a durable remove intent; explicit replay is required before another remove request",
                )),
            });
        }

        if record.state == CompletionState::UnknownRemoveRecord {
            record = self.journal.retry_remove_record(&record.operation_id)?;
        }

        match self.client.remove_keep_files(&record.torrent_id).await {
            EffectAttempt::NotSent(problem) => Ok(HandoffProgress::Halt {
                status: CompletionExecutionStatus::RemoveRecordPending,
                record,
                problem: Some(problem),
            }),
            EffectAttempt::Rejected(problem) => {
                let failed = self
                    .journal
                    .mark_completion_failed(&record.operation_id, problem.code)?;
                Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::Failed,
                    record: failed,
                    problem: Some(problem),
                })
            }
            EffectAttempt::Uncertain(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown_remove_record(&record.operation_id, "QBIT_REMOVE_UNCERTAIN")?;
                Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownRemoveRecord,
                    record: unknown,
                    problem: Some(problem),
                })
            }
            EffectAttempt::Accepted => {
                for attempt in 0..self.observation_attempts {
                    match self.client.get(&record.torrent_id).await {
                        Ok(None) => {
                            let finished = self.journal.finish_completion(&record.operation_id)?;
                            return Ok(HandoffProgress::Continue(finished));
                        }
                        Ok(Some(_)) => {}
                        Err(problem) => {
                            let unknown = self.journal.mark_unknown_remove_record(
                                &record.operation_id,
                                "QBIT_REMOVE_UNCERTAIN",
                            )?;
                            return Ok(HandoffProgress::Halt {
                                status: CompletionExecutionStatus::UnknownRemoveRecord,
                                record: unknown,
                                problem: Some(problem),
                            });
                        }
                    }
                    if attempt + 1 < self.observation_attempts {
                        tokio::time::sleep(self.observation_delay).await;
                    }
                }

                let unknown = self.journal.mark_unknown_remove_record(
                    &record.operation_id,
                    "QBIT_REMOVE_POSTCONDITION_UNCONFIRMED",
                )?;
                Ok(HandoffProgress::Halt {
                    status: CompletionExecutionStatus::UnknownRemoveRecord,
                    record: unknown,
                    problem: Some(PortError::new(
                        "QBIT_REMOVE_POSTCONDITION_UNCONFIRMED",
                        "qBittorrent accepted record removal but fresh bounded observation did not confirm absence",
                    )),
                })
            }
        }
    }

    fn observe_same_volume_handoff(
        &self,
        source_root: ManagedRoot,
        destination_root: ManagedRoot,
        relative_path: &str,
        expected_source: &FileEvidence,
    ) -> Result<SameVolumeObservation, PortError> {
        let source = self.storage.observe_file(source_root, relative_path)?;
        let destination = self.storage.observe_file(destination_root, relative_path)?;

        if let Some(source) = source.as_ref() {
            if source != expected_source {
                return Err(PortError::new(
                    "HANDOFF_SOURCE_CHANGED",
                    format!("source evidence changed for {relative_path}"),
                ));
            }
        }
        if let Some(destination) = destination.as_ref() {
            if destination != expected_source {
                return Err(PortError::new(
                    "DESTINATION_CONFLICT",
                    format!("destination evidence conflicts for {relative_path}"),
                ));
            }
        }

        match (source, destination) {
            (Some(_), None) => Ok(SameVolumeObservation::SourceReady),
            (None, Some(destination)) => Ok(SameVolumeObservation::Applied(destination)),
            (Some(_), Some(_)) => Err(PortError::new(
                "HANDOFF_AMBIGUOUS",
                format!("both source and destination exist for {relative_path}"),
            )),
            (None, None) => Err(PortError::new(
                "HANDOFF_EVIDENCE_MISSING",
                format!("neither source nor destination exists for {relative_path}"),
            )),
        }
    }

    fn block_handoff(
        &self,
        record: CompletionRecord,
        problem: PortError,
    ) -> Result<HandoffProgress, PortError> {
        let blocked = self
            .journal
            .mark_completion_blocked(&record.operation_id, problem.code)?;
        Ok(HandoffProgress::Halt {
            status: CompletionExecutionStatus::Blocked,
            record: blocked,
            problem: Some(problem),
        })
    }

    fn block_file_handoff(
        &self,
        record: CompletionRecord,
        file: &CompletionFileRecord,
        problem: PortError,
    ) -> Result<HandoffProgress, PortError> {
        let updated =
            self.journal
                .mark_file_blocked(&record.operation_id, file.index, problem.code)?;
        self.block_handoff(updated, problem)
    }

    async fn revalidate_record(&self, record: &CompletionRecord) -> Result<(), PortError> {
        let fresh = self
            .preflight
            .preflight(&CompletionRequest {
                request_id: record.request_id.clone(),
                registry_id: record.registry_id.clone(),
            })
            .await?;

        if fresh.torrent_id != record.torrent_id
            || fresh.identity != record.identity
            || fresh.source_relative != record.source_relative
            || fresh.source_evidence != record.source_evidence
            || fresh.source_metainfo_digest != record.source_metainfo_digest
            || fresh.working_volume_id != record.working_volume_id
            || fresh.completed_volume_id != record.completed_volume_id
            || fresh.archive_volume_id != record.archive_volume_id
            || !fresh
                .working_save_path
                .eq_ignore_ascii_case(&record.working_save_path)
            || fresh.total_bytes != record.total_bytes
            || fresh.files.len() != record.files.len()
        {
            return Err(PortError::new(
                "COMPLETION_PREFLIGHT_STALE",
                "fresh completion preflight no longer matches the durable operation",
            ));
        }

        for (fresh_file, durable_file) in fresh.files.iter().zip(&record.files) {
            if fresh_file.relative_path != durable_file.relative_path
                || fresh_file.size != durable_file.size
                || fresh_file.source_evidence != durable_file.source_evidence
            {
                return Err(PortError::new(
                    "COMPLETION_PREFLIGHT_STALE",
                    format!(
                        "fresh completion file evidence changed for {}",
                        durable_file.relative_path
                    ),
                ));
            }
        }
        Ok(())
    }

    async fn observe_stop_once(
        &self,
        record: &CompletionRecord,
    ) -> Result<StopObservation, PortError> {
        let torrent = self.client.get(&record.torrent_id).await?.ok_or_else(|| {
            PortError::new(
                "TORRENT_NOT_FOUND",
                "qBittorrent record disappeared before completion handoff",
            )
        })?;
        if !torrent.is_complete() {
            return Err(PortError::new(
                "TORRENT_NOT_COMPLETE",
                "torrent is no longer complete during completion stop observation",
            ));
        }
        if !torrent
            .save_path
            .eq_ignore_ascii_case(&record.working_save_path)
        {
            return Err(PortError::new(
                "WORKING_OWNERSHIP_MISMATCH",
                "qBittorrent save path changed during completion stop observation",
            ));
        }
        Ok(if torrent.state.is_stopped() {
            StopObservation::Stopped
        } else {
            StopObservation::Running
        })
    }

    async fn observe_stop_bounded(
        &self,
        record: &CompletionRecord,
    ) -> Result<StopObservation, PortError> {
        for attempt in 0..self.observation_attempts {
            if self.observe_stop_once(record).await? == StopObservation::Stopped {
                return Ok(StopObservation::Stopped);
            }
            if attempt + 1 < self.observation_attempts {
                tokio::time::sleep(self.observation_delay).await;
            }
        }
        Ok(StopObservation::Running)
    }
}

fn completion_execution(
    status: CompletionExecutionStatus,
    record: CompletionRecord,
    problem: Option<PortError>,
    replayed: bool,
) -> CompletionExecution {
    CompletionExecution {
        status,
        record,
        problem,
        replayed,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use qb_domain::torrent::{ManifestFile, TorrentManifest, TorrentState};

    use crate::{
        registry::{RegisterIncoming, RegisterIncomingResult, RegistryRecord},
        storage::{FileIdentity, IncomingDeleteOutcome, IncomingFileSnapshot, StorageVolumeStatus},
        torrent::{
            AddTorrentRequest, EffectAttempt, EffectFuture, NetworkPreferences, PortFuture,
            QbitProbe, QueueSettings, TrackerEvidence, TransferInfo,
        },
    };

    use super::*;

    fn identity() -> TorrentIdentity {
        TorrentIdentity::new(Some([0x11; 20]), None).expect("identity")
    }

    fn registry() -> RegistryRecord {
        RegistryRecord {
            registry_id: "registry-1".into(),
            identity: identity(),
            state: RegistryState::Processing,
            source_relative: "sample.torrent".into(),
            source_metainfo_digest: Sha256::digest(b"metainfo").into(),
            operation_id: Some("admission-1".into()),
            archive_ref: None,
            handoff_file_count: 0,
            handoff_receipt_count: 0,
        }
    }

    struct FakeRegistry(RegistryRecord);

    impl TorrentRegistry for FakeRegistry {
        fn find_by_identity(
            &self,
            _identity: &TorrentIdentity,
        ) -> Result<Option<RegistryRecord>, PortError> {
            Ok(Some(self.0.clone()))
        }

        fn get_by_id(&self, registry_id: &str) -> Result<Option<RegistryRecord>, PortError> {
            Ok((registry_id == self.0.registry_id).then(|| self.0.clone()))
        }

        fn register_incoming(
            &self,
            _candidate: &RegisterIncoming,
        ) -> Result<RegisterIncomingResult, PortError> {
            Err(unused())
        }
    }

    struct FakeMetainfo;

    impl MetainfoReader for FakeMetainfo {
        fn parse(&self, _bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
            Ok(TorrentMetainfo {
                identity: identity(),
                manifest: TorrentManifest::new(vec![
                    ManifestFile {
                        path: "dir/a.bin".into(),
                        size: 10,
                    },
                    ManifestFile {
                        path: "dir/b.bin".into(),
                        size: 20,
                    },
                ])
                .expect("manifest"),
            })
        }
    }

    struct FakeStorage {
        files: Mutex<Vec<(ManagedRoot, String, FileEvidence)>>,
        cross_volume_payload: bool,
        cross_volume_archive: bool,
        delete_failures: Mutex<usize>,
        delete_calls: AtomicUsize,
    }

    impl FakeStorage {
        fn populated() -> Arc<Self> {
            Self::with_cross_volumes(false, false, 0)
        }

        fn cross_volume(delete_failures: usize) -> Arc<Self> {
            Self::with_cross_volumes(true, false, delete_failures)
        }

        fn cross_volume_archive(delete_failures: usize) -> Arc<Self> {
            Self::with_cross_volumes(false, true, delete_failures)
        }

        fn with_cross_volumes(
            cross_volume_payload: bool,
            cross_volume_archive: bool,
            delete_failures: usize,
        ) -> Arc<Self> {
            Arc::new(Self {
                files: Mutex::new(vec![
                    (
                        ManagedRoot::Incoming,
                        "sample.torrent".into(),
                        evidence(1, 1, 8),
                    ),
                    (ManagedRoot::Working, "dir/a.bin".into(), evidence(2, 2, 10)),
                    (ManagedRoot::Working, "dir/b.bin".into(), evidence(2, 3, 20)),
                ]),
                cross_volume_payload,
                cross_volume_archive,
                delete_failures: Mutex::new(delete_failures),
                delete_calls: AtomicUsize::new(0),
            })
        }
    }

    impl Storage for FakeStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            let (volume_id, free_bytes) = match root {
                ManagedRoot::Incoming => (1, 1_000),
                ManagedRoot::Archive if self.cross_volume_archive => (4, 1_000),
                ManagedRoot::Archive => (1, 1_000),
                ManagedRoot::Working => (2, 1_000),
                ManagedRoot::Completed if self.cross_volume_payload => (3, 1_000),
                ManagedRoot::Completed => (2, 1_000),
                ManagedRoot::Runtime => return Err(unused()),
            };
            Ok(StorageVolumeStatus {
                root,
                volume_id,
                free_bytes,
                total_bytes: 2_000,
            })
        }

        fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
            Ok(format!("{root:?}"))
        }

        fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
            Ok(root == ManagedRoot::Working && observed == "Working")
        }

        fn observe_file(
            &self,
            root: ManagedRoot,
            relative_path: &str,
        ) -> Result<Option<FileEvidence>, PortError> {
            Ok(self
                .files
                .lock()
                .expect("files mutex")
                .iter()
                .find(|(candidate_root, candidate_path, _)| {
                    *candidate_root == root && candidate_path == relative_path
                })
                .map(|(_, _, evidence)| evidence.clone()))
        }

        fn move_same_volume_no_replace(
            &self,
            source_root: ManagedRoot,
            source_relative: &str,
            destination_root: ManagedRoot,
            destination_relative: &str,
            expected_source: &FileEvidence,
        ) -> Result<SameVolumeMoveOutcome, PortError> {
            let mut files = self.files.lock().expect("files mutex");
            let Some(source_index) = files
                .iter()
                .position(|(root, path, _)| *root == source_root && path == source_relative)
            else {
                return Ok(SameVolumeMoveOutcome::SourceMissing);
            };
            let source_evidence = files[source_index].2.clone();
            if &source_evidence != expected_source {
                return Ok(SameVolumeMoveOutcome::SourceChanged {
                    observed: source_evidence,
                });
            }
            if let Some((_, _, observed)) = files
                .iter()
                .find(|(root, path, _)| *root == destination_root && path == destination_relative)
            {
                return Ok(SameVolumeMoveOutcome::DestinationExists {
                    observed: observed.clone(),
                });
            }
            files.remove(source_index);
            files.push((
                destination_root,
                destination_relative.to_owned(),
                source_evidence.clone(),
            ));
            Ok(SameVolumeMoveOutcome::Moved {
                destination: source_evidence,
            })
        }

        fn copy_to_temp_verified(
            &self,
            source_root: ManagedRoot,
            source_relative: &str,
            destination_root: ManagedRoot,
            temp_relative: &str,
            expected_source: &FileEvidence,
        ) -> Result<VerifiedCopyOutcome, PortError> {
            let allowed = (source_root == ManagedRoot::Working
                && destination_root == ManagedRoot::Completed
                && self.cross_volume_payload)
                || (source_root == ManagedRoot::Incoming
                    && destination_root == ManagedRoot::Archive
                    && self.cross_volume_archive);
            if !allowed {
                return Err(unused());
            }
            let mut files = self.files.lock().expect("files mutex");
            let Some((_, _, source)) = files
                .iter()
                .find(|(root, path, _)| *root == source_root && path == source_relative)
                .cloned()
            else {
                return Ok(VerifiedCopyOutcome::SourceMissing);
            };
            if &source != expected_source {
                return Ok(VerifiedCopyOutcome::SourceChanged { observed: source });
            }

            let sha256: [u8; 32] = if source_root == ManagedRoot::Incoming {
                Sha256::digest(b"metainfo").into()
            } else {
                Sha256::digest(source_relative.as_bytes()).into()
            };
            if let Some((_, _, existing)) = files
                .iter()
                .find(|(root, path, _)| *root == destination_root && path == temp_relative)
                .cloned()
            {
                if existing.size == source.size {
                    return Ok(VerifiedCopyOutcome::Verified {
                        temp: existing,
                        sha256,
                        created: false,
                    });
                }
                return Ok(VerifiedCopyOutcome::TempConflict {
                    observed: existing,
                    sha256,
                });
            }

            let temp = FileEvidence {
                identity: FileIdentity {
                    volume_id: match destination_root {
                        ManagedRoot::Archive => 4,
                        ManagedRoot::Completed => 3,
                        _ => expected_source.identity.volume_id,
                    },
                    file_id: source.identity.file_id + 100,
                },
                size: source.size,
                modified_marker: source.modified_marker + 1,
            };
            files.push((destination_root, temp_relative.to_owned(), temp.clone()));
            Ok(VerifiedCopyOutcome::Verified {
                temp,
                sha256,
                created: true,
            })
        }

        fn delete_managed_exact(
            &self,
            root: ManagedRoot,
            relative_path: &str,
            expected_evidence: &FileEvidence,
            _expected_sha256: &[u8; 32],
        ) -> Result<ManagedDeleteOutcome, PortError> {
            self.delete_calls.fetch_add(1, Ordering::SeqCst);
            let mut failures = self.delete_failures.lock().expect("delete failures mutex");
            if *failures > 0 {
                *failures -= 1;
                return Err(PortError::new(
                    "STORAGE_DELETE_UNCERTAIN",
                    "simulated exact-delete uncertainty",
                ));
            }
            drop(failures);

            let mut files = self.files.lock().expect("files mutex");
            let Some(index) = files.iter().position(|(candidate_root, path, _)| {
                *candidate_root == root && path == relative_path
            }) else {
                return Ok(ManagedDeleteOutcome::Missing);
            };
            if &files[index].2 != expected_evidence {
                return Ok(ManagedDeleteOutcome::Changed);
            }
            files.remove(index);
            Ok(ManagedDeleteOutcome::Deleted)
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Ok(vec!["sample.torrent".into()])
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            if relative_path != "sample.torrent" {
                return Err(PortError::new("STORAGE_NOT_FOUND", relative_path));
            }
            Ok(IncomingFileSnapshot {
                relative_path: relative_path.into(),
                evidence: evidence(1, 1, 8),
                bytes: b"metainfo".to_vec(),
            })
        }

        fn delete_incoming_exact(
            &self,
            _relative_path: &str,
            _expected_evidence: &FileEvidence,
            _expected_bytes: &[u8],
            _max_bytes: usize,
        ) -> Result<IncomingDeleteOutcome, PortError> {
            Err(unused())
        }
    }

    #[derive(Clone, Copy)]
    enum FakeStopEffect {
        NotSent,
        Uncertain,
        AcceptedAndStop,
    }

    #[derive(Clone, Copy)]
    enum FakeRemoveEffect {
        NotSent,
        Uncertain,
        AcceptedAndRemove,
    }

    struct FakeClient {
        torrent: Mutex<TorrentView>,
        files: Mutex<Vec<FileObservation>>,
        stop_effects: Mutex<Vec<FakeStopEffect>>,
        stop_calls: AtomicUsize,
        remove_effects: Mutex<Vec<FakeRemoveEffect>>,
        remove_calls: AtomicUsize,
        removed: std::sync::atomic::AtomicBool,
    }

    impl FakeClient {
        fn complete() -> Arc<Self> {
            Arc::new(Self {
                torrent: Mutex::new(TorrentView {
                    id: TorrentId::new("1111111111111111111111111111111111111111").expect("id"),
                    name: "sample".into(),
                    save_path: "Working".into(),
                    state: TorrentState::Uploading,
                    total_bytes: 30,
                    remaining_bytes: 0,
                    download_rate_bps: 0,
                    upload_rate_bps: 0,
                    progress_ppm: 1_000_000,
                    availability: Some(1.0),
                    peers_connected: 0,
                    peers_known: 0,
                    seeds_connected: 0,
                    seeds_known: 0,
                }),
                files: Mutex::new(vec![
                    FileObservation {
                        index: 0,
                        path: "dir/a.bin".into(),
                        size: 10,
                        progress_ppm: 1_000_000,
                        selected: true,
                        is_seed: true,
                        availability: Some(1.0),
                    },
                    FileObservation {
                        index: 1,
                        path: "dir/b.bin".into(),
                        size: 20,
                        progress_ppm: 1_000_000,
                        selected: true,
                        is_seed: true,
                        availability: Some(1.0),
                    },
                ]),
                stop_effects: Mutex::new(vec![FakeStopEffect::NotSent]),
                stop_calls: AtomicUsize::new(0),
                remove_effects: Mutex::new(vec![FakeRemoveEffect::NotSent]),
                remove_calls: AtomicUsize::new(0),
                removed: std::sync::atomic::AtomicBool::new(false),
            })
        }

        fn with_stop_effects(self: &Arc<Self>, effects: Vec<FakeStopEffect>) {
            *self.stop_effects.lock().expect("stop effects mutex") = effects;
        }

        fn with_remove_effects(self: &Arc<Self>, effects: Vec<FakeRemoveEffect>) {
            *self.remove_effects.lock().expect("remove effects mutex") = effects;
        }

        fn set_state(&self, state: TorrentState) {
            self.torrent.lock().expect("torrent mutex").state = state;
        }
    }

    impl TorrentClient for FakeClient {
        fn probe(&self) -> PortFuture<'_, QbitProbe> {
            Box::pin(async { Err(unused()) })
        }

        fn list(&self) -> PortFuture<'_, Vec<TorrentView>> {
            Box::pin(async { Err(unused()) })
        }

        fn get<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Option<TorrentView>> {
            Box::pin(async move {
                if self.removed.load(Ordering::SeqCst) {
                    return Ok(None);
                }
                let torrent = self.torrent.lock().expect("torrent mutex").clone();
                Ok((id == &torrent.id).then_some(torrent))
            })
        }

        fn transfer_info(&self) -> PortFuture<'_, TransferInfo> {
            Box::pin(async { Err(unused()) })
        }

        fn queue_settings(&self) -> PortFuture<'_, QueueSettings> {
            Box::pin(async { Err(unused()) })
        }

        fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences> {
            Box::pin(async {
                Ok(NetworkPreferences {
                    listen_port: 0,
                    upnp: false,
                    dht: false,
                    pex: false,
                    lsd: false,
                    current_network_interface: String::new(),
                    current_interface_address: String::new(),
                    max_connections: 0,
                    max_connections_per_torrent: 0,
                })
            })
        }

        fn trackers<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Vec<TrackerEvidence>> {
            Box::pin(async { Err(unused()) })
        }

        fn files<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Vec<FileObservation>> {
            Box::pin(async move { Ok(self.files.lock().expect("files mutex").clone()) })
        }

        fn add_torrent<'a>(&'a self, _request: &'a AddTorrentRequest) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }

        fn stop<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async move {
                self.stop_calls.fetch_add(1, Ordering::SeqCst);
                let effect = {
                    let mut effects = self.stop_effects.lock().expect("stop effects mutex");
                    if effects.is_empty() {
                        FakeStopEffect::NotSent
                    } else {
                        effects.remove(0)
                    }
                };
                match effect {
                    FakeStopEffect::NotSent => EffectAttempt::NotSent(unused()),
                    FakeStopEffect::Uncertain => EffectAttempt::Uncertain(PortError::new(
                        "QBIT_MUTATION_UNCERTAIN",
                        "simulated stop timeout after send",
                    )),
                    FakeStopEffect::AcceptedAndStop => {
                        self.set_state(TorrentState::Stopped);
                        EffectAttempt::Accepted
                    }
                }
            })
        }

        fn start<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }

        fn remove_keep_files<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async move {
                self.remove_calls.fetch_add(1, Ordering::SeqCst);
                let effect = {
                    let mut effects = self.remove_effects.lock().expect("remove effects mutex");
                    if effects.is_empty() {
                        FakeRemoveEffect::NotSent
                    } else {
                        effects.remove(0)
                    }
                };
                match effect {
                    FakeRemoveEffect::NotSent => EffectAttempt::NotSent(unused()),
                    FakeRemoveEffect::Uncertain => EffectAttempt::Uncertain(PortError::new(
                        "QBIT_MUTATION_UNCERTAIN",
                        "simulated record-removal timeout after send",
                    )),
                    FakeRemoveEffect::AcceptedAndRemove => {
                        self.removed.store(true, Ordering::SeqCst);
                        EffectAttempt::Accepted
                    }
                }
            })
        }

        fn set_active_downloads(&self, _value: u32) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }

        fn set_download_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }

        fn set_upload_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }
    }

    #[derive(Default)]
    struct FakeCompletionJournal {
        record: Mutex<Option<CompletionRecord>>,
    }

    impl FakeCompletionJournal {
        fn transition_file(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            next: CompletionFileState,
            destination: Option<&FileEvidence>,
            destination_sha256: Option<[u8; 32]>,
            problem_code: Option<&str>,
        ) -> Result<CompletionRecord, PortError> {
            let mut state = self.record.lock().expect("completion journal mutex");
            let record = state
                .as_mut()
                .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
            if &record.operation_id != operation_id
                || record.state != CompletionState::PayloadPending
            {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "fake file transition requires PayloadPending",
                ));
            }
            let file = record
                .files
                .iter_mut()
                .find(|file| file.index == file_index)
                .ok_or_else(|| {
                    PortError::new("COMPLETION_FILE_NOT_FOUND", file_index.to_string())
                })?;
            let allowed = match next {
                CompletionFileState::MovePending => matches!(
                    file.state,
                    CompletionFileState::Prepared | CompletionFileState::UnknownMove
                ),
                CompletionFileState::UnknownMove => file.state == CompletionFileState::MovePending,
                CompletionFileState::DestinationReceipted => matches!(
                    file.state,
                    CompletionFileState::MovePending | CompletionFileState::UnknownMove
                ),
                CompletionFileState::SourceDeletePending => matches!(
                    file.state,
                    CompletionFileState::DestinationReceipted
                        | CompletionFileState::UnknownSourceDelete
                ),
                CompletionFileState::UnknownSourceDelete => {
                    file.state == CompletionFileState::SourceDeletePending
                }
                CompletionFileState::HandedOff => matches!(
                    file.state,
                    CompletionFileState::MovePending
                        | CompletionFileState::UnknownMove
                        | CompletionFileState::SourceDeletePending
                        | CompletionFileState::UnknownSourceDelete
                ),
                CompletionFileState::Blocked => matches!(
                    file.state,
                    CompletionFileState::Prepared
                        | CompletionFileState::MovePending
                        | CompletionFileState::UnknownMove
                        | CompletionFileState::DestinationReceipted
                        | CompletionFileState::SourceDeletePending
                        | CompletionFileState::UnknownSourceDelete
                ),
                _ => false,
            };
            if !allowed {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "invalid fake completion file transition",
                ));
            }
            file.state = next;
            if let Some(destination) = destination {
                file.destination_evidence = Some(destination.clone());
            }
            if let Some(destination_sha256) = destination_sha256 {
                file.destination_sha256 = Some(destination_sha256);
            }
            file.problem_code = problem_code.map(str::to_owned);
            file.revision += 1;
            Ok(record.clone())
        }

        fn transition(
            &self,
            operation_id: &OperationId,
            expected: &[CompletionState],
            next: CompletionState,
            problem_code: Option<&str>,
        ) -> Result<CompletionRecord, PortError> {
            let mut state = self.record.lock().expect("completion journal mutex");
            let record = state
                .as_mut()
                .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
            if &record.operation_id != operation_id || !expected.contains(&record.state) {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "invalid fake completion transition",
                ));
            }
            record.state = next;
            record.problem_code = problem_code.map(str::to_owned);
            record.revision += 1;
            Ok(record.clone())
        }
    }

    impl CompletionJournal for FakeCompletionJournal {
        fn lookup_completion_request(
            &self,
            request: &CompletionRequest,
        ) -> Result<Option<CompletionReservation>, PortError> {
            Ok(self
                .record
                .lock()
                .expect("completion journal mutex")
                .as_ref()
                .filter(|record| record.request_id == request.request_id)
                .cloned()
                .map(CompletionReservation::Replay))
        }

        fn reserve_completion(
            &self,
            preflight: &CompletionPreflight,
        ) -> Result<CompletionReservation, PortError> {
            let mut state = self.record.lock().expect("completion journal mutex");
            if let Some(record) = state.as_ref() {
                return Ok(CompletionReservation::ActiveConflict {
                    operation_id: record.operation_id.clone(),
                });
            }
            let record = CompletionRecord {
                request_id: preflight.request_id.clone(),
                operation_id: OperationId::new("completion-operation-1").expect("operation id"),
                registry_id: preflight.registry_id.clone(),
                torrent_id: preflight.torrent_id.clone(),
                identity: preflight.identity.clone(),
                source_relative: preflight.source_relative.clone(),
                source_evidence: preflight.source_evidence.clone(),
                source_metainfo_digest: preflight.source_metainfo_digest,
                working_volume_id: preflight.working_volume_id,
                completed_volume_id: preflight.completed_volume_id,
                archive_volume_id: preflight.archive_volume_id,
                working_save_path: preflight.working_save_path.clone(),
                total_bytes: preflight.total_bytes,
                archive_destination_evidence: None,
                archive_sha256: None,
                state: CompletionState::Prepared,
                problem_code: None,
                revision: 1,
                files: preflight
                    .files
                    .iter()
                    .enumerate()
                    .map(|(index, file)| CompletionFileRecord {
                        index: u32::try_from(index).expect("file index"),
                        relative_path: file.relative_path.clone(),
                        size: file.size,
                        source_evidence: file.source_evidence.clone(),
                        strategy: preflight.payload_strategy(),
                        state: CompletionFileState::Prepared,
                        destination_evidence: None,
                        destination_sha256: None,
                        problem_code: None,
                        revision: 1,
                    })
                    .collect(),
            };
            *state = Some(record.clone());
            Ok(CompletionReservation::New(record))
        }

        fn get_completion(
            &self,
            operation_id: &OperationId,
        ) -> Result<Option<CompletionRecord>, PortError> {
            Ok(self
                .record
                .lock()
                .expect("completion journal mutex")
                .as_ref()
                .filter(|record| &record.operation_id == operation_id)
                .cloned())
        }

        fn list_recoverable_completions(&self) -> Result<Vec<CompletionRecord>, PortError> {
            Ok(self
                .record
                .lock()
                .expect("completion journal mutex")
                .as_ref()
                .filter(|record| {
                    !matches!(
                        record.state,
                        CompletionState::Finished
                            | CompletionState::Blocked
                            | CompletionState::Failed
                    )
                })
                .cloned()
                .into_iter()
                .collect())
        }

        fn mark_stop_pending(
            &self,
            operation_id: &OperationId,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::Prepared],
                CompletionState::StopPending,
                None,
            )
        }

        fn mark_unknown_stop(
            &self,
            operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::StopPending],
                CompletionState::UnknownStop,
                Some(problem_code),
            )
        }

        fn retry_stop(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::UnknownStop, CompletionState::StopPending],
                CompletionState::Prepared,
                None,
            )
        }

        fn mark_stopped(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::StopPending, CompletionState::UnknownStop],
                CompletionState::Stopped,
                None,
            )
        }

        fn mark_completion_blocked(
            &self,
            operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[
                    CompletionState::Prepared,
                    CompletionState::StopPending,
                    CompletionState::UnknownStop,
                    CompletionState::Stopped,
                    CompletionState::ArchivePending,
                    CompletionState::UnknownArchive,
                    CompletionState::PayloadPending,
                    CompletionState::RemoveRecordPending,
                    CompletionState::UnknownRemoveRecord,
                ],
                CompletionState::Blocked,
                Some(problem_code),
            )
        }

        fn mark_completion_failed(
            &self,
            operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[
                    CompletionState::StopPending,
                    CompletionState::RemoveRecordPending,
                ],
                CompletionState::Failed,
                Some(problem_code),
            )
        }

        fn mark_archive_pending(
            &self,
            operation_id: &OperationId,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::Stopped],
                CompletionState::ArchivePending,
                None,
            )
        }

        fn mark_unknown_archive(
            &self,
            operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::ArchivePending],
                CompletionState::UnknownArchive,
                Some(problem_code),
            )
        }

        fn retry_archive(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[
                    CompletionState::ArchivePending,
                    CompletionState::UnknownArchive,
                ],
                CompletionState::Stopped,
                None,
            )
        }

        fn mark_archive_destination_receipted(
            &self,
            operation_id: &OperationId,
            destination: &FileEvidence,
            destination_sha256: [u8; 32],
        ) -> Result<CompletionRecord, PortError> {
            let mut state = self.record.lock().expect("completion journal mutex");
            let record = state
                .as_mut()
                .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
            if &record.operation_id != operation_id
                || !matches!(
                    record.state,
                    CompletionState::ArchivePending | CompletionState::UnknownArchive
                )
            {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "invalid fake Archive destination receipt transition",
                ));
            }
            record.archive_destination_evidence = Some(destination.clone());
            record.archive_sha256 = Some(destination_sha256);
            record.problem_code = None;
            record.revision += 1;
            Ok(record.clone())
        }

        fn mark_archive_receipted(
            &self,
            operation_id: &OperationId,
            destination: &FileEvidence,
            destination_sha256: [u8; 32],
        ) -> Result<CompletionRecord, PortError> {
            let mut state = self.record.lock().expect("completion journal mutex");
            let record = state
                .as_mut()
                .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
            if &record.operation_id != operation_id
                || !matches!(
                    record.state,
                    CompletionState::ArchivePending | CompletionState::UnknownArchive
                )
            {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "invalid fake archive receipt transition",
                ));
            }
            record.archive_destination_evidence = Some(destination.clone());
            record.archive_sha256 = Some(destination_sha256);
            record.state = CompletionState::PayloadPending;
            record.problem_code = None;
            record.revision += 1;
            Ok(record.clone())
        }

        fn mark_file_move_pending(
            &self,
            operation_id: &OperationId,
            file_index: u32,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::MovePending,
                None,
                None,
                None,
            )
        }

        fn mark_file_unknown_move(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::UnknownMove,
                None,
                None,
                Some(problem_code),
            )
        }

        fn mark_file_destination_receipted(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            destination: &FileEvidence,
            destination_sha256: [u8; 32],
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::DestinationReceipted,
                Some(destination),
                Some(destination_sha256),
                None,
            )
        }

        fn mark_file_source_delete_pending(
            &self,
            operation_id: &OperationId,
            file_index: u32,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::SourceDeletePending,
                None,
                None,
                None,
            )
        }

        fn mark_file_unknown_source_delete(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::UnknownSourceDelete,
                None,
                None,
                Some(problem_code),
            )
        }

        fn mark_file_handed_off(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            destination: &FileEvidence,
            destination_sha256: Option<[u8; 32]>,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::HandedOff,
                Some(destination),
                destination_sha256,
                None,
            )
        }

        fn mark_file_blocked(
            &self,
            operation_id: &OperationId,
            file_index: u32,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition_file(
                operation_id,
                file_index,
                CompletionFileState::Blocked,
                None,
                None,
                Some(problem_code),
            )
        }

        fn mark_payload_handed_off(
            &self,
            operation_id: &OperationId,
        ) -> Result<CompletionRecord, PortError> {
            let state = self.record.lock().expect("completion journal mutex");
            let record = state
                .as_ref()
                .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
            if &record.operation_id != operation_id
                || record.state != CompletionState::PayloadPending
                || record
                    .files
                    .iter()
                    .any(|file| file.state != CompletionFileState::HandedOff)
            {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "payload is not fully handed off",
                ));
            }
            drop(state);
            self.transition(
                operation_id,
                &[CompletionState::PayloadPending],
                CompletionState::RemoveRecordPending,
                None,
            )
        }

        fn mark_unknown_remove_record(
            &self,
            operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::RemoveRecordPending],
                CompletionState::UnknownRemoveRecord,
                Some(problem_code),
            )
        }

        fn retry_remove_record(
            &self,
            operation_id: &OperationId,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[CompletionState::UnknownRemoveRecord],
                CompletionState::RemoveRecordPending,
                None,
            )
        }

        fn finish_completion(
            &self,
            operation_id: &OperationId,
        ) -> Result<CompletionRecord, PortError> {
            self.transition(
                operation_id,
                &[
                    CompletionState::RemoveRecordPending,
                    CompletionState::UnknownRemoveRecord,
                ],
                CompletionState::Finished,
                None,
            )
        }
    }

    fn evidence(volume_id: u64, file_id: u64, size: u64) -> FileEvidence {
        FileEvidence {
            identity: FileIdentity { volume_id, file_id },
            size,
            modified_marker: 1,
        }
    }

    fn service(storage: Arc<FakeStorage>, client: Arc<FakeClient>) -> CompletionPreflightService {
        CompletionPreflightService::new(
            Arc::new(FakeRegistry(registry())),
            storage,
            Arc::new(FakeMetainfo),
            client,
            1024,
        )
    }

    fn request() -> CompletionRequest {
        CompletionRequest {
            request_id: RequestId::new("completion-request-1").expect("request id"),
            registry_id: "registry-1".into(),
        }
    }

    fn completion_service(
        journal: Arc<FakeCompletionJournal>,
        storage: Arc<FakeStorage>,
        client: Arc<FakeClient>,
    ) -> CompletionService {
        CompletionService::new(
            journal,
            Arc::new(FakeRegistry(registry())),
            storage,
            Arc::new(FakeMetainfo),
            client,
            1024,
        )
        .with_observation_policy(1, Duration::ZERO)
    }

    fn execution(result: CompletionExecutionResult) -> CompletionExecution {
        match result {
            CompletionExecutionResult::Execution(execution) => *execution,
            other => panic!("unexpected completion result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn preflight_requires_complete_exact_selected_manifest_and_absent_destinations() {
        let result = service(FakeStorage::populated(), FakeClient::complete())
            .preflight(&request())
            .await
            .expect("preflight");

        assert_eq!(result.files.len(), 2);
        assert_eq!(result.total_bytes, 30);
        assert_eq!(result.working_volume_id, result.completed_volume_id);
        assert_eq!(result.source_relative, "sample.torrent");
    }

    #[tokio::test]
    async fn incomplete_qbit_suffix_cannot_satisfy_manifest() {
        let client = FakeClient::complete();
        client.files.lock().expect("files mutex")[0].path = "dir/a.bin.!qB".into();

        let error = service(FakeStorage::populated(), client)
            .preflight(&request())
            .await
            .expect_err("must reject incomplete temporary name");

        assert_eq!(error.code, "MANIFEST_MISMATCH");
    }

    #[tokio::test]
    async fn existing_completed_destination_blocks_before_any_mutation() {
        let storage = FakeStorage::populated();
        storage.files.lock().expect("files mutex").push((
            ManagedRoot::Completed,
            "dir/a.bin".into(),
            evidence(2, 9, 10),
        ));

        let error = service(storage, FakeClient::complete())
            .preflight(&request())
            .await
            .expect_err("must reject destination conflict");

        assert_eq!(error.code, "DESTINATION_CONFLICT");
    }

    #[tokio::test]
    async fn same_volume_completion_handoff_receipts_gate_remove_record() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::populated();
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);

        let completed = execution(
            completion_service(journal, storage.clone(), client)
                .execute(&request())
                .await
                .expect("execute completion"),
        );

        assert_eq!(
            completed.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(completed.record.state, CompletionState::RemoveRecordPending);
        assert!(completed.record.archive_destination_evidence.is_some());
        assert_eq!(
            completed.record.archive_sha256,
            Some(Sha256::digest(b"metainfo").into())
        );
        assert!(completed
            .record
            .files
            .iter()
            .all(|file| file.state == CompletionFileState::HandedOff));
        assert!(completed
            .record
            .files
            .iter()
            .all(|file| file.destination_evidence.is_some()));

        let files = storage.files.lock().expect("files mutex");
        assert!(!files
            .iter()
            .any(|(root, path, _)| { *root == ManagedRoot::Incoming && path == "sample.torrent" }));
        assert!(files
            .iter()
            .any(|(root, path, _)| { *root == ManagedRoot::Archive && path == "sample.torrent" }));
        assert!(!files
            .iter()
            .any(|(root, _, _)| *root == ManagedRoot::Working));
        assert_eq!(
            files
                .iter()
                .filter(|(root, _, _)| *root == ManagedRoot::Completed)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn completion_removes_qbit_record_after_receipts_and_finishes() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::populated();
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);
        client.with_remove_effects(vec![FakeRemoveEffect::AcceptedAndRemove]);

        let completed = execution(
            completion_service(journal, storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("execute completion"),
        );

        assert_eq!(completed.status, CompletionExecutionStatus::Finished);
        assert_eq!(completed.record.state, CompletionState::Finished);
        assert_eq!(client.remove_calls.load(Ordering::SeqCst), 1);
        assert!(client.removed.load(Ordering::SeqCst));

        let files = storage.files.lock().expect("files mutex");
        assert!(files
            .iter()
            .any(|(root, path, _)| *root == ManagedRoot::Archive && path == "sample.torrent"));
        assert_eq!(
            files
                .iter()
                .filter(|(root, _, _)| *root == ManagedRoot::Completed)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn remove_uncertainty_restart_does_not_duplicate_remove_request() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::populated();
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);
        client.with_remove_effects(vec![
            FakeRemoveEffect::Uncertain,
            FakeRemoveEffect::AcceptedAndRemove,
        ]);

        let first = execution(
            completion_service(journal.clone(), storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("first completion"),
        );
        assert_eq!(first.status, CompletionExecutionStatus::UnknownRemoveRecord);
        assert_eq!(first.record.state, CompletionState::UnknownRemoveRecord);
        assert_eq!(client.remove_calls.load(Ordering::SeqCst), 1);

        let recovered = completion_service(journal.clone(), storage.clone(), client.clone())
            .recover_all()
            .await
            .expect("restart recovery");
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].status,
            CompletionExecutionStatus::UnknownRemoveRecord
        );
        assert_eq!(
            recovered[0].record.state,
            CompletionState::UnknownRemoveRecord
        );
        assert_eq!(client.remove_calls.load(Ordering::SeqCst), 1);

        let replay = execution(
            completion_service(journal, storage, client.clone())
                .execute(&request())
                .await
                .expect("explicit replay"),
        );
        assert!(replay.replayed);
        assert_eq!(replay.status, CompletionExecutionStatus::Finished);
        assert_eq!(replay.record.state, CompletionState::Finished);
        assert_eq!(client.remove_calls.load(Ordering::SeqCst), 2);
        assert!(client.removed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cross_volume_archive_is_verified_published_receipted_then_source_deleted() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::cross_volume_archive(0);
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);

        let completed = execution(
            completion_service(journal, storage.clone(), client)
                .execute(&request())
                .await
                .expect("cross-volume Archive completion"),
        );

        assert_eq!(
            completed.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(completed.record.state, CompletionState::RemoveRecordPending);
        assert_eq!(
            completed.record.archive_sha256,
            Some(Sha256::digest(b"metainfo").into())
        );
        assert_eq!(
            completed
                .record
                .archive_destination_evidence
                .as_ref()
                .map(|evidence| evidence.identity.volume_id),
            Some(4)
        );
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 1);

        let files = storage.files.lock().expect("files mutex");
        assert!(!files
            .iter()
            .any(|(root, path, _)| *root == ManagedRoot::Incoming && path == "sample.torrent"));
        assert!(files
            .iter()
            .any(|(root, path, _)| *root == ManagedRoot::Archive && path == "sample.torrent"));
        assert!(!files
            .iter()
            .any(|(_, path, _)| path.starts_with("_qbctl_tmp/")));
    }

    #[tokio::test]
    async fn cross_volume_archive_delete_uncertainty_requires_explicit_replay() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::cross_volume_archive(1);
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);

        let first = execution(
            completion_service(journal.clone(), storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("first cross-volume Archive completion"),
        );
        assert_eq!(first.status, CompletionExecutionStatus::UnknownArchive);
        assert_eq!(first.record.state, CompletionState::UnknownArchive);
        assert!(first.record.archive_destination_evidence.is_some());
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 1);

        let recovered = completion_service(journal.clone(), storage.clone(), client.clone())
            .recover_all()
            .await
            .expect("cross-volume Archive recovery");
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].status,
            CompletionExecutionStatus::UnknownArchive
        );
        assert_eq!(recovered[0].record.state, CompletionState::UnknownArchive);
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 1);

        let replay = execution(
            completion_service(journal, storage.clone(), client)
                .execute(&request())
                .await
                .expect("explicit Archive replay"),
        );
        assert!(replay.replayed);
        assert_eq!(
            replay.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(replay.record.state, CompletionState::RemoveRecordPending);
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cross_volume_payload_is_verified_receipted_then_source_deleted() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::cross_volume(0);
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);

        let completed = execution(
            completion_service(journal, storage.clone(), client)
                .execute(&request())
                .await
                .expect("cross-volume completion"),
        );

        assert_eq!(
            completed.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(completed.record.state, CompletionState::RemoveRecordPending);
        assert!(completed.record.files.iter().all(|file| {
            file.state == CompletionFileState::HandedOff
                && file.destination_evidence.is_some()
                && file.destination_sha256.is_some()
        }));
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 2);

        let files = storage.files.lock().expect("files mutex");
        assert!(!files
            .iter()
            .any(|(root, _, _)| *root == ManagedRoot::Working));
        assert_eq!(
            files
                .iter()
                .filter(|(root, path, _)| {
                    *root == ManagedRoot::Completed && !path.starts_with("_qbctl_tmp/")
                })
                .count(),
            2
        );
        assert!(!files
            .iter()
            .any(|(_, path, _)| path.starts_with("_qbctl_tmp/")));
    }

    #[tokio::test]
    async fn cross_volume_source_delete_uncertainty_requires_explicit_replay() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::cross_volume(1);
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::AcceptedAndStop]);

        let first = execution(
            completion_service(journal.clone(), storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("first cross-volume completion"),
        );
        assert_eq!(first.status, CompletionExecutionStatus::UnknownSourceDelete);
        assert_eq!(
            first.record.files[0].state,
            CompletionFileState::UnknownSourceDelete
        );
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 1);

        let recovered = completion_service(journal.clone(), storage.clone(), client.clone())
            .recover_all()
            .await
            .expect("restart recovery");
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].status,
            CompletionExecutionStatus::UnknownSourceDelete
        );
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 1);

        let replay = execution(
            completion_service(journal, storage.clone(), client)
                .execute(&request())
                .await
                .expect("explicit replay"),
        );
        assert!(replay.replayed);
        assert_eq!(
            replay.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(replay.record.state, CompletionState::RemoveRecordPending);
        assert_eq!(storage.delete_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stop_uncertainty_recovers_by_observation_without_duplicate_stop() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::populated();
        let client = FakeClient::complete();
        client.with_stop_effects(vec![FakeStopEffect::Uncertain]);

        let first = execution(
            completion_service(journal.clone(), storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("execute completion"),
        );
        assert_eq!(first.status, CompletionExecutionStatus::UnknownStop);
        assert_eq!(first.record.state, CompletionState::UnknownStop);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);

        client.set_state(TorrentState::Stopped);
        let recovered = completion_service(journal, storage, client.clone())
            .recover_all()
            .await
            .expect("recover completion");
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].status,
            CompletionExecutionStatus::UnknownRemoveRecord
        );
        assert_eq!(
            recovered[0].record.state,
            CompletionState::UnknownRemoveRecord
        );
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn explicit_replay_retries_unknown_stop_only_after_running_observation() {
        let journal = Arc::new(FakeCompletionJournal::default());
        let storage = FakeStorage::populated();
        let client = FakeClient::complete();
        client.with_stop_effects(vec![
            FakeStopEffect::Uncertain,
            FakeStopEffect::AcceptedAndStop,
        ]);

        let first = execution(
            completion_service(journal.clone(), storage.clone(), client.clone())
                .execute(&request())
                .await
                .expect("first completion"),
        );
        assert_eq!(first.status, CompletionExecutionStatus::UnknownStop);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);

        let replay = execution(
            completion_service(journal, storage, client.clone())
                .execute(&request())
                .await
                .expect("explicit replay"),
        );
        assert!(replay.replayed);
        assert_eq!(
            replay.status,
            CompletionExecutionStatus::RemoveRecordPending
        );
        assert_eq!(replay.record.state, CompletionState::RemoveRecordPending);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 2);
    }

    fn unused() -> PortError {
        PortError::new("UNUSED", "unused in completion preflight test")
    }
}
