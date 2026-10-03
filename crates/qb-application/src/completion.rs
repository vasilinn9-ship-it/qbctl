use std::{collections::BTreeMap, sync::Arc, time::Duration};

use qb_domain::{
    torrent::{TorrentId, TorrentIdentity, TorrentMetainfo},
    OperationId, RequestId,
};
use sha2::{Digest, Sha256};

use crate::{
    registry::{RegistryState, TorrentRegistry},
    storage::{FileEvidence, ManagedRoot, Storage},
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

    fn mark_stop_pending(&self, operation_id: &OperationId)
        -> Result<CompletionRecord, PortError>;

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
    Blocked,
    UnknownStop,
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
                storage,
                metainfo,
                client.clone(),
                max_metainfo_bytes,
            ),
            journal,
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
                    completion_problem("COMPLETION_BLOCKED", "completion is blocked"),
                    replayed,
                ));
            }
            CompletionState::Failed => {
                return Ok(completion_execution(
                    CompletionExecutionStatus::Failed,
                    record,
                    completion_problem("COMPLETION_FAILED", "completion failed"),
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
                return Ok(completion_execution(
                    CompletionExecutionStatus::Stopped,
                    record,
                    None,
                    replayed,
                ));
            }
            CompletionState::StopPending | CompletionState::UnknownStop => {
                match self.observe_stop_bounded(&record).await {
                    Ok(StopObservation::Stopped) => {
                        let stopped = self.journal.mark_stopped(&record.operation_id)?;
                        return Ok(completion_execution(
                            CompletionExecutionStatus::Stopped,
                            stopped,
                            None,
                            replayed,
                        ));
                    }
                    Ok(StopObservation::Running) if !explicit_request => {
                        let unknown = if record.state == CompletionState::UnknownStop {
                            record
                        } else {
                            self.journal.mark_unknown_stop(
                                &record.operation_id,
                                "QBIT_STOP_UNCERTAIN",
                            )?
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
                            self.journal.mark_unknown_stop(
                                &record.operation_id,
                                "QBIT_STOP_UNCERTAIN",
                            )?
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
                return Ok(completion_execution(
                    CompletionExecutionStatus::Stopped,
                    stopped,
                    None,
                    replayed,
                ));
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
                    Ok(completion_execution(
                        CompletionExecutionStatus::Stopped,
                        stopped,
                        None,
                        replayed,
                    ))
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
        if !torrent.save_path.eq_ignore_ascii_case(&record.working_save_path) {
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

fn completion_problem(
    fallback_code: &'static str,
    fallback_message: &'static str,
) -> Option<PortError> {
    Some(PortError::new(fallback_code, fallback_message))
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
    use std::sync::{Arc, Mutex};

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
    }

    impl FakeStorage {
        fn populated() -> Arc<Self> {
            Arc::new(Self {
                files: Mutex::new(vec![
                    (ManagedRoot::Working, "dir/a.bin".into(), evidence(2, 2, 10)),
                    (ManagedRoot::Working, "dir/b.bin".into(), evidence(2, 3, 20)),
                ]),
            })
        }
    }

    impl Storage for FakeStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            let (volume_id, free_bytes) = match root {
                ManagedRoot::Incoming | ManagedRoot::Archive => (1, 1_000),
                ManagedRoot::Working | ManagedRoot::Completed => (2, 1_000),
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

    struct FakeClient {
        torrent: TorrentView,
        files: Mutex<Vec<FileObservation>>,
    }

    impl FakeClient {
        fn complete() -> Arc<Self> {
            Arc::new(Self {
                torrent: TorrentView {
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
                },
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
            })
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
            Box::pin(async move { Ok((id == &self.torrent.id).then(|| self.torrent.clone())) })
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
            Box::pin(async { EffectAttempt::NotSent(unused()) })
        }

        fn start<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused()) })
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

    fn unused() -> PortError {
        PortError::new("UNUSED", "unused in completion preflight test")
    }
}
