use std::{sync::Arc, time::Duration};

use qb_domain::{
    torrent::{TorrentIdentity, TorrentState},
    OperationId, RequestId,
};
use sha2::{Digest, Sha256};

use crate::{
    mutation::MutationDisposition,
    storage::{FileEvidence, IncomingScan, ManagedRoot, Storage},
    torrent::{AddTorrentRequest, EffectAttempt, TorrentClient},
    PortError,
};

pub const ADMISSION_FINGERPRINT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionReservationRequest {
    pub request_id: RequestId,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub reserved_bytes: u64,
    pub working_save_path: String,
}

impl AdmissionReservationRequest {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-admission-fingerprint-v1\0");
        if let Some(v1) = self.identity.v1 {
            digest.update(b"v1");
            digest.update(v1);
        }
        digest.update([0]);
        if let Some(v2) = self.identity.v2 {
            digest.update(b"v2");
            digest.update(v2);
        }
        digest.update([0]);
        digest.update(self.source_relative.as_bytes());
        digest.update([0]);
        digest.update(self.source_evidence.identity.volume_id.to_be_bytes());
        digest.update(self.source_evidence.identity.file_id.to_be_bytes());
        digest.update(self.source_evidence.size.to_be_bytes());
        digest.update(self.source_evidence.modified_marker.to_be_bytes());
        digest.update(self.source_metainfo_digest);
        digest.update(self.working_volume_id.to_be_bytes());
        digest.update(self.reserved_bytes.to_be_bytes());
        digest.update(self.working_save_path.as_bytes());
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionRecord {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub registry_id: String,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub reserved_bytes: u64,
    pub working_save_path: String,
    pub reservation_active: bool,
    pub checkpoint: String,
    pub disposition: MutationDisposition,
    pub pending_effect_kind: Option<String>,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionReservationResult {
    New(AdmissionRecord),
    Replay(AdmissionRecord),
    Conflict { operation_id: OperationId },
}

pub trait AdmissionJournal: Send + Sync {
    fn reserve_admission(
        &self,
        request: &AdmissionReservationRequest,
    ) -> Result<AdmissionReservationResult, PortError>;

    fn get_admission(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<AdmissionRecord>, PortError>;

    fn list_recoverable_admissions(&self) -> Result<Vec<AdmissionRecord>, PortError>;

    fn capacity_reservations(
        &self,
        working_volume_id: u64,
    ) -> Result<Vec<CapacityReservation>, PortError>;

    fn mark_admission_effect_pending(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError>;

    fn mark_admission_not_submitted(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError>;

    fn mark_admission_retry_ready(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError>;

    fn mark_admission_unknown(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError>;

    fn mark_admission_observed_applied(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError>;

    fn finish_admission(&self, operation_id: &OperationId) -> Result<AdmissionRecord, PortError>;

    fn mark_admission_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionExecutionStatus {
    Finished,
    Blocked,
    Unknown,
    Failed,
}

#[derive(Debug)]
pub struct AdmissionExecution {
    pub status: AdmissionExecutionStatus,
    pub record: AdmissionRecord,
    pub problem: Option<PortError>,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum AdmissionExecutionResult {
    Execution(Box<AdmissionExecution>),
    Conflict { operation_id: OperationId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AddObservation {
    Absent,
    Pending,
    Applied,
}

pub struct AdmissionService {
    journal: Arc<dyn AdmissionJournal>,
    storage: Arc<dyn Storage>,
    client: Arc<dyn TorrentClient>,
    max_metainfo_bytes: usize,
    capacity_reserve_bytes: u64,
    observation_attempts: usize,
    observation_delay: Duration,
    lane: tokio::sync::Mutex<()>,
}

impl AdmissionService {
    pub fn new(
        journal: Arc<dyn AdmissionJournal>,
        storage: Arc<dyn Storage>,
        client: Arc<dyn TorrentClient>,
        max_metainfo_bytes: usize,
        capacity_reserve_bytes: u64,
    ) -> Self {
        Self {
            journal,
            storage,
            client,
            max_metainfo_bytes,
            capacity_reserve_bytes,
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
        request: &AdmissionReservationRequest,
    ) -> Result<AdmissionExecutionResult, PortError> {
        let _guard = self.lane.lock().await;
        let (record, replayed) = match self.journal.reserve_admission(request)? {
            AdmissionReservationResult::New(record) => (record, false),
            AdmissionReservationResult::Replay(record) => (record, true),
            AdmissionReservationResult::Conflict { operation_id } => {
                return Ok(AdmissionExecutionResult::Conflict { operation_id });
            }
        };

        self.advance(record, replayed, true)
            .await
            .map(|execution| AdmissionExecutionResult::Execution(Box::new(execution)))
    }

    pub async fn recover_all(&self) -> Result<Vec<AdmissionExecution>, PortError> {
        let _guard = self.lane.lock().await;
        let records = self.journal.list_recoverable_admissions()?;
        let mut executions = Vec::with_capacity(records.len());
        for record in records {
            executions.push(self.advance(record, true, false).await?);
        }
        Ok(executions)
    }

    async fn advance(
        &self,
        mut record: AdmissionRecord,
        replayed: bool,
        explicit_request: bool,
    ) -> Result<AdmissionExecution, PortError> {
        if explicit_request && record.disposition == MutationDisposition::Blocked {
            record = self
                .journal
                .mark_admission_retry_ready(&record.operation_id)?;
        }

        match record.disposition {
            MutationDisposition::Finished => {
                return Ok(admission_execution(
                    AdmissionExecutionStatus::Finished,
                    record,
                    None,
                    replayed,
                ));
            }
            MutationDisposition::Failed => {
                let problem = record
                    .problem_code
                    .as_deref()
                    .map(|code| PortError::new(admission_problem_code(code), "admission failed"));
                return Ok(admission_execution(
                    AdmissionExecutionStatus::Failed,
                    record,
                    problem,
                    replayed,
                ));
            }
            MutationDisposition::Blocked => {
                let problem = record
                    .problem_code
                    .as_deref()
                    .map(|code| PortError::new(admission_problem_code(code), "admission blocked"));
                return Ok(admission_execution(
                    AdmissionExecutionStatus::Blocked,
                    record,
                    problem,
                    replayed,
                ));
            }
            MutationDisposition::ObservedApplied => {
                let finished = self.journal.finish_admission(&record.operation_id)?;
                return Ok(admission_execution(
                    AdmissionExecutionStatus::Finished,
                    finished,
                    None,
                    replayed,
                ));
            }
            MutationDisposition::EffectPending | MutationDisposition::Unknown => {
                match self.observe_bounded(&record).await {
                    Ok(AddObservation::Applied) => {
                        let observed = self
                            .journal
                            .mark_admission_observed_applied(&record.operation_id)?;
                        let finished = self.journal.finish_admission(&observed.operation_id)?;
                        return Ok(admission_execution(
                            AdmissionExecutionStatus::Finished,
                            finished,
                            None,
                            replayed,
                        ));
                    }
                    Ok(AddObservation::Pending) => {
                        let problem = PortError::new(
                            "QBIT_POSTCONDITION_UNCONFIRMED",
                            "qBittorrent contains the intended torrent at the managed Working path, but it has not settled into the requested stopped state",
                        );
                        let unknown = if record.disposition == MutationDisposition::Unknown {
                            record
                        } else {
                            self.journal
                                .mark_admission_unknown(&record.operation_id, problem.code)?
                        };
                        return Ok(admission_execution(
                            AdmissionExecutionStatus::Unknown,
                            unknown,
                            Some(problem),
                            replayed,
                        ));
                    }
                    Ok(AddObservation::Absent) => {
                        if record.disposition == MutationDisposition::Unknown && !explicit_request {
                            let problem = PortError::new(
                                "QBIT_MUTATION_UNCERTAIN",
                                "admission remains uncertain after restart because the intended torrent was not observed; an explicit replay is required before a new add attempt",
                            );
                            return Ok(admission_execution(
                                AdmissionExecutionStatus::Unknown,
                                record,
                                Some(problem),
                                replayed,
                            ));
                        }
                        record = self
                            .journal
                            .mark_admission_retry_ready(&record.operation_id)?;
                    }
                    Err(problem) => {
                        let unknown = if record.disposition == MutationDisposition::Unknown {
                            record
                        } else {
                            self.journal.mark_admission_unknown(
                                &record.operation_id,
                                "QBIT_MUTATION_UNCERTAIN",
                            )?
                        };
                        return Ok(admission_execution(
                            AdmissionExecutionStatus::Unknown,
                            unknown,
                            Some(problem),
                            replayed,
                        ));
                    }
                }
            }
            MutationDisposition::Prepared => {}
        }

        let metainfo = match self.final_preflight(&record).await {
            Ok(metainfo) => metainfo,
            Err(problem) => {
                let blocked = self
                    .journal
                    .mark_admission_not_submitted(&record.operation_id, problem.code)?;
                return Ok(admission_execution(
                    AdmissionExecutionStatus::Blocked,
                    blocked,
                    Some(problem),
                    replayed,
                ));
            }
        };

        let pending = self
            .journal
            .mark_admission_effect_pending(&record.operation_id)?;
        let request = AddTorrentRequest {
            metainfo,
            save_path: record.working_save_path.clone(),
            stopped: true,
        };

        match self.client.add_torrent(&request).await {
            EffectAttempt::NotSent(problem) => {
                let prepared = self
                    .journal
                    .mark_admission_retry_ready(&pending.operation_id)?;
                Ok(admission_execution(
                    AdmissionExecutionStatus::Blocked,
                    prepared,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Rejected(problem) => {
                let failed = self
                    .journal
                    .mark_admission_failed(&pending.operation_id, problem.code)?;
                Ok(admission_execution(
                    AdmissionExecutionStatus::Failed,
                    failed,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Uncertain(problem) => {
                let unknown = self
                    .journal
                    .mark_admission_unknown(&pending.operation_id, "QBIT_MUTATION_UNCERTAIN")?;
                Ok(admission_execution(
                    AdmissionExecutionStatus::Unknown,
                    unknown,
                    Some(problem),
                    replayed,
                ))
            }
            EffectAttempt::Accepted => match self.observe_bounded(&pending).await {
                Ok(AddObservation::Applied) => {
                    let observed = self
                        .journal
                        .mark_admission_observed_applied(&pending.operation_id)?;
                    let finished = self.journal.finish_admission(&observed.operation_id)?;
                    Ok(admission_execution(
                        AdmissionExecutionStatus::Finished,
                        finished,
                        None,
                        replayed,
                    ))
                }
                Ok(AddObservation::Absent | AddObservation::Pending) => {
                    let unknown = self.journal.mark_admission_unknown(
                        &pending.operation_id,
                        "QBIT_POSTCONDITION_UNCONFIRMED",
                    )?;
                    Ok(admission_execution(
                        AdmissionExecutionStatus::Unknown,
                        unknown,
                        Some(PortError::new(
                            "QBIT_POSTCONDITION_UNCONFIRMED",
                            "qBittorrent accepted the add request but bounded fresh observation did not confirm stopped state at the managed Working path",
                        )),
                        replayed,
                    ))
                }
                Err(problem) => {
                    let unknown = self
                        .journal
                        .mark_admission_unknown(&pending.operation_id, "QBIT_MUTATION_UNCERTAIN")?;
                    Ok(admission_execution(
                        AdmissionExecutionStatus::Unknown,
                        unknown,
                        Some(problem),
                        replayed,
                    ))
                }
            },
        }
    }

    async fn final_preflight(&self, record: &AdmissionRecord) -> Result<Vec<u8>, PortError> {
        if !record.reservation_active {
            return Err(PortError::new(
                "ADMISSION_RESERVATION_INACTIVE",
                "admission capacity reservation is no longer active",
            ));
        }

        let snapshot = self
            .storage
            .read_incoming(&record.source_relative, self.max_metainfo_bytes)?;
        if snapshot.evidence != record.source_evidence {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "Incoming source evidence changed after admission reservation",
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&snapshot.bytes).into();
        if digest != record.source_metainfo_digest {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "Incoming metainfo bytes changed after admission reservation",
            ));
        }

        let volume = self.storage.volume_status(ManagedRoot::Working)?;
        if volume.volume_id != record.working_volume_id {
            return Err(PortError::new(
                "STORAGE_VOLUME_CHANGED",
                "Working volume identity changed after admission reservation",
            ));
        }
        if !self
            .storage
            .matches_root_path(ManagedRoot::Working, &record.working_save_path)?
        {
            return Err(PortError::new(
                "STORAGE_ROOT_CHANGED",
                "managed Working path changed after admission reservation",
            ));
        }

        let reservations = self
            .journal
            .capacity_reservations(record.working_volume_id)?;
        let reserved_bytes = checked_sum(
            reservations.iter().map(|reservation| reservation.bytes),
            "durable admission reservations",
        )?;
        let required_bytes = reserved_bytes
            .checked_add(self.capacity_reserve_bytes)
            .ok_or_else(|| {
                PortError::new(
                    "INTERNAL_INVARIANT_VIOLATION",
                    "final admission capacity calculation overflow",
                )
            })?;
        if required_bytes > volume.free_bytes {
            return Err(PortError::new(
                "INSUFFICIENT_CAPACITY",
                format!(
                    "Working volume has {} free bytes but {} are required by durable reservations plus reserve",
                    volume.free_bytes, required_bytes
                ),
            ));
        }

        let probe = self.client.probe().await?;
        if !probe.mutation_ready {
            return Err(PortError::new(
                "QBIT_API_UNSUPPORTED",
                format!(
                    "qBittorrent {} WebAPI {} is not mutation-ready",
                    probe.application_version, probe.webapi_version
                ),
            ));
        }

        match self.observe_once(record).await? {
            AddObservation::Absent => Ok(snapshot.bytes),
            AddObservation::Applied | AddObservation::Pending => Err(PortError::new(
                "TORRENT_ALREADY_PRESENT",
                "qBittorrent already contains the intended torrent before this add effect",
            )),
        }
    }

    async fn observe_bounded(&self, record: &AdmissionRecord) -> Result<AddObservation, PortError> {
        let mut last = AddObservation::Absent;
        for attempt in 0..self.observation_attempts {
            let observation = self.observe_once(record).await?;
            match observation {
                AddObservation::Applied => return Ok(AddObservation::Applied),
                AddObservation::Pending => last = AddObservation::Pending,
                AddObservation::Absent => {}
            }
            if attempt + 1 < self.observation_attempts && !self.observation_delay.is_zero() {
                tokio::time::sleep(self.observation_delay).await;
            }
        }
        Ok(last)
    }

    async fn observe_once(&self, record: &AdmissionRecord) -> Result<AddObservation, PortError> {
        for selector in record.identity.qbit_selector_ids() {
            let Some(torrent) = self.client.get(&selector).await? else {
                continue;
            };
            if !self
                .storage
                .matches_root_path(ManagedRoot::Working, &torrent.save_path)?
            {
                return Err(PortError::new(
                    "QBIT_SAVE_PATH_CONFLICT",
                    format!(
                        "qBittorrent already has {} outside the managed Working path",
                        selector
                    ),
                ));
            }
            return if torrent.state == TorrentState::Stopped {
                Ok(AddObservation::Applied)
            } else {
                Ok(AddObservation::Pending)
            };
        }
        Ok(AddObservation::Absent)
    }
}

fn admission_execution(
    status: AdmissionExecutionStatus,
    record: AdmissionRecord,
    problem: Option<PortError>,
    replayed: bool,
) -> AdmissionExecution {
    AdmissionExecution {
        status,
        record,
        problem,
        replayed,
    }
}

fn admission_problem_code(code: &str) -> &'static str {
    match code {
        "SOURCE_AMBIGUOUS" => "SOURCE_AMBIGUOUS",
        "STORAGE_VOLUME_CHANGED" => "STORAGE_VOLUME_CHANGED",
        "STORAGE_ROOT_CHANGED" => "STORAGE_ROOT_CHANGED",
        "INSUFFICIENT_CAPACITY" => "INSUFFICIENT_CAPACITY",
        "QBIT_API_UNSUPPORTED" => "QBIT_API_UNSUPPORTED",
        "TORRENT_ALREADY_PRESENT" => "TORRENT_ALREADY_PRESENT",
        "QBIT_SAVE_PATH_CONFLICT" => "QBIT_SAVE_PATH_CONFLICT",
        "QBIT_MUTATION_UNCERTAIN" => "QBIT_MUTATION_UNCERTAIN",
        "QBIT_POSTCONDITION_UNCONFIRMED" => "QBIT_POSTCONDITION_UNCONFIRMED",
        "QBIT_MUTATION_REJECTED" => "QBIT_MUTATION_REJECTED",
        "QBIT_UNAVAILABLE" => "QBIT_UNAVAILABLE",
        _ => "ADMISSION_FAILED",
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityReservation {
    pub responsible: String,
    pub volume_id: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityCandidate {
    pub key: String,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityDisposition {
    Accepted,
    DeferredInsufficientCapacity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityExplanation {
    pub volume_id: u64,
    pub free_bytes: u64,
    pub already_reserved_bytes: u64,
    pub candidate_bytes: u64,
    pub reserve_bytes: u64,
    pub required_bytes: u64,
    pub shortfall_bytes: u64,
    pub reservations: Vec<CapacityReservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityDecision {
    pub candidate: CapacityCandidate,
    pub disposition: CapacityDisposition,
    pub explanation: CapacityExplanation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionCapacityPlan {
    pub decisions: Vec<CapacityDecision>,
    pub final_reserved_bytes: u64,
}

pub fn plan_incoming_capacity_from_storage(
    storage: &dyn Storage,
    scan: &IncomingScan,
    reserve_bytes: u64,
    existing_reservations: Vec<CapacityReservation>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let volume = storage.volume_status(ManagedRoot::Working)?;
    plan_incoming_capacity(
        scan,
        volume.volume_id,
        volume.free_bytes,
        reserve_bytes,
        existing_reservations,
    )
}

pub fn plan_incoming_capacity(
    scan: &IncomingScan,
    volume_id: u64,
    free_bytes: u64,
    reserve_bytes: u64,
    existing_reservations: Vec<CapacityReservation>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let candidates = scan
        .eligible
        .iter()
        .map(|candidate| CapacityCandidate {
            key: candidate.relative_path.clone(),
            bytes: candidate.metainfo.manifest.total_size,
        })
        .collect();
    plan_capacity(
        volume_id,
        free_bytes,
        reserve_bytes,
        existing_reservations,
        candidates,
    )
}

pub fn plan_capacity(
    volume_id: u64,
    free_bytes: u64,
    reserve_bytes: u64,
    reservations: Vec<CapacityReservation>,
    mut candidates: Vec<CapacityCandidate>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let mut reservations: Vec<CapacityReservation> = reservations
        .into_iter()
        .filter(|reservation| reservation.volume_id == volume_id)
        .collect();
    reservations.sort_by(|left, right| capacity_key_order(&left.responsible, &right.responsible));
    candidates.sort_by(|left, right| capacity_key_order(&left.key, &right.key));

    let mut reserved_bytes = checked_sum(
        reservations.iter().map(|reservation| reservation.bytes),
        "existing capacity reservations",
    )?;
    let mut decisions = Vec::with_capacity(candidates.len());

    for candidate in candidates {
        let retained = reservations.iter().any(|reservation| {
            reservation
                .responsible
                .strip_prefix("retained:")
                .is_some_and(|path| path.eq_ignore_ascii_case(&candidate.key))
                && reservation.bytes == candidate.bytes
        });
        let incremental_bytes = if retained { 0 } else { candidate.bytes };
        let required_bytes = reserved_bytes
            .checked_add(incremental_bytes)
            .and_then(|value| value.checked_add(reserve_bytes))
            .ok_or_else(|| {
                PortError::new(
                    "INTERNAL_INVARIANT_VIOLATION",
                    "admission capacity calculation overflow",
                )
            })?;
        let shortfall_bytes = required_bytes.saturating_sub(free_bytes);
        let disposition = if shortfall_bytes == 0 {
            CapacityDisposition::Accepted
        } else {
            CapacityDisposition::DeferredInsufficientCapacity
        };

        decisions.push(CapacityDecision {
            candidate: candidate.clone(),
            disposition,
            explanation: CapacityExplanation {
                volume_id,
                free_bytes,
                already_reserved_bytes: reserved_bytes,
                candidate_bytes: candidate.bytes,
                reserve_bytes,
                required_bytes,
                shortfall_bytes,
                reservations: reservations.clone(),
            },
        });

        if disposition == CapacityDisposition::Accepted && !retained {
            reserved_bytes = reserved_bytes.checked_add(candidate.bytes).ok_or_else(|| {
                PortError::new(
                    "INTERNAL_INVARIANT_VIOLATION",
                    "admission reservation total overflow",
                )
            })?;
            reservations.push(CapacityReservation {
                responsible: candidate.key,
                volume_id,
                bytes: candidate.bytes,
            });
        }
    }

    Ok(AdmissionCapacityPlan {
        decisions,
        final_reserved_bytes: reserved_bytes,
    })
}

fn checked_sum(values: impl IntoIterator<Item = u64>, context: &str) -> Result<u64, PortError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total.checked_add(value).ok_or_else(|| {
            PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                format!("{context} overflow"),
            )
        })
    })
}

fn capacity_key_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.to_ascii_lowercase()
        .cmp(&right.to_ascii_lowercase())
        .then_with(|| left.cmp(right))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    use qb_domain::torrent::{
        ManifestFile, TorrentId, TorrentIdentity, TorrentManifest, TorrentMetainfo,
    };

    use crate::{
        storage::{
            FileEvidence, FileIdentity, IncomingCandidate, IncomingFileSnapshot, IncomingScan,
            StorageVolumeStatus,
        },
        torrent::{
            EffectFuture, FileObservation, NetworkPreferences, PortFuture, QbitProbe,
            QueueSettings, TorrentView, TrackerEvidence, TransferInfo,
        },
    };

    use super::*;

    struct CapacityStorage;

    impl Storage for CapacityStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(StorageVolumeStatus {
                root,
                volume_id: 7,
                free_bytes: 120,
                total_bytes: 1_000,
            })
        }

        fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(r"C:\Managed\Working".into())
        }

        fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
            Ok(self.root_path(root)?.eq_ignore_ascii_case(observed))
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Err(PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                "capacity planning must not rescan Incoming",
            ))
        }

        fn read_incoming(
            &self,
            _relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            Err(PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                "capacity planning must not reread Incoming",
            ))
        }
    }

    #[derive(Default)]
    struct FakeAdmissionJournal {
        state: Mutex<FakeAdmissionState>,
    }

    #[derive(Default)]
    struct FakeAdmissionState {
        fingerprint: Option<[u8; 32]>,
        record: Option<AdmissionRecord>,
    }

    impl FakeAdmissionJournal {
        fn update(
            &self,
            expected: &[MutationDisposition],
            next: MutationDisposition,
            checkpoint: &str,
            pending_effect_kind: Option<&str>,
            problem_code: Option<&str>,
            reservation_active: Option<bool>,
        ) -> Result<AdmissionRecord, PortError> {
            let mut state = self.state.lock().expect("journal mutex");
            let record = state
                .record
                .as_mut()
                .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", "missing fake admission"))?;
            if !expected.contains(&record.disposition) {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    "unexpected fake admission disposition",
                ));
            }
            record.disposition = next;
            record.checkpoint = checkpoint.into();
            record.pending_effect_kind = pending_effect_kind.map(str::to_string);
            record.problem_code = problem_code.map(str::to_string);
            record.revision += 1;
            if let Some(active) = reservation_active {
                record.reservation_active = active;
            }
            Ok(record.clone())
        }
    }

    impl AdmissionJournal for FakeAdmissionJournal {
        fn reserve_admission(
            &self,
            request: &AdmissionReservationRequest,
        ) -> Result<AdmissionReservationResult, PortError> {
            let mut state = self.state.lock().expect("journal mutex");
            let fingerprint = request.fingerprint();
            if let Some(record) = state.record.clone() {
                if state.fingerprint == Some(fingerprint) {
                    return Ok(AdmissionReservationResult::Replay(record));
                }
                return Ok(AdmissionReservationResult::Conflict {
                    operation_id: record.operation_id,
                });
            }

            let record = AdmissionRecord {
                request_id: request.request_id.clone(),
                operation_id: OperationId::new("operation-1").expect("operation id"),
                registry_id: "registry-1".into(),
                identity: request.identity.clone(),
                source_relative: request.source_relative.clone(),
                source_evidence: request.source_evidence.clone(),
                source_metainfo_digest: request.source_metainfo_digest,
                working_volume_id: request.working_volume_id,
                reserved_bytes: request.reserved_bytes,
                working_save_path: request.working_save_path.clone(),
                reservation_active: true,
                checkpoint: "prepared".into(),
                disposition: MutationDisposition::Prepared,
                pending_effect_kind: None,
                problem_code: None,
                revision: 1,
            };
            state.fingerprint = Some(fingerprint);
            state.record = Some(record.clone());
            Ok(AdmissionReservationResult::New(record))
        }

        fn get_admission(
            &self,
            operation_id: &OperationId,
        ) -> Result<Option<AdmissionRecord>, PortError> {
            let state = self.state.lock().expect("journal mutex");
            Ok(state
                .record
                .as_ref()
                .filter(|record| &record.operation_id == operation_id)
                .cloned())
        }

        fn list_recoverable_admissions(&self) -> Result<Vec<AdmissionRecord>, PortError> {
            let state = self.state.lock().expect("journal mutex");
            Ok(state
                .record
                .as_ref()
                .filter(|record| {
                    matches!(
                        record.disposition,
                        MutationDisposition::Prepared
                            | MutationDisposition::EffectPending
                            | MutationDisposition::ObservedApplied
                            | MutationDisposition::Unknown
                    )
                })
                .cloned()
                .into_iter()
                .collect())
        }

        fn capacity_reservations(
            &self,
            working_volume_id: u64,
        ) -> Result<Vec<CapacityReservation>, PortError> {
            let state = self.state.lock().expect("journal mutex");
            Ok(state
                .record
                .as_ref()
                .filter(|record| {
                    record.reservation_active && record.working_volume_id == working_volume_id
                })
                .map(|record| {
                    vec![CapacityReservation {
                        responsible: record.operation_id.to_string(),
                        volume_id: record.working_volume_id,
                        bytes: record.reserved_bytes,
                    }]
                })
                .unwrap_or_default())
        }

        fn mark_admission_effect_pending(
            &self,
            _operation_id: &OperationId,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[MutationDisposition::Prepared],
                MutationDisposition::EffectPending,
                "effect_pending",
                Some("qbit.add"),
                None,
                None,
            )
        }

        fn mark_admission_not_submitted(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[MutationDisposition::Prepared],
                MutationDisposition::Blocked,
                "not_submitted",
                None,
                Some(problem_code),
                None,
            )
        }

        fn mark_admission_retry_ready(
            &self,
            _operation_id: &OperationId,
        ) -> Result<AdmissionRecord, PortError> {
            let state = self.state.lock().expect("journal mutex");
            let current = state
                .record
                .as_ref()
                .map(|record| record.disposition)
                .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", "missing fake admission"))?;
            drop(state);
            let checkpoint = if current == MutationDisposition::Blocked {
                "retry_ready"
            } else {
                "observed_not_applied"
            };
            self.update(
                &[
                    MutationDisposition::Blocked,
                    MutationDisposition::EffectPending,
                    MutationDisposition::Unknown,
                ],
                MutationDisposition::Prepared,
                checkpoint,
                None,
                None,
                None,
            )
        }

        fn mark_admission_unknown(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[MutationDisposition::EffectPending],
                MutationDisposition::Unknown,
                "unknown",
                Some("qbit.add"),
                Some(problem_code),
                None,
            )
        }

        fn mark_admission_observed_applied(
            &self,
            _operation_id: &OperationId,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[
                    MutationDisposition::EffectPending,
                    MutationDisposition::Unknown,
                ],
                MutationDisposition::ObservedApplied,
                "observed_applied",
                Some("qbit.add"),
                None,
                None,
            )
        }

        fn finish_admission(
            &self,
            _operation_id: &OperationId,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[MutationDisposition::ObservedApplied],
                MutationDisposition::Finished,
                "finished",
                None,
                None,
                None,
            )
        }

        fn mark_admission_failed(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<AdmissionRecord, PortError> {
            self.update(
                &[MutationDisposition::EffectPending],
                MutationDisposition::Failed,
                "failed",
                Some("qbit.add"),
                Some(problem_code),
                Some(false),
            )
        }
    }

    struct FakeAdmissionStorage {
        snapshot: IncomingFileSnapshot,
        volume_id: u64,
        free_bytes: u64,
        root: String,
    }

    impl Storage for FakeAdmissionStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(StorageVolumeStatus {
                root,
                volume_id: self.volume_id,
                free_bytes: self.free_bytes,
                total_bytes: self.free_bytes.saturating_mul(2),
            })
        }

        fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(self.root.clone())
        }

        fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
            Ok(self.root_path(root)?.eq_ignore_ascii_case(observed))
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Ok(vec![self.snapshot.relative_path.clone()])
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            if relative_path != self.snapshot.relative_path {
                return Err(PortError::new("STORAGE_NOT_FOUND", relative_path));
            }
            Ok(self.snapshot.clone())
        }
    }

    #[derive(Clone, Copy)]
    enum FakeAddEffect {
        Accepted,
        Uncertain,
    }

    struct FakeTorrentClient {
        observations: Mutex<VecDeque<Result<Option<TorrentView>, PortError>>>,
        effect: FakeAddEffect,
        add_calls: AtomicUsize,
    }

    impl FakeTorrentClient {
        fn new(
            observations: Vec<Result<Option<TorrentView>, PortError>>,
            effect: FakeAddEffect,
        ) -> Self {
            Self {
                observations: Mutex::new(observations.into()),
                effect,
                add_calls: AtomicUsize::new(0),
            }
        }
    }

    impl TorrentClient for FakeTorrentClient {
        fn probe(&self) -> PortFuture<'_, QbitProbe> {
            Box::pin(async {
                Ok(QbitProbe {
                    application_version: "v5.2.3".into(),
                    webapi_version: "2.16.1".into(),
                    mutation_ready: true,
                })
            })
        }

        fn list(&self) -> PortFuture<'_, Vec<TorrentView>> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn get<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Option<TorrentView>> {
            Box::pin(async move {
                self.observations
                    .lock()
                    .expect("observations mutex")
                    .pop_front()
                    .unwrap_or(Ok(None))
            })
        }

        fn transfer_info(&self) -> PortFuture<'_, TransferInfo> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn queue_settings(&self) -> PortFuture<'_, QueueSettings> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn trackers<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Vec<TrackerEvidence>> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn files<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Vec<FileObservation>> {
            Box::pin(async { Err(unused_client_call()) })
        }

        fn add_torrent<'a>(&'a self, _request: &'a AddTorrentRequest) -> EffectFuture<'a> {
            Box::pin(async move {
                self.add_calls.fetch_add(1, Ordering::SeqCst);
                match self.effect {
                    FakeAddEffect::Accepted => EffectAttempt::Accepted,
                    FakeAddEffect::Uncertain => EffectAttempt::Uncertain(PortError::new(
                        "QBIT_MUTATION_UNCERTAIN",
                        "fixture response dropped after send",
                    )),
                }
            })
        }

        fn stop<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn start<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn set_active_downloads(&self, _value: u32) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn set_download_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn set_upload_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }
    }

    fn unused_client_call() -> PortError {
        PortError::new(
            "INTERNAL_INVARIANT_VIOLATION",
            "unexpected fake TorrentClient call",
        )
    }

    fn admission_fixture() -> (AdmissionReservationRequest, IncomingFileSnapshot, TorrentId) {
        let bytes = b"d4:infod4:name4:testee".to_vec();
        let source_evidence = FileEvidence {
            identity: FileIdentity {
                volume_id: 3,
                file_id: 5,
            },
            size: u64::try_from(bytes.len()).expect("fixture size"),
            modified_marker: 7,
        };
        let identity = TorrentIdentity::new(Some([0x11; 20]), None).expect("identity");
        let selector = identity.qbit_selector_ids().remove(0);
        let request = AdmissionReservationRequest {
            request_id: RequestId::new("admission-service-1").expect("request id"),
            identity,
            source_relative: "candidate.torrent".into(),
            source_evidence: source_evidence.clone(),
            source_metainfo_digest: Sha256::digest(&bytes).into(),
            working_volume_id: 9,
            reserved_bytes: 100,
            working_save_path: r"C:\Managed\Working".into(),
        };
        let snapshot = IncomingFileSnapshot {
            relative_path: request.source_relative.clone(),
            evidence: source_evidence,
            bytes,
        };
        (request, snapshot, selector)
    }

    fn torrent_view(id: TorrentId, state: TorrentState, save_path: &str) -> TorrentView {
        TorrentView {
            id,
            name: "fixture".into(),
            save_path: save_path.into(),
            state,
            total_bytes: 100,
            remaining_bytes: 100,
            download_rate_bps: 0,
            upload_rate_bps: 0,
            progress_ppm: 0,
            availability: None,
            peers_connected: 0,
            peers_known: 0,
            seeds_connected: 0,
            seeds_known: 0,
        }
    }

    fn admission_execution_result(result: AdmissionExecutionResult) -> AdmissionExecution {
        match result {
            AdmissionExecutionResult::Execution(execution) => *execution,
            AdmissionExecutionResult::Conflict { operation_id } => {
                panic!("unexpected conflict: {operation_id}")
            }
        }
    }

    fn admission_service(
        journal: Arc<FakeAdmissionJournal>,
        storage: Arc<FakeAdmissionStorage>,
        client: Arc<FakeTorrentClient>,
        reserve_bytes: u64,
    ) -> AdmissionService {
        AdmissionService::new(journal, storage, client, 1024, reserve_bytes)
            .with_observation_policy(3, Duration::ZERO)
    }

    #[tokio::test]
    async fn admission_source_change_blocks_before_qbit_effect() {
        let (request, mut snapshot, _) = admission_fixture();
        snapshot.evidence.modified_marker += 1;
        let journal = Arc::new(FakeAdmissionJournal::default());
        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: 10_000,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(Vec::new(), FakeAddEffect::Accepted));
        let service = admission_service(journal, storage, client.clone(), 10);

        let execution =
            admission_execution_result(service.execute(&request).await.expect("execute"));

        assert_eq!(execution.status, AdmissionExecutionStatus::Blocked);
        assert_eq!(execution.record.checkpoint, "not_submitted");
        assert_eq!(
            execution.problem.as_ref().map(|problem| problem.code),
            Some("SOURCE_AMBIGUOUS")
        );
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn admission_low_space_race_blocks_before_qbit_effect() {
        let (request, snapshot, _) = admission_fixture();
        let journal = Arc::new(FakeAdmissionJournal::default());
        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: request.reserved_bytes + 9,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(Vec::new(), FakeAddEffect::Accepted));
        let service = admission_service(journal, storage, client.clone(), 10);

        let execution =
            admission_execution_result(service.execute(&request).await.expect("execute"));

        assert_eq!(execution.status, AdmissionExecutionStatus::Blocked);
        assert_eq!(
            execution.problem.as_ref().map(|problem| problem.code),
            Some("INSUFFICIENT_CAPACITY")
        );
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn admission_timeout_after_send_becomes_unknown() {
        let (request, snapshot, _) = admission_fixture();
        let journal = Arc::new(FakeAdmissionJournal::default());
        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: 10_000,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(
            vec![Ok(None)],
            FakeAddEffect::Uncertain,
        ));
        let service = admission_service(journal, storage, client.clone(), 10);

        let execution =
            admission_execution_result(service.execute(&request).await.expect("execute"));

        assert_eq!(execution.status, AdmissionExecutionStatus::Unknown);
        assert_eq!(execution.record.disposition, MutationDisposition::Unknown);
        assert_eq!(
            execution.record.pending_effect_kind.as_deref(),
            Some("qbit.add")
        );
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admission_acceptance_waits_through_checking_for_stopped_receipt() {
        let (request, snapshot, selector) = admission_fixture();
        let journal = Arc::new(FakeAdmissionJournal::default());
        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: 10_000,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(
            vec![
                Ok(None),
                Ok(Some(torrent_view(
                    selector.clone(),
                    TorrentState::Checking,
                    &request.working_save_path,
                ))),
                Ok(Some(torrent_view(
                    selector,
                    TorrentState::Stopped,
                    &request.working_save_path,
                ))),
            ],
            FakeAddEffect::Accepted,
        ));
        let service = admission_service(journal, storage, client.clone(), 10);

        let execution =
            admission_execution_result(service.execute(&request).await.expect("execute"));

        assert_eq!(execution.status, AdmissionExecutionStatus::Finished);
        assert_eq!(execution.record.disposition, MutationDisposition::Finished);
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admission_restart_observes_effect_pending_without_duplicate_add() {
        let (request, snapshot, selector) = admission_fixture();
        let journal = Arc::new(FakeAdmissionJournal::default());
        let reserved = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record,
            other => panic!("unexpected reserve: {other:?}"),
        };
        journal
            .mark_admission_effect_pending(&reserved.operation_id)
            .expect("effect pending");

        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: 10_000,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(
            vec![Ok(Some(torrent_view(
                selector,
                TorrentState::Stopped,
                &request.working_save_path,
            )))],
            FakeAddEffect::Accepted,
        ));
        let service = admission_service(journal, storage, client.clone(), 10);

        let executions = service.recover_all().await.expect("recover");

        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].status, AdmissionExecutionStatus::Finished);
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn admission_restart_keeps_unknown_absent_without_blind_retry() {
        let (request, snapshot, _) = admission_fixture();
        let journal = Arc::new(FakeAdmissionJournal::default());
        let reserved = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record,
            other => panic!("unexpected reserve: {other:?}"),
        };
        journal
            .mark_admission_effect_pending(&reserved.operation_id)
            .expect("effect pending");
        journal
            .mark_admission_unknown(&reserved.operation_id, "QBIT_MUTATION_UNCERTAIN")
            .expect("unknown");

        let storage = Arc::new(FakeAdmissionStorage {
            snapshot,
            volume_id: request.working_volume_id,
            free_bytes: 10_000,
            root: request.working_save_path.clone(),
        });
        let client = Arc::new(FakeTorrentClient::new(
            vec![Ok(None), Ok(None), Ok(None)],
            FakeAddEffect::Accepted,
        ));
        let service = admission_service(journal, storage, client.clone(), 10);

        let executions = service.recover_all().await.expect("recover");

        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].status, AdmissionExecutionStatus::Unknown);
        assert_eq!(client.add_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn admission_fingerprint_covers_source_and_capacity_identity() {
        let base = AdmissionReservationRequest {
            request_id: RequestId::new("admission-1").expect("request id"),
            identity: TorrentIdentity::new(Some([0x11; 20]), Some([0x22; 32])).expect("identity"),
            source_relative: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: crate::storage::FileIdentity {
                    volume_id: 7,
                    file_id: 9,
                },
                size: 123,
                modified_marker: 456,
            },
            source_metainfo_digest: [0x33; 32],
            working_volume_id: 42,
            reserved_bytes: 1_000,
            working_save_path: r"C:\Managed\Working".into(),
        };
        let mut changed = base.clone();
        changed.source_evidence.modified_marker += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.reserved_bytes += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.working_save_path.push_str("-other");
        assert_ne!(base.fingerprint(), changed.fingerprint());

        let mut different_request = base.clone();
        different_request.request_id = RequestId::new("admission-2").expect("request id");
        assert_eq!(base.fingerprint(), different_request.fingerprint());
        assert_eq!(base.fingerprint(), base.fingerprint());
    }

    #[test]
    fn incoming_capacity_reads_fresh_working_volume_status() {
        let candidate = IncomingCandidate {
            relative_path: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 1,
                    file_id: 2,
                },
                size: 3,
                modified_marker: 4,
            },
            source_sha256: [0x55; 32],
            metainfo: TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                manifest: TorrentManifest::new(vec![ManifestFile {
                    path: "payload.bin".into(),
                    size: 90,
                }])
                .expect("manifest"),
            },
        };
        let scan = IncomingScan {
            eligible: vec![candidate],
            already_processed: Vec::new(),
            redundant_identical: Vec::new(),
            rejected: Vec::new(),
        };

        let plan = plan_incoming_capacity_from_storage(&CapacityStorage, &scan, 10, Vec::new())
            .expect("capacity plan");

        assert_eq!(plan.decisions[0].explanation.volume_id, 7);
        assert_eq!(plan.decisions[0].explanation.free_bytes, 120);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 100);
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
    }

    #[test]
    fn defers_large_candidate_and_continues_with_later_independent_candidate() {
        let plan = plan_capacity(
            7,
            100,
            10,
            vec![CapacityReservation {
                responsible: "existing-operation".into(),
                volume_id: 7,
                bytes: 20,
            }],
            vec![
                CapacityCandidate {
                    key: "a-large.torrent".into(),
                    bytes: 80,
                },
                CapacityCandidate {
                    key: "b-small.torrent".into(),
                    bytes: 30,
                },
            ],
        )
        .expect("plan");

        assert_eq!(plan.decisions.len(), 2);
        assert_eq!(
            plan.decisions[0].disposition,
            CapacityDisposition::DeferredInsufficientCapacity
        );
        assert_eq!(plan.decisions[0].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 110);
        assert_eq!(plan.decisions[0].explanation.shortfall_bytes, 10);

        assert_eq!(plan.decisions[1].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.decisions[1].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.decisions[1].explanation.required_bytes, 60);
        assert_eq!(plan.decisions[1].explanation.shortfall_bytes, 0);
        assert_eq!(plan.final_reserved_bytes, 50);
    }

    #[test]
    fn accepted_candidate_reserves_budget_for_following_candidates() {
        let plan = plan_capacity(
            7,
            100,
            10,
            Vec::new(),
            vec![
                CapacityCandidate {
                    key: "b-second.torrent".into(),
                    bytes: 60,
                },
                CapacityCandidate {
                    key: "a-first.torrent".into(),
                    bytes: 30,
                },
            ],
        )
        .expect("plan");

        assert_eq!(plan.decisions[0].candidate.key, "a-first.torrent");
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.decisions[1].candidate.key, "b-second.torrent");
        assert_eq!(plan.decisions[1].explanation.already_reserved_bytes, 30);
        assert_eq!(plan.decisions[1].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.final_reserved_bytes, 90);
    }

    #[test]
    fn plan_exposes_reservations_responsible_for_reserved_amount() {
        let plan = plan_capacity(
            7,
            100,
            5,
            vec![
                CapacityReservation {
                    responsible: "operation-z".into(),
                    volume_id: 7,
                    bytes: 7,
                },
                CapacityReservation {
                    responsible: "operation-a".into(),
                    volume_id: 7,
                    bytes: 8,
                },
            ],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 20,
            }],
        )
        .expect("plan");

        let explanation = &plan.decisions[0].explanation;
        assert_eq!(explanation.already_reserved_bytes, 15);
        assert_eq!(
            explanation
                .reservations
                .iter()
                .map(|reservation| reservation.responsible.as_str())
                .collect::<Vec<_>>(),
            vec!["operation-a", "operation-z"]
        );
    }

    #[test]
    fn incoming_plan_uses_manifest_total_size_and_only_eligible_candidates() {
        let candidate = IncomingCandidate {
            relative_path: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 1,
                    file_id: 2,
                },
                size: 3,
                modified_marker: 4,
            },
            source_sha256: [0x55; 32],
            metainfo: TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                manifest: TorrentManifest::new(vec![
                    ManifestFile {
                        path: "a.bin".into(),
                        size: 40,
                    },
                    ManifestFile {
                        path: "b.bin".into(),
                        size: 50,
                    },
                ])
                .expect("manifest"),
            },
        };
        let scan = IncomingScan {
            eligible: vec![candidate],
            already_processed: Vec::new(),
            redundant_identical: Vec::new(),
            rejected: Vec::new(),
        };

        let plan = plan_incoming_capacity(&scan, 7, 120, 10, Vec::new()).expect("plan");

        assert_eq!(plan.decisions.len(), 1);
        assert_eq!(plan.decisions[0].candidate.bytes, 90);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 100);
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
    }

    #[test]
    fn retained_capacity_is_not_double_counted_for_same_incoming_candidate() {
        let plan = plan_capacity(
            7,
            120,
            10,
            vec![CapacityReservation {
                responsible: "retained:Candidate.torrent".into(),
                volume_id: 7,
                bytes: 90,
            }],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 90,
            }],
        )
        .expect("plan");

        assert_eq!(plan.decisions.len(), 1);
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.decisions[0].explanation.already_reserved_bytes, 90);
        assert_eq!(plan.decisions[0].explanation.candidate_bytes, 90);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 100);
        assert_eq!(plan.final_reserved_bytes, 90);
    }

    #[test]
    fn capacity_plan_ignores_reservations_on_other_volumes() {
        let plan = plan_capacity(
            7,
            100,
            10,
            vec![
                CapacityReservation {
                    responsible: "same-volume".into(),
                    volume_id: 7,
                    bytes: 20,
                },
                CapacityReservation {
                    responsible: "other-volume".into(),
                    volume_id: 9,
                    bytes: 70,
                },
            ],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 30,
            }],
        )
        .expect("plan");

        assert_eq!(plan.decisions[0].explanation.volume_id, 7);
        assert_eq!(plan.decisions[0].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.final_reserved_bytes, 50);
        assert_eq!(plan.decisions[0].explanation.reservations.len(), 1);
        assert_eq!(
            plan.decisions[0].explanation.reservations[0].responsible,
            "same-volume"
        );
    }

    #[test]
    fn capacity_overflow_fails_closed() {
        let error = plan_capacity(
            7,
            u64::MAX,
            1,
            vec![CapacityReservation {
                responsible: "existing".into(),
                volume_id: 7,
                bytes: u64::MAX,
            }],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 1,
            }],
        )
        .expect_err("overflow must fail");

        assert_eq!(error.code, "INTERNAL_INVARIANT_VIOLATION");
    }
}
