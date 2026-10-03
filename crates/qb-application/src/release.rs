use std::{sync::Arc, time::Duration};

use qb_domain::{torrent::TorrentIdentity, OperationId, RequestId};
use sha2::{Digest, Sha256};

use crate::{
    storage::{FileEvidence, ManagedRoot, Storage},
    torrent::{EffectAttempt, TorrentClient, TorrentView},
    PortError,
};

pub const RELEASE_FINGERPRINT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseRequest {
    pub request_id: RequestId,
    pub registry_id: String,
}

impl ReleaseRequest {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-release-fingerprint-v1\0");
        digest.update(self.registry_id.as_bytes());
        digest.finalize().into()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseState {
    Prepared,
    StopPending,
    Stopped,
    UnknownStop,
    DeletePending,
    UnknownDelete,
    Finished,
    Blocked,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseRecord {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub registry_id: String,
    pub admission_operation_id: OperationId,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub retained_bytes: u64,
    pub working_save_path: String,
    pub state: ReleaseState,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseReservation {
    New(ReleaseRecord),
    Replay(ReleaseRecord),
    Conflict { operation_id: OperationId },
}

pub trait ReleaseJournal: Send + Sync {
    fn reserve_release(&self, request: &ReleaseRequest) -> Result<ReleaseReservation, PortError>;
    fn list_recoverable_releases(&self) -> Result<Vec<ReleaseRecord>, PortError>;
    fn mark_stop_pending(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
    fn mark_stopped(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
    fn mark_unknown_stop(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError>;
    fn retry_stop(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
    fn mark_delete_pending(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
    fn mark_unknown_delete(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError>;
    fn retry_delete(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
    fn mark_release_blocked(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError>;
    fn mark_release_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError>;
    fn finish_release(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseExecutionStatus {
    Finished,
    Blocked,
    UnknownStop,
    UnknownDelete,
    Failed,
}

#[derive(Debug)]
pub struct ReleaseExecution {
    pub status: ReleaseExecutionStatus,
    pub record: ReleaseRecord,
    pub problem: Option<PortError>,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum ReleaseExecutionResult {
    Execution(Box<ReleaseExecution>),
    Conflict { operation_id: OperationId },
}

enum TargetObservation {
    Absent,
    Present(TorrentView),
}

pub struct ReleaseService {
    journal: Arc<dyn ReleaseJournal>,
    storage: Arc<dyn Storage>,
    client: Arc<dyn TorrentClient>,
    max_metainfo_bytes: usize,
    observation_attempts: usize,
    observation_delay: Duration,
    lane: tokio::sync::Mutex<()>,
}

impl ReleaseService {
    pub fn new(
        journal: Arc<dyn ReleaseJournal>,
        storage: Arc<dyn Storage>,
        client: Arc<dyn TorrentClient>,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            journal,
            storage,
            client,
            max_metainfo_bytes,
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
        request: &ReleaseRequest,
    ) -> Result<ReleaseExecutionResult, PortError> {
        let _guard = self.lane.lock().await;
        let (record, replayed) = match self.journal.reserve_release(request)? {
            ReleaseReservation::New(record) => (record, false),
            ReleaseReservation::Replay(record) => (record, true),
            ReleaseReservation::Conflict { operation_id } => {
                return Ok(ReleaseExecutionResult::Conflict { operation_id });
            }
        };

        self.advance(record, replayed, true)
            .await
            .map(|execution| ReleaseExecutionResult::Execution(Box::new(execution)))
    }

    pub async fn recover_all(&self) -> Result<Vec<ReleaseExecution>, PortError> {
        let _guard = self.lane.lock().await;
        let records = self.journal.list_recoverable_releases()?;
        let mut executions = Vec::with_capacity(records.len());
        for record in records {
            executions.push(self.advance(record, true, false).await?);
        }
        Ok(executions)
    }

    async fn advance(
        &self,
        mut record: ReleaseRecord,
        replayed: bool,
        explicit_request: bool,
    ) -> Result<ReleaseExecution, PortError> {
        loop {
            match record.state {
                ReleaseState::Finished => {
                    return Ok(execution(
                        ReleaseExecutionStatus::Finished,
                        record,
                        None,
                        replayed,
                    ));
                }
                ReleaseState::Blocked => {
                    let problem = stored_problem(&record, "release blocked");
                    return Ok(execution(
                        ReleaseExecutionStatus::Blocked,
                        record,
                        problem,
                        replayed,
                    ));
                }
                ReleaseState::Failed => {
                    let problem = stored_problem(&record, "release failed");
                    return Ok(execution(
                        ReleaseExecutionStatus::Failed,
                        record,
                        problem,
                        replayed,
                    ));
                }
                ReleaseState::StopPending | ReleaseState::UnknownStop => {
                    if let Err(problem) = self.validate_retained_ownership(&record) {
                        return self.block(record, problem, replayed);
                    }
                    let observation = match self.observe_until_stop_settles(&record).await {
                        Ok(observation) => observation,
                        Err(problem) => {
                            let unknown = if record.state == ReleaseState::UnknownStop {
                                record
                            } else {
                                self.journal.mark_unknown_stop(
                                    &record.operation_id,
                                    "QBIT_STOP_UNCERTAIN",
                                )?
                            };
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownStop,
                                unknown,
                                Some(problem),
                                replayed,
                            ));
                        }
                    };
                    match observation {
                        TargetObservation::Absent => {
                            return self.block(
                                record,
                                PortError::new(
                                    "TORRENT_NOT_PRESENT",
                                    "qBittorrent record disappeared while only a stop effect was pending",
                                ),
                                replayed,
                            );
                        }
                        TargetObservation::Present(torrent) if torrent.is_complete() => {
                            return self.block(record, complete_problem(), replayed);
                        }
                        TargetObservation::Present(torrent) if torrent.state.is_stopped() => {
                            record = self.journal.mark_stopped(&record.operation_id)?;
                            continue;
                        }
                        TargetObservation::Present(_) if explicit_request
                            && record.state == ReleaseState::UnknownStop =>
                        {
                            record = self.journal.retry_stop(&record.operation_id)?;
                            continue;
                        }
                        TargetObservation::Present(_) => {
                            let unknown = if record.state == ReleaseState::UnknownStop {
                                record
                            } else {
                                self.journal.mark_unknown_stop(
                                    &record.operation_id,
                                    "QBIT_STOP_UNCONFIRMED",
                                )?
                            };
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownStop,
                                unknown,
                                Some(PortError::new(
                                    "QBIT_STOP_UNCONFIRMED",
                                    "fresh observation did not confirm that qBittorrent stopped the torrent",
                                )),
                                replayed,
                            ));
                        }
                    }
                }
                ReleaseState::DeletePending | ReleaseState::UnknownDelete => {
                    if let Err(problem) = self.validate_retained_ownership(&record) {
                        return self.block(record, problem, replayed);
                    }
                    let observation = match self.observe_until_delete_settles(&record).await {
                        Ok(observation) => observation,
                        Err(problem) => {
                            let unknown = if record.state == ReleaseState::UnknownDelete {
                                record
                            } else {
                                self.journal.mark_unknown_delete(
                                    &record.operation_id,
                                    "QBIT_DELETE_UNCERTAIN",
                                )?
                            };
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownDelete,
                                unknown,
                                Some(problem),
                                replayed,
                            ));
                        }
                    };
                    match observation {
                        TargetObservation::Absent => {
                            let finished = self.journal.finish_release(&record.operation_id)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Finished,
                                finished,
                                None,
                                replayed,
                            ));
                        }
                        TargetObservation::Present(torrent) if torrent.is_complete() => {
                            return self.block(record, complete_problem(), replayed);
                        }
                        TargetObservation::Present(_) if explicit_request
                            && record.state == ReleaseState::UnknownDelete =>
                        {
                            record = self.journal.retry_delete(&record.operation_id)?;
                            continue;
                        }
                        TargetObservation::Present(_) => {
                            let unknown = if record.state == ReleaseState::UnknownDelete {
                                record
                            } else {
                                self.journal.mark_unknown_delete(
                                    &record.operation_id,
                                    "QBIT_DELETE_UNCONFIRMED",
                                )?
                            };
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownDelete,
                                unknown,
                                Some(PortError::new(
                                    "QBIT_DELETE_UNCONFIRMED",
                                    "fresh observation did not confirm qBittorrent record removal",
                                )),
                                replayed,
                            ));
                        }
                    }
                }
                ReleaseState::Prepared => {
                    let torrent = match self.preflight(&record, false).await {
                        Ok(torrent) => torrent,
                        Err(problem) => return self.block(record, problem, replayed),
                    };
                    if torrent.state.is_stopped() {
                        record = self.journal.mark_stopped(&record.operation_id)?;
                        continue;
                    }

                    let pending = self.journal.mark_stop_pending(&record.operation_id)?;
                    match self.client.stop(&torrent.id).await {
                        EffectAttempt::NotSent(problem) => {
                            return self.block(pending, problem, replayed);
                        }
                        EffectAttempt::Rejected(problem) => {
                            let failed = self
                                .journal
                                .mark_release_failed(&pending.operation_id, problem.code)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Failed,
                                failed,
                                Some(problem),
                                replayed,
                            ));
                        }
                        EffectAttempt::Uncertain(problem) => {
                            let unknown = self.journal.mark_unknown_stop(
                                &pending.operation_id,
                                "QBIT_STOP_UNCERTAIN",
                            )?;
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownStop,
                                unknown,
                                Some(problem),
                                replayed,
                            ));
                        }
                        EffectAttempt::Accepted => {
                            record = pending;
                            continue;
                        }
                    }
                }
                ReleaseState::Stopped => {
                    let torrent = match self.preflight(&record, true).await {
                        Ok(torrent) => torrent,
                        Err(problem) => return self.block(record, problem, replayed),
                    };
                    let pending = self.journal.mark_delete_pending(&record.operation_id)?;
                    match self.client.remove_keep_files(&torrent.id).await {
                        EffectAttempt::NotSent(problem) => {
                            return self.block(pending, problem, replayed);
                        }
                        EffectAttempt::Rejected(problem) => {
                            let failed = self
                                .journal
                                .mark_release_failed(&pending.operation_id, problem.code)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Failed,
                                failed,
                                Some(problem),
                                replayed,
                            ));
                        }
                        EffectAttempt::Uncertain(problem) => {
                            let unknown = self.journal.mark_unknown_delete(
                                &pending.operation_id,
                                "QBIT_DELETE_UNCERTAIN",
                            )?;
                            return Ok(execution(
                                ReleaseExecutionStatus::UnknownDelete,
                                unknown,
                                Some(problem),
                                replayed,
                            ));
                        }
                        EffectAttempt::Accepted => {
                            record = pending;
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn preflight(
        &self,
        record: &ReleaseRecord,
        require_stopped: bool,
    ) -> Result<TorrentView, PortError> {
        self.validate_retained_ownership(record)?;
        let observation = self.observe_once(record).await?;
        let torrent = match observation {
            TargetObservation::Absent => {
                return Err(PortError::new(
                    "TORRENT_NOT_PRESENT",
                    "qBittorrent no longer contains the release target",
                ));
            }
            TargetObservation::Present(torrent) => torrent,
        };
        if torrent.is_complete() {
            return Err(complete_problem());
        }
        if require_stopped && !torrent.state.is_stopped() {
            return Err(PortError::new(
                "TORRENT_NOT_STOPPED",
                "torrent changed state after the stop receipt; release will not delete the record",
            ));
        }
        Ok(torrent)
    }

    fn validate_retained_ownership(&self, record: &ReleaseRecord) -> Result<(), PortError> {
        let snapshot = self
            .storage
            .read_incoming(&record.source_relative, self.max_metainfo_bytes)?;
        if snapshot.evidence != record.source_evidence {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "retained Incoming source evidence changed during release",
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&snapshot.bytes).into();
        if digest != record.source_metainfo_digest {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "retained Incoming metainfo bytes changed during release",
            ));
        }

        let volume = self.storage.volume_status(ManagedRoot::Working)?;
        if volume.volume_id != record.working_volume_id {
            return Err(PortError::new(
                "STORAGE_VOLUME_CHANGED",
                "Working volume identity changed during release",
            ));
        }
        if !self
            .storage
            .matches_root_path(ManagedRoot::Working, &record.working_save_path)?
        {
            return Err(PortError::new(
                "STORAGE_ROOT_CHANGED",
                "release ownership no longer points at the managed Working root",
            ));
        }
        Ok(())
    }

    async fn observe_once(&self, record: &ReleaseRecord) -> Result<TargetObservation, PortError> {
        let mut found: Option<TorrentView> = None;
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
                    "release target is no longer owned by the managed Working root",
                ));
            }
            if let Some(previous) = &found {
                if previous.id != torrent.id {
                    return Err(PortError::new(
                        "IDENTITY_CONFLICT",
                        "multiple qBittorrent records match aliases of the release identity",
                    ));
                }
            } else {
                found = Some(torrent);
            }
        }
        Ok(match found {
            Some(torrent) => TargetObservation::Present(torrent),
            None => TargetObservation::Absent,
        })
    }

    async fn observe_until_stop_settles(
        &self,
        record: &ReleaseRecord,
    ) -> Result<TargetObservation, PortError> {
        let mut last = TargetObservation::Absent;
        for attempt in 0..self.observation_attempts {
            let observation = self.observe_once(record).await?;
            let settled = match &observation {
                TargetObservation::Absent => true,
                TargetObservation::Present(torrent) => {
                    torrent.is_complete() || torrent.state.is_stopped()
                }
            };
            if settled {
                return Ok(observation);
            }
            last = observation;
            self.delay_observation(attempt).await;
        }
        Ok(last)
    }

    async fn observe_until_delete_settles(
        &self,
        record: &ReleaseRecord,
    ) -> Result<TargetObservation, PortError> {
        let mut last = TargetObservation::Absent;
        for attempt in 0..self.observation_attempts {
            let observation = self.observe_once(record).await?;
            let settled = match &observation {
                TargetObservation::Absent => true,
                TargetObservation::Present(torrent) => torrent.is_complete(),
            };
            if settled {
                return Ok(observation);
            }
            last = observation;
            self.delay_observation(attempt).await;
        }
        Ok(last)
    }

    async fn delay_observation(&self, attempt: usize) {
        if attempt + 1 < self.observation_attempts && !self.observation_delay.is_zero() {
            tokio::time::sleep(self.observation_delay).await;
        }
    }

    fn block(
        &self,
        record: ReleaseRecord,
        problem: PortError,
        replayed: bool,
    ) -> Result<ReleaseExecution, PortError> {
        let blocked = self
            .journal
            .mark_release_blocked(&record.operation_id, problem.code)?;
        Ok(execution(
            ReleaseExecutionStatus::Blocked,
            blocked,
            Some(problem),
            replayed,
        ))
    }
}

fn execution(
    status: ReleaseExecutionStatus,
    record: ReleaseRecord,
    problem: Option<PortError>,
    replayed: bool,
) -> ReleaseExecution {
    ReleaseExecution {
        status,
        record,
        problem,
        replayed,
    }
}

fn complete_problem() -> PortError {
    PortError::new(
        "TORRENT_COMPLETE",
        "torrent became complete; release stops before qBittorrent record deletion",
    )
}

fn stored_problem(record: &ReleaseRecord, message: &str) -> Option<PortError> {
    record
        .problem_code
        .as_deref()
        .map(|code| PortError::new(release_problem_code(code), format!("{message}: {code}")))
}

fn release_problem_code(code: &str) -> &'static str {
    match code {
        "SOURCE_AMBIGUOUS" => "SOURCE_AMBIGUOUS",
        "STORAGE_VOLUME_CHANGED" => "STORAGE_VOLUME_CHANGED",
        "STORAGE_ROOT_CHANGED" => "STORAGE_ROOT_CHANGED",
        "QBIT_SAVE_PATH_CONFLICT" => "QBIT_SAVE_PATH_CONFLICT",
        "IDENTITY_CONFLICT" => "IDENTITY_CONFLICT",
        "TORRENT_NOT_PRESENT" => "TORRENT_NOT_PRESENT",
        "TORRENT_NOT_STOPPED" => "TORRENT_NOT_STOPPED",
        "TORRENT_COMPLETE" => "TORRENT_COMPLETE",
        "QBIT_STOP_UNCERTAIN" => "QBIT_STOP_UNCERTAIN",
        "QBIT_STOP_UNCONFIRMED" => "QBIT_STOP_UNCONFIRMED",
        "QBIT_DELETE_UNCERTAIN" => "QBIT_DELETE_UNCERTAIN",
        "QBIT_DELETE_UNCONFIRMED" => "QBIT_DELETE_UNCONFIRMED",
        "QBIT_MUTATION_REJECTED" => "QBIT_MUTATION_REJECTED",
        "QBIT_UNAVAILABLE" => "QBIT_UNAVAILABLE",
        _ => "RELEASE_FAILED",
    }
}
