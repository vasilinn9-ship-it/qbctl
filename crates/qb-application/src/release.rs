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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseResolution {
    PreservedIncomplete,
    BecameComplete,
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
    pub resolution: Option<ReleaseResolution>,
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
    fn finish_became_complete(
        &self,
        operation_id: &OperationId,
    ) -> Result<ReleaseRecord, PortError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseExecutionStatus {
    Finished,
    Retryable,
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
                            let finished =
                                self.journal.finish_became_complete(&record.operation_id)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Finished,
                                finished,
                                None,
                                replayed,
                            ));
                        }
                        TargetObservation::Present(torrent) if torrent.state.is_stopped() => {
                            record = self.journal.mark_stopped(&record.operation_id)?;
                            continue;
                        }
                        TargetObservation::Present(_)
                            if explicit_request && record.state == ReleaseState::UnknownStop =>
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
                        TargetObservation::Present(_)
                            if explicit_request && record.state == ReleaseState::UnknownDelete =>
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
                    if torrent.is_complete() {
                        let finished = self.journal.finish_became_complete(&record.operation_id)?;
                        return Ok(execution(
                            ReleaseExecutionStatus::Finished,
                            finished,
                            None,
                            replayed,
                        ));
                    }
                    if torrent.state.is_stopped() {
                        record = self.journal.mark_stopped(&record.operation_id)?;
                        continue;
                    }

                    let pending = self.journal.mark_stop_pending(&record.operation_id)?;
                    match self.client.stop(&torrent.id).await {
                        EffectAttempt::NotSent(problem) => {
                            let retry = self.journal.retry_stop(&pending.operation_id)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Retryable,
                                retry,
                                Some(problem),
                                replayed,
                            ));
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
                            let unknown = self
                                .journal
                                .mark_unknown_stop(&pending.operation_id, "QBIT_STOP_UNCERTAIN")?;
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
                    if torrent.is_complete() {
                        let finished = self.journal.finish_became_complete(&record.operation_id)?;
                        return Ok(execution(
                            ReleaseExecutionStatus::Finished,
                            finished,
                            None,
                            replayed,
                        ));
                    }
                    let pending = self.journal.mark_delete_pending(&record.operation_id)?;
                    match self.client.remove_keep_files(&torrent.id).await {
                        EffectAttempt::NotSent(problem) => {
                            let retry = self.journal.retry_delete(&pending.operation_id)?;
                            return Ok(execution(
                                ReleaseExecutionStatus::Retryable,
                                retry,
                                Some(problem),
                                replayed,
                            ));
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    use qb_domain::torrent::{TorrentId, TorrentState};

    use crate::{
        storage::{FileIdentity, IncomingFileSnapshot, StorageVolumeStatus},
        torrent::{
            AddTorrentRequest, EffectFuture, FileObservation, NetworkPreferences, PortFuture,
            QbitProbe, QueueSettings, TrackerEvidence, TransferInfo,
        },
    };

    use super::*;

    #[derive(Default)]
    struct FakeReleaseJournal {
        state: Mutex<FakeReleaseState>,
    }

    #[derive(Default)]
    struct FakeReleaseState {
        fingerprint: Option<[u8; 32]>,
        record: Option<ReleaseRecord>,
    }

    impl FakeReleaseJournal {
        fn update(
            &self,
            expected: &[ReleaseState],
            next: ReleaseState,
            problem_code: Option<&str>,
        ) -> Result<ReleaseRecord, PortError> {
            let mut state = self.state.lock().expect("release journal mutex");
            let record = state
                .record
                .as_mut()
                .ok_or_else(|| PortError::new("RELEASE_NOT_FOUND", "missing fake release"))?;
            if !expected.contains(&record.state) {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    format!("unexpected fake release state: {:?}", record.state),
                ));
            }
            record.state = next;
            record.resolution = None;
            record.problem_code = problem_code.map(str::to_owned);
            record.revision += 1;
            Ok(record.clone())
        }
    }

    impl ReleaseJournal for FakeReleaseJournal {
        fn reserve_release(
            &self,
            request: &ReleaseRequest,
        ) -> Result<ReleaseReservation, PortError> {
            let mut state = self.state.lock().expect("release journal mutex");
            let fingerprint = request.fingerprint();
            if let Some(record) = state.record.clone() {
                if state.fingerprint == Some(fingerprint) && record.request_id == request.request_id
                {
                    return Ok(ReleaseReservation::Replay(record));
                }
                return Ok(ReleaseReservation::Conflict {
                    operation_id: record.operation_id,
                });
            }

            let bytes = b"release-fixture".to_vec();
            let record = ReleaseRecord {
                request_id: request.request_id.clone(),
                operation_id: OperationId::new("release-operation").expect("operation id"),
                registry_id: request.registry_id.clone(),
                admission_operation_id: OperationId::new("admission-operation")
                    .expect("admission operation id"),
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                source_relative: "candidate.torrent".into(),
                source_evidence: FileEvidence {
                    identity: FileIdentity {
                        volume_id: 7,
                        file_id: 9,
                    },
                    size: u64::try_from(bytes.len()).expect("fixture size"),
                    modified_marker: 11,
                },
                source_metainfo_digest: Sha256::digest(&bytes).into(),
                working_volume_id: 13,
                retained_bytes: 100,
                working_save_path: r"C:\Managed\Working".into(),
                state: ReleaseState::Prepared,
                resolution: None,
                problem_code: None,
                revision: 1,
            };
            state.fingerprint = Some(fingerprint);
            state.record = Some(record.clone());
            Ok(ReleaseReservation::New(record))
        }

        fn list_recoverable_releases(&self) -> Result<Vec<ReleaseRecord>, PortError> {
            let state = self.state.lock().expect("release journal mutex");
            Ok(state
                .record
                .as_ref()
                .filter(|record| {
                    matches!(
                        record.state,
                        ReleaseState::Prepared
                            | ReleaseState::StopPending
                            | ReleaseState::Stopped
                            | ReleaseState::UnknownStop
                            | ReleaseState::DeletePending
                            | ReleaseState::UnknownDelete
                    )
                })
                .cloned()
                .into_iter()
                .collect())
        }

        fn mark_stop_pending(
            &self,
            _operation_id: &OperationId,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(&[ReleaseState::Prepared], ReleaseState::StopPending, None)
        }

        fn mark_stopped(&self, _operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[
                    ReleaseState::Prepared,
                    ReleaseState::StopPending,
                    ReleaseState::UnknownStop,
                ],
                ReleaseState::Stopped,
                None,
            )
        }

        fn mark_unknown_stop(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[ReleaseState::StopPending],
                ReleaseState::UnknownStop,
                Some(problem_code),
            )
        }

        fn retry_stop(&self, _operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[ReleaseState::UnknownStop, ReleaseState::StopPending],
                ReleaseState::Prepared,
                None,
            )
        }

        fn mark_delete_pending(
            &self,
            _operation_id: &OperationId,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(&[ReleaseState::Stopped], ReleaseState::DeletePending, None)
        }

        fn mark_unknown_delete(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[ReleaseState::DeletePending],
                ReleaseState::UnknownDelete,
                Some(problem_code),
            )
        }

        fn retry_delete(&self, _operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[ReleaseState::UnknownDelete, ReleaseState::DeletePending],
                ReleaseState::Stopped,
                None,
            )
        }

        fn mark_release_blocked(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[
                    ReleaseState::Prepared,
                    ReleaseState::StopPending,
                    ReleaseState::Stopped,
                    ReleaseState::UnknownStop,
                    ReleaseState::DeletePending,
                    ReleaseState::UnknownDelete,
                ],
                ReleaseState::Blocked,
                Some(problem_code),
            )
        }

        fn mark_release_failed(
            &self,
            _operation_id: &OperationId,
            problem_code: &str,
        ) -> Result<ReleaseRecord, PortError> {
            self.update(
                &[ReleaseState::StopPending, ReleaseState::DeletePending],
                ReleaseState::Failed,
                Some(problem_code),
            )
        }

        fn finish_release(&self, _operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
            let mut record = self.update(
                &[ReleaseState::DeletePending, ReleaseState::UnknownDelete],
                ReleaseState::Finished,
                None,
            )?;
            record.resolution = Some(ReleaseResolution::PreservedIncomplete);
            self.state.lock().expect("release journal mutex").record = Some(record.clone());
            Ok(record)
        }

        fn finish_became_complete(
            &self,
            _operation_id: &OperationId,
        ) -> Result<ReleaseRecord, PortError> {
            let mut record = self.update(
                &[
                    ReleaseState::Prepared,
                    ReleaseState::StopPending,
                    ReleaseState::Stopped,
                    ReleaseState::UnknownStop,
                ],
                ReleaseState::Finished,
                None,
            )?;
            record.resolution = Some(ReleaseResolution::BecameComplete);
            self.state.lock().expect("release journal mutex").record = Some(record.clone());
            Ok(record)
        }
    }

    struct FakeReleaseStorage {
        snapshot: Mutex<IncomingFileSnapshot>,
        volume_id: u64,
        root: String,
    }

    impl Storage for FakeReleaseStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(StorageVolumeStatus {
                root,
                volume_id: self.volume_id,
                free_bytes: 10_000,
                total_bytes: 20_000,
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
            Ok(vec![self
                .snapshot
                .lock()
                .expect("snapshot mutex")
                .relative_path
                .clone()])
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            let snapshot = self.snapshot.lock().expect("snapshot mutex");
            if relative_path != snapshot.relative_path {
                return Err(PortError::new("STORAGE_NOT_FOUND", relative_path));
            }
            Ok(snapshot.clone())
        }
    }

    #[derive(Clone, Copy)]
    enum FakeEffect {
        Accepted,
        Uncertain,
        NotSent,
        Rejected,
    }

    struct FakeReleaseClient {
        observations: Mutex<VecDeque<Result<Option<TorrentView>, PortError>>>,
        stop_effects: Mutex<VecDeque<FakeEffect>>,
        delete_effects: Mutex<VecDeque<FakeEffect>>,
        stop_calls: AtomicUsize,
        delete_calls: AtomicUsize,
    }

    impl FakeReleaseClient {
        fn new(
            observations: Vec<Result<Option<TorrentView>, PortError>>,
            stop_effects: Vec<FakeEffect>,
            delete_effects: Vec<FakeEffect>,
        ) -> Self {
            Self {
                observations: Mutex::new(observations.into()),
                stop_effects: Mutex::new(stop_effects.into()),
                delete_effects: Mutex::new(delete_effects.into()),
                stop_calls: AtomicUsize::new(0),
                delete_calls: AtomicUsize::new(0),
            }
        }

        fn effect(queue: &Mutex<VecDeque<FakeEffect>>, context: &'static str) -> EffectAttempt {
            match queue
                .lock()
                .expect("effect mutex")
                .pop_front()
                .unwrap_or(FakeEffect::NotSent)
            {
                FakeEffect::Accepted => EffectAttempt::Accepted,
                FakeEffect::Uncertain => EffectAttempt::Uncertain(PortError::new(
                    "QBIT_MUTATION_UNCERTAIN",
                    format!("{context} response dropped after send"),
                )),
                FakeEffect::NotSent => EffectAttempt::NotSent(PortError::new(
                    "QBIT_UNAVAILABLE",
                    format!("{context} request was not sent"),
                )),
                FakeEffect::Rejected => EffectAttempt::Rejected(PortError::new(
                    "QBIT_MUTATION_REJECTED",
                    format!("{context} request was rejected"),
                )),
            }
        }
    }

    impl TorrentClient for FakeReleaseClient {
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
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn stop<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async move {
                self.stop_calls.fetch_add(1, Ordering::SeqCst);
                Self::effect(&self.stop_effects, "stop")
            })
        }

        fn start<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async { EffectAttempt::NotSent(unused_client_call()) })
        }

        fn remove_keep_files<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
            Box::pin(async move {
                self.delete_calls.fetch_add(1, Ordering::SeqCst);
                Self::effect(&self.delete_effects, "delete")
            })
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

    fn release_request() -> ReleaseRequest {
        ReleaseRequest {
            request_id: RequestId::new("release-service-1").expect("request id"),
            registry_id: "registry-1".into(),
        }
    }

    fn release_storage() -> Arc<FakeReleaseStorage> {
        let bytes = b"release-fixture".to_vec();
        Arc::new(FakeReleaseStorage {
            snapshot: Mutex::new(IncomingFileSnapshot {
                relative_path: "candidate.torrent".into(),
                evidence: FileEvidence {
                    identity: FileIdentity {
                        volume_id: 7,
                        file_id: 9,
                    },
                    size: u64::try_from(bytes.len()).expect("fixture size"),
                    modified_marker: 11,
                },
                bytes,
            }),
            volume_id: 13,
            root: r"C:\Managed\Working".into(),
        })
    }

    fn torrent_view(state: TorrentState, remaining_bytes: u64) -> TorrentView {
        TorrentView {
            id: TorrentId::new("1111111111111111111111111111111111111111").expect("torrent id"),
            name: "fixture".into(),
            save_path: r"C:\Managed\Working".into(),
            state,
            total_bytes: 100,
            remaining_bytes,
            download_rate_bps: 0,
            upload_rate_bps: 0,
            progress_ppm: if remaining_bytes == 0 {
                1_000_000
            } else {
                500_000
            },
            availability: None,
            peers_connected: 0,
            peers_known: 0,
            seeds_connected: 0,
            seeds_known: 0,
        }
    }

    fn execution(result: ReleaseExecutionResult) -> ReleaseExecution {
        match result {
            ReleaseExecutionResult::Execution(execution) => *execution,
            ReleaseExecutionResult::Conflict { operation_id } => {
                panic!("unexpected release conflict: {operation_id}")
            }
        }
    }

    fn service(
        journal: Arc<FakeReleaseJournal>,
        storage: Arc<FakeReleaseStorage>,
        client: Arc<FakeReleaseClient>,
    ) -> ReleaseService {
        ReleaseService::new(journal, storage, client, 1024)
            .with_observation_policy(3, Duration::ZERO)
    }

    #[tokio::test]
    async fn stop_timeout_after_send_becomes_unknown() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![Ok(Some(torrent_view(TorrentState::Downloading, 50)))],
            vec![FakeEffect::Uncertain],
            Vec::new(),
        ));
        let service = service(journal, storage, client.clone());

        let result = execution(
            service
                .execute(&release_request())
                .await
                .expect("execute release"),
        );

        assert_eq!(result.status, ReleaseExecutionStatus::UnknownStop);
        assert_eq!(result.record.state, ReleaseState::UnknownStop);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restart_observes_stop_pending_without_duplicate_stop() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let request = release_request();
        let record = match journal.reserve_release(&request).expect("reserve") {
            ReleaseReservation::New(record) => record,
            other => panic!("unexpected reservation: {other:?}"),
        };
        journal
            .mark_stop_pending(&record.operation_id)
            .expect("stop pending");

        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
            ],
            Vec::new(),
            vec![FakeEffect::NotSent],
        ));
        let service = service(journal, storage, client.clone());

        let results = service.recover_all().await.expect("recover");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ReleaseExecutionStatus::Retryable);
        assert_eq!(results[0].record.state, ReleaseState::Stopped);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 0);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn restart_does_not_blindly_retry_unknown_stop() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let request = release_request();
        let record = match journal.reserve_release(&request).expect("reserve") {
            ReleaseReservation::New(record) => record,
            other => panic!("unexpected reservation: {other:?}"),
        };
        journal
            .mark_stop_pending(&record.operation_id)
            .expect("pending");
        journal
            .mark_unknown_stop(&record.operation_id, "QBIT_STOP_UNCERTAIN")
            .expect("unknown");

        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
            ],
            vec![FakeEffect::Accepted],
            Vec::new(),
        ));
        let service = service(journal, storage, client.clone());

        let results = service.recover_all().await.expect("recover");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ReleaseExecutionStatus::UnknownStop);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn explicit_replay_retries_unknown_stop_only_after_observation() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let request = release_request();
        let record = match journal.reserve_release(&request).expect("reserve") {
            ReleaseReservation::New(record) => record,
            other => panic!("unexpected reservation: {other:?}"),
        };
        journal
            .mark_stop_pending(&record.operation_id)
            .expect("pending");
        journal
            .mark_unknown_stop(&record.operation_id, "QBIT_STOP_UNCERTAIN")
            .expect("unknown");

        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Downloading, 50))),
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
            ],
            vec![FakeEffect::Accepted],
            vec![FakeEffect::NotSent],
        ));
        let service = service(journal, storage, client.clone());

        let result = execution(service.execute(&request).await.expect("explicit replay"));

        assert_eq!(result.status, ReleaseExecutionStatus::Retryable);
        assert_eq!(result.record.state, ReleaseState::Stopped);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn becoming_complete_before_delete_finishes_without_delete_effect() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
                Ok(Some(torrent_view(TorrentState::Stopped, 0))),
            ],
            Vec::new(),
            vec![FakeEffect::Accepted],
        ));
        let service = service(journal, storage, client.clone());

        let result = execution(
            service
                .execute(&release_request())
                .await
                .expect("execute release"),
        );

        assert_eq!(result.status, ReleaseExecutionStatus::Finished);
        assert_eq!(result.record.state, ReleaseState::Finished);
        assert_eq!(
            result.record.resolution,
            Some(ReleaseResolution::BecameComplete)
        );
        assert!(result.problem.is_none());
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn delete_uncertainty_recovers_by_observing_record_absent() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
                Ok(Some(torrent_view(TorrentState::Stopped, 50))),
                Ok(None),
            ],
            Vec::new(),
            vec![FakeEffect::Uncertain],
        ));
        let release_service = service(journal.clone(), storage.clone(), client.clone());
        let request = release_request();

        let first = execution(
            release_service
                .execute(&request)
                .await
                .expect("execute release"),
        );
        assert_eq!(first.status, ReleaseExecutionStatus::UnknownDelete);
        assert_eq!(first.record.state, ReleaseState::UnknownDelete);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 1);

        let restarted = service(journal, storage, client.clone());
        let recovered = restarted.recover_all().await.expect("recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status, ReleaseExecutionStatus::Finished);
        assert_eq!(recovered[0].record.state, ReleaseState::Finished);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unsent_stop_returns_to_prepared_for_safe_retry() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let storage = release_storage();
        let client = Arc::new(FakeReleaseClient::new(
            vec![Ok(Some(torrent_view(TorrentState::Downloading, 50)))],
            vec![FakeEffect::NotSent],
            Vec::new(),
        ));
        let service = service(journal, storage, client.clone());

        let result = execution(
            service
                .execute(&release_request())
                .await
                .expect("execute release"),
        );

        assert_eq!(result.status, ReleaseExecutionStatus::Retryable);
        assert_eq!(result.record.state, ReleaseState::Prepared);
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn changed_incoming_source_blocks_before_any_qbit_effect() {
        let journal = Arc::new(FakeReleaseJournal::default());
        let request = release_request();
        let storage = release_storage();
        {
            let mut snapshot = storage.snapshot.lock().expect("snapshot mutex");
            snapshot.evidence.modified_marker += 1;
        }
        let client = Arc::new(FakeReleaseClient::new(
            Vec::new(),
            vec![FakeEffect::Accepted],
            vec![FakeEffect::Accepted],
        ));
        let service = service(journal, storage, client.clone());

        let result = execution(service.execute(&request).await.expect("execute release"));

        assert_eq!(result.status, ReleaseExecutionStatus::Blocked);
        assert_eq!(
            result.problem.as_ref().map(|problem| problem.code),
            Some("SOURCE_AMBIGUOUS")
        );
        assert_eq!(client.stop_calls.load(Ordering::SeqCst), 0);
        assert_eq!(client.delete_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rejected_effect_fixture_variant_is_constructible() {
        assert!(matches!(FakeEffect::Rejected, FakeEffect::Rejected));
    }
}
