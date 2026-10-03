use qb_domain::{torrent::TorrentId, OperationId, RequestId};
use sha2::{Digest, Sha256};

use crate::PortError;

pub const FINGERPRINT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TorrentControlAction {
    Stop,
    Start,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationCommand {
    TorrentControl {
        torrent_id: TorrentId,
        action: TorrentControlAction,
    },
    SetQueueTarget {
        target_client_count: u32,
    },
    SetActiveDownloads {
        max_active_downloads: u32,
    },
    SetDownloadLimit {
        bytes_per_sec: u64,
    },
    SetUploadLimit {
        bytes_per_sec: u64,
    },
}

impl MutationCommand {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::TorrentControl {
                action: TorrentControlAction::Stop,
                ..
            } => "torrent.stop",
            Self::TorrentControl {
                action: TorrentControlAction::Start,
                ..
            } => "torrent.start",
            Self::SetQueueTarget { .. } => "queue.target.set",
            Self::SetActiveDownloads { .. } => "queue.downloads.set",
            Self::SetDownloadLimit { .. } => "transfer.download_limit.set",
            Self::SetUploadLimit { .. } => "transfer.upload_limit.set",
        }
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-mutation-fingerprint-v1\0");
        digest.update(self.kind().as_bytes());
        digest.update([0]);

        match self {
            Self::TorrentControl { torrent_id, action } => {
                digest.update(torrent_id.as_str().as_bytes());
                digest.update([0]);
                digest.update(match action {
                    TorrentControlAction::Stop => b"stop".as_slice(),
                    TorrentControlAction::Start => b"start".as_slice(),
                });
            }
            Self::SetQueueTarget {
                target_client_count,
            } => digest.update(target_client_count.to_be_bytes()),
            Self::SetActiveDownloads {
                max_active_downloads,
            } => digest.update(max_active_downloads.to_be_bytes()),
            Self::SetDownloadLimit { bytes_per_sec } | Self::SetUploadLimit { bytes_per_sec } => {
                digest.update(bytes_per_sec.to_be_bytes());
            }
        }

        digest.finalize().into()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationDisposition {
    Prepared,
    EffectPending,
    ObservedApplied,
    Finished,
    Blocked,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub command: MutationCommand,
    pub fingerprint_version: u32,
    pub command_fingerprint: [u8; 32],
    pub checkpoint: String,
    pub disposition: MutationDisposition,
    pub pending_effect_kind: Option<String>,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestReservation {
    New(MutationRecord),
    Replay(MutationRecord),
    Conflict { operation_id: OperationId },
}

pub trait MutationJournal: Send + Sync {
    fn reserve_request(
        &self,
        request_id: &RequestId,
        command: &MutationCommand,
    ) -> Result<RequestReservation, PortError>;

    fn mark_effect_pending(
        &self,
        operation_id: &OperationId,
        effect_kind: &str,
    ) -> Result<MutationRecord, PortError>;

    fn mark_observed_applied(
        &self,
        operation_id: &OperationId,
    ) -> Result<MutationRecord, PortError>;

    fn mark_retry_ready(&self, operation_id: &OperationId) -> Result<MutationRecord, PortError>;

    fn finish(&self, operation_id: &OperationId) -> Result<MutationRecord, PortError>;

    fn mark_unknown(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<MutationRecord, PortError>;

    fn mark_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<MutationRecord, PortError>;

    fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<MutationRecord>, PortError>;

    fn list_recoverable(&self) -> Result<Vec<MutationRecord>, PortError>;

    fn queue_target(&self) -> Result<Option<u32>, PortError>;

    fn apply_queue_target(
        &self,
        operation_id: &OperationId,
        target_client_count: u32,
    ) -> Result<MutationRecord, PortError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_semantic_and_stable() {
        let id = TorrentId::new("abcdef0123456789abcdef0123456789abcdef01").expect("id");
        let command = MutationCommand::TorrentControl {
            torrent_id: id,
            action: TorrentControlAction::Stop,
        };

        assert_eq!(command.fingerprint(), command.fingerprint());
        assert_ne!(
            command.fingerprint(),
            MutationCommand::SetQueueTarget {
                target_client_count: 10,
            }
            .fingerprint()
        );
    }

    #[test]
    fn command_values_affect_fingerprint() {
        assert_ne!(
            MutationCommand::SetDownloadLimit {
                bytes_per_sec: 24_000_000,
            }
            .fingerprint(),
            MutationCommand::SetDownloadLimit {
                bytes_per_sec: 23_999_488,
            }
            .fingerprint()
        );
        assert_ne!(
            MutationCommand::SetDownloadLimit { bytes_per_sec: 0 }.fingerprint(),
            MutationCommand::SetUploadLimit { bytes_per_sec: 0 }.fingerprint()
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationExecutionStatus {
    Finished,
    Blocked,
    Unknown,
    Failed,
}

#[derive(Debug)]
pub struct MutationExecution {
    pub status: MutationExecutionStatus,
    pub record: MutationRecord,
    pub problem: Option<PortError>,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum MutationExecutionResult {
    Execution(MutationExecution),
    Conflict { operation_id: OperationId },
}

pub struct MutationService {
    journal: std::sync::Arc<dyn MutationJournal>,
    client: std::sync::Arc<dyn crate::torrent::TorrentClient>,
    lane: tokio::sync::Mutex<()>,
}

impl MutationService {
    pub fn new(
        journal: std::sync::Arc<dyn MutationJournal>,
        client: std::sync::Arc<dyn crate::torrent::TorrentClient>,
    ) -> Self {
        Self {
            journal,
            client,
            lane: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn execute(
        &self,
        request_id: &RequestId,
        command: MutationCommand,
    ) -> Result<MutationExecutionResult, PortError> {
        let _guard = self.lane.lock().await;
        let (record, replayed) = match self.journal.reserve_request(request_id, &command)? {
            RequestReservation::New(record) => (record, false),
            RequestReservation::Replay(record) => (record, true),
            RequestReservation::Conflict { operation_id } => {
                return Ok(MutationExecutionResult::Conflict { operation_id });
            }
        };

        self.advance(record, replayed)
            .await
            .map(MutationExecutionResult::Execution)
    }

    pub async fn recover_all(&self) -> Result<Vec<MutationExecution>, PortError> {
        let _guard = self.lane.lock().await;
        let records = self.journal.list_recoverable()?;
        let mut results = Vec::with_capacity(records.len());

        for record in records {
            results.push(self.advance(record, true).await?);
        }

        Ok(results)
    }

    pub fn queue_target(&self) -> Result<Option<u32>, PortError> {
        self.journal.queue_target()
    }

    async fn advance(
        &self,
        record: MutationRecord,
        replayed: bool,
    ) -> Result<MutationExecution, PortError> {
        let local_queue_target = match &record.command {
            MutationCommand::SetQueueTarget {
                target_client_count,
            } => Some(*target_client_count),
            _ => None,
        };
        if let Some(target_client_count) = local_queue_target {
            return self.advance_queue_target(record, target_client_count, replayed);
        }

        match record.disposition {
            MutationDisposition::Finished => {
                return Ok(execution(
                    MutationExecutionStatus::Finished,
                    record,
                    None,
                    replayed,
                ));
            }
            MutationDisposition::Failed => {
                let problem = record
                    .problem_code
                    .as_deref()
                    .map(|code| PortError::new(leak_problem_code(code), "mutation failed"));
                return Ok(execution(
                    MutationExecutionStatus::Failed,
                    record,
                    problem,
                    replayed,
                ));
            }
            MutationDisposition::Blocked => {
                let problem = record
                    .problem_code
                    .as_deref()
                    .map(|code| PortError::new(leak_problem_code(code), "mutation blocked"));
                return Ok(execution(
                    MutationExecutionStatus::Blocked,
                    record,
                    problem,
                    replayed,
                ));
            }
            MutationDisposition::ObservedApplied => {
                let finished = self.journal.finish(&record.operation_id)?;
                return Ok(execution(
                    MutationExecutionStatus::Finished,
                    finished,
                    None,
                    replayed,
                ));
            }
            MutationDisposition::EffectPending | MutationDisposition::Unknown => {
                match self.observe_desired(&record.command).await {
                    Ok(true) => {
                        let observed = self.journal.mark_observed_applied(&record.operation_id)?;
                        let finished = self.journal.finish(&observed.operation_id)?;
                        return Ok(execution(
                            MutationExecutionStatus::Finished,
                            finished,
                            None,
                            replayed,
                        ));
                    }
                    Ok(false) => {
                        self.journal.mark_retry_ready(&record.operation_id)?;
                    }
                    Err(problem) => {
                        let unknown = if record.disposition == MutationDisposition::Unknown {
                            record
                        } else {
                            self.journal
                                .mark_unknown(&record.operation_id, "QBIT_MUTATION_UNCERTAIN")?
                        };
                        return Ok(execution(
                            MutationExecutionStatus::Unknown,
                            unknown,
                            Some(problem),
                            replayed,
                        ));
                    }
                }
            }
            MutationDisposition::Prepared => {}
        }

        if let Err(problem) = self.preflight(&record.command).await {
            return Ok(execution(
                MutationExecutionStatus::Blocked,
                self.journal
                    .get_operation(&record.operation_id)?
                    .unwrap_or(record),
                Some(problem),
                replayed,
            ));
        }

        let pending = self
            .journal
            .mark_effect_pending(&record.operation_id, effect_kind(&record.command))?;

        match self.perform_effect(&record.command).await {
            crate::torrent::EffectAttempt::NotSent(problem) => {
                let prepared = self.journal.mark_retry_ready(&pending.operation_id)?;
                Ok(execution(
                    MutationExecutionStatus::Blocked,
                    prepared,
                    Some(problem),
                    replayed,
                ))
            }
            crate::torrent::EffectAttempt::Rejected(problem) => {
                let failed = self
                    .journal
                    .mark_failed(&pending.operation_id, problem.code)?;
                Ok(execution(
                    MutationExecutionStatus::Failed,
                    failed,
                    Some(problem),
                    replayed,
                ))
            }
            crate::torrent::EffectAttempt::Uncertain(problem) => {
                let unknown = self
                    .journal
                    .mark_unknown(&pending.operation_id, "QBIT_MUTATION_UNCERTAIN")?;
                Ok(execution(
                    MutationExecutionStatus::Unknown,
                    unknown,
                    Some(problem),
                    replayed,
                ))
            }
            crate::torrent::EffectAttempt::Accepted => {
                match self.observe_desired(&record.command).await {
                    Ok(true) => {
                        let observed = self.journal.mark_observed_applied(&pending.operation_id)?;
                        let finished = self.journal.finish(&observed.operation_id)?;
                        Ok(execution(
                            MutationExecutionStatus::Finished,
                            finished,
                            None,
                            replayed,
                        ))
                    }
                    Ok(false) => {
                        let unknown = self.journal.mark_unknown(
                            &pending.operation_id,
                            "QBIT_POSTCONDITION_UNCONFIRMED",
                        )?;
                        Ok(execution(
                            MutationExecutionStatus::Unknown,
                            unknown,
                            Some(PortError::new(
                                "QBIT_POSTCONDITION_UNCONFIRMED",
                                "qBittorrent accepted the request but the fresh observation did not confirm the requested state",
                            )),
                            replayed,
                        ))
                    }
                    Err(problem) => {
                        let unknown = self
                            .journal
                            .mark_unknown(&pending.operation_id, "QBIT_MUTATION_UNCERTAIN")?;
                        Ok(execution(
                            MutationExecutionStatus::Unknown,
                            unknown,
                            Some(problem),
                            replayed,
                        ))
                    }
                }
            }
        }
    }

    fn advance_queue_target(
        &self,
        record: MutationRecord,
        target_client_count: u32,
        replayed: bool,
    ) -> Result<MutationExecution, PortError> {
        if record.disposition == MutationDisposition::Finished {
            return Ok(execution(
                MutationExecutionStatus::Finished,
                record,
                None,
                replayed,
            ));
        }
        if record.disposition != MutationDisposition::Prepared {
            return Err(PortError::new(
                "OPERATION_TRANSITION_INVALID",
                "local queue target operation is not in a recoverable state",
            ));
        }

        let finished = self
            .journal
            .apply_queue_target(&record.operation_id, target_client_count)?;
        Ok(execution(
            MutationExecutionStatus::Finished,
            finished,
            None,
            replayed,
        ))
    }

    async fn preflight(&self, command: &MutationCommand) -> Result<(), PortError> {
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

        match command {
            MutationCommand::TorrentControl { torrent_id, .. } => {
                let torrent =
                    self.client.get(torrent_id).await?.ok_or_else(|| {
                        PortError::new("TORRENT_NOT_FOUND", "torrent was not found")
                    })?;
                if torrent.state == qb_domain::torrent::TorrentState::Unknown {
                    return Err(PortError::new(
                        "TORRENT_STATE_UNKNOWN",
                        "qBittorrent returned an unknown torrent state",
                    ));
                }
            }
            MutationCommand::SetActiveDownloads { .. } => {
                let queue = self.client.queue_settings().await?;
                if !queue.queueing_enabled {
                    return Err(PortError::new(
                        "QBIT_QUEUEING_DISABLED",
                        "qBittorrent queueing is disabled",
                    ));
                }
            }
            MutationCommand::SetDownloadLimit { .. } | MutationCommand::SetUploadLimit { .. } => {}
            MutationCommand::SetQueueTarget { .. } => unreachable!("handled locally"),
        }

        Ok(())
    }

    async fn observe_desired(&self, command: &MutationCommand) -> Result<bool, PortError> {
        match command {
            MutationCommand::TorrentControl { torrent_id, action } => {
                let torrent =
                    self.client.get(torrent_id).await?.ok_or_else(|| {
                        PortError::new("TORRENT_NOT_FOUND", "torrent was not found")
                    })?;
                if torrent.state == qb_domain::torrent::TorrentState::Unknown {
                    return Err(PortError::new(
                        "TORRENT_STATE_UNKNOWN",
                        "qBittorrent returned an unknown torrent state",
                    ));
                }
                Ok(match action {
                    TorrentControlAction::Stop => torrent.state.is_stopped(),
                    TorrentControlAction::Start => !torrent.state.is_stopped(),
                })
            }
            MutationCommand::SetActiveDownloads {
                max_active_downloads,
            } => {
                let queue = self.client.queue_settings().await?;
                if !queue.queueing_enabled {
                    return Err(PortError::new(
                        "QBIT_QUEUEING_DISABLED",
                        "qBittorrent queueing is disabled",
                    ));
                }
                Ok(queue.max_active_downloads == i64::from(*max_active_downloads))
            }
            MutationCommand::SetDownloadLimit { bytes_per_sec } => {
                Ok(self.client.transfer_info().await?.download_limit_bps == *bytes_per_sec)
            }
            MutationCommand::SetUploadLimit { bytes_per_sec } => {
                Ok(self.client.transfer_info().await?.upload_limit_bps == *bytes_per_sec)
            }
            MutationCommand::SetQueueTarget {
                target_client_count,
            } => Ok(self.journal.queue_target()? == Some(*target_client_count)),
        }
    }

    async fn perform_effect(&self, command: &MutationCommand) -> crate::torrent::EffectAttempt {
        match command {
            MutationCommand::TorrentControl { torrent_id, action } => match action {
                TorrentControlAction::Stop => self.client.stop(torrent_id).await,
                TorrentControlAction::Start => self.client.start(torrent_id).await,
            },
            MutationCommand::SetActiveDownloads {
                max_active_downloads,
            } => {
                self.client
                    .set_active_downloads(*max_active_downloads)
                    .await
            }
            MutationCommand::SetDownloadLimit { bytes_per_sec } => {
                self.client.set_download_limit(*bytes_per_sec).await
            }
            MutationCommand::SetUploadLimit { bytes_per_sec } => {
                self.client.set_upload_limit(*bytes_per_sec).await
            }
            MutationCommand::SetQueueTarget { .. } => unreachable!("handled locally"),
        }
    }
}

fn execution(
    status: MutationExecutionStatus,
    record: MutationRecord,
    problem: Option<PortError>,
    replayed: bool,
) -> MutationExecution {
    MutationExecution {
        status,
        record,
        problem,
        replayed,
    }
}

fn effect_kind(command: &MutationCommand) -> &'static str {
    match command {
        MutationCommand::TorrentControl {
            action: TorrentControlAction::Stop,
            ..
        } => "qbit.stop",
        MutationCommand::TorrentControl {
            action: TorrentControlAction::Start,
            ..
        } => "qbit.start",
        MutationCommand::SetActiveDownloads { .. } => "qbit.set_active_downloads",
        MutationCommand::SetDownloadLimit { .. } => "qbit.set_download_limit",
        MutationCommand::SetUploadLimit { .. } => "qbit.set_upload_limit",
        MutationCommand::SetQueueTarget { .. } => "local.set_queue_target",
    }
}

fn leak_problem_code(code: &str) -> &'static str {
    match code {
        "QBIT_MUTATION_UNCERTAIN" => "QBIT_MUTATION_UNCERTAIN",
        "QBIT_POSTCONDITION_UNCONFIRMED" => "QBIT_POSTCONDITION_UNCONFIRMED",
        "QBIT_MUTATION_REJECTED" => "QBIT_MUTATION_REJECTED",
        "QBIT_AUTH_FAILED" => "QBIT_AUTH_FAILED",
        "QBIT_UNAVAILABLE" => "QBIT_UNAVAILABLE",
        "QBIT_API_UNSUPPORTED" => "QBIT_API_UNSUPPORTED",
        "TORRENT_NOT_FOUND" => "TORRENT_NOT_FOUND",
        "TORRENT_STATE_UNKNOWN" => "TORRENT_STATE_UNKNOWN",
        "QBIT_QUEUEING_DISABLED" => "QBIT_QUEUEING_DISABLED",
        _ => "MUTATION_FAILED",
    }
}
