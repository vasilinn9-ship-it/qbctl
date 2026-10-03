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

    fn mark_retry_ready(
        &self,
        operation_id: &OperationId,
    ) -> Result<MutationRecord, PortError>;

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
