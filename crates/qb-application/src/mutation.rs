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
    QueueSet {
        target_client_count: Option<u32>,
        max_active_downloads: Option<u32>,
    },
    TransferLimitsSet {
        download_limit_bps: Option<u64>,
        upload_limit_bps: Option<u64>,
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
            Self::QueueSet { .. } => "queue.set",
            Self::TransferLimitsSet { .. } => "transfer.limits.set",
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
            Self::QueueSet {
                target_client_count,
                max_active_downloads,
            } => {
                update_optional_u64(&mut digest, target_client_count.map(u64::from));
                update_optional_u64(&mut digest, max_active_downloads.map(u64::from));
            }
            Self::TransferLimitsSet {
                download_limit_bps,
                upload_limit_bps,
            } => {
                update_optional_u64(&mut digest, *download_limit_bps);
                update_optional_u64(&mut digest, *upload_limit_bps);
            }
        }

        digest.finalize().into()
    }
}

fn update_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
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
            MutationCommand::QueueSet {
                target_client_count: Some(10),
                max_active_downloads: None,
            }
            .fingerprint()
        );
    }

    #[test]
    fn optional_fields_affect_fingerprint() {
        let first = MutationCommand::TransferLimitsSet {
            download_limit_bps: Some(24_000_000),
            upload_limit_bps: None,
        };
        let second = MutationCommand::TransferLimitsSet {
            download_limit_bps: Some(24_000_000),
            upload_limit_bps: Some(0),
        };

        assert_ne!(first.fingerprint(), second.fingerprint());
    }
}
