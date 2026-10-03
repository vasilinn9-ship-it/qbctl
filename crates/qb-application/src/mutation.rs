use qb_domain::{OperationId, RequestId};

use crate::PortError;

pub const FINGERPRINT_VERSION: u32 = 1;

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
    pub command_kind: String,
    pub fingerprint_version: u32,
    pub command_fingerprint: [u8; 32],
    pub checkpoint: String,
    pub disposition: MutationDisposition,
    pub pending_effect_kind: Option<String>,
    pub problem_code: Option<String>,
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
        command_kind: &str,
        fingerprint: [u8; 32],
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

    fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<MutationRecord>, PortError>;
}
