pub mod system;

use std::{error::Error, fmt};

use qb_domain::{JobId, OperationId, PlanId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeStatus {
    Ok,
    Pending,
    Partial,
    Blocked,
    Unknown,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryGuidance {
    DoNotRetry,
    ObserveOrRecover,
    RetrySameRequestAfterBackoff,
    RetryAfterStateChange,
    NewRequestRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationCertainty {
    NoMutation,
    ConfirmedNotApplied,
    ConfirmedApplied,
    MayHaveApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Problem {
    pub code: String,
    pub retry_guidance: RetryGuidance,
    pub mutation_certainty: MutationCertainty,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outcome<T> {
    pub status: OutcomeStatus,
    pub value: Option<T>,
    pub problems: Vec<Problem>,
    pub operation_id: Option<OperationId>,
    pub job_id: Option<JobId>,
    pub plan_id: Option<PlanId>,
}

impl<T> Outcome<T> {
    pub fn ok(value: T) -> Self {
        Self {
            status: OutcomeStatus::Ok,
            value: Some(value),
            problems: Vec::new(),
            operation_id: None,
            job_id: None,
            plan_id: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortError {
    pub code: &'static str,
    pub message: String,
}

impl PortError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for PortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl Error for PortError {}

pub trait Clock: Send + Sync {
    fn now_unix_millis(&self) -> i64;
}

pub trait IdGenerator: Send + Sync {
    fn operation_id(&self) -> OperationId;
    fn job_id(&self) -> JobId;
    fn plan_id(&self) -> PlanId;
}

pub trait JournalHealthPort: Send + Sync {
    fn schema_version(&self) -> Result<u32, PortError>;
    fn quick_check(&self) -> Result<(), PortError>;
}
