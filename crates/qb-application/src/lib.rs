pub mod admission;
pub mod cleanup;
pub mod completion;
pub mod incoming;
pub mod mutation;
pub mod registry;
pub mod release;
pub mod storage;
pub mod system;
pub mod torrent;

use std::{error::Error, fmt, sync::Arc};

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

pub type MutationLane = Arc<tokio::sync::Mutex<()>>;

pub fn mutation_lane() -> MutationLane {
    Arc::new(tokio::sync::Mutex::new(()))
}

pub trait JournalHealthPort: Send + Sync {
    fn schema_version(&self) -> Result<u32, PortError>;
    fn quick_check(&self) -> Result<(), PortError>;
}
