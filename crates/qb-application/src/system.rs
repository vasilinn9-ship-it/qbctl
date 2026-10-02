use std::sync::Arc;

use crate::{JournalHealthPort, PortError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonPhase {
    Starting,
    Migrating,
    Recovering,
    Degraded,
    Ready,
    Draining,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeSnapshot {
    pub phase: DaemonPhase,
    pub instance_id: String,
    pub mutation_admission_enabled: bool,
}

pub trait RuntimeHealthPort: Send + Sync {
    fn snapshot(&self) -> RuntimeSnapshot;
    fn runtime_root_check(&self) -> Result<String, PortError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemStatus {
    pub phase: DaemonPhase,
    pub instance_id: String,
    pub schema_version: u32,
    pub mutation_admission_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoctorCheck {
    pub name: String,
    pub ok: bool,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

pub struct SystemService {
    journal: Arc<dyn JournalHealthPort>,
    runtime: Arc<dyn RuntimeHealthPort>,
}

impl SystemService {
    pub fn new(
        journal: Arc<dyn JournalHealthPort>,
        runtime: Arc<dyn RuntimeHealthPort>,
    ) -> Self {
        Self { journal, runtime }
    }

    pub fn status(&self) -> Result<SystemStatus, PortError> {
        let runtime = self.runtime.snapshot();
        let schema_version = self.journal.schema_version()?;

        Ok(SystemStatus {
            phase: runtime.phase,
            instance_id: runtime.instance_id,
            schema_version,
            mutation_admission_enabled: runtime.mutation_admission_enabled,
        })
    }

    pub fn doctor(&self) -> DoctorReport {
        let journal = match self.journal.quick_check() {
            Ok(()) => DoctorCheck {
                name: "journal".into(),
                ok: true,
                message: "SQLite quick_check: ok".into(),
            },
            Err(error) => DoctorCheck {
                name: "journal".into(),
                ok: false,
                message: error.to_string(),
            },
        };

        let runtime_root = match self.runtime.runtime_root_check() {
            Ok(message) => DoctorCheck {
                name: "runtime_root".into(),
                ok: true,
                message,
            },
            Err(error) => DoctorCheck {
                name: "runtime_root".into(),
                ok: false,
                message: error.to_string(),
            },
        };

        DoctorReport {
            checks: vec![journal, runtime_root],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeJournal;

    impl JournalHealthPort for FakeJournal {
        fn schema_version(&self) -> Result<u32, PortError> {
            Ok(7)
        }

        fn quick_check(&self) -> Result<(), PortError> {
            Ok(())
        }
    }

    struct FakeRuntime;

    impl RuntimeHealthPort for FakeRuntime {
        fn snapshot(&self) -> RuntimeSnapshot {
            RuntimeSnapshot {
                phase: DaemonPhase::Ready,
                instance_id: "test-instance".into(),
                mutation_admission_enabled: false,
            }
        }

        fn runtime_root_check(&self) -> Result<String, PortError> {
            Ok("test-runtime".into())
        }
    }

    #[test]
    fn status_uses_ports_without_protocol_types() {
        let service = SystemService::new(Arc::new(FakeJournal), Arc::new(FakeRuntime));
        let status = service.status().expect("status");

        assert_eq!(status.phase, DaemonPhase::Ready);
        assert_eq!(status.schema_version, 7);
        assert_eq!(status.instance_id, "test-instance");
        assert!(!status.mutation_admission_enabled);
    }

    #[test]
    fn doctor_reports_each_authoritative_source() {
        let service = SystemService::new(Arc::new(FakeJournal), Arc::new(FakeRuntime));
        let report = service.doctor();

        assert_eq!(report.checks.len(), 2);
        assert!(report.checks.iter().all(|check| check.ok));
    }
}
