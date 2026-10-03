use std::sync::Arc;

use crate::{JournalHealthPort, PortError, RecoveryBlocker};

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
    pub recovery_blockers: Vec<RecoveryBlocker>,
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
    pub fn new(journal: Arc<dyn JournalHealthPort>, runtime: Arc<dyn RuntimeHealthPort>) -> Self {
        Self { journal, runtime }
    }

    pub fn status(&self) -> Result<SystemStatus, PortError> {
        let runtime = self.runtime.snapshot();
        let schema_version = self.journal.schema_version()?;
        let recovery_blockers = self.journal.recovery_blockers()?;

        Ok(SystemStatus {
            phase: runtime.phase,
            instance_id: runtime.instance_id,
            schema_version,
            mutation_admission_enabled: runtime.mutation_admission_enabled,
            recovery_blockers,
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

        let recovery = match self.journal.recovery_blockers() {
            Ok(blockers) if blockers.is_empty() => DoctorCheck {
                name: "recovery".into(),
                ok: true,
                message: "no durable Unknown/Blocked recovery blockers".into(),
            },
            Ok(blockers) => DoctorCheck {
                name: "recovery".into(),
                ok: false,
                message: blockers
                    .iter()
                    .map(|blocker| {
                        format!(
                            "{}:{}:{}={}",
                            blocker.kind,
                            blocker.state,
                            blocker.problem_code.as_deref().unwrap_or("none"),
                            blocker.count
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            },
            Err(error) => DoctorCheck {
                name: "recovery".into(),
                ok: false,
                message: error.to_string(),
            },
        };

        DoctorReport {
            checks: vec![journal, runtime_root, recovery],
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

        fn recovery_blockers(&self) -> Result<Vec<RecoveryBlocker>, PortError> {
            Ok(Vec::new())
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
        assert!(status.recovery_blockers.is_empty());
    }

    #[test]
    fn doctor_reports_each_authoritative_source() {
        let service = SystemService::new(Arc::new(FakeJournal), Arc::new(FakeRuntime));
        let report = service.doctor();

        assert_eq!(report.checks.len(), 3);
        assert!(report.checks.iter().all(|check| check.ok));
    }
}
