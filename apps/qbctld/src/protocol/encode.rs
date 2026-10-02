use qb_application::system::{
    DaemonPhase, DoctorReport as ApplicationDoctorReport, SystemStatus as ApplicationSystemStatus,
};
use qb_proto::v1::{DaemonState, DoctorCheck, DoctorResponse, StatusResponse};

pub fn daemon_state(phase: DaemonPhase) -> DaemonState {
    match phase {
        DaemonPhase::Starting => DaemonState::Starting,
        DaemonPhase::Migrating => DaemonState::Migrating,
        DaemonPhase::Recovering => DaemonState::Recovering,
        DaemonPhase::Degraded => DaemonState::Degraded,
        DaemonPhase::Ready => DaemonState::Ready,
        DaemonPhase::Draining => DaemonState::Draining,
        DaemonPhase::Stopped => DaemonState::Stopped,
        DaemonPhase::Failed => DaemonState::Failed,
    }
}

pub fn system_status(status: ApplicationSystemStatus) -> StatusResponse {
    StatusResponse {
        daemon_state: daemon_state(status.phase) as i32,
        instance_id: status.instance_id,
        schema_version: status.schema_version,
        mutation_admission_enabled: status.mutation_admission_enabled,
    }
}

pub fn doctor(report: ApplicationDoctorReport) -> DoctorResponse {
    DoctorResponse {
        checks: report
            .checks
            .into_iter()
            .map(|check| DoctorCheck {
                name: check.name,
                ok: check.ok,
                message: check.message,
            })
            .collect(),
    }
}
