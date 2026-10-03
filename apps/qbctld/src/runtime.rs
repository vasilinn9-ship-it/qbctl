use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Context as _, Result};
use qb_application::{
    cleanup::IncomingCleanupState,
    completion::CompletionExecutionStatus,
    mutation::MutationExecutionStatus,
    release::ReleaseExecutionStatus,
    system::{DaemonPhase, RuntimeHealthPort, RuntimeSnapshot},
    PortError,
};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::{bootstrap, protocol, server::Server};

const MAX_CLIENT_TASKS: usize = 32;

pub struct RuntimeContext {
    instance_id: String,
    runtime_root: PathBuf,
    phase: RwLock<DaemonPhase>,
}

impl RuntimeContext {
    pub fn new(instance_id: String, runtime_root: PathBuf) -> Self {
        Self {
            instance_id,
            runtime_root,
            phase: RwLock::new(DaemonPhase::Starting),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    pub fn set_phase(&self, phase: DaemonPhase) {
        match self.phase.write() {
            Ok(mut state) => *state = phase,
            Err(poisoned) => *poisoned.into_inner() = phase,
        }
    }

    fn phase(&self) -> DaemonPhase {
        match self.phase.read() {
            Ok(state) => *state,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }
}

impl RuntimeHealthPort for RuntimeContext {
    fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeSnapshot {
            phase: self.phase(),
            instance_id: self.instance_id.clone(),
            mutation_admission_enabled: self.phase() == DaemonPhase::Ready,
        }
    }

    fn runtime_root_check(&self) -> Result<String, PortError> {
        if self.runtime_root.is_dir() {
            Ok(self.runtime_root.display().to_string())
        } else {
            Err(PortError::new(
                "RUNTIME_ROOT_UNAVAILABLE",
                format!(
                    "runtime root is not a directory: {}",
                    self.runtime_root.display()
                ),
            ))
        }
    }
}

pub async fn run(runtime_override: Option<PathBuf>) -> Result<()> {
    let bootstrap = bootstrap::build(runtime_override)?;
    initialize_runtime(&bootstrap).await;
    let mut server = Server::bind(bootstrap.config.pipe.clone()).context("bind named pipe")?;
    let mut tasks = JoinSet::new();

    info!(
        pipe = %bootstrap.config.pipe,
        runtime_root = %bootstrap.runtime.runtime_root().display(),
        instance_id = %bootstrap.runtime.instance_id(),
        "qbctld ready"
    );

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for Ctrl+C")?;
                bootstrap.runtime.set_phase(DaemonPhase::Draining);
                info!("shutdown requested");
                break;
            }
            accepted = server.accept(), if tasks.len() < MAX_CLIENT_TASKS => {
                match accepted {
                    Ok(connection) => {
                        let runtime = Arc::clone(&bootstrap.runtime);
                        let system = Arc::clone(&bootstrap.system);
                        let storage = bootstrap.storage_status.as_ref().map(Arc::clone);
                        let torrents = bootstrap.torrents.as_ref().map(Arc::clone);
                        let mutations = Arc::clone(&bootstrap.mutations);
                        let completion = bootstrap.completion.as_ref().map(Arc::clone);
                        let qbit_startup_problem =
                            bootstrap.qbit_startup_problem.as_deref().map(Arc::<str>::from);
                        tasks.spawn(async move {
                            if let Err(error) = protocol::serve_connection(
                                connection,
                                runtime,
                                system,
                                storage,
                                torrents,
                                mutations,
                                completion,
                                qbit_startup_problem,
                            )
                            .await
                            {
                                warn!(error = %error, "client connection ended with error");
                            }
                        });
                    }
                    Err(error) => return Err(error).context("accept named pipe client"),
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    error!(error = %error, "client task panicked");
                }
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            error!(error = %error, "client task panicked during shutdown");
        }
    }

    bootstrap.runtime.set_phase(DaemonPhase::Stopped);
    Ok(())
}

async fn initialize_runtime(bootstrap: &bootstrap::Bootstrap) {
    bootstrap.runtime.set_phase(DaemonPhase::Recovering);

    let unresolved = match bootstrap.mutations.recover_all().await {
        Ok(results) => results
            .iter()
            .filter(|result| result.status != MutationExecutionStatus::Finished)
            .count(),
        Err(error) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            error!(error = %error, "startup mutation recovery failed");
            return;
        }
    };

    if unresolved > 0 {
        bootstrap.runtime.set_phase(DaemonPhase::Degraded);
        warn!(
            unresolved,
            "one or more durable mutations remain unresolved after startup recovery"
        );
        return;
    }

    if let Some(incoming) = bootstrap.incoming.as_ref() {
        let recovered = match incoming.recover_cleanup().await {
            Ok(records) => records,
            Err(error) => {
                bootstrap.runtime.set_phase(DaemonPhase::Degraded);
                error!(error = %error, "startup Incoming cleanup recovery failed");
                return;
            }
        };
        let recovered_blocked = recovered
            .iter()
            .filter(|record| record.state == IncomingCleanupState::Blocked)
            .count();
        if recovered_blocked > 0 {
            warn!(
                recovered_blocked,
                "one or more Incoming cleanup intents were blocked by changed source evidence"
            );
        }

        if bootstrap
            .config
            .storage
            .as_ref()
            .is_some_and(|storage| storage.cleanup_exact_duplicates_on_startup)
        {
            let pass = match incoming.run_once().await {
                Ok(pass) => pass,
                Err(error) => {
                    bootstrap.runtime.set_phase(DaemonPhase::Degraded);
                    error!(error = %error, "startup Incoming scan/cleanup failed");
                    return;
                }
            };
            let cleanup_deleted = pass
                .cleanup
                .iter()
                .filter(|record| record.state == IncomingCleanupState::Deleted)
                .count();
            let cleanup_blocked = pass
                .cleanup
                .iter()
                .filter(|record| record.state == IncomingCleanupState::Blocked)
                .count();
            info!(
                eligible = pass.scan.eligible.len(),
                already_processed = pass.scan.already_processed.len(),
                redundant_exact = pass.scan.redundant_identical.len(),
                rejected = pass.scan.rejected.len(),
                cleanup_deleted,
                cleanup_blocked,
                "startup Incoming scan/cleanup completed"
            );
        }
    }

    if let Some(release) = bootstrap.release.as_ref() {
        let recovered = match release.recover_all().await {
            Ok(results) => results,
            Err(error) => {
                bootstrap.runtime.set_phase(DaemonPhase::Degraded);
                error!(error = %error, "startup queue release recovery failed");
                return;
            }
        };
        let unresolved = recovered
            .iter()
            .filter(|execution| execution.status != ReleaseExecutionStatus::Finished)
            .count();
        if unresolved > 0 {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(
                unresolved,
                "one or more durable queue releases remain unresolved after observation-first recovery"
            );
            return;
        }
        if !recovered.is_empty() {
            info!(
                recovered = recovered.len(),
                "startup durable queue release recovery completed"
            );
        }
    }

    if let Some(completion) = bootstrap.completion.as_ref() {
        let recovered = match completion.recover_all().await {
            Ok(results) => results,
            Err(error) => {
                bootstrap.runtime.set_phase(DaemonPhase::Degraded);
                error!(error = %error, "startup completion recovery failed");
                return;
            }
        };
        let unresolved = recovered
            .iter()
            .filter(|execution| execution.status != CompletionExecutionStatus::Finished)
            .count();
        if unresolved > 0 {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(
                unresolved,
                "one or more durable completions remain unresolved after observation-first recovery"
            );
            return;
        }
        if !recovered.is_empty() {
            info!(
                recovered = recovered.len(),
                "startup durable completion recovery completed"
            );
        }
    }

    let Some(torrents) = bootstrap.torrents.as_ref() else {
        bootstrap.runtime.set_phase(DaemonPhase::Degraded);
        warn!(
            problem = %bootstrap
                .qbit_startup_problem
                .as_deref()
                .unwrap_or("qBittorrent is unavailable"),
            "daemon started without qBittorrent mutation capability"
        );
        return;
    };

    match torrents.probe().await {
        Ok(probe) if probe.mutation_ready => {
            bootstrap.runtime.set_phase(DaemonPhase::Ready);
        }
        Ok(probe) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(
                application_version = %probe.application_version,
                webapi_version = %probe.webapi_version,
                "qBittorrent is readable but not mutation-ready"
            );
        }
        Err(error) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(error = %error, "qBittorrent probe failed; daemon is degraded");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_snapshot_is_application_typed() {
        let context = RuntimeContext::new("instance".into(), PathBuf::from("."));
        let snapshot = RuntimeHealthPort::snapshot(&context);

        assert_eq!(snapshot.phase, DaemonPhase::Starting);
        assert_eq!(snapshot.instance_id, "instance");
        assert!(!snapshot.mutation_admission_enabled);

        context.set_phase(DaemonPhase::Ready);
        let ready = RuntimeHealthPort::snapshot(&context);
        assert!(ready.mutation_admission_enabled);
    }
}
