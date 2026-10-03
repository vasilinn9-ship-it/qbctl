use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Context as _, Result};
use qb_application::{
    mutation::MutationExecutionStatus,
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
                        let torrents = bootstrap.torrents.as_ref().map(Arc::clone);
                        let qbit_startup_problem =
                            bootstrap.qbit_startup_problem.as_deref().map(Arc::<str>::from);
                        tasks.spawn(async move {
                            if let Err(error) = protocol::serve_connection(
                                connection,
                                runtime,
                                system,
                                torrents,
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
        Ok(probe) if probe.mutation_ready => {}
        Ok(probe) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(
                application_version = %probe.application_version,
                webapi_version = %probe.webapi_version,
                "qBittorrent is readable but not mutation-ready"
            );
            return;
        }
        Err(error) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            warn!(error = %error, "qBittorrent probe failed; daemon is degraded");
            return;
        }
    }

    let Some(mutations) = bootstrap.mutations.as_ref() else {
        bootstrap.runtime.set_phase(DaemonPhase::Degraded);
        warn!("mutation service is unavailable");
        return;
    };

    bootstrap.runtime.set_phase(DaemonPhase::Recovering);
    match mutations.recover_all().await {
        Ok(results)
            if results
                .iter()
                .all(|result| result.status == MutationExecutionStatus::Finished) =>
        {
            bootstrap.runtime.set_phase(DaemonPhase::Ready);
        }
        Ok(results) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            let unresolved = results
                .iter()
                .filter(|result| result.status != MutationExecutionStatus::Finished)
                .count();
            warn!(
                unresolved,
                "one or more durable mutations remain unresolved after startup recovery"
            );
        }
        Err(error) => {
            bootstrap.runtime.set_phase(DaemonPhase::Degraded);
            error!(error = %error, "startup mutation recovery failed");
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
